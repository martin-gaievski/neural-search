/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.InnerHitContextBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.SearchPipelineService;

/**
 * Coordinator-level fusion for the hybrid query {@code mode: "fused"} path: fire each sub-query as an independent
 * parallel search leg, fuse the globally-merged leg results, and produce the standard query the fused-mode hybrid
 * query self-erases into. All methods are static and take the {@link SearchRequest}/{@link MultiSearchResponse}
 * explicitly.
 *
 * <p>This is the coordinator-collection path only (each leg = one standalone search reduced to global top-K); the
 * per-shard collection mode, the stage-B-free fast path, and PIT-backed snapshot consistency from the resolver POC
 * are intentionally out of scope for this POC. The fusion math (RRF / min_max / z_score / l2 + arithmetic mean)
 * mirrors the classic hybrid normalization+combination techniques so fused-mode relevance matches classic hybrid.
 */
final class HybridFusionOrchestrator {

    private static final Logger log = LogManager.getLogger(HybridFusionOrchestrator.class);

    private HybridFusionOrchestrator() {}

    /**
     * Build the leg MultiSearch: one standalone search per sub-query, each reduced to the global top-{@code
     * rankWindowSize}. Id-only (no {@code _source}); totals disabled (the Tail supplies the full-match-set count).
     *
     * <p>Each leg is pinned to the no-op search pipeline ({@code _none}). Otherwise a leg — being a plain
     * {@link SearchRequest} with no explicit pipeline — would inherit the index's {@code index.search.default_pipeline}
     * and re-run its request/response processors once per leg. That is redundant at best (e.g. the enricher is already
     * baked into the sub-query builders) and incorrect at worst (e.g. a {@code rerank} response processor runs per leg
     * against id-only, {@code fetchSource(false)} hits with no {@code ext.rerank} context). The outer fused request
     * still carries the pipeline, so top-level request/response processors run exactly once, as intended.
     */
    static MultiSearchRequest buildLegMultiSearch(SearchRequest request, List<QueryBuilder> legs, int rankWindowSize) {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        for (QueryBuilder leg : legs) {
            SearchSourceBuilder legSource = new SearchSourceBuilder().query(leg)
                .size(rankWindowSize)
                .from(0)
                .fetchSource(false)
                .trackTotalHits(false);
            multiSearchRequest.add(
                new SearchRequest(request.indices()).indicesOptions(request.indicesOptions())
                    .source(legSource)
                    .pipeline(SearchPipelineService.NOOP_PIPELINE_ID)
            );
        }
        return multiSearchRequest;
    }

    /**
     * Fuse the leg results into the standard query the fused-mode hybrid self-erases into — a {@link HybridFusionQuery}
     * (Top + conditional Tail), or a {@link MatchNoneQueryBuilder} when nothing fused. Pure: returns the query and
     * mutates nothing.
     *
     * <p>The Tail (non-scoring {@code bool{should: legs}} surfacing the full match set) is included only when the
     * request needs it and this marker is the whole query. A nested fused query is always Top-only, so an enclosing
     * filter intersects the fused window at the query phase (fuse-then-filter).
     */
    static QueryBuilder buildFusedQuery(
        SearchSourceBuilder source,
        MultiSearchResponse multiSearchResponse,
        List<QueryBuilder> legs,
        FusionSpec fusion,
        int rankWindowSize,
        boolean topLevel
    ) {
        MultiSearchResponse.Item[] items = multiSearchResponse.getResponses();
        SearchHit[][] legHits = groupLegHits(items, legs.size());
        RankedDocs ranked = computeRankedDocs(legHits, legs.size(), fusion, rankWindowSize);
        if (ranked.ids.length == 0) {
            return new MatchNoneQueryBuilder();
        }
        boolean topOnly;
        if (topLevel == false) {
            topOnly = true; // nested: enclosing filter intersects at the query phase
        } else if (needsExecutionTail(source) || legsHaveInnerHits(legs)) {
            topOnly = false; // aggregations / explain / profile / highlight / leg inner_hits need the legs IN the query
        } else if (wantsTotalsBeyondWindow(source, ranked.ids.length)) {
            topOnly = false; // keep the Tail for an accurate index-wide count
        } else {
            topOnly = true; // track_total_hits:false -> plain top-K, no Tail
        }
        List<QueryBuilder> tail = topOnly ? List.of() : survivingLegQueries(legs, legHits);
        return new HybridFusionQuery(ranked.ids, ranked.scores, tail);
    }

    /** The sub-query legs restricted to those that survived (non-null hits slot); used for the Tail so a failed leg is
     *  not re-executed in stage B (graceful degradation). */
    private static List<QueryBuilder> survivingLegQueries(List<QueryBuilder> legs, SearchHit[][] legHits) {
        List<QueryBuilder> surviving = new ArrayList<>(legs.size());
        for (int legIndex = 0; legIndex < legs.size(); legIndex++) {
            if (legIndex >= legHits.length || legHits[legIndex] != null) {
                QueryBuilder leg = legs.get(legIndex);
                // A kNN/neural leg's match set IS its returned top-k — re-running it in the Tail would walk the HNSW
                // graph again purely to count. Materialize such legs as their already-retrieved ids instead.
                if (isMaterializableLeg(leg) && legIndex < legHits.length && legHits[legIndex] != null) {
                    IdsQueryBuilder ids = new IdsQueryBuilder();
                    for (SearchHit hit : legHits[legIndex]) {
                        ids.addIds(hit.getId());
                    }
                    surviving.add(ids);
                } else {
                    surviving.add(leg);
                }
            }
        }
        return surviving;
    }

    /** Legs whose Lucene match set is their own top-k (re-running them in the Tail = a redundant ANN pass). */
    private static boolean isMaterializableLeg(QueryBuilder leg) {
        String name = leg.getWriteableName();
        return "knn".equals(name) || "neural".equals(name) || "neural_knn".equals(name);
    }

    /**
     * Reduce the raw MultiSearch items into a per-leg array of hits (coordinator plan: one item per leg). Graceful
     * per-leg failure: a failed sub-search sets its slot to null and is skipped by fusion; only when ALL legs failed
     * do we throw (nothing to fuse).
     */
    private static SearchHit[][] groupLegHits(MultiSearchResponse.Item[] items, int legCount) {
        if (items.length != legCount) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "[hybrid] expected %d leg sub-search responses but got %d", legCount, items.length)
            );
        }
        SearchHit[][] legHits = new SearchHit[legCount][];
        int survivingLegs = 0;
        for (int leg = 0; leg < legCount; leg++) {
            MultiSearchResponse.Item item = items[leg];
            if (item.isFailure()) {
                log.warn("[hybrid] fused-mode sub-query {} dropped: {}", leg, item.getFailureMessage());
                legHits[leg] = null;
            } else {
                legHits[leg] = item.getResponse().getHits().getHits();
                survivingLegs++;
            }
        }
        if (survivingLegs == 0) {
            MultiSearchResponse.Item firstFailure = firstFailure(items);
            throw new IllegalStateException(
                "[hybrid] all fused-mode sub-queries failed" + (firstFailure == null ? "" : ": " + firstFailure.getFailureMessage()),
                firstFailure == null ? null : firstFailure.getFailure()
            );
        }
        return legHits;
    }

    private static MultiSearchResponse.Item firstFailure(MultiSearchResponse.Item[] items) {
        for (MultiSearchResponse.Item item : items) {
            if (item.isFailure()) {
                return item;
            }
        }
        return null;
    }

    // ---------------------------------------------------------------------------------------------
    // Fusion math (mirrors the classic hybrid normalization + combination techniques)
    // ---------------------------------------------------------------------------------------------

    private static RankedDocs computeRankedDocs(SearchHit[][] legHits, int legCount, FusionSpec fusion, int rankWindowSize) {
        Map<String, Float> combined;
        if (FusionSpec.TECHNIQUE_ARITHMETIC_MEAN.equals(fusion.combinationTechnique())) {
            String norm = fusion.normalizationTechnique();
            if (FusionSpec.NORMALIZATION_Z_SCORE.equals(norm)) {
                combined = zScoreArithmeticMean(legHits, fusion);
            } else if (FusionSpec.NORMALIZATION_L2.equals(norm)) {
                combined = l2ArithmeticMean(legHits, fusion);
            } else {
                combined = minMaxArithmeticMean(legHits, fusion);
            }
        } else {
            combined = rrf(legHits, fusion);
        }
        return toRankedDocs(combined, rankWindowSize);
    }

    private static Map<String, Float> rrf(SearchHit[][] legHits, FusionSpec fusion) {
        Map<String, Float> scores = new LinkedHashMap<>();
        for (int legIndex = 0; legIndex < legHits.length; legIndex++) {
            SearchHit[] hits = legHits[legIndex];
            if (hits == null) {
                continue;
            }
            float weight = weightForLeg(fusion.weights(), legIndex);
            for (int rank = 0; rank < hits.length; rank++) {
                String id = hits[rank].getId();
                if (id == null) {
                    continue;
                }
                scores.merge(id, weight / (fusion.rankConstant() + rank + 1), Float::sum);
            }
        }
        return scores;
    }

    private static Map<String, Float> minMaxArithmeticMean(SearchHit[][] legHits, FusionSpec fusion) {
        float totalWeight = survivingWeight(legHits, fusion.weights());
        Map<String, Float> weightedSum = new LinkedHashMap<>();
        for (int legIndex = 0; legIndex < legHits.length; legIndex++) {
            SearchHit[] hits = legHits[legIndex];
            if (hits == null) {
                continue;
            }
            float min = Float.MAX_VALUE;
            float max = -Float.MAX_VALUE;
            for (SearchHit hit : hits) {
                min = Math.min(min, hit.getScore());
                max = Math.max(max, hit.getScore());
            }
            float weight = weightForLeg(fusion.weights(), legIndex);
            for (SearchHit hit : hits) {
                String id = hit.getId();
                if (id == null) {
                    continue;
                }
                weightedSum.merge(id, weight * normalizeMinMax(hit.getScore(), min, max), Float::sum);
            }
        }
        return dividedByWeight(weightedSum, totalWeight);
    }

    private static float normalizeMinMax(float score, float min, float max) {
        if (Float.compare(max, min) == 0) {
            return 1.0f;
        }
        float normalized = (score - min) / (max - min);
        return normalized == 0.0f ? 0.001f : normalized;
    }

    private static Map<String, Float> zScoreArithmeticMean(SearchHit[][] legHits, FusionSpec fusion) {
        float totalWeight = survivingWeight(legHits, fusion.weights());
        Map<String, Float> weightedSum = new LinkedHashMap<>();
        for (int legIndex = 0; legIndex < legHits.length; legIndex++) {
            SearchHit[] hits = legHits[legIndex];
            if (hits == null) {
                continue;
            }
            double sum = 0.0;
            int n = 0;
            for (SearchHit hit : hits) {
                sum += hit.getScore();
                n++;
            }
            double mean = n == 0 ? 0.0 : sum / n;
            double sumSq = 0.0;
            for (SearchHit hit : hits) {
                double d = hit.getScore() - mean;
                sumSq += d * d;
            }
            double std = n < 2 ? 0.0 : Math.sqrt(sumSq / (n - 1));
            float weight = weightForLeg(fusion.weights(), legIndex);
            for (SearchHit hit : hits) {
                String id = hit.getId();
                if (id == null) {
                    continue;
                }
                weightedSum.merge(id, weight * normalizeZScore(hit.getScore(), mean, std), Float::sum);
            }
        }
        return dividedByWeight(weightedSum, totalWeight);
    }

    private static float normalizeZScore(float score, double mean, double std) {
        if (std == 0.0) {
            return 0.5f;
        }
        double lower = mean - 3.0 * std;
        double normalized = (score - lower) / (6.0 * std);
        if (normalized <= 0.0) {
            return 0.001f;
        }
        return normalized >= 1.0 ? 1.0f : (float) normalized;
    }

    private static Map<String, Float> l2ArithmeticMean(SearchHit[][] legHits, FusionSpec fusion) {
        float totalWeight = survivingWeight(legHits, fusion.weights());
        Map<String, Float> weightedSum = new LinkedHashMap<>();
        for (int legIndex = 0; legIndex < legHits.length; legIndex++) {
            SearchHit[] hits = legHits[legIndex];
            if (hits == null) {
                continue;
            }
            double sumSq = 0.0;
            for (SearchHit hit : hits) {
                sumSq += (double) hit.getScore() * hit.getScore();
            }
            float norm = (float) Math.sqrt(sumSq);
            float weight = weightForLeg(fusion.weights(), legIndex);
            for (SearchHit hit : hits) {
                String id = hit.getId();
                if (id == null) {
                    continue;
                }
                weightedSum.merge(id, weight * normalizeL2(hit.getScore(), norm), Float::sum);
            }
        }
        return dividedByWeight(weightedSum, totalWeight);
    }

    private static float normalizeL2(float score, float l2Norm) {
        return l2Norm == 0.0f ? 0.0f : score / l2Norm;
    }

    private static Map<String, Float> dividedByWeight(Map<String, Float> weightedSum, float totalWeight) {
        Map<String, Float> combined = new LinkedHashMap<>();
        for (Map.Entry<String, Float> e : weightedSum.entrySet()) {
            combined.put(e.getKey(), totalWeight == 0.0f ? 0.0f : e.getValue() / totalWeight);
        }
        return combined;
    }

    private static float weightForLeg(float[] weights, int legIndex) {
        return (weights == null || weights.length == 0) ? 1.0f : weights[legIndex];
    }

    private static float survivingWeight(SearchHit[][] legHits, float[] weights) {
        float total = 0.0f;
        for (int legIndex = 0; legIndex < legHits.length; legIndex++) {
            if (legHits[legIndex] != null) {
                total += weightForLeg(weights, legIndex);
            }
        }
        return total;
    }

    private static RankedDocs toRankedDocs(Map<String, Float> scoresById, int rankWindowSize) {
        List<Map.Entry<String, Float>> ranked = new ArrayList<>(scoresById.entrySet());
        ranked.sort(Comparator.<Map.Entry<String, Float>>comparingDouble(e -> -e.getValue()).thenComparing(Map.Entry::getKey));
        if (ranked.size() > rankWindowSize) {
            ranked = ranked.subList(0, rankWindowSize);
        }
        String[] ids = new String[ranked.size()];
        float[] scores = new float[ranked.size()];
        for (int i = 0; i < ranked.size(); i++) {
            ids[i] = ranked.get(i).getKey();
            scores[i] = ranked.get(i).getValue();
        }
        return new RankedDocs(ids, scores);
    }

    private static boolean needsExecutionTail(SearchSourceBuilder source) {
        return source != null
            && (source.aggregations() != null || Boolean.TRUE.equals(source.explain()) || source.profile() || source.highlighter() != null);
    }

    /**
     * True if any leg declares inner_hits (e.g. a {@code nested} / {@code has_child} sub-query). The Tail must then be
     * retained so the leg builder survives in the fused query's {@code sourceQueries}, where
     * {@link HybridFusionQuery#extractInnerHitBuilders} can reach it — otherwise a top-K-only fused query would silently
     * drop leg-level inner_hits.
     */
    private static boolean legsHaveInnerHits(List<QueryBuilder> legs) {
        Map<String, InnerHitContextBuilder> innerHits = new HashMap<>();
        for (QueryBuilder leg : legs) {
            InnerHitContextBuilder.extractInnerHits(leg, innerHits);
        }
        return innerHits.isEmpty() == false;
    }

    private static boolean wantsTotalsBeyondWindow(SearchSourceBuilder source, int numRankedDocs) {
        if (source == null) {
            return true;
        }
        Integer trackTotalHitsUpTo = source.trackTotalHitsUpTo();
        return trackTotalHitsUpTo == null || trackTotalHitsUpTo > numRankedDocs;
    }

    private record RankedDocs(String[] ids, float[] scores) {
    }
}
