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
import java.util.Objects;

import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.InnerHitContextBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.neuralsearch.fusion.CoordinatorScoreFusion;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationFactory;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationUtil;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.SearchPipelineService;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Coordinator-side machinery for the resolver (fused) mode: fan the sub-query legs out as a parallel {@code MultiSearch},
 * then fuse the leg hits into the standard query the {@code hybrid} query self-erases into ({@link HybridFusionQuery},
 * or {@code match_none} when nothing fused). All methods are static and take the {@link SearchRequest} /
 * {@link MultiSearchResponse} explicitly so the class holds no state.
 *
 * <p>Fusion arithmetic is NOT reimplemented here — it delegates to {@link CoordinatorScoreFusion}, the shared core that
 * classic hybrid also calls, so fused-mode relevance matches classic for the same hit set. Current scope: {@code min_max}
 * normalization + {@code arithmetic_mean} combination (the caller rejects other techniques at rewrite for now).
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class HybridFusionOrchestrator {

    private static final ScoreCombinationFactory SCORE_COMBINATION_FACTORY = new ScoreCombinationFactory();

    /**
     * Build the leg MultiSearch: one standalone search per sub-query, each reduced to the global top-{@code windowSize}.
     * Id-only (no {@code _source}); totals disabled (the Tail supplies the full-match-set count when needed).
     *
     * <p>Each leg is pinned to the no-op search pipeline ({@code _none}). Otherwise a leg — a plain {@link SearchRequest}
     * with no explicit pipeline — would inherit the index's {@code index.search.default_pipeline} and re-run its
     * request/response processors once per leg (redundant, and incorrect for processors like {@code rerank} that expect
     * request context absent from an id-only leg). The outer fused request still carries the pipeline, so top-level
     * processors run exactly once.
     *
     * <p>Legs disable partial results ({@code allowPartialSearchResults(false)}): fused relevance is computed across a
     * leg's full window, so a leg silently truncated by a down shard would be as corrupting as a fully failed leg.
     * Failing the leg instead lets {@link #groupLegHits} fail the whole request fast, rather than fusing over incomplete
     * data — this is the leg-level half of the fail-fast contract.
     */
    static MultiSearchRequest buildLegMultiSearch(SearchRequest request, List<QueryBuilder> legs, int windowSize) {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        for (QueryBuilder leg : legs) {
            SearchSourceBuilder legSource = new SearchSourceBuilder().query(leg)
                .size(windowSize)
                .from(0)
                .fetchSource(false)
                .trackTotalHits(false);
            multiSearchRequest.add(
                new SearchRequest(request.indices()).indicesOptions(request.indicesOptions())
                    .source(legSource)
                    .pipeline(SearchPipelineService.NOOP_PIPELINE_ID)
                    .allowPartialSearchResults(false)
            );
        }
        return multiSearchRequest;
    }

    /**
     * Fuse the leg results into the standard query the fused-mode hybrid self-erases into — a {@link HybridFusionQuery}
     * (Top + conditional Tail), or a {@link MatchNoneQueryBuilder} when nothing fused. Pure: returns the query and
     * mutates nothing.
     *
     * <p>The Tail (non-scoring {@code bool{should: legs}} surfacing the full match set) is included only when the request
     * needs it (aggregations / explain / profile / highlight / leg inner_hits / totals beyond the window) and this
     * marker is the whole query. A nested fused query is always Top-only, so an enclosing filter intersects the fused
     * window at the query phase (fuse-then-filter).
     */
    static QueryBuilder buildFusedQuery(
        SearchSourceBuilder source,
        MultiSearchResponse multiSearchResponse,
        List<QueryBuilder> legs,
        FusionSpec fusion,
        int windowSize,
        boolean topLevel
    ) {
        MultiSearchResponse.Item[] items = multiSearchResponse.getResponses();
        SearchHit[][] legHits = groupLegHits(items, legs.size());
        RankedDocs ranked = computeRankedDocs(legHits, fusion, windowSize);
        if (ranked.ids().length == 0) {
            return new MatchNoneQueryBuilder();
        }
        boolean topOnly;
        if (topLevel == false) {
            topOnly = true; // nested: enclosing filter intersects at the query phase
        } else if (needsExecutionTail(source) || legsHaveInnerHits(legs)) {
            topOnly = false; // aggregations / explain / profile / highlight / leg inner_hits need the legs IN the query
        } else if (wantsTotalsBeyondWindow(source, ranked.ids().length)) {
            topOnly = false; // keep the Tail for an accurate index-wide count
        } else {
            topOnly = true; // track_total_hits:false -> plain top-K, no Tail
        }
        List<QueryBuilder> tail = topOnly ? List.of() : legQueriesForTail(legs, legHits);
        return new HybridFusionQuery(ranked.ids(), ranked.scores(), tail);
    }

    /**
     * Reduce the raw MultiSearch items into a per-leg array of hits (one item per leg). Fail fast on ANY leg failure:
     * fused relevance is computed across all legs (min_max normalization + combination), so a dropped leg would silently
     * change the ranking function — a partial fused result is semantically different, not just smaller. So a single
     * failed sub-search fails the whole request rather than degrading to the surviving legs.
     */
    private static SearchHit[][] groupLegHits(MultiSearchResponse.Item[] items, int legCount) {
        if (items.length != legCount) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "[hybrid] expected %d leg sub-search responses but got %d", legCount, items.length)
            );
        }
        SearchHit[][] legHits = new SearchHit[legCount][];
        for (int leg = 0; leg < legCount; leg++) {
            MultiSearchResponse.Item item = items[leg];
            if (item.isFailure()) {
                throw new IllegalStateException(
                    String.format(Locale.ROOT, "[hybrid] fused-mode sub-query %d failed: %s", leg, item.getFailureMessage()),
                    item.getFailure()
                );
            }
            legHits[leg] = item.getResponse().getHits().getHits();
        }
        return legHits;
    }

    /**
     * Fuse via the shared {@link CoordinatorScoreFusion} core (min_max + arithmetic_mean), then rank by fused score and
     * cut to the window. Converts the coordinator's {@code SearchHit[][]} view into the {@code _id}-keyed per-leg maps
     * the shared core consumes; a leg that matched nothing contributes an empty map (groupLegHits fails fast on failures,
     * so every slot is non-null).
     */
    private static RankedDocs computeRankedDocs(SearchHit[][] legHits, FusionSpec fusion, int windowSize) {
        List<Map<String, Float>> legRawScores = new ArrayList<>(legHits.length);
        for (SearchHit[] hits : legHits) {
            Map<String, Float> byId = new LinkedHashMap<>();
            for (SearchHit hit : hits) {
                byId.put(hit.getId(), hit.getScore());
            }
            legRawScores.add(byId);
        }
        ScoreCombinationTechnique combination = SCORE_COMBINATION_FACTORY.createCombination(
            fusion.combinationTechnique(),
            weightsParams(fusion.weights())
        );
        Map<String, Float> combined = CoordinatorScoreFusion.fuseMinMax(legRawScores, combination);
        return toRankedDocs(combined, windowSize);
    }

    /**
     * Fail fast on a malformed {@code weights} array BEFORE the leg fan-out fires — otherwise a bad weights array (out
     * of range, not summing to 1.0, or the wrong count) wastes a full N-leg MultiSearch before {@link ScoreCombinationUtil}
     * errors in the async callback. Constructing the combination technique here reuses core's existing
     * {@code validateParams}/{@code validateWeights} range-and-sum checks with no duplication; the count-vs-legs check
     * (only enforced later, in {@code combine()}) is added explicitly so a mismatch also fails before the fan-out.
     *
     * @param fusion resolved fusion config (inline or pipeline)
     * @param legCount number of sub-query legs (weights, when supplied, must match this)
     */
    static void validateFusionParams(final FusionSpec fusion, final int legCount) {
        // Triggers ScoreCombinationUtil.validateParams + getWeights -> validateWeights (range 0.0..1.0 and sum == 1.0).
        SCORE_COMBINATION_FACTORY.createCombination(fusion.combinationTechnique(), weightsParams(fusion.weights()));
        float[] weights = fusion.weights();
        if (weights.length != 0 && weights.length != legCount) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "number of weights [%d] must match number of sub-queries [%d] in hybrid query",
                    weights.length,
                    legCount
                )
            );
        }
    }

    private static Map<String, Object> weightsParams(float[] weights) {
        if (Objects.isNull(weights) || weights.length == 0) {
            return Map.of();
        }
        List<Double> weightsList = new ArrayList<>(weights.length);
        for (float weight : weights) {
            weightsList.add((double) weight);
        }
        return Map.of(ScoreCombinationUtil.PARAM_NAME_WEIGHTS, weightsList);
    }

    private static RankedDocs toRankedDocs(Map<String, Float> scoresById, int windowSize) {
        List<Map.Entry<String, Float>> ranked = new ArrayList<>(scoresById.entrySet());
        ranked.sort(Comparator.<Map.Entry<String, Float>>comparingDouble(e -> -e.getValue()).thenComparing(Map.Entry::getKey));
        if (ranked.size() > windowSize) {
            ranked = ranked.subList(0, windowSize);
        }
        String[] ids = new String[ranked.size()];
        float[] scores = new float[ranked.size()];
        for (int i = 0; i < ranked.size(); i++) {
            ids[i] = ranked.get(i).getKey();
            scores[i] = ranked.get(i).getValue();
        }
        return new RankedDocs(ids, scores);
    }

    /** The sub-query legs in their Tail form. groupLegHits fails fast on any leg failure, so every leg is present here
     *  (legHits.length == legs.size(), no null slots). A kNN/neural leg's match set IS its returned top-k, so it is
     *  materialized as an {@link IdsQueryBuilder} of its already-retrieved ids rather than re-walking the HNSW graph in
     *  the Tail purely to count; other legs are used as-is. */
    private static List<QueryBuilder> legQueriesForTail(List<QueryBuilder> legs, SearchHit[][] legHits) {
        List<QueryBuilder> tail = new ArrayList<>(legs.size());
        for (int legIndex = 0; legIndex < legs.size(); legIndex++) {
            QueryBuilder leg = legs.get(legIndex);
            if (isMaterializableLeg(leg)) {
                IdsQueryBuilder ids = new IdsQueryBuilder();
                for (SearchHit hit : legHits[legIndex]) {
                    ids.addIds(hit.getId());
                }
                tail.add(ids);
            } else {
                tail.add(leg);
            }
        }
        return tail;
    }

    /** Legs whose Lucene match set is their own top-k (re-running them in the Tail = a redundant ANN pass). */
    private static boolean isMaterializableLeg(QueryBuilder leg) {
        String name = leg.getWriteableName();
        return "knn".equals(name) || "neural".equals(name) || "neural_knn".equals(name);
    }

    private static boolean needsExecutionTail(SearchSourceBuilder source) {
        return Objects.nonNull(source)
            && (Objects.nonNull(source.aggregations())
                || Boolean.TRUE.equals(source.explain())
                || source.profile()
                || Objects.nonNull(source.highlighter()));
    }

    private static boolean legsHaveInnerHits(List<QueryBuilder> legs) {
        Map<String, InnerHitContextBuilder> innerHits = new HashMap<>();
        for (QueryBuilder leg : legs) {
            InnerHitContextBuilder.extractInnerHits(leg, innerHits);
        }
        return innerHits.isEmpty() == false;
    }

    private static boolean wantsTotalsBeyondWindow(SearchSourceBuilder source, int numRankedDocs) {
        if (Objects.isNull(source)) {
            return true;
        }
        Integer trackTotalHitsUpTo = source.trackTotalHitsUpTo();
        return Objects.isNull(trackTotalHitsUpTo) || trackTotalHitsUpTo > numRankedDocs;
    }

    private record RankedDocs(String[] ids, float[] scores) {
    }
}
