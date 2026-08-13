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
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.logging.HeaderWarning;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.InnerHitContextBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.knn.index.query.KNNQueryBuilder;
import org.opensearch.neuralsearch.fusion.CoordinatorScoreFusion;
import org.opensearch.neuralsearch.fusion.ScalarNormalizer;
import org.opensearch.neuralsearch.fusion.ScalarNormalizerFactory;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationFactory;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationUtil;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.SearchPipelineService;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Coordinator-side machinery for the resolver (fused) mode: fan the sub-query legs out as a parallel {@code MultiSearch},
 * then fuse the leg hits into the standard query the {@code hybrid} query self-erases into ({@link HybridFusionQueryBuilder},
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
     * <p>Note on ANN legs: {@code size} sets how many hits a leg returns, but an ANN leg's retrieval depth is bounded by
     * its own {@code k} (collected per shard), not by {@code size} — a {@code knn}/{@code neural} leg with a small or
     * default {@code k} (10) contributes at most that many candidates regardless of {@code windowSize}. This matches
     * classic hybrid, which likewise never rewrites {@code k}; for a full window, set {@code k >= window_size} on the
     * sub-query. We deliberately do NOT rewrite {@code k} here — it would diverge from classic and has no analog for
     * radial knn ({@code min_score}/{@code max_distance} have no {@code k}).
     *
     * <p>Each leg is pinned to the no-op search pipeline ({@code _none}). Otherwise a leg — a plain {@link SearchRequest}
     * with no explicit pipeline — would inherit the index's {@code index.search.default_pipeline} and re-run its
     * request/response processors once per leg (redundant, and incorrect for processors like {@code rerank} that expect
     * request context absent from an id-only leg). The outer fused request still carries the pipeline, so top-level
     * processors run exactly once.
     *
     * <p>{@code allow_partial_search_results} follows the request: an explicitly set value is propagated to every leg,
     * and when the user left it unset the leg flag is left unset too so each leg resolves the cluster default at
     * execution (default {@code true}) exactly like a normal search. Propagating only the explicit value matters because
     * the outer flag is a nullable {@code Boolean} that core has not yet resolved to the cluster default at rewrite
     * time, so an unconditional pass-through would unbox {@code null}. With an effective {@code true} a shard failing
     * for one leg degrades that leg to partial results; with {@code false} the leg fails and {@link #groupLegHits} turns
     * that into a whole-request failure. Note the fused-specific caveat: because normalization is per-leg, a
     * partially-degraded leg shifts its own min/max, so the fused ranking may differ from a complete run — not merely
     * return fewer docs (see {@link #groupLegHits} and the LLD "Partial-leg failure" note under *Consistency*).
     *
     * <p>A user-supplied point-in-time is passed through to every leg, so all legs and the self-erased round-2 query read
     * the same immutable view instead of N+1 independent reader instants — the consistency window that otherwise exists
     * on a live-ingest index. {@code keepAlive} is deliberately left unset on the legs so the PIT's original keep-alive
     * governs; the legs never extend it. Copying the request's indices alongside a PIT is safe: core's REST layer has
     * already resolved a PIT request's indices to the PIT's own, and the transport layer derives shards from the PIT
     * context regardless, so the leg is consistent with the outer request rather than in conflict with it.
     */
    static MultiSearchRequest buildLegMultiSearch(SearchRequest request, List<QueryBuilder> legs, int windowSize) {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        PointInTimeBuilder pointInTime = Objects.isNull(request.source()) ? null : request.source().pointInTimeBuilder();
        for (QueryBuilder leg : legs) {
            SearchSourceBuilder legSource = new SearchSourceBuilder().query(leg).size(windowSize).fetchSource(false).trackTotalHits(false);
            if (Objects.nonNull(pointInTime)) {
                legSource.pointInTimeBuilder(new PointInTimeBuilder(pointInTime.getId()));
            }
            SearchRequest legRequest = new SearchRequest(request.indices()).indicesOptions(request.indicesOptions())
                .source(legSource)
                .pipeline(SearchPipelineService.NOOP_PIPELINE_ID);
            if (Objects.nonNull(request.allowPartialSearchResults())) {
                legRequest.allowPartialSearchResults(request.allowPartialSearchResults());
            }
            multiSearchRequest.add(legRequest);
        }
        return multiSearchRequest;
    }

    /**
     * Fuse the leg results into the standard query the fused-mode hybrid self-erases into — a {@link HybridFusionQueryBuilder}
     * (Top + conditional Tail), or a {@link MatchNoneQueryBuilder} when nothing fused. Pure: returns the query and
     * mutates nothing.
     *
     * <p>The Tail (non-scoring {@code bool{should: legs}} surfacing the full match set) is included only when the request
     * needs it (aggregations / highlight / leg inner_hits / totals beyond the window — see {@link #needsTail}).
     *
     * <p>The Tail decision is <b>depth-independent</b>: it is derived from the request alone, not from whether this
     * hybrid is the whole query or nested inside a container. A nested fused query still self-erases into
     * {@code bool{Top + Tail}}, and an enclosing clause simply intersects that (fuse-then-filter), so aggregations and
     * {@code total_hits} stay correct at any nesting depth. This deliberately avoids inferring nesting from the query
     * instance (a reference-identity check against {@code source().query()} would silently drop the Tail — and with it
     * agg/total_hits accuracy — for any request-rewrite layer that clones the query first).
     */
    static QueryBuilder buildFusedQuery(
        SearchSourceBuilder source,
        MultiSearchResponse multiSearchResponse,
        List<QueryBuilder> legs,
        FusionSpec fusion,
        int windowSize
    ) {
        MultiSearchResponse.Item[] items = multiSearchResponse.getResponses();
        SearchHit[][] legHits = groupLegHits(items, legs.size());
        RankedDocs ranked = computeRankedDocs(legHits, fusion, windowSize);
        if (ranked.ids().length == 0) {
            return new MatchNoneQueryBuilder();
        }
        List<QueryBuilder> tail = needsTail(source, legs, ranked.ids().length) ? legQueriesForTail(legs, legHits) : List.of();
        return new HybridFusionQueryBuilder(ranked.ids(), ranked.scores(), tail);
    }

    /**
     * Reduce the raw MultiSearch items into a per-leg array of hits (one item per leg). A wholly-failed leg (all shards
     * down or a non-partial error → {@code Item.isFailure()}) fails the whole request — fusing over a missing leg would
     * silently change the ranking function, not merely return fewer docs. A leg that only lost some shards under
     * {@code allow_partial_search_results=true} comes back as a successful item with fewer hits and is fused as-is —
     * matching OpenSearch's default partial-results behavior. Because normalization is per-leg, that degraded leg shifts
     * its own min/max, so the fused <i>ranking</i> can differ from a complete run rather than merely losing docs; a
     * response {@code Warning} header names the affected legs so the degradation is not silent. Under an effective
     * {@code allow_partial_search_results=false} the leg itself fails, which the check below turns into a whole-request
     * failure — so honoring that flag needs no separate handling here.
     */
    private static SearchHit[][] groupLegHits(MultiSearchResponse.Item[] items, int legCount) {
        if (items.length != legCount) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "[hybrid] expected %d leg sub-search responses but got %d", legCount, items.length)
            );
        }
        SearchHit[][] legHits = new SearchHit[legCount][];
        List<Integer> degradedLegs = new ArrayList<>();
        for (int leg = 0; leg < legCount; leg++) {
            MultiSearchResponse.Item item = items[leg];
            if (item.isFailure()) {
                throw new IllegalStateException(
                    String.format(Locale.ROOT, "[hybrid] fused-mode sub-query %d failed: %s", leg, item.getFailureMessage()),
                    item.getFailure()
                );
            }
            // Shard failures (not successful<total) — skipped/can-match shards are not failures. Read the array rather
            // than getFailedShards(), which dereferences it unguarded.
            ShardSearchFailure[] shardFailures = item.getResponse().getShardFailures();
            if (Objects.nonNull(shardFailures) && shardFailures.length > 0) {
                degradedLegs.add(leg);
            }
            legHits[leg] = item.getResponse().getHits().getHits();
        }
        warnOnDegradedLegs(degradedLegs);
        return legHits;
    }

    /**
     * Surface partially-degraded legs as a response {@code Warning} header. Uses the same mechanism as deprecation
     * warnings, emitted from the coordinator rewrite's async callback so it rides the request's thread context onto the
     * response.
     */
    private static void warnOnDegradedLegs(final List<Integer> degradedLegs) {
        if (degradedLegs.isEmpty()) {
            return;
        }
        HeaderWarning.addWarning(
            String.format(
                Locale.ROOT,
                "[hybrid] fused-mode sub-quer%s %s returned partial results (shard failures); fused scores were computed "
                    + "over an incomplete result set, so ranking may differ from a complete run",
                degradedLegs.size() == 1 ? "y" : "ies",
                degradedLegs
            )
        );
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
        // Normalization is resolved by name, so widening technique support is a new ScalarNormalizer + factory entry —
        // no change here. The caller already rejected techniques outside the current scope at rewrite.
        ScalarNormalizer normalizer = ScalarNormalizerFactory.create(fusion.normalizationTechnique());
        Map<String, Float> combined = CoordinatorScoreFusion.fuse(legRawScores, normalizer, combination);
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

    /**
     * Legs whose full Lucene match set equals their returned top-k, so re-running them in the Tail would be a redundant
     * ANN pass (kNN/neural re-walk the HNSW graph). Only such legs are safe to materialize as an {@link IdsQueryBuilder}
     * of the already-retrieved window ids. A leg whose match set is NOT bounded to its top-k — e.g. {@code neural_sparse},
     * whose match set is every doc containing a query token (far larger than the window) — must NOT be materialized, or
     * the Tail would drop the rest and undercount total_hits/aggregations (and re-running it is cheap: no graph to walk).
     */
    private static boolean isMaterializableLeg(QueryBuilder leg) {
        String name = leg.getWriteableName();
        return KNNQueryBuilder.NAME.equals(name) || NeuralQueryBuilder.NAME.equals(name) || NeuralKNNQueryBuilder.NAME.equals(name);
    }

    /**
     * Single source of truth for whether a top-level fused query needs the executed Tail (the non-scoring
     * {@code bool{should: legs}}): aggregations or highlighting need the full match set in the query phase, leg
     * inner_hits currently require the legs registered via the Tail, and an accurate index-wide {@code total_hits}
     * beyond the fused window needs the legs counted. {@code explain}/{@code profile} are intentionally NOT triggers —
     * the fused score is computed on the coordinator and the Top is a {@code constant_score(ids)}, so the Lucene tree
     * carries no fusion breakdown to explain, and profiling the Tail would only time a redundant re-execution of legs
     * that already ran in the fan-out (fusion-aware explain/profile is scoped to a later PR).
     */
    private static boolean needsTail(SearchSourceBuilder source, List<QueryBuilder> legs, int numRankedDocs) {
        if (Objects.nonNull(source) && (Objects.nonNull(source.aggregations()) || Objects.nonNull(source.highlighter()))) {
            return true;
        }
        if (legsHaveInnerHits(legs)) {
            return true;
        }
        return wantsTotalsBeyondWindow(source, numRankedDocs);
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
