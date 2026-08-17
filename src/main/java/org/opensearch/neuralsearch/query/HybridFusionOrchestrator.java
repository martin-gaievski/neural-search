/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
import org.opensearch.search.builder.SearchSourceBuilder;

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
    /** Separator for the composite _index+_id fusion key. Never parsed back — see computeRankedDocs. */
    private static final String KEY_SEPARATOR = "#";

    /**
     * Build the leg MultiSearch: one standalone search per sub-query, each reduced to the global top-{@code windowSize}.
     *
     * <p>What each leg inherits from the user's request is decided in exactly one place — {@link CandidateScope}, which
     * classifies every field of {@link SearchRequest} and {@link SearchSourceBuilder} as propagated, overridden,
     * rejected, Tail-forcing, or deliberately dropped, with the reason recorded next to it. This method only assembles
     * the per-leg requests it produces; it holds no propagation policy of its own, so no request field can reach a leg
     * (or fail to) by omission here.
     *
     * <p>Note on ANN legs: {@code size} sets how many hits a leg returns, but an ANN leg's retrieval depth is bounded by
     * its own {@code k} (collected per shard), not by {@code size} — a {@code knn}/{@code neural} leg with a small or
     * default {@code k} (10) contributes at most that many candidates regardless of {@code windowSize}. This matches
     * classic hybrid, which likewise never rewrites {@code k}; for a full window, set {@code k >= window_size} on the
     * sub-query. We deliberately do NOT rewrite {@code k} here — it would diverge from classic and has no analog for
     * radial knn ({@code min_score}/{@code max_distance} have no {@code k}).
     */
    static MultiSearchRequest buildLegMultiSearch(CandidateScope scope, List<QueryBuilder> legs, int windowSize) {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        for (QueryBuilder leg : legs) {
            multiSearchRequest.add(scope.newLegRequest(leg, windowSize));
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
        List<QueryBuilder> tail = needsTail(source, ranked.ids().length) ? legQueriesForTail(legs, legHits) : List.of();
        // inner_hits are registered from the legs themselves, independent of whether the Tail executes them.
        return new HybridFusionQueryBuilder(ranked.ids(), ranked.indices(), ranked.scores(), tail, innerHitsLegs(legs));
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
     * Fuse via the shared {@link CoordinatorScoreFusion} core, then rank by fused score and cut to the window. Converts
     * the coordinator's {@code SearchHit[][]} view into the per-leg key→score maps the shared core consumes; a leg that
     * matched nothing contributes an empty map (groupLegHits fails fast on failures, so every slot is non-null).
     *
     * <p>Documents are keyed by {@code _index} + {@code _id}, not {@code _id} alone: {@code _id} is unique only within an
     * index, so across indices two different documents can share one and fusion would otherwise combine their scores as
     * if they were one document. The composite key is built with a separator but is never parsed back — an {@code _id}
     * may itself contain the separator, so the original identity is carried in a side map instead. To fusion and to every
     * normalizer the key stays opaque.
     */
    private static RankedDocs computeRankedDocs(SearchHit[][] legHits, FusionSpec fusion, int windowSize) {
        List<Map<String, Float>> legRawScores = new ArrayList<>(legHits.length);
        Map<String, SearchHit> identityByKey = new HashMap<>();
        for (SearchHit[] hits : legHits) {
            Map<String, Float> byKey = new LinkedHashMap<>();
            for (SearchHit hit : hits) {
                String key = documentKey(hit);
                byKey.put(key, hit.getScore());
                identityByKey.putIfAbsent(key, hit);
            }
            legRawScores.add(byKey);
        }
        ScoreCombinationTechnique combination = SCORE_COMBINATION_FACTORY.createCombination(
            fusion.combinationTechnique(),
            weightsParams(fusion.weights())
        );
        // Normalization is resolved by name, so widening technique support is a new ScalarNormalizer + factory entry —
        // no change here. The caller already rejected techniques outside the current scope at rewrite.
        ScalarNormalizer normalizer = ScalarNormalizerFactory.create(fusion.normalizationTechnique());
        Map<String, Float> combined = CoordinatorScoreFusion.fuse(legRawScores, normalizer, combination);
        return toRankedDocs(combined, identityByKey, windowSize);
    }

    /**
     * Fusion key for a hit: {@code _index}-qualified when the hit carries an index, else the bare {@code _id}.
     *
     * <p>Limitation in custom routing. {@code _index} + {@code _id} is not a total identity when custom routing is
     * used: the same {@code _id} can be written to different shards of one index under different routing values, giving
     * two genuinely distinct documents that share this key and are therefore fused as one. Qualifying further is not
     * possible from here — a leg's {@link SearchHit} exposes {@link SearchHit#getShard()} but no routing value, so the
     * coordinator has nothing to add to the key, and the {@code _routing} metadata field (queryable in principle) would
     * first have to be fetched per leg hit, which the id-only leg deliberately does not do. One possible way to resolve this
     * is leg-fetch.
     */
    private static String documentKey(SearchHit hit) {
        return Objects.isNull(hit.getIndex()) ? hit.getId() : hit.getIndex() + KEY_SEPARATOR + hit.getId();
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

    /**
     * Rank the fused scores, cut to the window, and resolve each key back to its {@code (_index, _id)} identity via the
     * side map — never by parsing the key. The returned {@code indices} array is null unless the window actually spans
     * more than one index, so a single-index search keeps the leaner {@code _id}-only Top clause.
     */
    private static RankedDocs toRankedDocs(Map<String, Float> scoresByKey, Map<String, SearchHit> identityByKey, int windowSize) {
        List<Map.Entry<String, Float>> ranked = new ArrayList<>(scoresByKey.entrySet());
        ranked.sort(Comparator.<Map.Entry<String, Float>>comparingDouble(e -> -e.getValue()).thenComparing(Map.Entry::getKey));
        if (ranked.size() > windowSize) {
            ranked = ranked.subList(0, windowSize);
        }
        String[] ids = new String[ranked.size()];
        String[] indices = new String[ranked.size()];
        float[] scores = new float[ranked.size()];
        Set<String> distinctIndices = new HashSet<>();
        for (int i = 0; i < ranked.size(); i++) {
            String key = ranked.get(i).getKey();
            SearchHit hit = identityByKey.get(key);
            // Defensive: a key always has an identity (both maps are built from the same hits), but never fall back to
            // parsing the composite key — an _id may contain the separator.
            ids[i] = Objects.isNull(hit) ? key : hit.getId();
            indices[i] = Objects.isNull(hit) ? null : hit.getIndex();
            scores[i] = ranked.get(i).getValue();
            distinctIndices.add(indices[i]);
        }
        boolean needsIndexQualification = distinctIndices.size() > 1;
        return new RankedDocs(ids, needsIndexQualification ? indices : null, scores);
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
     * Single source of truth for whether a fused query needs the executed Tail (the non-scoring
     * {@code bool{should: legs}}): aggregations or highlighting need the full match set in the query phase, an accurate
     * index-wide {@code total_hits} beyond the fused window needs the legs counted, and a sort that is not by
     * {@code _score} ranks over the match set rather than the fused window (see
     * {@link CandidateScope.Disposition#FORCES_TAIL}) — with
     * Top only, such a request would sort an arbitrary window-sized subset of its matches.
     *
     * <p>Deliberately NOT triggers:
     * <ul>
     *   <li>{@code explain}/{@code profile} — the fused score is computed on the coordinator and the Top is a
     *       {@code constant_score}, so the Lucene tree carries no fusion breakdown to explain, and profiling the Tail
     *       would only time a redundant re-execution of legs that already ran in the fan-out.</li>
     *   <li>leg {@code inner_hits} — inner_hits are built in the fetch phase from the <i>registered</i> inner-hit
     *       contexts per returned parent doc, so the leg never has to be executed for them to be returned. They are
     *       carried separately (see {@link #innerHitsLegs}), which keeps a Top-only query cheap without losing them.</li>
     * </ul>
     */
    private static boolean needsTail(SearchSourceBuilder source, int numRankedDocs) {
        if (Objects.nonNull(source) && (Objects.nonNull(source.aggregations()) || Objects.nonNull(source.highlighter()))) {
            return true;
        }
        if (CandidateScope.sortDiscardsFusedRanking(source)) {
            return true;
        }
        return wantsTotalsBeyondWindow(source, numRankedDocs);
    }

    /**
     * The legs that declare {@code inner_hits}, in their original (un-materialized) form, for fetch-phase registration.
     * Only legs actually carrying an inner_hits definition are kept, so the common case registers nothing. Note these are
     * the original leg builders rather than the Tail's possibly id-materialized form — a kNN/neural leg materialized to
     * ids has no inner_hits definition left to extract.
     */
    private static List<QueryBuilder> innerHitsLegs(List<QueryBuilder> legs) {
        List<QueryBuilder> withInnerHits = new ArrayList<>();
        for (QueryBuilder leg : legs) {
            Map<String, InnerHitContextBuilder> innerHits = new HashMap<>();
            InnerHitContextBuilder.extractInnerHits(leg, innerHits);
            if (innerHits.isEmpty() == false) {
                withInnerHits.add(leg);
            }
        }
        return withInnerHits;
    }

    private static boolean wantsTotalsBeyondWindow(SearchSourceBuilder source, int numRankedDocs) {
        if (Objects.isNull(source)) {
            return true;
        }
        Integer trackTotalHitsUpTo = source.trackTotalHitsUpTo();
        return Objects.isNull(trackTotalHitsUpTo) || trackTotalHitsUpTo > numRankedDocs;
    }

    private record RankedDocs(String[] ids, String[] indices, float[] scores) {
    }
}
