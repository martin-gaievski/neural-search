/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.resolver;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Shared coordinator-level orchestration for the resolver, independent of the interception point
 * (search pipeline processor or ActionFilter).
 *
 * <p>Two placement modes:
 * <ul>
 *   <li><b>Top-level</b> ({@link #buildLegMultiSearch}/{@link #applyFusedResults}): the resolver IS
 *       the whole query. Rewrites {@code source.query()} into a {@link RankDocsQueryBuilder} with a
 *       <em>conditional Tail</em> (aggregations / total-hits / explain / highlight).</li>
 *   <li><b>Nested</b> ({@link #collectMarkers}/{@link #buildMarkerMultiSearch}/{@link #resolveMarkers}
 *       /{@link #replaceMarkers}): one or more resolver markers appear inside a {@code bool} tree.
 *       Each marker is resolved to a Top-only {@link RankDocsQueryBuilder} and spliced back in place;
 *       enclosing {@code bool} filter clauses are <b>pushed down</b> into each leg so fusion runs over
 *       the filtered candidate set. This is possible because the resolver self-erases into a standard
 *       query — something the hybrid query (which emits non-standard CompoundTopDocs) cannot do.</li>
 * </ul>
 *
 * <p>Nested traversal is scoped to {@code bool} containers in this prototype.
 */
public final class ResolverOrchestrator {

    private ResolverOrchestrator() {}

    // ---------------------------------------------------------------------------------------------
    // Top-level path (single resolver as the whole query)
    // ---------------------------------------------------------------------------------------------

    /** Maximum leg-search fan-out (legs x shards) before per-shard collection falls back to the coordinator path,
     *  to bound the MultiSearch size / coordinator memory at very high shard counts. */
    static final int MAX_PER_SHARD_FANOUT = 128;

    /** Secondary preference appended after {@code _shards:i} to pin every leg of a shard to the same (primary) copy. */
    private static final String PRIMARY_PREFERENCE = "_primary";

    /**
     * Decision for how a top-level resolver's legs are collected. When {@link #perShard} is false the legs are
     * fired as one standalone search each (coordinator reduce to the global top-{@code rank_window_size}); when
     * true, each leg is fired once PER SHARD ({@code preference=_shards:i|<copyPin>}) so fusion sees the same
     * {@code num_shards x depth} candidate pool as the hybrid query.
     *
     * <p>The plan is computed ONCE (in the interception point, via {@link #planCollection}) and threaded into
     * both {@link #buildLegMultiSearch} and {@link #applyFusedResults} — it MUST NOT be recomputed between them,
     * because it reads live cluster state and the item layout produced during the build must match the layout the
     * reduce reads back (a concurrent index/alias change between two independent reads would otherwise mis-index
     * the MultiSearch responses).
     */
    public record CollectionPlan(boolean perShard, int numShards, int depth, String copyPin) {
        static CollectionPlan coordinator() {
            return new CollectionPlan(false, 1, 0, null);
        }
    }

    /**
     * Resolve the per-shard collection plan for a top-level resolver. Falls back to the coordinator path (returns
     * {@code perShard=false}) unless ALL of the following hold: the marker requests per-shard collection for
     * min_max+arithmetic_mean; the request targets exactly one concrete index (so a single shard ordinal is
     * unambiguous — {@code _shards:i} would otherwise hit shard i of every co-targeted index); no custom
     * {@code routing}/{@code preference} is set (which would narrow the shard set / select a copy and break naive
     * enumeration); the index has >= 2 shards (with 1 shard the global top-K already equals the shard's local
     * top-K); and the fan-out stays within {@link #MAX_PER_SHARD_FANOUT}.
     *
     * <p>Call this ONCE per request and thread the result; see {@link CollectionPlan}.
     */
    public static CollectionPlan planCollection(SearchRequest request, ResolverQueryBuilder resolver) {
        if (resolver.isPerShardCollection() == false) {
            return CollectionPlan.coordinator();
        }
        if (request.routing() != null || request.preference() != null) {
            // routing narrows the shard set; a custom preference selects a copy (and can't be safely composed after
            // "_shards:i|"). In both cases don't hand-roll enumeration — fall back to the coordinator path.
            return CollectionPlan.coordinator();
        }
        List<IndexMetadata> indices = NeuralSearchClusterUtil.instance().getIndexMetadataList(request);
        // Exactly one non-null concrete index: _shards:i is ambiguous across co-targeted indices, and a null entry
        // means the resolved index was concurrently removed — fall back rather than NPE on getNumberOfShards().
        if (indices.size() != 1 || indices.get(0) == null) {
            return CollectionPlan.coordinator();
        }
        int numShards = indices.get(0).getNumberOfShards();
        if (numShards < 2) {
            return CollectionPlan.coordinator(); // single shard: per-shard pool == coordinator pool
        }
        int fanout = numShards * resolver.queries().size();
        if (fanout > MAX_PER_SHARD_FANOUT) {
            return CollectionPlan.coordinator(); // bound MultiSearch size / coordinator memory
        }
        // Pin every leg of shard i to the PRIMARY copy so they all read the identical segment view — the
        // union-then-normalize equivalence with hybrid requires it under replicas. "_primary" is a valid secondary
        // preference after "_shards:i|" (a custom session string is not; verified against core Preference.parse), and
        // the caller can't have set their own preference here (that path already fell back to coordinator above).
        // Known trade-off (acceptable for the POC; opt-in per_shard only): pinning to the primary means that if a
        // shard's primary is transiently unavailable while a replica is up, that per-shard leg fails and the whole
        // search errors — whereas the coordinator path would serve from the replica. There is no preference string
        // that means "the same arbitrary copy across N independent searches", so primary is the only deterministic
        // same-copy pin available plugin-side; production would gather per-shard candidates in one collector pass.
        return new CollectionPlan(true, numShards, resolver.candidateDepth(), PRIMARY_PREFERENCE);
    }

    /** Build the leg MultiSearch for a top-level resolver, per the pre-computed {@code plan}: one search per leg
     *  (coordinator), or one search per (leg, shard) when the plan is per-shard. Items are laid out LEG-MAJOR: for
     *  per-shard, leg L's shards occupy indices {@code [L*numShards, (L+1)*numShards)}. */
    public static MultiSearchRequest buildLegMultiSearch(SearchRequest request, ResolverQueryBuilder resolver, CollectionPlan plan) {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        if (plan.perShard()) {
            // Per-shard sub-searches gather candidate scores only; totals come from the Tail (see applyFusedResults).
            for (QueryBuilder leg : resolver.queries()) {
                for (int shard = 0; shard < plan.numShards(); shard++) {
                    multiSearchRequest.add(perShardLegSearch(request, leg, plan.depth(), shard, plan.copyPin()));
                }
            }
        } else {
            int trackTotalHitsUpTo = trackTotalHitsCap(request.source());
            for (QueryBuilder leg : resolver.queries()) {
                multiSearchRequest.add(legSearch(request, leg, List.of(), resolver.rankWindowSize(), trackTotalHitsUpTo));
            }
        }
        return multiSearchRequest;
    }

    /** The main query's effective track_total_hits cap (default 10000 when unset), propagated to the legs so
     *  the leg-union total reproduces what the main query would report. */
    private static final int DEFAULT_TRACK_TOTAL_HITS = 10000;

    private static int trackTotalHitsCap(SearchSourceBuilder source) {
        Integer trackTotalHitsUpTo = source == null ? null : source.trackTotalHitsUpTo();
        return trackTotalHitsUpTo == null ? DEFAULT_TRACK_TOTAL_HITS : trackTotalHitsUpTo;
    }

    /** Compute coordinator RRF / min_max+AM and rewrite {@code source.query()} into a {@link RankDocsQueryBuilder}.
     *  Returns a {@link TotalHits} to patch onto the response when accurate total-hits are derivable from the legs'
     *  own totals (Tail avoided); null when Top-only (no accurate totals needed) or when the Tail is kept (it carries
     *  totals / aggregations / explain / highlight). */
    public static TotalHits applyFusedResults(
        SearchSourceBuilder source,
        MultiSearchResponse multiSearchResponse,
        ResolverQueryBuilder resolver,
        CollectionPlan plan
    ) {
        MultiSearchResponse.Item[] items = multiSearchResponse.getResponses();
        SearchHit[][] legHits = groupLegHits(items, resolver.queries().size(), plan);
        RankedDocs ranked = computeRankedDocs(legHits, resolver);
        if (ranked.ids.length == 0) {
            source.query(new MatchNoneQueryBuilder());
            return null;
        }
        boolean topOnly;
        TotalHits patchTotal = null;
        if (needsExecutionTail(source)) {
            topOnly = false; // aggregations / explain / profile / highlight need the full match set IN the query
        } else if (wantsTotalsBeyondWindow(source, ranked.ids.length)) {
            // Per-shard slices each report only their own shard's total, so the leg-union derivation is invalid;
            // fall back to the Tail for an accurate index-wide count. Otherwise derive the union from the legs.
            patchTotal = plan.perShard() ? null : legUnionTotalHits(items);
            topOnly = patchTotal != null; // else fall back to the Tail for an exact count
        } else {
            topOnly = true; // track_total_hits:false -> plain top-K, no Tail
        }
        source.query(new RankDocsQueryBuilder(ranked.ids, ranked.scores, topOnly ? List.of() : resolver.queries()));
        return patchTotal;
    }

    /** Reduce the raw MultiSearch items into a per-leg array of hits. Coordinator plan: one item per leg. Per-shard
     *  plan: leg L owns the {@code numShards} items at {@code [L*numShards, (L+1)*numShards)}, whose hits are
     *  concatenated into leg L's union pool (over which min/max is later computed — the same pool hybrid normalizes
     *  over). */
    private static SearchHit[][] groupLegHits(MultiSearchResponse.Item[] items, int legCount, CollectionPlan plan) {
        int expected = plan.perShard() ? legCount * plan.numShards() : legCount;
        if (items.length != expected) {
            // The plan that laid out the MultiSearch and the response items must agree. They are threaded from a
            // single planCollection() call, so a mismatch here means an internal invariant was violated (never the
            // recompute race the plan-threading eliminated) — fail loudly rather than mis-index the responses.
            throw new IllegalStateException(
                String.format(
                    Locale.ROOT,
                    "[resolver] expected %d leg sub-search responses (perShard=%b, legs=%d, shards=%d) but got %d",
                    expected,
                    plan.perShard(),
                    legCount,
                    plan.numShards(),
                    items.length
                )
            );
        }
        SearchHit[][] legHits = new SearchHit[legCount][];
        if (plan.perShard()) {
            int n = plan.numShards();
            for (int leg = 0; leg < legCount; leg++) {
                List<SearchHit> union = new ArrayList<>();
                for (int shard = 0; shard < n; shard++) {
                    int itemIndex = leg * n + shard;
                    for (SearchHit hit : hitsOrThrow(items[itemIndex], itemIndex)) {
                        union.add(hit);
                    }
                }
                legHits[leg] = union.toArray(new SearchHit[0]);
            }
        } else {
            for (int leg = 0; leg < legCount; leg++) {
                legHits[leg] = hitsOrThrow(items[leg], leg);
            }
        }
        return legHits;
    }

    // ---------------------------------------------------------------------------------------------
    // Nested path (resolver markers inside a bool tree)
    // ---------------------------------------------------------------------------------------------

    /** One resolver marker found in the query tree, with the enclosing-bool filters to push down. */
    public record MarkerContext(ResolverQueryBuilder marker, List<QueryBuilder> pushDownFilters) {
    }

    /** Recursively collect resolver markers inside {@code bool} containers, accumulating enclosing
     *  {@code filter} clauses (non-resolver) as push-down filters. */
    public static List<MarkerContext> collectMarkers(QueryBuilder root) {
        List<MarkerContext> markers = new ArrayList<>();
        collect(root, List.of(), markers);
        return markers;
    }

    private static void collect(QueryBuilder qb, List<QueryBuilder> pushDown, List<MarkerContext> out) {
        if (qb instanceof ResolverQueryBuilder resolver) {
            out.add(new MarkerContext(resolver, pushDown));
            return;
        }
        if (qb instanceof BoolQueryBuilder bool) {
            List<QueryBuilder> childPushDown = new ArrayList<>(pushDown);
            for (QueryBuilder f : bool.filter()) {
                if ((f instanceof ResolverQueryBuilder) == false) {
                    childPushDown.add(f);
                }
            }
            for (QueryBuilder c : bool.must()) {
                collect(c, childPushDown, out);
            }
            for (QueryBuilder c : bool.should()) {
                collect(c, childPushDown, out);
            }
            for (QueryBuilder c : bool.filter()) {
                collect(c, childPushDown, out);
            }
            // must_not intentionally not traversed (a resolver as a negative clause is nonsensical).
        }
        // Leaves and non-bool containers are not traversed in this prototype.
    }

    /** Flatten every marker's legs (with push-down filters applied) into a single MultiSearch. */
    public static MultiSearchRequest buildMarkerMultiSearch(SearchRequest request, List<MarkerContext> markers) {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        for (MarkerContext mc : markers) {
            for (QueryBuilder leg : mc.marker().queries()) {
                multiSearchRequest.add(legSearch(request, leg, mc.pushDownFilters(), mc.marker().rankWindowSize(), 0));
            }
        }
        return multiSearchRequest;
    }

    /** Resolve each marker to a Top-only {@link RankDocsQueryBuilder} (identity-keyed for replacement). */
    public static IdentityHashMap<ResolverQueryBuilder, QueryBuilder> resolveMarkers(
        List<MarkerContext> markers,
        MultiSearchResponse multiSearchResponse
    ) {
        MultiSearchResponse.Item[] items = multiSearchResponse.getResponses();
        IdentityHashMap<ResolverQueryBuilder, QueryBuilder> resolved = new IdentityHashMap<>();
        int offset = 0;
        for (MarkerContext mc : markers) {
            int legCount = mc.marker().queries().size();
            // Nested markers always collect one item per leg (per-shard collection is top-level-only), so the
            // slice maps 1:1 to legs regardless of the marker's collection knob.
            SearchHit[][] legHits = new SearchHit[legCount][];
            for (int leg = 0; leg < legCount; leg++) {
                legHits[leg] = hitsOrThrow(items[offset + leg], offset + leg);
            }
            offset += legCount;
            RankedDocs ranked = computeRankedDocs(legHits, mc.marker());
            resolved.put(
                mc.marker(),
                ranked.ids.length == 0 ? new MatchNoneQueryBuilder() : new RankDocsQueryBuilder(ranked.ids, ranked.scores, List.of()) // Top-only
                                                                                                                                      // for
                                                                                                                                      // nested
                                                                                                                                      // markers
            );
        }
        return resolved;
    }

    /** Rebuild the query tree, swapping each resolver marker for its resolved standard query. */
    public static QueryBuilder replaceMarkers(QueryBuilder qb, IdentityHashMap<ResolverQueryBuilder, QueryBuilder> resolved) {
        if (qb instanceof ResolverQueryBuilder resolver) {
            QueryBuilder replacement = resolved.get(resolver);
            return replacement != null ? replacement : qb;
        }
        if (qb instanceof BoolQueryBuilder bool) {
            BoolQueryBuilder rebuilt = new BoolQueryBuilder();
            for (QueryBuilder c : bool.must()) {
                rebuilt.must(replaceMarkers(c, resolved));
            }
            for (QueryBuilder c : bool.should()) {
                rebuilt.should(replaceMarkers(c, resolved));
            }
            for (QueryBuilder c : bool.filter()) {
                rebuilt.filter(replaceMarkers(c, resolved));
            }
            for (QueryBuilder c : bool.mustNot()) {
                rebuilt.mustNot(replaceMarkers(c, resolved));
            }
            if (bool.minimumShouldMatch() != null) {
                rebuilt.minimumShouldMatch(bool.minimumShouldMatch());
            }
            rebuilt.adjustPureNegative(bool.adjustPureNegative());
            rebuilt.boost(bool.boost());
            if (bool.queryName() != null) {
                rebuilt.queryName(bool.queryName());
            }
            return rebuilt;
        }
        return qb;
    }

    // ---------------------------------------------------------------------------------------------
    // Shared helpers
    // ---------------------------------------------------------------------------------------------

    /** A single leg search: the leg query, optionally constrained by pushed-down filters, id-only. */
    private static SearchRequest legSearch(
        SearchRequest request,
        QueryBuilder leg,
        List<QueryBuilder> pushDownFilters,
        int size,
        int trackTotalHitsUpTo
    ) {
        QueryBuilder legQuery = leg;
        if (pushDownFilters.isEmpty() == false) {
            BoolQueryBuilder constrained = new BoolQueryBuilder().must(leg);
            for (QueryBuilder f : pushDownFilters) {
                constrained.filter(f);
            }
            legQuery = constrained;
        }
        SearchSourceBuilder legSource = new SearchSourceBuilder().query(legQuery).size(size).from(0).fetchSource(false);
        if (trackTotalHitsUpTo > 0) {
            // count matches (up to the cap) so the union total can be derived from the legs without a Tail re-run
            legSource.trackTotalHitsUpTo(trackTotalHitsUpTo);
        } else {
            legSource.trackTotalHits(false);
        }
        return new SearchRequest(request.indices()).indicesOptions(request.indicesOptions()).source(legSource);
    }

    /** A single leg's LOCAL top-{@code depth} on ONE shard: routed to shard {@code shard} via
     *  {@code preference=_shards:<shard>|<copyPin>} (the copy-pin keeps every leg of a shard on the same replica),
     *  id-only, totals disabled (per-shard slices can't reconstruct index-wide totals — the Tail supplies those). */
    private static SearchRequest perShardLegSearch(SearchRequest request, QueryBuilder leg, int depth, int shard, String copyPin) {
        SearchSourceBuilder legSource = new SearchSourceBuilder().query(leg).size(depth).from(0).fetchSource(false).trackTotalHits(false);
        return new SearchRequest(request.indices()).indicesOptions(request.indicesOptions())
            .preference("_shards:" + shard + "|" + copyPin)
            .source(legSource);
    }

    /** Fuse the per-leg candidate hits ({@code legHits[legIndex]} = that leg's pool; for per-shard collection this
     *  is already the union across shards) into a ranked, truncated id/score list. */
    private static RankedDocs computeRankedDocs(SearchHit[][] legHits, ResolverQueryBuilder resolver) {
        Map<String, Float> combined = ResolverQueryBuilder.TECHNIQUE_ARITHMETIC_MEAN.equals(resolver.technique())
            ? minMaxArithmeticMean(legHits, resolver)
            : rrf(legHits, resolver);
        return toRankedDocs(combined, resolver.rankWindowSize());
    }

    /**
     * Rank-based Reciprocal Rank Fusion: score(d) = Σ 1 / (rank_constant + rank_i(d) + 1).
     * <p>Note: RRF stays on the coordinator (global-top-K) collection path — {@code isPerShardCollection()} is
     * false for RRF — so each {@code legHits[legIndex]} is a single globally-merged, rank-ordered leg result and
     * the array index is the doc's rank. RRF is rank-based and already at hybrid parity, so it does not need the
     * per-shard pool.
     */
    private static Map<String, Float> rrf(SearchHit[][] legHits, ResolverQueryBuilder resolver) {
        Map<String, Float> scores = new LinkedHashMap<>();
        for (SearchHit[] hits : legHits) {
            for (int rank = 0; rank < hits.length; rank++) {
                String id = hits[rank].getId();
                if (id == null) {
                    continue;
                }
                scores.merge(id, 1.0f / (resolver.rankConstant() + rank + 1), Float::sum);
            }
        }
        return scores;
    }

    /**
     * Score-based min-max normalization + (weighted) arithmetic mean. Per leg, raw {@code _score}s are
     * min-max normalized over that leg's candidate pool; per doc, the combined score is the weighted mean over
     * ALL legs (a non-matched leg contributes 0). Mirrors hybrid's {@code MinMaxScoreNormalizationTechnique}
     * (degenerate min==max -> 1.0; normalized 0 -> 0.001 floor so a matched doc is never confused with a
     * non-match) and {@code ArithmeticMeanScoreCombinationTechnique} (denominator = sum of ALL leg weights,
     * so a doc strong in both legs outranks one strong in only a single leg).
     * <p>Faithfulness to hybrid depends on the leg pool: under per-shard collection {@code legHits[legIndex]} is
     * the union of every shard's local top-depth, so the min/max computed here equals hybrid's global per-subquery
     * min/max (which {@code MinMaxScoreNormalizationTechnique.getMinScores/getMaxScores} take across all shards).
     */
    private static Map<String, Float> minMaxArithmeticMean(SearchHit[][] legHits, ResolverQueryBuilder resolver) {
        float totalWeight = 0.0f;
        for (int legIndex = 0; legIndex < legHits.length; legIndex++) {
            totalWeight += weightForLeg(resolver.weights(), legIndex);
        }
        Map<String, Float> weightedSum = new LinkedHashMap<>(); // id -> Σ weight_leg * normalized_leg
        for (int legIndex = 0; legIndex < legHits.length; legIndex++) {
            SearchHit[] hits = legHits[legIndex];
            float min = Float.MAX_VALUE;
            float max = -Float.MAX_VALUE;
            for (SearchHit hit : hits) {
                min = Math.min(min, hit.getScore());
                max = Math.max(max, hit.getScore());
            }
            float weight = weightForLeg(resolver.weights(), legIndex);
            for (SearchHit hit : hits) {
                String id = hit.getId();
                if (id == null) {
                    continue;
                }
                weightedSum.merge(id, weight * normalizeMinMax(hit.getScore(), min, max), Float::sum);
            }
        }
        Map<String, Float> combined = new LinkedHashMap<>();
        for (Map.Entry<String, Float> e : weightedSum.entrySet()) {
            combined.put(e.getKey(), totalWeight == 0.0f ? 0.0f : e.getValue() / totalWeight);
        }
        return combined;
    }

    private static float normalizeMinMax(float score, float min, float max) {
        if (Float.compare(max, min) == 0) {
            return 1.0f; // single/degenerate leg
        }
        float normalized = (score - min) / (max - min);
        return normalized == 0.0f ? 0.001f : normalized; // floor: matched-with-min != not-matched(0)
    }

    private static float weightForLeg(float[] weights, int legIndex) {
        return (weights == null || weights.length == 0) ? 1.0f : weights[legIndex];
    }

    private static SearchHit[] hitsOrThrow(MultiSearchResponse.Item item, int legIndex) {
        if (item.isFailure()) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "[resolver] sub-query %d failed: %s", legIndex, item.getFailureMessage()),
                item.getFailure()
            );
        }
        return item.getResponse().getHits().getHits();
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

    /** Aggregations / explain / profile / highlight require the full match set to be present IN the query
     *  execution (stage B) — only the Tail provides that. */
    private static boolean needsExecutionTail(SearchSourceBuilder source) {
        return source.aggregations() != null || Boolean.TRUE.equals(source.explain()) || source.profile() || source.highlighter() != null;
    }

    private static boolean wantsTotalsBeyondWindow(SearchSourceBuilder source, int numRankedDocs) {
        Integer trackTotalHitsUpTo = source.trackTotalHitsUpTo();
        return trackTotalHitsUpTo == null || trackTotalHitsUpTo > numRankedDocs;
    }

    /** Union total-hits derived from the legs' OWN totals + retrieved ids — no leg re-run. Returns:
     *  - (max leg total, GTE) when a leg hit its track_total_hits cap → union ≥ that; matches what the Tail reports;
     *  - (id-set-union size, EQUAL_TO) when every leg's full match set was retrieved (leg total ≤ retrieved) → exact;
     *  - null when neither holds (a moderate uncapped set only partially retrieved) → caller keeps the Tail for an
     *    exact count. Data (BEIR Quora/TREC-COVID): the capped branch covers the common large-corpus case exactly. */
    private static TotalHits legUnionTotalHits(MultiSearchResponse.Item[] items) {
        boolean anyCapped = false;
        boolean allFullyRetrieved = true;
        long maxLegTotal = 0;
        Set<String> unionIds = new HashSet<>();
        for (MultiSearchResponse.Item item : items) {
            if (item.isFailure()) {
                continue;
            }
            SearchHits hits = item.getResponse().getHits();
            TotalHits legTotalHits = hits.getTotalHits();
            long legTotal = legTotalHits == null ? 0 : legTotalHits.value();
            maxLegTotal = Math.max(maxLegTotal, legTotal);
            if (legTotalHits != null && legTotalHits.relation() == TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO) {
                anyCapped = true;
            }
            SearchHit[] legHits = hits.getHits();
            if (legTotal > legHits.length) {
                allFullyRetrieved = false;
            }
            for (SearchHit hit : legHits) {
                if (hit.getId() != null) {
                    unionIds.add(hit.getId());
                }
            }
        }
        if (anyCapped) {
            return new TotalHits(maxLegTotal, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
        }
        if (allFullyRetrieved) {
            return new TotalHits(unionIds.size(), TotalHits.Relation.EQUAL_TO);
        }
        return null;
    }

    private record RankedDocs(String[] ids, float[] scores) {
    }
}
