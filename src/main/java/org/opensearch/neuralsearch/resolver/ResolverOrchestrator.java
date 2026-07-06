/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.resolver;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.InternalSearchResponse;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Shared coordinator-level orchestration for the resolver: build the leg {@code MultiSearch}, fuse (RRF or
 * min_max+arithmetic_mean), and produce the standard query the resolver self-erases into. Interception-agnostic —
 * all methods are static and take the {@link SearchRequest}/{@link MultiSearchResponse} explicitly.
 *
 * <p>Two entry points, both driven from {@link ResolverQueryBuilder#doRewrite} (the {@code registerAsyncAction}
 * self-erase) except the fast path, which the thin {@link ResolverActionFilter} drives:
 * <ul>
 *   <li><b>Self-erase</b> ({@link #buildLegMultiSearch} + {@link #buildFusedQuery}): fuse and return a
 *       {@link RankDocsQueryBuilder} (Top + conditional Tail) or {@code match_none}. Used for the standard, nested,
 *       and per-shard paths. A nested marker self-erases to a Top-only query and any enclosing filter intersects the
 *       fused window at the query phase (fuse-then-filter) — the resolver no longer pushes filters into the legs.</li>
 *   <li><b>Fast path</b> ({@link #fastPathEligible} + {@link #fabricateFastPathResponse}): for plain top-K, fire the
 *       legs with {@code _source} and fabricate the response from the fused window, skipping the stage-B search.</li>
 * </ul>
 */
public final class ResolverOrchestrator {

    private ResolverOrchestrator() {}

    // ---------------------------------------------------------------------------------------------
    // Leg collection + fusion (drives the self-erase and the fast path)
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
     * <p>The plan is computed ONCE (in {@link ResolverQueryBuilder#doRewrite}, or the fast-path filter, via
     * {@link #planCollection}) and threaded into both {@link #buildLegMultiSearch} and the reduce
     * ({@link #buildFusedQuery} / {@link #fabricateFastPathResponse}) — it MUST NOT be recomputed between them,
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
        return buildLegMultiSearch(request, resolver, plan, false);
    }

    /** As {@link #buildLegMultiSearch(SearchRequest, ResolverQueryBuilder, CollectionPlan)}, but when
     *  {@code fetchSource} is true the legs return {@code _source} (and stored fields), so the fused window's hits are
     *  already hydrated and the stage-B main search can be skipped (the fast path — see
     *  {@link #fabricateFastPathResponse}). Only used on the coordinator-collection fast path. */
    public static MultiSearchRequest buildLegMultiSearch(
        SearchRequest request,
        ResolverQueryBuilder resolver,
        CollectionPlan plan,
        boolean fetchSource
    ) {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        if (plan.perShard()) {
            // Per-shard sub-searches gather candidate scores only (totals come from the Tail); on the fast path they
            // also hydrate _source so the fused window can be returned directly (see fabricateFastPathResponse).
            for (QueryBuilder leg : resolver.queries()) {
                for (int shard = 0; shard < plan.numShards(); shard++) {
                    multiSearchRequest.add(perShardLegSearch(request, leg, plan.depth(), shard, plan.copyPin(), fetchSource));
                }
            }
        } else {
            int trackTotalHitsUpTo = trackTotalHitsCap(request.source());
            for (QueryBuilder leg : resolver.queries()) {
                multiSearchRequest.add(legSearch(request, leg, resolver.rankWindowSize(), trackTotalHitsUpTo, fetchSource));
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

    /**
     * Fuse the leg results into the standard query this resolver self-erases into — a {@link RankDocsQueryBuilder}
     * (Top + conditional Tail), or a {@link MatchNoneQueryBuilder} when nothing fused. Pure: returns the query and
     * mutates nothing (the caller — {@code ResolverQueryBuilder.doRewrite} — returns it to the rewrite framework).
     *
     * <p>The Tail (non-scoring {@code bool{should: legs}} that surfaces the full match set) is included only when the
     * request needs it and this marker is the whole query:
     * <ul>
     *   <li><b>Nested</b> ({@code topLevel == false}): always <b>Top-only</b>. An enclosing {@code bool} filter
     *       intersects the fused window at the query phase (fuse-then-filter) — the resolver no longer pushes filters
     *       into the legs, so a nested marker's fused set is the unfiltered union and the outer clause narrows it.</li>
     *   <li><b>Top-level</b>: keep the Tail when aggregations / explain / profile / highlight need the full match set,
     *       or when an accurate total-hits count beyond the fused window is wanted (the Tail supplies that count in the
     *       single query phase — a rewrite cannot patch the response, so the legs'-own-totals shortcut the ActionFilter
     *       used is intentionally not reproduced here). Otherwise ({@code track_total_hits:false}) Top-only.</li>
     * </ul>
     */
    public static QueryBuilder buildFusedQuery(
        SearchSourceBuilder source,
        MultiSearchResponse multiSearchResponse,
        ResolverQueryBuilder resolver,
        CollectionPlan plan,
        boolean topLevel
    ) {
        MultiSearchResponse.Item[] items = multiSearchResponse.getResponses();
        SearchHit[][] legHits = groupLegHits(items, resolver.queries().size(), plan);
        RankedDocs ranked = computeRankedDocs(legHits, resolver);
        if (ranked.ids.length == 0) {
            return new MatchNoneQueryBuilder();
        }
        boolean topOnly;
        if (topLevel == false) {
            topOnly = true; // nested: enclosing filter intersects at the query phase
        } else if (needsExecutionTail(source)) {
            topOnly = false; // aggregations / explain / profile / highlight need the full match set IN the query
        } else if (wantsTotalsBeyondWindow(source, ranked.ids.length)) {
            topOnly = false; // keep the Tail for an accurate index-wide count (no response patch available in a rewrite)
        } else {
            topOnly = true; // track_total_hits:false -> plain top-K, no Tail
        }
        return new RankDocsQueryBuilder(ranked.ids, ranked.scores, topOnly ? List.of() : resolver.queries());
    }

    // ---------------------------------------------------------------------------------------------
    // Fast path: skip the stage-B main search for plain top-K retrieval by fabricating the response
    // directly from the fused window. The legs are fired with _source enabled, so the fused window's
    // hits are already hydrated; we override each hit's score with the fused score, sort, and page.
    // ---------------------------------------------------------------------------------------------

    /**
     * True when a top-level resolver request can be served by the stage-B-free fast path: plain top-K retrieval only.
     * We require the whole page to fit inside the fused window ({@code from + size <= rank_window_size}), and NONE of
     * the features whose semantics a fabricated response cannot faithfully reproduce without a real query phase:
     * aggregations / explain / profile / highlight (need the full match set IN the query — {@link #needsExecutionTail}),
     * user {@code sort} / {@code collapse} / {@code rescore} / {@code post_filter} / {@code search_after} /
     * {@code min_score} (need real collection over the executed query), {@code suggest} (a separate execution section
     * the fabricated response would drop), and per-hit fetch customization — {@code _source} include/exclude filtering,
     * {@code script_fields} / {@code docvalue_fields} / {@code fields} / {@code stored_fields}, {@code version} and
     * {@code seq_no_primary_term} — which the fast path's fixed full-{@code _source} leg fetch cannot reproduce.
     * Anything else falls back to the self-erasing {@link RankDocsQueryBuilder} path, which handles all of these.
     *
     * <p>Note: {@code scroll} is a property of the {@link SearchRequest}, not the {@link SearchSourceBuilder}, so it is
     * gated separately at the interception point (the fast path cannot produce a scroll cursor).
     */
    public static boolean fastPathEligible(SearchSourceBuilder source, ResolverQueryBuilder resolver) {
        if (source == null) {
            return false;
        }
        if (needsExecutionTail(source)) {
            return false;
        }
        int from = Math.max(0, source.from());
        int size = source.size() < 0 ? 10 : source.size();
        if (from + size > resolver.rankWindowSize()) {
            return false; // page extends beyond the fused window — the window cannot serve it
        }
        // Accurate total_hits BEYOND the fused window cannot be reconstructed from a window-sized fabricated response
        // (the legs retrieve only rankWindowSize hits, so a leg with more matches than the window would undercount);
        // that case needs the Tail / leg-union path. Only serve the fast path when totals need not exceed the window.
        if (wantsTotalsBeyondWindow(source, size)) {
            return false;
        }
        // Suggest is a separate execution section a fabricated response cannot carry — defer to the real query phase.
        if (source.suggest() != null) {
            return false;
        }
        // Per-hit fetch customization: the fast path returns each hit's FULL _source (the legs are fired with
        // fetchSource=true) and no derived per-hit fields, so any of these would be silently ignored — defer.
        if (source.fetchSource() != null
            || (source.scriptFields() != null && source.scriptFields().isEmpty() == false)
            || (source.docValueFields() != null && source.docValueFields().isEmpty() == false)
            || (source.fetchFields() != null && source.fetchFields().isEmpty() == false)
            || source.storedFields() != null
            || Boolean.TRUE.equals(source.version())
            || Boolean.TRUE.equals(source.seqNoAndPrimaryTerm())) {
            return false;
        }
        boolean hasSort = source.sorts() != null && source.sorts().isEmpty() == false;
        boolean hasRescore = source.rescores() != null && source.rescores().isEmpty() == false;
        return hasSort == false
            && hasRescore == false
            && source.collapse() == null
            && source.postFilter() == null
            && source.searchAfter() == null
            && source.minScore() == null;
    }

    /**
     * Fabricate the final {@link SearchResponse} directly from the fused window — no stage-B search. The legs were
     * fired with {@code _source} enabled (so their hits are hydrated); we take the fused ranked ids, reuse each
     * doc's already-fetched hit, override its {@code _score} with the fused score, sort by score desc, page to
     * {@code [from, from+size)}, and attach a leg-union {@code total_hits}. {@code template} is any of the leg
     * responses, used only to copy shard-count / took / clusters envelope fields.
     */
    public static SearchResponse fabricateFastPathResponse(
        SearchRequest request,
        SearchSourceBuilder source,
        MultiSearchResponse multiSearchResponse,
        ResolverQueryBuilder resolver,
        CollectionPlan plan
    ) {
        MultiSearchResponse.Item[] items = multiSearchResponse.getResponses();
        SearchHit[][] legHits = groupLegHits(items, resolver.queries().size(), plan);
        RankedDocs ranked = computeRankedDocs(legHits, resolver);

        // id -> a hydrated hit for that doc (first occurrence across legs; all carry the same _source).
        Map<String, SearchHit> hitById = new HashMap<>();
        for (SearchHit[] legHit : legHits) {
            for (SearchHit hit : legHit) {
                if (hit.getId() != null) {
                    hitById.putIfAbsent(hit.getId(), hit);
                }
            }
        }

        int from = Math.max(0, source.from());
        int size = source.size() < 0 ? 10 : source.size();
        List<SearchHit> page = new ArrayList<>();
        float maxScore = Float.NaN;
        for (int rank = from; rank < ranked.ids.length && page.size() < size; rank++) {
            SearchHit hit = hitById.get(ranked.ids[rank]);
            if (hit == null) {
                continue; // fused id whose hit wasn't hydrated (shouldn't happen with fetchSource legs) — skip
            }
            hit.score(ranked.scores[rank]);
            if (Float.isNaN(maxScore)) {
                maxScore = ranked.scores[rank];
            }
            page.add(hit);
        }

        // total_hits: exact/capped leg-union when derivable, else the size of the fused id set (a lower bound).
        TotalHits total = legUnionTotalHits(items);
        if (total == null) {
            total = new TotalHits(ranked.ids.length, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
        }

        SearchHits searchHits = new SearchHits(page.toArray(new SearchHit[0]), total, maxScore);
        InternalSearchResponse internal = new InternalSearchResponse(
            searchHits,
            null, // aggregations — fast path is gated to no-aggs
            null, // suggest
            null, // profile — gated out
            false,
            null,
            1
        );
        MultiSearchResponse.Item template = firstSuccess(items);
        SearchResponse t = template == null ? null : template.getResponse();
        return new SearchResponse(
            internal,
            null, // scrollId
            t == null ? plan.numShards() : t.getTotalShards(),
            t == null ? plan.numShards() : t.getSuccessfulShards(),
            t == null ? 0 : t.getSkippedShards(),
            t == null ? 0 : t.getTook().millis(),
            t == null ? new org.opensearch.action.search.ShardSearchFailure[0] : t.getShardFailures(),
            t == null ? SearchResponse.Clusters.EMPTY : t.getClusters()
        );
    }

    private static MultiSearchResponse.Item firstSuccess(MultiSearchResponse.Item[] items) {
        for (MultiSearchResponse.Item item : items) {
            if (item.isFailure() == false) {
                return item;
            }
        }
        return null;
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
    // Shared helpers
    // ---------------------------------------------------------------------------------------------

    /** A single leg search (coordinator collection): the leg query reduced to the global top-{@code size}. Id-only
     *  unless {@code fetchSource} (fast path), which hydrates {@code _source} so the fused window can be returned
     *  directly. Filters are no longer pushed down here — a nested resolver self-erases to a Top-only query and any
     *  enclosing filter intersects the fused window at the query phase (fuse-then-filter). */
    private static SearchRequest legSearch(SearchRequest request, QueryBuilder leg, int size, int trackTotalHitsUpTo, boolean fetchSource) {
        SearchSourceBuilder legSource = new SearchSourceBuilder().query(leg).size(size).from(0).fetchSource(fetchSource);
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
     *  totals disabled (per-shard slices can't reconstruct index-wide totals — the Tail supplies those). Id-only unless
     *  {@code fetchSource} (fast path), which hydrates {@code _source} so the fused window can be returned directly. */
    private static SearchRequest perShardLegSearch(
        SearchRequest request,
        QueryBuilder leg,
        int depth,
        int shard,
        String copyPin,
        boolean fetchSource
    ) {
        SearchSourceBuilder legSource = new SearchSourceBuilder().query(leg)
            .size(depth)
            .from(0)
            .fetchSource(fetchSource)
            .trackTotalHits(false);
        return new SearchRequest(request.indices()).indicesOptions(request.indicesOptions())
            .preference("_shards:" + shard + "|" + copyPin)
            .source(legSource);
    }

    /** Fuse the per-leg candidate hits ({@code legHits[legIndex]} = that leg's pool; for per-shard collection this
     *  is already the union across shards) into a ranked, truncated id/score list. RRF is rank-based; arithmetic_mean
     *  is score-based and normalizes each leg first — by min_max (range), z_score (distribution / DBSF-style), or
     *  l2 (magnitude). */
    private static RankedDocs computeRankedDocs(SearchHit[][] legHits, ResolverQueryBuilder resolver) {
        Map<String, Float> combined;
        if (ResolverQueryBuilder.TECHNIQUE_ARITHMETIC_MEAN.equals(resolver.technique())) {
            String norm = resolver.normalization();
            if (ResolverQueryBuilder.NORMALIZATION_Z_SCORE.equals(norm)) {
                combined = zScoreArithmeticMean(legHits, resolver);
            } else if (ResolverQueryBuilder.NORMALIZATION_L2.equals(norm)) {
                combined = l2ArithmeticMean(legHits, resolver);
            } else {
                combined = minMaxArithmeticMean(legHits, resolver);
            }
        } else {
            combined = rrf(legHits, resolver);
        }
        return toRankedDocs(combined, resolver.rankWindowSize());
    }

    /**
     * Rank-based Reciprocal Rank Fusion: score(d) = Σ weight_i / (rank_constant + rank_i(d) + 1).
     * <p>Per-leg {@code weights} (POC v2; mirrors ES 9.2 weighted RRF, {@code rrf_score = Σ weight_i × rrf_score_i})
     * multiply each leg's reciprocal-rank contribution, so a trusted leg can be biased WITHOUT the score-scale
     * fragility of score normalization — RRF stays rank-based (immune to leg-scale mismatch). Empty weights =>
     * unweighted (all 1.0), identical to plain RRF.
     * <p>Note: RRF stays on the coordinator (global-top-K) collection path — {@code isPerShardCollection()} is
     * false for RRF — so each {@code legHits[legIndex]} is a single globally-merged, rank-ordered leg result and
     * the array index is the doc's rank. RRF is rank-based and already at hybrid parity, so it does not need the
     * per-shard pool.
     */
    private static Map<String, Float> rrf(SearchHit[][] legHits, ResolverQueryBuilder resolver) {
        Map<String, Float> scores = new LinkedHashMap<>();
        for (int legIndex = 0; legIndex < legHits.length; legIndex++) {
            SearchHit[] hits = legHits[legIndex];
            float weight = weightForLeg(resolver.weights(), legIndex);
            for (int rank = 0; rank < hits.length; rank++) {
                String id = hits[rank].getId();
                if (id == null) {
                    continue;
                }
                scores.merge(id, weight / (resolver.rankConstant() + rank + 1), Float::sum);
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

    /**
     * POC v2 adaptive-fusion #1 — DBSF-style per-query z-score normalization + (weighted) arithmetic mean. Per leg,
     * compute the mean {@code mu} and sample std {@code sigma} of that leg's returned raw scores (this query's
     * distribution, nothing global/offline), then remap each score to {@code (s - (mu - 3*sigma)) / (6*sigma)} — a
     * linear rescaling of the z-score onto the +/-3-sigma span, clamped to [0,1]. Per doc, the combined score is the
     * weighted mean over ALL legs (a non-matched leg contributes 0), identical in shape to {@link #minMaxArithmeticMean}
     * so weighting/denominator semantics match. This is exactly Qdrant's Distribution-Based Score Fusion, adapted to
     * the resolver: unsupervised, label-free, computed from the score lists already in memory. Motivation: unlike
     * min_max (whose range is set by a single outlier), z_score adapts to each query's per-leg score SPREAD, so a leg
     * whose scores are tightly clustered vs one that is spread out are normalized on their own terms per query.
     */
    private static Map<String, Float> zScoreArithmeticMean(SearchHit[][] legHits, ResolverQueryBuilder resolver) {
        float totalWeight = 0.0f;
        for (int legIndex = 0; legIndex < legHits.length; legIndex++) {
            totalWeight += weightForLeg(resolver.weights(), legIndex);
        }
        Map<String, Float> weightedSum = new LinkedHashMap<>(); // id -> Σ weight_leg * normalized_leg
        for (int legIndex = 0; legIndex < legHits.length; legIndex++) {
            SearchHit[] hits = legHits[legIndex];
            // Mean and sample standard deviation of this leg's returned scores (the per-query distribution).
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
            // Sample std (n-1). With < 2 points there is no spread → the normalizer is degenerate (handled below).
            double std = n < 2 ? 0.0 : Math.sqrt(sumSq / (n - 1));
            float weight = weightForLeg(resolver.weights(), legIndex);
            for (SearchHit hit : hits) {
                String id = hit.getId();
                if (id == null) {
                    continue;
                }
                weightedSum.merge(id, weight * normalizeZScore(hit.getScore(), mean, std), Float::sum);
            }
        }
        Map<String, Float> combined = new LinkedHashMap<>();
        for (Map.Entry<String, Float> e : weightedSum.entrySet()) {
            combined.put(e.getKey(), totalWeight == 0.0f ? 0.0f : e.getValue() / totalWeight);
        }
        return combined;
    }

    /** Map a raw score onto [0,1] via DBSF's z-score rescaling: {@code (s - (mu - 3*sigma)) / (6*sigma)}, clamped.
     *  Degenerate leg (sigma == 0: identical or single score) → 0.5, matching Qdrant DBSF. The 0.001 floor mirrors
     *  {@link #normalizeMinMax}: a matched doc that lands at the low extreme must not read as a non-match (0). */
    private static float normalizeZScore(float score, double mean, double std) {
        if (std == 0.0) {
            return 0.5f; // no spread — every score maps to the distribution center (DBSF convention)
        }
        double lower = mean - 3.0 * std;
        double normalized = (score - lower) / (6.0 * std);
        if (normalized <= 0.0) {
            return 0.001f; // floor: matched-at-or-below-lower-extreme != not-matched(0)
        }
        return normalized >= 1.0 ? 1.0f : (float) normalized;
    }

    /**
     * POC v2 — L2 normalization + (weighted) arithmetic mean. Per leg, divide each raw {@code _score} by the leg's
     * L2 norm {@code sqrt(Σ s_i^2)} over its returned scores, then combine as the weighted mean over ALL legs (a
     * non-matched leg contributes 0). Same weighted-mean skeleton as {@link #minMaxArithmeticMean}/{@link #zScoreArithmeticMean}
     * so weighting/denominator semantics match. Mirrors the OpenSearch hybrid processor's {@code L2ScoreNormalizationTechnique}
     * (and ES {@code l2_norm}): magnitude-preserving (unlike range/rank normalizers), norm==0 leg → 0.
     */
    private static Map<String, Float> l2ArithmeticMean(SearchHit[][] legHits, ResolverQueryBuilder resolver) {
        float totalWeight = 0.0f;
        for (int legIndex = 0; legIndex < legHits.length; legIndex++) {
            totalWeight += weightForLeg(resolver.weights(), legIndex);
        }
        Map<String, Float> weightedSum = new LinkedHashMap<>(); // id -> Σ weight_leg * normalized_leg
        for (int legIndex = 0; legIndex < legHits.length; legIndex++) {
            SearchHit[] hits = legHits[legIndex];
            double sumSq = 0.0;
            for (SearchHit hit : hits) {
                sumSq += (double) hit.getScore() * hit.getScore();
            }
            float norm = (float) Math.sqrt(sumSq);
            float weight = weightForLeg(resolver.weights(), legIndex);
            for (SearchHit hit : hits) {
                String id = hit.getId();
                if (id == null) {
                    continue;
                }
                weightedSum.merge(id, weight * normalizeL2(hit.getScore(), norm), Float::sum);
            }
        }
        Map<String, Float> combined = new LinkedHashMap<>();
        for (Map.Entry<String, Float> e : weightedSum.entrySet()) {
            combined.put(e.getKey(), totalWeight == 0.0f ? 0.0f : e.getValue() / totalWeight);
        }
        return combined;
    }

    /** L2: score / ||scores||_2 for the leg. Zero norm (all-zero/empty leg) → 0, mirroring the hybrid processor. */
    private static float normalizeL2(float score, float l2Norm) {
        return l2Norm == 0.0f ? 0.0f : score / l2Norm;
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
