/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.resolver;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.Arrays;
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

    /** Build one independent search per leg, to be fired together as a parallel MultiSearch. */
    public static MultiSearchRequest buildLegMultiSearch(SearchRequest request, ResolverQueryBuilder resolver) {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        int trackTotalHitsUpTo = trackTotalHitsCap(request.source());
        for (QueryBuilder leg : resolver.queries()) {
            multiSearchRequest.add(legSearch(request, leg, List.of(), resolver.rankWindowSize(), trackTotalHitsUpTo));
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
        ResolverQueryBuilder resolver
    ) {
        MultiSearchResponse.Item[] items = multiSearchResponse.getResponses();
        RankedDocs ranked = computeRankedDocs(items, resolver);
        if (ranked.ids.length == 0) {
            source.query(new MatchNoneQueryBuilder());
            return null;
        }
        boolean topOnly;
        TotalHits patchTotal = null;
        if (needsExecutionTail(source)) {
            topOnly = false; // aggregations / explain / profile / highlight need the full match set IN the query
        } else if (wantsTotalsBeyondWindow(source, ranked.ids.length)) {
            patchTotal = legUnionTotalHits(items); // derive the union total from the legs (no re-run) when exact/capped
            topOnly = patchTotal != null;          // else fall back to the Tail for an exact count
        } else {
            topOnly = true; // track_total_hits:false -> plain top-K, no Tail
        }
        source.query(new RankDocsQueryBuilder(ranked.ids, ranked.scores, topOnly ? List.of() : resolver.queries()));
        return patchTotal;
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
            MultiSearchResponse.Item[] slice = Arrays.copyOfRange(items, offset, offset + legCount);
            offset += legCount;
            RankedDocs ranked = computeRankedDocs(slice, mc.marker());
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

    private static RankedDocs computeRankedDocs(MultiSearchResponse.Item[] items, ResolverQueryBuilder resolver) {
        Map<String, Float> combined = ResolverQueryBuilder.TECHNIQUE_ARITHMETIC_MEAN.equals(resolver.technique())
            ? minMaxArithmeticMean(items, resolver)
            : rrf(items, resolver);
        return toRankedDocs(combined, resolver.rankWindowSize());
    }

    /** Rank-based Reciprocal Rank Fusion: score(d) = Σ 1 / (rank_constant + rank_i(d) + 1). */
    private static Map<String, Float> rrf(MultiSearchResponse.Item[] items, ResolverQueryBuilder resolver) {
        Map<String, Float> scores = new LinkedHashMap<>();
        for (int legIndex = 0; legIndex < items.length; legIndex++) {
            SearchHit[] hits = hitsOrThrow(items[legIndex], legIndex);
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
     * min-max normalized over the returned window; per doc, the combined score is the weighted mean over
     * ALL legs (a non-matched leg contributes 0). Mirrors hybrid's {@code MinMaxScoreNormalizationTechnique}
     * (degenerate min==max -> 1.0; normalized 0 -> 0.001 floor so a matched doc is never confused with a
     * non-match) and {@code ArithmeticMeanScoreCombinationTechnique} (denominator = sum of ALL leg weights,
     * so a doc strong in both legs outranks one strong in only a single leg).
     */
    private static Map<String, Float> minMaxArithmeticMean(MultiSearchResponse.Item[] items, ResolverQueryBuilder resolver) {
        float totalWeight = 0.0f;
        for (int legIndex = 0; legIndex < items.length; legIndex++) {
            totalWeight += weightForLeg(resolver.weights(), legIndex);
        }
        Map<String, Float> weightedSum = new LinkedHashMap<>(); // id -> Σ weight_leg * normalized_leg
        for (int legIndex = 0; legIndex < items.length; legIndex++) {
            SearchHit[] hits = hitsOrThrow(items[legIndex], legIndex);
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
