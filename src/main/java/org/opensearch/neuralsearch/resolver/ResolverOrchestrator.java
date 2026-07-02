/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.resolver;

import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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
        for (QueryBuilder leg : resolver.queries()) {
            multiSearchRequest.add(legSearch(request, leg, List.of(), resolver.rankWindowSize()));
        }
        return multiSearchRequest;
    }

    /** Compute coordinator RRF and rewrite {@code source.query()} into a {@link RankDocsQueryBuilder}
     *  (Top + conditional Tail). */
    public static void applyFusedResults(
        SearchSourceBuilder source,
        MultiSearchResponse multiSearchResponse,
        ResolverQueryBuilder resolver
    ) {
        RankedDocs ranked = computeRankedDocs(multiSearchResponse.getResponses(), resolver);
        if (ranked.ids.length == 0) {
            source.query(new MatchNoneQueryBuilder());
            return;
        }
        List<QueryBuilder> tail = needsTail(source, ranked.ids.length) ? resolver.queries() : List.of();
        source.query(new RankDocsQueryBuilder(ranked.ids, ranked.scores, tail));
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
                multiSearchRequest.add(legSearch(request, leg, mc.pushDownFilters(), mc.marker().rankWindowSize()));
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
    private static SearchRequest legSearch(SearchRequest request, QueryBuilder leg, List<QueryBuilder> pushDownFilters, int size) {
        QueryBuilder legQuery = leg;
        if (pushDownFilters.isEmpty() == false) {
            BoolQueryBuilder constrained = new BoolQueryBuilder().must(leg);
            for (QueryBuilder f : pushDownFilters) {
                constrained.filter(f);
            }
            legQuery = constrained;
        }
        SearchSourceBuilder legSource = new SearchSourceBuilder().query(legQuery)
            .size(size)
            .from(0)
            .fetchSource(false)
            .trackTotalHits(false);
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

    /** The Tail (re-running the source legs as a filter) is only needed for aggregations / explain /
     *  highlight / accurate total hits; plain top-K skips it. Applies to the top-level path only. */
    private static boolean needsTail(SearchSourceBuilder source, int numRankedDocs) {
        if (source.aggregations() != null) {
            return true;
        }
        if (Boolean.TRUE.equals(source.explain())) {
            return true;
        }
        if (source.profile()) {
            return true;
        }
        if (source.highlighter() != null) {
            return true;
        }
        Integer trackTotalHitsUpTo = source.trackTotalHitsUpTo();
        return trackTotalHitsUpTo == null || trackTotalHitsUpTo > numRankedDocs;
    }

    private record RankedDocs(String[] ids, float[] scores) {
    }
}
