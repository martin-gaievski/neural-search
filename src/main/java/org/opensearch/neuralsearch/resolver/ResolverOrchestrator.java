/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.resolver;

import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Shared coordinator-level orchestration for the resolver, independent of the interception point.
 * Used by both {@link ResolverProcessor} (search pipeline) and {@link ResolverActionFilter}
 * (pipeline-free). Builds the parallel-leg MultiSearch and applies coordinator RRF by rewriting the
 * search source into a {@link RankDocsQueryBuilder} (Top + conditional Tail).
 */
public final class ResolverOrchestrator {

    private ResolverOrchestrator() {}

    /** Build one independent search per leg, to be fired together as a parallel MultiSearch. */
    public static MultiSearchRequest buildLegMultiSearch(SearchRequest request, ResolverQueryBuilder resolver) {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        for (QueryBuilder leg : resolver.queries()) {
            SearchSourceBuilder legSource = new SearchSourceBuilder().query(leg)
                .size(resolver.rankWindowSize())
                .from(0)
                .fetchSource(false)
                .trackTotalHits(false);
            multiSearchRequest.add(new SearchRequest(request.indices()).indicesOptions(request.indicesOptions()).source(legSource));
        }
        return multiSearchRequest;
    }

    /** Compute coordinator-level RRF over the per-leg responses and rewrite {@code source.query()}
     *  into a {@link RankDocsQueryBuilder} (Top ranked docs + conditional Tail). */
    public static void applyFusedResults(
        SearchSourceBuilder source,
        MultiSearchResponse multiSearchResponse,
        ResolverQueryBuilder resolver
    ) {
        MultiSearchResponse.Item[] items = multiSearchResponse.getResponses();

        Map<String, Float> rrfScores = new LinkedHashMap<>();
        for (int legIndex = 0; legIndex < items.length; legIndex++) {
            MultiSearchResponse.Item item = items[legIndex];
            if (item.isFailure()) {
                throw new IllegalStateException(
                    String.format(Locale.ROOT, "[resolver] sub-query %d failed: %s", legIndex, item.getFailureMessage()),
                    item.getFailure()
                );
            }
            SearchResponse legResponse = item.getResponse();
            SearchHit[] hits = legResponse.getHits().getHits();
            for (int rank = 0; rank < hits.length; rank++) {
                String id = hits[rank].getId();
                if (id == null) {
                    continue;
                }
                float contribution = 1.0f / (resolver.rankConstant() + rank + 1);
                rrfScores.merge(id, contribution, Float::sum);
            }
        }

        if (rrfScores.isEmpty()) {
            source.query(new MatchNoneQueryBuilder());
            return;
        }

        List<Map.Entry<String, Float>> ranked = new ArrayList<>(rrfScores.entrySet());
        ranked.sort(Comparator.<Map.Entry<String, Float>>comparingDouble(e -> -e.getValue()).thenComparing(Map.Entry::getKey));
        if (ranked.size() > resolver.rankWindowSize()) {
            ranked = ranked.subList(0, resolver.rankWindowSize());
        }

        String[] rankedIds = new String[ranked.size()];
        float[] rankedScores = new float[ranked.size()];
        for (int i = 0; i < ranked.size(); i++) {
            rankedIds[i] = ranked.get(i).getKey();
            rankedScores[i] = ranked.get(i).getValue();
        }

        List<QueryBuilder> tail = needsTail(source, rankedIds.length) ? resolver.queries() : List.of();
        source.query(new RankDocsQueryBuilder(rankedIds, rankedScores, tail));
    }

    /** The Tail (re-running the source legs as a filter) is only needed for aggregations / explain /
     *  highlight / accurate total hits; plain top-K skips it. */
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
}
