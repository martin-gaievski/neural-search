/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.resolver;

import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.transport.client.Client;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * POC search request processor for the Resolver framework (Phase 1).
 *
 * <p>Detects a top-level {@link ResolverQueryBuilder} on the search request, then:
 * <ol>
 *   <li>fires each sub-query as an independent search via a single {@link MultiSearchRequest}
 *       (sub-queries run in parallel; each leg fans out to all shards and is globally merged);</li>
 *   <li>fuses the per-leg results with Reciprocal Rank Fusion at the coordinator
 *       ({@code score(d) = sum_i 1 / (k + rank_i(d))}) &mdash; this is the coordinator-level RRF
 *       that preserves multi-shard relevance quality;</li>
 *   <li>rewrites the request into a standard {@link RankDocsQueryBuilder} — a <b>Top</b> (ranked
 *       docs with RRF scores) + <b>Tail</b> (source-query matches as a non-scoring filter) query —
 *       and removes the resolver marker. The resolver has now "self-erased"; the query phase runs a
 *       standard query, so explain / profile / aggregations / total-hits work natively.</li>
 * </ol>
 *
 * <p>POC simplifications vs. the production design:
 * <ul>
 *   <li>fusion legs are matched back by {@code _id} (no point-in-time snapshot), so results can
 *       drift if the index changes between legs; production uses PIT + {@code _shard_doc};</li>
 *   <li>the Tail filter makes total-hits and aggregations cover the full match set, but explain
 *       shows the {@code constant_score} plus the source-query details rather than a per-leg RRF
 *       rank breakdown (a clean breakdown needs a custom Top query — a follow-up).</li>
 * </ul>
 */
public class ResolverProcessor extends AbstractProcessor implements SearchRequestProcessor {

    public static final String TYPE = "resolver";

    private final Client client;

    protected ResolverProcessor(String tag, String description, boolean ignoreFailure, Client client) {
        super(tag, description, ignoreFailure);
        this.client = client;
    }

    @Override
    public void processRequestAsync(
        SearchRequest request,
        PipelineProcessingContext requestContext,
        ActionListener<SearchRequest> requestListener
    ) {
        SearchSourceBuilder source = request.source();
        if (source == null || (source.query() instanceof ResolverQueryBuilder) == false) {
            // Not a resolver request - pass through unchanged.
            requestListener.onResponse(request);
            return;
        }

        final ResolverQueryBuilder resolver = (ResolverQueryBuilder) source.query();
        final List<QueryBuilder> legs = resolver.queries();
        final int rankWindowSize = resolver.rankWindowSize();
        final int rankConstant = resolver.rankConstant();

        // Build one independent search per leg, fired together as a MultiSearch (parallel).
        final MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        for (QueryBuilder leg : legs) {
            SearchSourceBuilder legSource = new SearchSourceBuilder().query(leg)
                .size(rankWindowSize)
                .from(0)
                .fetchSource(false)
                .trackTotalHits(false);
            SearchRequest legRequest = new SearchRequest(request.indices()).indicesOptions(request.indicesOptions()).source(legSource);
            multiSearchRequest.add(legRequest);
        }

        client.multiSearch(multiSearchRequest, ActionListener.wrap(multiSearchResponse -> {
            try {
                rewriteWithFusedResults(request, source, multiSearchResponse, rankConstant, rankWindowSize, legs);
                requestListener.onResponse(request);
            } catch (Exception e) {
                requestListener.onFailure(e);
            }
        }, requestListener::onFailure));
    }

    /**
     * Compute coordinator-level RRF over the per-leg responses and replace the request's query with
     * a standard query carrying the fused scores.
     */
    private void rewriteWithFusedResults(
        SearchRequest request,
        SearchSourceBuilder source,
        MultiSearchResponse multiSearchResponse,
        int rankConstant,
        int rankWindowSize,
        List<QueryBuilder> legs
    ) {
        MultiSearchResponse.Item[] items = multiSearchResponse.getResponses();

        // RRF accumulation keyed by document _id. LinkedHashMap for deterministic iteration.
        Map<String, Float> rrfScores = new LinkedHashMap<>();
        for (int legIndex = 0; legIndex < items.length; legIndex++) {
            MultiSearchResponse.Item item = items[legIndex];
            if (item.isFailure()) {
                throw new IllegalStateException(
                    String.format(Locale.ROOT, "[%s] sub-query %d failed: %s", TYPE, legIndex, item.getFailureMessage()),
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
                // 1-based rank: hits[0] is rank 1.
                float contribution = 1.0f / (rankConstant + rank + 1);
                rrfScores.merge(id, contribution, Float::sum);
            }
        }

        if (rrfScores.isEmpty()) {
            source.query(new MatchNoneQueryBuilder());
            return;
        }

        // Sort by fused score desc, tie-break by id for determinism, then truncate to the window.
        List<Map.Entry<String, Float>> ranked = new ArrayList<>(rrfScores.entrySet());
        ranked.sort(Comparator.<Map.Entry<String, Float>>comparingDouble(e -> -e.getValue()).thenComparing(Map.Entry::getKey));
        if (ranked.size() > rankWindowSize) {
            ranked = ranked.subList(0, rankWindowSize);
        }

        String[] rankedIds = new String[ranked.size()];
        float[] rankedScores = new float[ranked.size()];
        for (int i = 0; i < ranked.size(); i++) {
            rankedIds[i] = ranked.get(i).getKey();
            rankedScores[i] = ranked.get(i).getValue();
        }

        // RankDocsQuery: Top (ranked docs with RRF scores) + Tail (source-query matches as a
        // non-scoring filter) so total-hits and aggregations cover the full match set.
        source.query(new RankDocsQueryBuilder(rankedIds, rankedScores, legs));
    }

    @Override
    public SearchRequest processRequest(SearchRequest request) throws Exception {
        throw new UnsupportedOperationException("Use processRequestAsync for the resolver processor");
    }

    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * Factory for {@link ResolverProcessor}. Receives the node {@link Client} (captured by the
     * plugin in createComponents) so the processor can fire the MultiSearch.
     */
    public static class Factory implements Processor.Factory<SearchRequestProcessor> {
        private final Client client;

        public Factory(Client client) {
            this.client = client;
        }

        @Override
        public ResolverProcessor create(
            Map<String, Processor.Factory<SearchRequestProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            Processor.PipelineContext pipelineContext
        ) {
            return new ResolverProcessor(tag, description, ignoreFailure, client);
        }
    }
}
