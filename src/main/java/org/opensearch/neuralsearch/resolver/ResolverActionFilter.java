/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.resolver;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.client.Client;

import java.util.List;
import java.util.function.Supplier;

/**
 * Pipeline-free entry point for the Resolver framework.
 *
 * <p>An {@link ActionFilter} that intercepts every search on the coordinator, detects a top-level
 * {@link ResolverQueryBuilder}, fires the legs as a parallel MultiSearch, fuses with coordinator RRF,
 * and rewrites the request into a standard query BEFORE the query phase — all with <b>no search
 * pipeline</b>. So {@code POST /index/_search} with a {@code resolver} query works on its own, with
 * no {@code ?search_pipeline=} and no pipeline object to create or manage (a win for managed /
 * serverless deployments).
 *
 * <p>Mirrors {@code HybridQuerySearchRequestFilter} (documented as working "transparently without any
 * pipeline"), but performs ASYNC orchestration: it fires the MultiSearch and defers
 * {@code chain.proceed(...)} to the response callback.
 *
 * <p>Recursion-safe: the leg sub-searches and the rewritten {@link RankDocsQueryBuilder} are not
 * {@code resolver} markers, so they pass through untouched.
 */
public class ResolverActionFilter implements ActionFilter {

    private final Supplier<Client> clientSupplier;

    public ResolverActionFilter(Supplier<Client> clientSupplier) {
        this.clientSupplier = clientSupplier;
    }

    @Override
    public int order() {
        // Run after security/auth filters (which use lower/negative order).
        return 10;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        if (SearchAction.NAME.equals(action) && request instanceof SearchRequest searchRequest) {
            SearchSourceBuilder source = searchRequest.source();
            if (source != null && source.query() != null) {
                QueryBuilder query = source.query();
                Client client = clientSupplier.get();

                // Case 1: top-level resolver — rewrite the whole query; patch total_hits from the legs when the Tail is skipped.
                if (query instanceof ResolverQueryBuilder resolver) {
                    // Compute the collection plan ONCE and thread it into both the build and the reduce, so the item
                    // layout the reduce reads back always matches what the build produced (no recompute race).
                    ResolverOrchestrator.CollectionPlan plan = ResolverOrchestrator.planCollection(searchRequest, resolver);

                    // Fast path: for plain top-K retrieval (no aggs/explain/highlight/sort/collapse/rescore/...), fire
                    // the legs with _source enabled and fabricate the response directly from the fused window — no
                    // second (stage-B) distributed search. Skipping stage B is the resolver's main latency lever.
                    if (ResolverOrchestrator.fastPathEligible(source, resolver)) {
                        client.multiSearch(
                            ResolverOrchestrator.buildLegMultiSearch(searchRequest, resolver, plan, true),
                            ActionListener.wrap(multiSearchResponse -> {
                                try {
                                    SearchResponse fabricated = ResolverOrchestrator.fabricateFastPathResponse(
                                        searchRequest,
                                        source,
                                        multiSearchResponse,
                                        resolver,
                                        plan
                                    );
                                    listener.onResponse((Response) fabricated);
                                } catch (Exception e) {
                                    listener.onFailure(e);
                                }
                            }, listener::onFailure)
                        );
                        return; // defer; no chain.proceed — the fabricated response IS the result
                    }

                    client.multiSearch(
                        ResolverOrchestrator.buildLegMultiSearch(searchRequest, resolver, plan),
                        ActionListener.wrap(multiSearchResponse -> {
                            try {
                                TotalHits patchedTotal = ResolverOrchestrator.applyFusedResults(
                                    source,
                                    multiSearchResponse,
                                    resolver,
                                    plan
                                );
                                ActionListener<Response> downstream = patchedTotal == null
                                    ? listener
                                    : patchTotalHits(listener, patchedTotal);
                                chain.proceed(task, action, request, downstream);
                            } catch (Exception e) {
                                listener.onFailure(e);
                            }
                        }, listener::onFailure)
                    );
                    return; // defer
                }

                // Case 2: one or more resolver markers nested inside a bool tree — resolve each in place.
                List<ResolverOrchestrator.MarkerContext> markers = ResolverOrchestrator.collectMarkers(query);
                if (markers.isEmpty() == false) {
                    client.multiSearch(
                        ResolverOrchestrator.buildMarkerMultiSearch(searchRequest, markers),
                        ActionListener.wrap(multiSearchResponse -> {
                            try {
                                source.query(
                                    ResolverOrchestrator.replaceMarkers(
                                        query,
                                        ResolverOrchestrator.resolveMarkers(markers, multiSearchResponse)
                                    )
                                );
                                chain.proceed(task, action, request, listener);
                            } catch (Exception e) {
                                listener.onFailure(e);
                            }
                        }, listener::onFailure)
                    );
                    return; // defer
                }
            }
        }
        chain.proceed(task, action, request, listener);
    }

    /** Wrap the listener to overwrite the response's total_hits with a leg-derived union count (Tail avoided). */
    @SuppressWarnings("unchecked")
    private static <Response extends ActionResponse> ActionListener<Response> patchTotalHits(
        ActionListener<Response> delegate,
        TotalHits total
    ) {
        return ActionListener.wrap(response -> {
            if (response instanceof SearchResponse searchResponse) {
                delegate.onResponse((Response) withTotalHits(searchResponse, total));
            } else {
                delegate.onResponse(response);
            }
        }, delegate::onFailure);
    }

    private static SearchResponse withTotalHits(SearchResponse response, TotalHits total) {
        SearchHits oldHits = response.getHits();
        SearchHits newHits = new SearchHits(
            oldHits.getHits(),
            total,
            oldHits.getMaxScore(),
            oldHits.getSortFields(),
            oldHits.getCollapseField(),
            oldHits.getCollapseValues()
        );
        InternalSearchResponse sections = new InternalSearchResponse(
            newHits,
            null, // aggregations — absent in the total-hits-only path (aggs force the Tail)
            response.getSuggest(),
            null, // profile — absent in the total-hits-only path
            response.isTimedOut(),
            response.isTerminatedEarly(),
            response.getNumReducePhases()
        );
        return new SearchResponse(
            sections,
            response.getScrollId(),
            response.getTotalShards(),
            response.getSuccessfulShards(),
            response.getSkippedShards(),
            response.getTook().millis(),
            response.getShardFailures(),
            response.getClusters(),
            response.pointInTimeId()
        );
    }
}
