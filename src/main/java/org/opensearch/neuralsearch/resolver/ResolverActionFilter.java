/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.resolver;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.client.Client;

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
            if (source != null && source.query() instanceof ResolverQueryBuilder resolver) {
                Client client = clientSupplier.get();
                client.multiSearch(
                    ResolverOrchestrator.buildLegMultiSearch(searchRequest, resolver),
                    ActionListener.wrap(multiSearchResponse -> {
                        try {
                            // Rewrites searchRequest.source().query() in place; proceed with the mutated request.
                            ResolverOrchestrator.applyFusedResults(source, multiSearchResponse, resolver);
                            chain.proceed(task, action, request, listener);
                        } catch (Exception e) {
                            listener.onFailure(e);
                        }
                    }, listener::onFailure)
                );
                return; // defer: proceed only after fusion completes
            }
        }
        chain.proceed(task, action, request, listener);
    }
}
