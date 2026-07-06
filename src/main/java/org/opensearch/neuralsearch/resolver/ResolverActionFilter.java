/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.resolver;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.client.Client;

import java.util.function.Supplier;

/**
 * Thin coordinator hook for the resolver's stage-B-free FAST PATH — the one thing a self-erasing
 * {@code QueryBuilder} rewrite cannot do.
 *
 * <p>The standard, nested, and per-shard resolver paths are re-homed onto
 * {@link ResolverQueryBuilder#doRewrite} (the {@code registerAsyncAction} self-erase); this filter is intentionally
 * NOT a general request interceptor. It fires ONLY when a search's top-level query is a {@link ResolverQueryBuilder}
 * AND the request is {@link ResolverOrchestrator#fastPathEligible fast-path eligible} (plain top-K, no
 * aggs/explain/highlight/sort/collapse/rescore/post_filter/search_after and no accurate-totals-beyond-window;
 * {@code min_score} IS supported — applied as a post-fusion threshold in the fabricated response, C1).
 * In that case it fires the legs with {@code _source} enabled and <b>fabricates the {@link SearchResponse} directly
 * from the fused window</b> — {@code listener.onResponse(...)} with NO {@code chain.proceed}, so the stage-B distributed
 * query phase is skipped (the below-hybrid-latency win). Skipping stage B requires sitting at the request/response
 * boundary: a rewrite can only return a {@code QueryBuilder}, never a {@code SearchResponse}, so this cannot be a
 * {@code doRewrite}.
 *
 * <p>Every other search — including a non-eligible resolver query — falls straight through to {@code chain.proceed};
 * {@link ResolverQueryBuilder#doRewrite} then orchestrates it at the coordinator rewrite. So there is no double
 * orchestration: an eligible request is fully served here and never reaches the rewrite; an ineligible one is untouched
 * here and served entirely by the rewrite.
 *
 * <p>Client is passed as a {@code Supplier<Client>} because {@code getActionFilters()} may run before
 * {@code createComponents}.
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
                // Fast path ONLY: fabricate the response from the fused window, skipping the stage-B search. Everything
                // else (standard / nested / per-shard, and any non-eligible top-level resolver) is handled by
                // ResolverQueryBuilder.doRewrite — fall straight through to chain.proceed so it can.
                // Scroll lives on the request (not the source), and the fabricated response cannot carry a scroll
                // cursor (scrollId would be null), so a scroll search must take the real query phase — gate it here.
                if (searchRequest.scroll() == null && ResolverOrchestrator.fastPathEligible(source, resolver)) {
                    Client client = clientSupplier.get();
                    ResolverOrchestrator.CollectionPlan plan = ResolverOrchestrator.planCollection(searchRequest, resolver);
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
            }
        }
        chain.proceed(task, action, request, listener);
    }
}
