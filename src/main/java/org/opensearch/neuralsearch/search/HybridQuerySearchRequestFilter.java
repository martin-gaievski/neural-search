/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import org.opensearch.core.action.ActionListener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.search.BooleanClause;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.search.profile.FusedLegProfileMerger;
import org.opensearch.neuralsearch.util.HybridQueryUtil;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;

import lombok.extern.log4j.Log4j2;

/**
 * An ActionFilter that automatically disables batched reduction for hybrid queries, and attaches per-leg profiling to
 * fused hybrids.
 *
 * This filter intercepts all search requests and checks if they contain a hybrid query.
 * If a hybrid query is detected with search_type=dfs_query_then_fetch, the request is rejected.
 * If a hybrid query is detected, it unconditionally sets batchedReduceSize to Integer.MAX_VALUE
 * to disable batched reduction, regardless of any user-specified value.
 *
 * This prevents the "topDocs already consumed" error that occurs when:
 * 1. Hybrid query is executed
 * 2. Batched reduction triggers (QueryPhaseResultConsumer.consume)
 * 3. TopDocs are consumed before NormalizationProcessor can access them
 *
 * Note: The batched_reduce_size parameter is not honored for hybrid queries because
 * batched reduction is fundamentally incompatible with hybrid query processing.
 * The NormalizationProcessor requires access to all shard results simultaneously
 * to perform score normalization and combination.
 *
 * This filter works transparently without any pipeline or query configuration.
 *
 * The same seam carries fused-mode per-leg profiling: a fused hybrid runs its legs as sub-searches during the
 * coordinator rewrite, and this is the one place that both sees the query as the user submitted it and still owns the
 * response listener, so it can hand the legs somewhere to publish their profile trees and merge those trees into the
 * response on the way out. See {@link #attachLegProfiling}.
 *
 */
@Log4j2
public class HybridQuerySearchRequestFilter implements ActionFilter {

    /**
     * Value to disable batched reduction.
     * Setting batchedReduceSize to Integer.MAX_VALUE effectively disables batched reduction
     * since the buffer will never reach this threshold.
     */
    private static final int DISABLE_BATCHED_REDUCE = Integer.MAX_VALUE;

    /**
     * Order of this filter in the filter chain.
     * Lower values execute first. We use 0 to ensure this runs early.
     */
    @Override
    public int order() {
        return 0;
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
        // only intercept search actions
        if (SearchAction.NAME.equals(action) && request instanceof SearchRequest) {
            SearchRequest searchRequest = (SearchRequest) request;

            // These workarounds apply to CLASSIC hybrid only. Resolver (fused) mode self-erases at the coordinator into
            // a standard bool query and never produces the sentinel CompoundTopDocs format, so it is DFS-compatible and
            // safe with batched reduction — the mode-aware check below skips it.
            if (containsClassicHybridQuery(searchRequest)) {
                if (searchRequest.searchType() == SearchType.DFS_QUERY_THEN_FETCH) {
                    listener.onFailure(new IllegalArgumentException(HybridQueryUtil.HYBRID_QUERY_DFS_SEARCH_TYPE_NOT_SUPPORTED_MESSAGE));
                    return;
                }
                if (searchRequest.getBatchedReduceSize() != DISABLE_BATCHED_REDUCE) {
                    log.debug(
                        String.format(
                            Locale.ROOT,
                            "Hybrid query detected, disabling batched reduction to prevent 'topDocs already consumed' error. "
                                + "Original batched_reduce_size: %d, new value: %d. "
                                + "Note: batched_reduce_size is not honored for hybrid queries.",
                            searchRequest.getBatchedReduceSize(),
                            DISABLE_BATCHED_REDUCE
                        )
                    );
                    searchRequest.setBatchedReduceSize(DISABLE_BATCHED_REDUCE);
                }
            }
        }
        chain.proceed(task, action, request, attachLegProfiling(action, request, listener));
    }

    /**
     * The response listener to proceed with: when a request asks for {@code profile} and carries at least one fused
     * {@code hybrid}, hand every such hybrid consumers writing into one request-scoped merger — one for its legs' profile
     * trees, one for what the coordinator itself spent fanning them out and fusing them — and wrap the listener so what the
     * merger collected ends up in the response's profile section. Otherwise the caller's own listener, unchanged.
     *
     * <p>No side channel is needed: the {@link HybridQueryBuilder} instances reachable from {@code source().query()} here
     * are the same instances rewrite round 1 runs on, so attaching to them directly is enough. Nothing happens when the
     * request is not profiled, and nothing happens to the response when nothing was ever collected.
     *
     * <p>This runs before search request processors do, so it reports the legs of the hybrids the request carries <b>as
     * submitted, with {@code profile} as submitted</b>. A pipeline that replaces the query — or changes {@code profile} —
     * is outside that boundary: the request still answers correctly, it simply carries no leg detail.
     */
    @SuppressWarnings("unchecked")
    private <Request extends ActionRequest, Response extends ActionResponse> ActionListener<Response> attachLegProfiling(
        final String action,
        final Request request,
        final ActionListener<Response> listener
    ) {
        if (SearchAction.NAME.equals(action) == false || (request instanceof SearchRequest) == false) {
            return listener;
        }
        SearchSourceBuilder source = ((SearchRequest) request).source();
        if (Objects.isNull(source) || source.profile() == false || Objects.isNull(source.query())) {
            return listener;
        }
        FusedHybridFinder finder = new FusedHybridFinder();
        // accept, not visit: the finder has to be the only thing driving the descent, or a builder that recurses in its own
        // visit would carry the walk past the fused hybrid the finder just stopped at.
        finder.accept(source.query());
        if (finder.found.isEmpty()) {
            return listener;
        }
        FusedLegProfileMerger legProfileMerger = new FusedLegProfileMerger();
        for (int i = 0; i < finder.found.size(); i++) {
            // One label per hybrid, shared by both consumers: the legs' entries and the coordinator's entry for the same
            // hybrid have to name it the same way for the profile section to read as one query.
            String hybridLabel = String.format(Locale.ROOT, "hybrid_%d", i);
            finder.found.get(i).legProfileConsumer(legProfileMerger.forHybrid(hybridLabel));
            finder.found.get(i).fusionTimingConsumer(legProfileMerger.forHybridTiming(hybridLabel));
        }
        return ActionListener.wrap(response -> {
            if ((response instanceof SearchResponse) == false) {
                listener.onResponse(response);
                return;
            }
            listener.onResponse((Response) legProfileMerger.getMergedResponse((SearchResponse) response));
        }, listener::onFailure);
    }

    /**
     * Collects the fused {@code hybrid} queries this request fans out itself, in walk order. Descends for itself and keys
     * by identity, because core's {@code visit} implementations disagree about recursing — see
     * {@code HybridQueryBuilder.LegSearchCounter}, which uses the same shape for the fan-out ceiling.
     *
     * <p>The walk stops at a fused hybrid rather than descending into it. A fused hybrid below another one is a leg of
     * that one, and a leg sub-search is a search action of its own: it re-enters this filter, gets its own merger, and
     * labels its legs itself, so the outer request must not number it too. That is what makes {@code hybrid_N} mean "the
     * Nth fused hybrid this request fans out" — a contiguous numbering over siblings, with nesting expressed by the label
     * path a leg's entries carry rather than by the index. Classic hybrids are walked through, since they fan nothing out
     * and may still contain a fused one. Stopping is only sound while this visitor owns the whole descent, so it has to be
     * entered by handing the query to {@link #accept} — {@code query.visit(finder)} would let the query's own {@code visit}
     * carry on into its children after {@code accept} declined to.
     */
    private static final class FusedHybridFinder implements QueryBuilderVisitor {
        private final Set<QueryBuilder> entered = Collections.newSetFromMap(new IdentityHashMap<>());
        private final List<HybridQueryBuilder> found = new ArrayList<>();

        @Override
        public void accept(final QueryBuilder queryBuilder) {
            if (entered.add(queryBuilder) == false) {
                return;
            }
            if (queryBuilder instanceof HybridQueryBuilder && Objects.nonNull(((HybridQueryBuilder) queryBuilder).fusion())) {
                found.add((HybridQueryBuilder) queryBuilder);
                return;
            }
            queryBuilder.visit(this);
        }

        @Override
        public QueryBuilderVisitor getChildVisitor(final BooleanClause.Occur occur) {
            return this;
        }
    }

    /**
     * Check if the search request's top-level query is a CLASSIC hybrid query (i.e. a {@link HybridQueryBuilder} with no
     * {@code fusion} block). Resolver (fused) mode is intentionally excluded — it needs neither the DFS rejection nor
     * the batched-reduce disable, since it self-erases into a standard query.
     *
     * @param searchRequest the search request to check
     * @return true if the request's top-level query is a classic (non-fused) hybrid query
     */
    private boolean containsClassicHybridQuery(SearchRequest searchRequest) {
        if (Objects.isNull(searchRequest.source())) {
            return false;
        }

        QueryBuilder query = searchRequest.source().query();
        if ((query instanceof HybridQueryBuilder) == false) {
            return false;
        }

        // Fused mode self-erases at the coordinator; the classic-only workarounds must not apply to it.
        return Objects.isNull(((HybridQueryBuilder) query).fusion());
    }
}
