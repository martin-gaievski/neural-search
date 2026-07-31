/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.Map;
import java.util.Objects;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.ProcessorGenerationContext;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.search.pipeline.SystemGeneratedProcessor;

import lombok.extern.log4j.Log4j2;

/**
 * System-generated response processor that replaces a fused-mode hybrid query's aggregations (and {@code total_hits})
 * with the values computed by the coordinator's <b>aggregation leg</b>.
 *
 * <p><b>Why.</b> In fused mode the hybrid self-erases into {@code Top + Tail}. The Tail reconstructs a dense
 * {@code knn}/{@code neural} leg from the ids that leg returned — the coordinator's global
 * top-{@code rank_window_size} — whereas classic hybrid runs the sub-query in place on every shard and therefore
 * aggregates the <b>per-shard-{@code k}</b> union. On a multi-shard index those differ, so Tail-based aggregations
 * silently undercount KNN-matched documents. A {@code min_score > 0} request undercounts too, because the score-0 Tail
 * docs are dropped by {@code MinimumScoreCollector} before aggregation.
 *
 * <p><b>Fix.</b> {@code HybridFusionOrchestrator#buildAggregationLegSource} adds one extra leg to the existing round-1
 * MultiSearch: {@code size:0}, non-scoring, query = {@code bool{filter: bool{should:[legs...]}}}, carrying the request's
 * aggregations. Because the legs execute in place per shard there, its aggregation match set is the true leg union —
 * matching classic hybrid. The rewrite stashes the result in the request-scoped {@link PipelineProcessingContext} and
 * this processor swaps it into the response.
 *
 * <p>Mirrors {@link HybridFusedProfileResponseProcessor}: same stash-then-swap channel, same
 * {@code POST_USER_DEFINED} stage, and it self-skips when the context attribute is absent (so a failed or absent
 * aggregation leg degrades gracefully to the Tail-based aggregations).
 */
@Log4j2
public class HybridFusedAggregationsResponseProcessor implements SearchResponseProcessor, SystemGeneratedProcessor {

    public static final String TYPE = "hybrid_fused_aggregations";

    /** Attribute key under which the fused-mode rewrite stashes the aggregation-leg aggregations. */
    public static final String AGG_LEG_AGGS_CONTEXT_KEY = "hybrid_fused_agg_leg_aggregations";

    /** Attribute key under which the fused-mode rewrite stashes the aggregation-leg total hits. */
    public static final String AGG_LEG_TOTAL_HITS_CONTEXT_KEY = "hybrid_fused_agg_leg_total_hits";

    private static final String DEFAULT_TAG = "system-generated-hybrid-fused-aggregations";
    private static final String DEFAULT_DESCRIPTION =
        "Replaces fused-mode hybrid aggregations/total_hits with the aggregation-leg (true leg-union) values";

    private final boolean ignoreFailure;

    public HybridFusedAggregationsResponseProcessor(boolean ignoreFailure) {
        this.ignoreFailure = ignoreFailure;
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) {
        return processResponse(request, response, null);
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response, PipelineProcessingContext requestContext) {
        if (Objects.isNull(requestContext)) {
            return response;
        }
        Object stashedAggs = requestContext.getAttribute(AGG_LEG_AGGS_CONTEXT_KEY);
        if ((stashedAggs instanceof InternalAggregations) == false) {
            return response;
        }
        InternalAggregations aggLegAggregations = (InternalAggregations) stashedAggs;

        // total_hits: the aggregation leg is the only place the true per-shard leg union is materialized, so its count
        // is the correct one. Keep the response's own hits (ranking comes from the fused Top window, untouched).
        SearchHits responseHits = response.getHits();
        SearchHits hits = responseHits;
        Object stashedTotal = requestContext.getAttribute(AGG_LEG_TOTAL_HITS_CONTEXT_KEY);
        if (stashedTotal instanceof TotalHits) {
            hits = new SearchHits(responseHits.getHits(), (TotalHits) stashedTotal, responseHits.getMaxScore());
        }

        InternalSearchResponse internalResponse = new InternalSearchResponse(
            hits,
            aggLegAggregations,
            response.getSuggest(),
            response.getProfileResults() == null
                ? null
                : new org.opensearch.search.profile.SearchProfileShardResults(response.getProfileResults()),
            response.isTimedOut(),
            response.isTerminatedEarly(),
            response.getNumReducePhases()
        );
        return new SearchResponse(
            internalResponse,
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

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public String getTag() {
        return DEFAULT_TAG;
    }

    @Override
    public String getDescription() {
        return DEFAULT_DESCRIPTION;
    }

    @Override
    public boolean isIgnoreFailure() {
        return ignoreFailure;
    }

    @Override
    public ExecutionStage getExecutionStage() {
        // After user-defined response processors, so any user reshaping happens first (same as the profile processor).
        return ExecutionStage.POST_USER_DEFINED;
    }

    /**
     * Generates the processor only for a fused-mode hybrid query that carries aggregations — the exact case where the
     * rewrite adds an aggregation leg and stashes its result. Self-skips anyway if the attribute is absent.
     */
    public static class Factory implements SystemGeneratedProcessor.SystemGeneratedFactory<SearchResponseProcessor> {

        @Override
        public boolean shouldGenerate(ProcessorGenerationContext context) {
            SearchRequest request = context.searchRequest();
            if (Objects.isNull(request) || Objects.isNull(request.source())) {
                return false;
            }
            SearchSourceBuilder source = request.source();
            if (Objects.isNull(source.aggregations()) || Objects.isNull(source.query())) {
                return false;
            }
            return source.query() instanceof HybridQueryBuilder
                && ((HybridQueryBuilder) source.query()).mode() == HybridQueryBuilder.Mode.FUSED;
        }

        @Override
        public SearchResponseProcessor create(
            Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            Processor.PipelineContext pipelineContext
        ) {
            return new HybridFusedAggregationsResponseProcessor(ignoreFailure);
        }
    }
}
