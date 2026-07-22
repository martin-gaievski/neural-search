/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.ProcessorGenerationContext;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.search.pipeline.SystemGeneratedProcessor;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.profile.SearchProfileShardResults;

import lombok.extern.log4j.Log4j2;

/**
 * System-generated response processor that surfaces per-sub-query profiling for a {@code hybrid} query in
 * {@code mode: "fused"}.
 *
 * <p>In fused mode the coordinator self-erases the hybrid query into a standard {@code constant_score(ids)} + Tail
 * query, so the profiler on the outer request only measures that fusion plumbing, not the sub-query scoring. The real
 * per-sub-query execution happens in the coordinator leg MultiSearch. When profiling is on, those legs are profiled
 * (see {@code HybridQueryBuilder#doRewriteFused}) and their profiles are stashed in the request-scoped
 * {@link PipelineProcessingContext} under {@link #LEG_PROFILES_CONTEXT_KEY}. This processor reads them and merges them
 * into the response {@code profile} section, so the response carries both the outer (fusion) profile and the per-leg
 * sub-query profiles (namespaced {@code [fused_leg_N]...}).
 */
@Log4j2
public class HybridFusedProfileResponseProcessor implements SearchResponseProcessor, SystemGeneratedProcessor {

    public static final String TYPE = "hybrid_fused_profile";

    /** Attribute key under which the fused-mode query rewrite stashes the per-leg profiles for this processor. */
    public static final String LEG_PROFILES_CONTEXT_KEY = "hybrid_fused_leg_profiles";

    private static final String DEFAULT_TAG = "system-generated-hybrid-fused-profile";
    private static final String DEFAULT_DESCRIPTION = "Merges fused-mode hybrid sub-query (leg) profiles into the response profile";

    private final boolean ignoreFailure;

    public HybridFusedProfileResponseProcessor(boolean ignoreFailure) {
        this.ignoreFailure = ignoreFailure;
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) {
        return processResponse(request, response, null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public SearchResponse processResponse(SearchRequest request, SearchResponse response, PipelineProcessingContext requestContext) {
        if (Objects.isNull(requestContext) || Objects.isNull(requestContext.getAttribute(LEG_PROFILES_CONTEXT_KEY))) {
            return response;
        }
        Object stashed = requestContext.getAttribute(LEG_PROFILES_CONTEXT_KEY);
        if ((stashed instanceof Map) == false) {
            return response;
        }
        Map<String, ProfileShardResult> legProfiles = (Map<String, ProfileShardResult>) stashed;
        if (legProfiles.isEmpty()) {
            return response;
        }

        // Merge the outer (self-erased query) profile — keyed by real shard id — with the per-leg profiles, whose keys
        // are already namespaced [fused_leg_N] so they never collide with the outer keys.
        Map<String, ProfileShardResult> merged = new LinkedHashMap<>();
        Map<String, ProfileShardResult> outer = response.getProfileResults();
        if (Objects.nonNull(outer)) {
            merged.putAll(outer);
        }
        merged.putAll(legProfiles);

        InternalSearchResponse internalResponse = new InternalSearchResponse(
            response.getHits(),
            (InternalAggregations) response.getAggregations(),
            response.getSuggest(),
            new SearchProfileShardResults(merged),
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
        // Run after user-defined response processors so any user reshaping of the response happens first.
        return ExecutionStage.POST_USER_DEFINED;
    }

    /**
     * Generates the processor only for a top-level fused-mode hybrid query with profiling enabled — the exact case
     * where the query rewrite stashes leg profiles. For any other request the processor is not generated (and even if
     * it were, it self-skips when the context attribute is absent).
     */
    public static class Factory implements SystemGeneratedProcessor.SystemGeneratedFactory<SearchResponseProcessor> {

        @Override
        public boolean shouldGenerate(ProcessorGenerationContext context) {
            SearchRequest request = context.searchRequest();
            if (Objects.isNull(request) || Objects.isNull(request.source())) {
                return false;
            }
            SearchSourceBuilder source = request.source();
            if (source.profile() == false || Objects.isNull(source.query())) {
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
            return new HybridFusedProfileResponseProcessor(ignoreFailure);
        }
    }
}
