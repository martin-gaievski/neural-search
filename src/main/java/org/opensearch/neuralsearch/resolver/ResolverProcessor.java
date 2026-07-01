/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.resolver;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.transport.client.Client;

import java.util.Map;

/**
 * Search-pipeline entry point for the Resolver framework (Phase 1). Detects a top-level
 * {@link ResolverQueryBuilder}, then delegates to {@link ResolverOrchestrator} to fire the legs as a
 * parallel MultiSearch, fuse with coordinator RRF, and rewrite the request into a standard
 * {@link RankDocsQueryBuilder} before the query phase.
 *
 * <p>NOTE: {@link ResolverActionFilter} performs the same orchestration WITHOUT requiring a search
 * pipeline. When that filter is registered it handles resolver queries first, so this processor is
 * optional (kept for the pipeline-based path / backward compatibility).
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
            requestListener.onResponse(request);
            return;
        }
        final ResolverQueryBuilder resolver = (ResolverQueryBuilder) source.query();
        client.multiSearch(ResolverOrchestrator.buildLegMultiSearch(request, resolver), ActionListener.wrap(multiSearchResponse -> {
            try {
                ResolverOrchestrator.applyFusedResults(source, multiSearchResponse, resolver);
                requestListener.onResponse(request);
            } catch (Exception e) {
                requestListener.onFailure(e);
            }
        }, requestListener::onFailure));
    }

    @Override
    public SearchRequest processRequest(SearchRequest request) throws Exception {
        throw new UnsupportedOperationException("Use processRequestAsync for the resolver processor");
    }

    @Override
    public String getType() {
        return TYPE;
    }

    /** Factory receiving the node {@link Client} (captured by the plugin in createComponents). */
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
