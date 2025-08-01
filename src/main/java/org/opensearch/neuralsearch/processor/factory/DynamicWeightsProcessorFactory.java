/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.factory;

import java.util.Map;

import org.opensearch.neuralsearch.processor.DynamicWeightsProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;
import org.opensearch.cluster.service.ClusterService;

import lombok.extern.log4j.Log4j2;

/**
 * Factory for creating dynamic weights processor instances
 */
@Log4j2
public class DynamicWeightsProcessorFactory implements Processor.Factory<SearchPhaseResultsProcessor> {

    public static final String PROCESSOR_TYPE = DynamicWeightsProcessor.TYPE;

    private final ClusterService clusterService;

    public DynamicWeightsProcessorFactory(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public SearchPhaseResultsProcessor create(
        final Map<String, Processor.Factory<SearchPhaseResultsProcessor>> processorFactories,
        final String tag,
        final String description,
        final boolean ignoreFailure,
        final Map<String, Object> config,
        final Processor.PipelineContext pipelineContext
    ) throws Exception {
        log.info("Creating dynamic weights processor with tag [{}]", tag);
        return new DynamicWeightsProcessor(tag, description, clusterService);
    }
}
