/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.factory;

import org.opensearch.neuralsearch.processor.rrf.RRFProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;

import java.util.Map;

public class RRFFactory implements Processor.Factory<SearchPhaseResultsProcessor> {

    @Override
    public SearchPhaseResultsProcessor create(Map<String, Processor.Factory<SearchPhaseResultsProcessor>> processorFactories,
                                              String tag,
                                              String description,
                                              boolean ignoreFailure,
                                              Map<String, Object> config,
                                              Processor.PipelineContext pipelineContext) throws Exception {
        return new RRFProcessor(
                tag,
                description
        );
    }
}
