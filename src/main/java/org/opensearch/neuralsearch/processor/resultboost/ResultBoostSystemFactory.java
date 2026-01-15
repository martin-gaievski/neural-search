/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.resultboost;

import lombok.extern.log4j.Log4j2;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.neuralsearch.processor.ResultBoostResponseProcessor;
import org.opensearch.neuralsearch.query.ext.ResultBoostSearchExtBuilder;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.ProcessorGenerationContext;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.search.pipeline.SystemGeneratedProcessor;

import java.util.List;
import java.util.Map;

/**
 * Factory for creating system-generated ResultBoost processors.
 * This factory automatically attaches a ResultBoostResponseProcessor when
 * a search request contains the "ext.result_boost" configuration.
 *
 * <p>This enables zero-configuration usage where users simply add boost
 * configuration to their query without needing to:
 * <ul>
 *     <li>Create a search pipeline</li>
 *     <li>Add the result_boost processor to the pipeline</li>
 *     <li>Configure the pipeline on the index or search request</li>
 * </ul>
 *
 * <p>Usage (no pipeline required):
 * <pre>
 * POST /my-index/_search
 * {
 *   "query": { "match": { "title": "laptop" } },
 *   "ext": {
 *     "result_boost": {
 *       "boosts": [
 *         { "document_id": "sponsored-1", "factor": 5.0 }
 *       ]
 *     }
 *   }
 * }
 * </pre>
 */
@Log4j2
public class ResultBoostSystemFactory implements SystemGeneratedProcessor.SystemGeneratedFactory<SearchResponseProcessor> {

    /**
     * The type identifier for this system factory.
     * Used for registration in the neural-search plugin.
     */
    public static final String SYSTEM_FACTORY_TYPE = "result_boost_auto";

    /**
     * Default processor tag for system-generated processors.
     */
    private static final String DEFAULT_TAG = "result_boost_system_generated";

    /**
     * Default description for system-generated processors.
     */
    private static final String DEFAULT_DESCRIPTION = "System-generated result boost processor that applies score boosts "
        + "to documents based on ext.result_boost configuration in the search request";

    /**
     * Determines whether a ResultBoostResponseProcessor should be automatically
     * generated for the given search request.
     *
     * <p>The processor is generated if the search request contains an
     * "ext.result_boost" section with boost configuration.
     *
     * @param context the processor generation context containing the search request
     * @return true if ext.result_boost is present and contains boost configuration
     */
    @Override
    public boolean shouldGenerate(ProcessorGenerationContext context) {
        SearchRequest request = context.searchRequest();
        if (request == null) {
            log.trace("No search request in context, skipping auto-generation");
            return false;
        }

        SearchSourceBuilder source = request.source();
        if (source == null) {
            log.trace("No source builder in request, skipping auto-generation");
            return false;
        }

        List<SearchExtBuilder> extBuilders = source.ext();
        if (extBuilders == null || extBuilders.isEmpty()) {
            log.trace("No ext builders in request, skipping auto-generation");
            return false;
        }

        // Check if ext.result_boost is present with valid configuration
        for (SearchExtBuilder extBuilder : extBuilders) {
            if (extBuilder instanceof ResultBoostSearchExtBuilder) {
                ResultBoostSearchExtBuilder boostExt = (ResultBoostSearchExtBuilder) extBuilder;
                ResultBoostConfig config = boostExt.getResultBoostConfig();
                if (config != null && config.hasBoosts()) {
                    log.debug("Found ext.result_boost with {} boost(s), will auto-generate processor", config.getBoosts().size());
                    return true;
                }
            }
        }

        log.trace("No ext.result_boost found in request, skipping auto-generation");
        return false;
    }

    /**
     * Creates a new ResultBoostResponseProcessor instance for system-generated use.
     *
     * <p>The created processor reads boost configuration from the search request's
     * ext.result_boost section at runtime, rather than from static pipeline config.
     *
     * @param processorFactories map of available processor factories (unused)
     * @param processorTag optional tag for the processor
     * @param description optional description for the processor
     * @param ignoreFailure whether to ignore failures during processing
     * @param config processor configuration map (unused for system-generated)
     * @param pipelineContext the pipeline context
     * @return a new ResultBoostResponseProcessor instance
     */
    @Override
    public SearchResponseProcessor create(
        Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories,
        String processorTag,
        String description,
        boolean ignoreFailure,
        Map<String, Object> config,
        Processor.PipelineContext pipelineContext
    ) {
        String tag = processorTag != null ? processorTag : DEFAULT_TAG;
        String desc = description != null ? description : DEFAULT_DESCRIPTION;

        log.debug("Creating system-generated ResultBoostResponseProcessor with tag: {}", tag);

        // Create processor with no static boost config - it will read from ext at runtime
        return new ResultBoostResponseProcessor(tag, desc, ignoreFailure, null);
    }
}
