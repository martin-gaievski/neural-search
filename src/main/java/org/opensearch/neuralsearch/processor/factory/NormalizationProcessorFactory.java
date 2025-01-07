/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.factory;

import static org.opensearch.ingest.ConfigurationUtils.readOptionalMap;
import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.opensearch.neuralsearch.processor.NormalizationProcessor;
import org.opensearch.neuralsearch.processor.NormalizationProcessorWorkflow;
import org.opensearch.neuralsearch.processor.combination.ArithmeticMeanScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationFactory;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.normalization.MinMaxScoreNormalizationTechnique;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationFactory;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationTechnique;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.opensearch.search.pipeline.SearchResponseProcessor;

/**
 * Factory for query results normalization processor for search pipeline. Instantiates processor based on user provided input.
 */
@AllArgsConstructor
@Log4j2
public class NormalizationProcessorFactory implements Processor.Factory<SearchPhaseResultsProcessor> {
    public static final String NORMALIZATION_CLAUSE = "normalization";
    public static final String COMBINATION_CLAUSE = "combination";
    public static final String TECHNIQUE = "technique";
    public static final String PARAMETERS = "parameters";

    private final NormalizationProcessorWorkflow normalizationProcessorWorkflow;
    private ScoreNormalizationFactory scoreNormalizationFactory;
    private ScoreCombinationFactory scoreCombinationFactory;

    @Override
    public SearchPhaseResultsProcessor create(
        final Map<String, Processor.Factory<SearchPhaseResultsProcessor>> processorFactories,
        final String tag,
        final String description,
        final boolean ignoreFailure,
        final Map<String, Object> config,
        final Processor.PipelineContext pipelineContext
    ) throws Exception {
        Map<String, Object> normalizationClause = readOptionalMap(NormalizationProcessor.TYPE, tag, config, NORMALIZATION_CLAUSE);
        ScoreNormalizationTechnique normalizationTechnique = ScoreNormalizationFactory.DEFAULT_METHOD;
        if (Objects.nonNull(normalizationClause)) {
            String normalizationTechniqueName = readStringProperty(
                NormalizationProcessor.TYPE,
                tag,
                normalizationClause,
                TECHNIQUE,
                MinMaxScoreNormalizationTechnique.TECHNIQUE_NAME
            );
            normalizationTechnique = scoreNormalizationFactory.createNormalization(normalizationTechniqueName);
        }

        Map<String, Object> combinationClause = readOptionalMap(NormalizationProcessor.TYPE, tag, config, COMBINATION_CLAUSE);

        ScoreCombinationTechnique scoreCombinationTechnique = ScoreCombinationFactory.DEFAULT_METHOD;
        if (Objects.nonNull(combinationClause)) {
            String combinationTechnique = readStringProperty(
                NormalizationProcessor.TYPE,
                tag,
                combinationClause,
                TECHNIQUE,
                ArithmeticMeanScoreCombinationTechnique.TECHNIQUE_NAME
            );
            // check for optional combination params
            Map<String, Object> combinationParams = readOptionalMap(NormalizationProcessor.TYPE, tag, combinationClause, PARAMETERS);
            scoreCombinationTechnique = scoreCombinationFactory.createCombination(combinationTechnique, combinationParams);
        }
        log.info(
            "Creating search phase results processor of type [{}] with normalization [{}] and combination [{}]",
            NormalizationProcessor.TYPE,
            normalizationTechnique,
            scoreCombinationTechnique
        );
        return new NormalizationProcessor(
            tag,
            description,
            normalizationTechnique,
            scoreCombinationTechnique,
            normalizationProcessorWorkflow
        );
    }

    @Override
    public <D extends Processor> D dependentProcessors(
        Map<String, Processor.Factory<D>> processorFactories,
        String tag,
        String description,
        boolean ignoreFailure,
        Map<String, Object> config,
        Processor.PipelineContext pipelineContext
    ) throws Exception {
        ExplanationResponseProcessorFactory explanationResponseProcessorFactory = new ExplanationResponseProcessorFactory();

        // Create a new map with the correct type
        Map<String, Processor.Factory<SearchResponseProcessor>> convertedFactories = new HashMap<>();
        for (Map.Entry<String, Processor.Factory<D>> entry : processorFactories.entrySet()) {
            // This cast might need to be validated depending on your specific use case
            @SuppressWarnings("unchecked")
            Processor.Factory<SearchResponseProcessor> converted = (Processor.Factory<SearchResponseProcessor>) entry.getValue();
            convertedFactories.put(entry.getKey(), converted);
        }

        return (D) explanationResponseProcessorFactory.create(convertedFactories, tag, description, ignoreFailure, config, pipelineContext);
    }

}
