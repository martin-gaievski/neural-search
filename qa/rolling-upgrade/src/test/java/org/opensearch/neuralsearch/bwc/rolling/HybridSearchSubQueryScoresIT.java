/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.rolling;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.opensearch.index.query.MatchQueryBuilder;
import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.PARAM_NAME_WEIGHTS;
import static org.opensearch.neuralsearch.util.TestUtils.TEXT_EMBEDDING_PROCESSOR;
import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_NORMALIZATION_METHOD;
import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_COMBINATION_METHOD;
import static org.opensearch.neuralsearch.util.TestUtils.getModelId;
import static org.opensearch.neuralsearch.util.TestUtils.getNestedHits;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.knn.index.query.rescore.RescoreContext;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

public class HybridSearchSubQueryScoresIT extends AbstractRollingUpgradeTestCase {

    private static final String PIPELINE_NAME = "nlp-hybrid-pipeline";
    private static final String SEARCH_PIPELINE_NAME = "nlp-search-pipeline";
    private static final String TEST_FIELD = "passage_text";
    private static final String TEXT = "Hello world";
    private static final String TEXT_MIXED = "Hi planet";
    private static final String TEXT_UPGRADED = "Hi earth";
    private static final String QUERY = "Hi world";
    private static final int NUM_DOCS_PER_ROUND = 1;
    private static final String VECTOR_EMBEDDING_FIELD = "passage_embedding";
    protected static final String RESCORE_QUERY = "hi";
    private static String modelId = "";

    public void testHybridizationScoresFieldInMixedCluster() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);

        switch (getClusterType()) {
            case OLD:
                modelId = uploadTextEmbeddingModel();
                createPipelineProcessor(modelId, PIPELINE_NAME);
                createIndexWithConfiguration(
                    getIndexNameForTest(),
                    Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI())),
                    PIPELINE_NAME
                );
                // Add documents to spread across shards
                for (int i = 0; i < NUM_DOCS_PER_ROUND; i++) {
                    addDocument(getIndexNameForTest(), String.valueOf(i), TEST_FIELD, TEXT + i, null, null);
                }
                createSearchPipeline(
                    SEARCH_PIPELINE_NAME,
                    DEFAULT_NORMALIZATION_METHOD,
                    DEFAULT_COMBINATION_METHOD,
                    Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.3f, 0.7f })),
                    false
                );
                break;

            case MIXED:
                modelId = getModelId(getIngestionPipeline(PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
                loadAndWaitForModelToBeReady(modelId);

                if (isFirstMixedRound()) {
                    HybridQueryBuilder query = getQueryBuilder(modelId, null, null, null, null);
                    validateTestIndexOnUpgrade(NUM_DOCS_PER_ROUND, modelId, query, null);
                    addDocument(getIndexNameForTest(), "mixed_1", TEST_FIELD, TEXT_MIXED, null, null);
                } else {
                    HybridQueryBuilder query = getQueryBuilder(modelId, null, null, null, null);
                    validateTestIndexOnUpgrade(NUM_DOCS_PER_ROUND + 1, modelId, query, null);
                }
                break;

            case UPGRADED:
                try {
                    modelId = getModelId(getIngestionPipeline(PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
                    loadAndWaitForModelToBeReady(modelId);

                    // Add a doc to trigger fetch on upgraded node
                    addDocument(getIndexNameForTest(), "upgraded", TEST_FIELD, TEXT_UPGRADED, null, null);

                    HybridQueryBuilder hybridQueryBuilder = getQueryBuilder(
                        modelId,
                        Boolean.FALSE,
                        Map.of("ef_search", 100),
                        RescoreContext.getDefault(),
                        new MatchQueryBuilder("_id", "upgraded")
                    );

                    createSearchPipeline(
                        SEARCH_PIPELINE_NAME,
                        DEFAULT_NORMALIZATION_METHOD,
                        DEFAULT_COMBINATION_METHOD,
                        Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.3f, 0.7f })),
                        true
                    );

                    Map<String, Object> searchResponseAsMap = search(
                        getIndexNameForTest(),
                        hybridQueryBuilder,
                        null,
                        1,
                        Map.of("search_pipeline", SEARCH_PIPELINE_NAME + "1")
                    );
                    assertNotNull(searchResponseAsMap);
                    assertHybridizationSubQueryScores(searchResponseAsMap, 2);
                } finally {
                    wipeOfTestResources(getIndexNameForTest(), PIPELINE_NAME, modelId, SEARCH_PIPELINE_NAME);
                }
                break;

            default:
                throw new IllegalStateException("Unexpected value: " + getClusterType());
        }
    }

    private void validateTestIndexOnUpgrade(
        final int numberOfDocs,
        final String modelId,
        HybridQueryBuilder hybridQueryBuilder,
        QueryBuilder rescorer
    ) throws Exception {
        int docCount = getDocCount(getIndexNameForTest());
        assertEquals(numberOfDocs, docCount);
        Map<String, Object> searchResponseAsMap = search(
            getIndexNameForTest(),
            hybridQueryBuilder,
            rescorer,
            1,
            Map.of("search_pipeline", SEARCH_PIPELINE_NAME)
        );
        assertNotNull(searchResponseAsMap);
        int hits = getHitCount(searchResponseAsMap);
        assertEquals(1, hits);
        List<Double> scoresList = getNormalizationScoreList(searchResponseAsMap);
        for (Double score : scoresList) {
            assertTrue(0 <= score && score <= 2);
        }
    }

    private void assertHybridizationSubQueryScores(Map<String, Object> searchResponseAsMap, int expectedSubQueryCount) {
        List<Map<String, Object>> hitsNestedList = getNestedHits(searchResponseAsMap);

        for (Map<String, Object> hit : hitsNestedList) {
            @SuppressWarnings("unchecked")
            Map<String, Object> fields = (Map<String, Object>) hit.get("fields");
            assertNotNull(fields);
            @SuppressWarnings("unchecked")
            List<Double> subQueryScores = (List<Double>) fields.get("hybridization_sub_query_scores");
            assertNotNull(subQueryScores);
            assertEquals(expectedSubQueryCount, subQueryScores.size());
            for (Double score : subQueryScores) {
                assertNotNull(score);
                assertTrue(score >= 0.0);
                assertTrue(score <= 1.0);
            }
        }
    }

    private HybridQueryBuilder getQueryBuilder(
        final String modelId,
        final Boolean expandNestedDocs,
        final Map<String, ?> methodParameters,
        final RescoreContext rescoreContextForNeuralQuery,
        final QueryBuilder filter
    ) {
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(VECTOR_EMBEDDING_FIELD)
            .modelId(modelId)
            .queryText(QUERY)
            .k(5)
            .build();
        if (expandNestedDocs != null) {
            neuralQueryBuilder.expandNested(expandNestedDocs);
        }
        if (methodParameters != null) {
            neuralQueryBuilder.methodParameters(methodParameters);
        }
        if (Objects.nonNull(rescoreContextForNeuralQuery)) {
            neuralQueryBuilder.rescoreContext(rescoreContextForNeuralQuery);
        }

        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("text", QUERY);

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(matchQueryBuilder);
        hybridQueryBuilder.add(neuralQueryBuilder);

        if (filter != null) {
            hybridQueryBuilder.filter(filter);
        }

        return hybridQueryBuilder;
    }
}
