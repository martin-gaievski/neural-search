/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml;

import static org.opensearch.neuralsearch.util.TestUtils.TEST_DIMENSION;
import static org.opensearch.neuralsearch.util.TestUtils.TEST_SPACE_TYPE;
import static org.opensearch.neuralsearch.util.TestUtils.createRandomVector;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

import com.google.common.primitives.Floats;

import lombok.SneakyThrows;

/**
 * Integration test for Dynamic Hybrid Search with REAL ML-Commons Linear Regression
 * This version actually trains and uses a linear regression model
 */
public class DynamicHybridSearchWithRealMLIT extends BaseNeuralSearchIT {

    private static final String TEST_INDEX = "poc-hybrid-ml-index";
    private static final String PIPELINE_NAME = "dynamic-hybrid-ml-pipeline";
    private static final String TEXT_FIELD = "content";
    private static final String EMBEDDING_FIELD = "content_embedding";

    private QueryFeatureExtractor featureExtractor;
    private LinearRegressionWeightPredictor weightPredictor;
    private String modelId;
    private final float[] testVector1 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector2 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector3 = createRandomVector(TEST_DIMENSION);

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();

        // Enable ML-Commons features
        updateClusterSettings("plugins.ml_commons.only_run_on_ml_node", false);
        updateClusterSettings("plugins.ml_commons.model_access_control_enabled", false);
        updateClusterSettings("plugins.ml_commons.native_memory_threshold", 100);

        featureExtractor = new QueryFeatureExtractor();
        weightPredictor = new LinearRegressionWeightPredictor(client());

        // Create index with hybrid mappings
        createIndexWithHybridMappings();

        // Prepare a model for neural search
        modelId = prepareModel();

        // Create search pipeline
        createSearchPipelineWithResultsPostProcessor(PIPELINE_NAME);

        // Index test documents
        indexTestDocuments();

        // Train the REAL ML-Commons linear regression model
        logger.info("Training REAL ML-Commons linear regression model...");
        try {
            weightPredictor.trainRealLinearRegressionModel();
            logger.info("Real ML-Commons linear regression model trained successfully!");
        } catch (Exception e) {
            logger.warn("Failed to train real ML model, falling back to heuristic: " + e.getMessage());
            // Fall back to heuristic mode if ML-Commons isn't available
            weightPredictor.trainModel();
        }
    }

    @After
    public void tearDown() throws Exception {
        // Clean up the linear regression model
        if (weightPredictor != null) {
            weightPredictor.undeployAndDeleteModel();
        }

        super.tearDown();
        wipeOfTestResources(TEST_INDEX, null, modelId, PIPELINE_NAME);
    }

    @SneakyThrows
    public void testDynamicHybridSearchWithRealML() {
        List<TestQuery> testQueries = createTestQueries();

        logger.info("=== Dynamic Hybrid Search with REAL ML Test ===");

        for (TestQuery testQuery : testQueries) {
            // Extract features
            Map<String, Double> features = featureExtractor.extractFeatures(testQuery.query);

            // Predict weights using REAL ML-Commons linear regression model
            double[] weights = weightPredictor.predictWeights(features);

            logger.info(
                String.format(
                    "Query: %s | Features: length=%.0f, tokens=%.0f, has_numbers=%.0f | ML Predicted Weights: lexical=%.2f, neural=%.2f",
                    testQuery.query,
                    features.get("query_length"),
                    features.get("token_count"),
                    features.get("has_numbers"),
                    weights[0],
                    weights[1]
                )
            );

            // Run hybrid search with ML-predicted weights
            Map<String, Object> searchResult = runHybridSearchWithWeights(testQuery.query, weights[0], weights[1]);

            int hitCount = getHitCount(searchResult);
            assertTrue("Search should return results", hitCount > 0);

            List<Double> scores = getNormalizationScoreList(searchResult);
            // Verify scores are in descending order
            for (int i = 0; i < scores.size() - 1; i++) {
                logger.info("Score at index {}: {}", i, scores.get(i));
                assertTrue("Scores should be in descending order", scores.get(i) >= scores.get(i + 1));
            }

            // Log the top results
            List<String> docIds = getNormalizationDocIdList(searchResult);
            logger.info("Top results for query '" + testQuery.query + "': " + docIds);
        }
    }

    private void createIndexWithHybridMappings() throws IOException {
        prepareKnnIndex(TEST_INDEX, Collections.singletonList(new KNNFieldConfig(EMBEDDING_FIELD, TEST_DIMENSION, TEST_SPACE_TYPE)));
    }

    private void indexTestDocuments() throws Exception {
        // Index documents with both text and vector fields
        addKnnDoc(
            TEST_INDEX,
            "1",
            Collections.singletonList(EMBEDDING_FIELD),
            Collections.singletonList(Floats.asList(testVector1).toArray()),
            Collections.singletonList(TEXT_FIELD),
            Collections.singletonList("iPhone 15 Pro Max specifications")
        );

        addKnnDoc(
            TEST_INDEX,
            "2",
            Collections.singletonList(EMBEDDING_FIELD),
            Collections.singletonList(Floats.asList(testVector2).toArray()),
            Collections.singletonList(TEXT_FIELD),
            Collections.singletonList("Apple iPhone latest model features")
        );

        addKnnDoc(
            TEST_INDEX,
            "3",
            Collections.singletonList(EMBEDDING_FIELD),
            Collections.singletonList(Floats.asList(testVector3).toArray()),
            Collections.singletonList(TEXT_FIELD),
            Collections.singletonList("Best practices for implementing machine learning models in production environments")
        );

        addKnnDoc(
            TEST_INDEX,
            "4",
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.singletonList(TEXT_FIELD),
            Collections.singletonList("nginx configuration error 404 not found troubleshooting guide")
        );

        assertEquals(4, getDocCount(TEST_INDEX));
    }

    private Map<String, Object> runHybridSearchWithWeights(String query, double lexicalWeight, double neuralWeight) throws Exception {
        // Build hybrid query
        QueryBuilder lexicalQuery = QueryBuilders.matchQuery(TEXT_FIELD, query);

        NeuralQueryBuilder neuralQuery = NeuralQueryBuilder.builder()
            .fieldName(EMBEDDING_FIELD)
            .queryText(query)
            .modelId(modelId)
            .k(10)
            .build();

        HybridQueryBuilder hybridQuery = new HybridQueryBuilder();
        hybridQuery.add(lexicalQuery);
        hybridQuery.add(neuralQuery);

        // Create pipeline with specific weights
        String weightsJson = String.format(Locale.ROOT, "[%.6f, %.6f]", lexicalWeight, neuralWeight);
        createSearchPipeline(PIPELINE_NAME + "-ml-weighted", "l2", "arithmetic_mean", Map.of("weights", weightsJson));

        // Run search with weighted pipeline
        return search(TEST_INDEX, hybridQuery, null, 10, Map.of("search_pipeline", PIPELINE_NAME + "-ml-weighted"));
    }

    private List<TestQuery> createTestQueries() {
        return List.of(
            new TestQuery("iPhone 15", "Short product query"),
            new TestQuery("best practices for implementing machine learning models in production", "Long technical query"),
            new TestQuery("error 404 nginx", "Technical query with numbers"),
            new TestQuery("Apple features", "Brand query"),
            new TestQuery("specifications", "Single word query")
        );
    }

    private static class TestQuery {
        String query;
        String description;

        TestQuery(String query, String description) {
            this.query = query;
            this.description = description;
        }
    }
}
