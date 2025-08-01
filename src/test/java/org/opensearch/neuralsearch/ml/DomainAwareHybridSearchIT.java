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
 * Integration test demonstrating domain-aware hybrid search with industry-specific features
 */
public class DomainAwareHybridSearchIT extends BaseNeuralSearchIT {

    private static final String TEST_INDEX = "domain-aware-hybrid-index";
    private static final String PIPELINE_NAME = "domain-hybrid-pipeline";
    private static final String TEXT_FIELD = "content";
    private static final String EMBEDDING_FIELD = "content_embedding";

    private LinearRegressionWeightPredictor weightPredictor;
    private String modelId;
    private final float[] testVector1 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector2 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector3 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector4 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector5 = createRandomVector(TEST_DIMENSION);

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();

        weightPredictor = new LinearRegressionWeightPredictor(client());

        // Create index with hybrid mappings
        createIndexWithHybridMappings();

        // Prepare a model for neural search
        modelId = prepareModel();

        // Create search pipeline
        createSearchPipelineWithResultsPostProcessor(PIPELINE_NAME);

        // Index domain-specific test documents
        indexDomainSpecificDocuments();

        // Train the model (using heuristic mode)
        weightPredictor.trainModel();
    }

    @After
    public void tearDown() throws Exception {
        if (weightPredictor != null) {
            weightPredictor.undeployAndDeleteModel();
        }
        super.tearDown();
        wipeOfTestResources(TEST_INDEX, null, modelId, PIPELINE_NAME);
    }

    @SneakyThrows
    public void testEcommerceDomainSearch() {
        logger.info("=== E-Commerce Domain Search Test ===");

        DomainAwareFeatureExtractor extractor = new DomainAwareFeatureExtractor(DomainAwareFeatureExtractor.Domain.ECOMMERCE);

        // Test different e-commerce query types
        List<TestQuery> ecommerceQueries = List.of(
            new TestQuery("iPhone 15 Pro 256GB", "Product with SKU"),
            new TestQuery("$999 smartphones", "Price query"),
            new TestQuery("best laptop under $1500", "Price range + product"),
            new TestQuery("Nike running shoes size 10", "Brand + size query"),
            new TestQuery("PROD12345", "SKU search")
        );

        runDomainTests(extractor, ecommerceQueries, "E-Commerce");
    }

    @SneakyThrows
    public void testMedicalDomainSearch() {
        logger.info("=== Medical Domain Search Test ===");

        DomainAwareFeatureExtractor extractor = new DomainAwareFeatureExtractor(DomainAwareFeatureExtractor.Domain.MEDICAL_SCIENTIFIC);

        List<TestQuery> medicalQueries = List.of(
            new TestQuery("COVID-19 vaccine mRNA", "Medical acronym query"),
            new TestQuery("ibuprofen 400mg dosage", "Medication with dosage"),
            new TestQuery("symptoms of diabetes type 2", "Symptom search"),
            new TestQuery("SARS-CoV-2 treatment protocols", "Technical medical query"),
            new TestQuery("clinical trials for Alzheimer's disease", "Research query")
        );

        runDomainTests(extractor, medicalQueries, "Medical");
    }

    @SneakyThrows
    public void testQADomainSearch() {
        logger.info("=== Q&A / Conversational Domain Search Test ===");

        DomainAwareFeatureExtractor extractor = new DomainAwareFeatureExtractor(DomainAwareFeatureExtractor.Domain.QA_CONVERSATIONAL);

        List<TestQuery> qaQueries = List.of(
            new TestQuery("What is machine learning?", "Direct question"),
            new TestQuery("How do I implement neural networks in Python?", "How-to question"),
            new TestQuery("Can anyone help me understand recursion?", "Conversational query"),
            new TestQuery("machine learning", "Short keyword"),
            new TestQuery("I think the best approach would be to use transformers", "Opinion statement")
        );

        runDomainTests(extractor, qaQueries, "Q&A");
    }

    @SneakyThrows
    public void testTechnicalDomainSearch() {
        logger.info("=== Technical Domain Search Test ===");

        DomainAwareFeatureExtractor extractor = new DomainAwareFeatureExtractor(DomainAwareFeatureExtractor.Domain.TECHNICAL);

        List<TestQuery> technicalQueries = List.of(
            new TestQuery("NullPointerException java.lang", "Error pattern"),
            new TestQuery("async/await JavaScript ES6", "Code pattern"),
            new TestQuery("OpenSearch v2.11.0 release notes", "Version query"),
            new TestQuery("getUserById() method implementation", "Function search"),
            new TestQuery("HTTP 404 error nginx configuration", "Technical error")
        );

        runDomainTests(extractor, technicalQueries, "Technical");
    }

    @SneakyThrows
    public void testDomainComparison() {
        logger.info("=== Domain Comparison Test ===");

        String testQuery = "how to implement search functionality";

        // Test same query across different domains
        for (var domain : DomainAwareFeatureExtractor.Domain.values()) {
            DomainAwareFeatureExtractor extractor = new DomainAwareFeatureExtractor(domain);
            Map<String, Double> features = extractor.extractFeatures(testQuery);
            double[] weights = weightPredictor.predictWeights(features);

            logger.info(
                String.format(
                    "Domain: %-20s | Default weights: lexical=%.2f, neural=%.2f | Features: %d",
                    domain,
                    weights[0],
                    weights[1],
                    features.size()
                )
            );
        }
    }

    private void runDomainTests(DomainAwareFeatureExtractor extractor, List<TestQuery> queries, String domainName) throws Exception {
        for (TestQuery testQuery : queries) {
            // Extract domain-specific features
            Map<String, Double> features = extractor.extractFeatures(testQuery.query);

            // Predict weights using domain-aware heuristics
            double[] weights = weightPredictor.predictWeights(features);

            logger.info(
                String.format(
                    "[%s] Query: %-50s | Weights: lexical=%.2f, neural=%.2f | Features: %d",
                    domainName,
                    testQuery.query,
                    weights[0],
                    weights[1],
                    features.size()
                )
            );

            // Log key features
            logKeyFeatures(features, domainName);

            // Run hybrid search with predicted weights
            Map<String, Object> searchResult = runHybridSearchWithWeights(testQuery.query, weights[0], weights[1]);

            int hitCount = getHitCount(searchResult);
            assertTrue("Search should return results for: " + testQuery.query, hitCount > 0);
        }
    }

    private void logKeyFeatures(Map<String, Double> features, String domain) {
        StringBuilder keyFeatures = new StringBuilder("    Key features: ");

        // Log domain-specific features
        switch (domain) {
            case "E-Commerce":
                if (features.getOrDefault("has_currency", 0.0) > 0) keyFeatures.append("has_currency ");
                if (features.getOrDefault("has_sku_pattern", 0.0) > 0) keyFeatures.append("has_sku ");
                if (features.getOrDefault("is_product_search", 0.0) > 0) keyFeatures.append("product_search ");
                if (features.getOrDefault("has_size_terms", 0.0) > 0) keyFeatures.append("has_size ");
                break;

            case "Medical":
                if (features.getOrDefault("medical_acronym_count", 0.0) > 0) keyFeatures.append("acronyms=")
                    .append(features.get("medical_acronym_count").intValue())
                    .append(" ");
                if (features.getOrDefault("has_dosage", 0.0) > 0) keyFeatures.append("has_dosage ");
                if (features.getOrDefault("has_symptom_keywords", 0.0) > 0) keyFeatures.append("symptoms ");
                break;

            case "Q&A":
                if (features.getOrDefault("is_question", 0.0) > 0) keyFeatures.append("is_question ");
                if (features.getOrDefault("conversational_score", 0.0) > 0) keyFeatures.append("conversational=")
                    .append(String.format("%.2f", features.get("conversational_score")))
                    .append(" ");
                break;

            case "Technical":
                if (features.getOrDefault("has_error_pattern", 0.0) > 0) keyFeatures.append("error_pattern ");
                if (features.getOrDefault("has_code_pattern", 0.0) > 0) keyFeatures.append("code_pattern ");
                if (features.getOrDefault("has_version_number", 0.0) > 0) keyFeatures.append("has_version ");
                break;
        }

        logger.info(keyFeatures.toString());
    }

    private void createIndexWithHybridMappings() throws IOException {
        prepareKnnIndex(TEST_INDEX, Collections.singletonList(new KNNFieldConfig(EMBEDDING_FIELD, TEST_DIMENSION, TEST_SPACE_TYPE)));
    }

    private void indexDomainSpecificDocuments() throws Exception {
        // E-commerce documents
        addKnnDoc(
            TEST_INDEX,
            "1",
            Collections.singletonList(EMBEDDING_FIELD),
            Collections.singletonList(Floats.asList(testVector1).toArray()),
            Collections.singletonList(TEXT_FIELD),
            Collections.singletonList("iPhone 15 Pro Max 256GB Space Black - Latest Apple smartphone with A17 Pro chip")
        );

        // Medical documents
        addKnnDoc(
            TEST_INDEX,
            "2",
            Collections.singletonList(EMBEDDING_FIELD),
            Collections.singletonList(Floats.asList(testVector2).toArray()),
            Collections.singletonList(TEXT_FIELD),
            Collections.singletonList("COVID-19 mRNA vaccine clinical trial results showing 95% efficacy in preventing severe disease")
        );

        // Q&A documents
        addKnnDoc(
            TEST_INDEX,
            "3",
            Collections.singletonList(EMBEDDING_FIELD),
            Collections.singletonList(Floats.asList(testVector3).toArray()),
            Collections.singletonList(TEXT_FIELD),
            Collections.singletonList("Machine learning is a subset of artificial intelligence that enables systems to learn from data")
        );

        // Technical documents
        addKnnDoc(
            TEST_INDEX,
            "4",
            Collections.singletonList(EMBEDDING_FIELD),
            Collections.singletonList(Floats.asList(testVector4).toArray()),
            Collections.singletonList(TEXT_FIELD),
            Collections.singletonList(
                "NullPointerException occurs when your code attempts to use a reference that points to no location in memory"
            )
        );

        // Mixed domain document
        addKnnDoc(
            TEST_INDEX,
            "5",
            Collections.singletonList(EMBEDDING_FIELD),
            Collections.singletonList(Floats.asList(testVector5).toArray()),
            Collections.singletonList(TEXT_FIELD),
            Collections.singletonList("The best laptop under $1500 features 16GB RAM and latest Intel processor for coding and development")
        );

        assertEquals(5, getDocCount(TEST_INDEX));
    }

    private Map<String, Object> runHybridSearchWithWeights(String query, double lexicalWeight, double neuralWeight) throws Exception {
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

        String weightsJson = String.format(Locale.ROOT, "[%.6f, %.6f]", lexicalWeight, neuralWeight);
        createSearchPipeline(PIPELINE_NAME + "-domain", "l2", "arithmetic_mean", Map.of("weights", weightsJson));

        return search(TEST_INDEX, hybridQuery, null, 10, Map.of("search_pipeline", PIPELINE_NAME + "-domain"));
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
