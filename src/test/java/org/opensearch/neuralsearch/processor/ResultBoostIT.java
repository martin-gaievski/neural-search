/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_COMBINATION_METHOD;
import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_NORMALIZATION_METHOD;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import lombok.SneakyThrows;

/**
 * Integration test for the Result Boost feature using SystemGeneratedProcessor (zero-config).
 *
 * This test validates that result boost works correctly via automatic processor attachment
 * when ext.result_boost is present in the search request. No explicit result_boost
 * response processor needs to be configured in the pipeline.
 *
 * Tests:
 * 1. Hybrid search without boost - observe natural ranking
 * 2. Hybrid search with multiplicative boost on a specific document (auto-attached)
 * 3. Hybrid search with additive boost on a specific document (auto-attached)
 * 4. Multi-shard test to verify it works in distributed environments
 */
public class ResultBoostIT extends BaseNeuralSearchIT {

    private static final String TEST_INDEX = "test-result-boost-index";
    private static final String MULTI_SHARD_INDEX = "test-result-boost-multi-shard";
    private static final String SEARCH_PIPELINE = "result-boost-test-pipeline";
    private static final String RESPONSE_PROCESSOR_PIPELINE = "result-boost-response-pipeline";
    private static final String TEXT_FIELD = "text";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
        // Enable the result-boost system-generated processor factory
        enableResultBoostSystemFactory();
    }

    /**
     * Enable the ResultBoostSystemFactory so that it auto-attaches when ext.result_boost is present.
     * This is required because SystemGeneratedProcessors are disabled by default.
     */
    @SneakyThrows
    private void enableResultBoostSystemFactory() {
        updateClusterSettings(
            "cluster.search.enabled_system_generated_factories",
            java.util.Collections.singletonList("result_boost_auto")
        );
    }

    /**
     * Happy path integration test for result boost feature.
     *
     * Scenario:
     * - Index has 5 documents with varying relevance to "hello" query
     * - Document "3" has text "Hello world" - natural top hit for "hello"
     * - Document "5" has text "hello test document" - lower relevance
     *
     * Test:
     * 1. Run hybrid search without boost - "3" should be top hit
     * 2. Run hybrid search with boost on document "5" with factor 10.0
     * 3. Verify document "5" now has a boosted score > its original score
     */
    @SneakyThrows
    public void testResultBoost_whenBoostApplied_thenDocumentScoreIncreases() {
        // Initialize test index
        initializeTestIndex();

        // Create search pipeline with normalization processor and response boost processor
        createPipelineWithResultBoostResponseProcessor(RESPONSE_PROCESSOR_PIPELINE);

        // Query 1: Hybrid search WITHOUT result boost (no ext parameter)
        String queryWithoutBoost = """
            {
                "query": {
                    "hybrid": {
                        "queries": [
                            { "term": { "text": "hello" } },
                            { "term": { "text": "world" } }
                        ]
                    }
                }
            }
            """;

        Map<String, Object> resultsWithoutBoost = searchWithPipeline(TEST_INDEX, queryWithoutBoost, 5, RESPONSE_PROCESSOR_PIPELINE);

        // Verify we have results
        List<Map<String, Object>> hitsWithoutBoost = getHits(resultsWithoutBoost);
        assertFalse("Should have search results", hitsWithoutBoost.isEmpty());

        // Find document "5" score in results without boost
        Float doc5ScoreWithoutBoost = getDocumentScore(hitsWithoutBoost, "5");
        Float doc3ScoreWithoutBoost = getDocumentScore(hitsWithoutBoost, "3");

        // Document 3 "Hello world" should have a good score for "hello" + "world"
        assertNotNull("Document 3 should be in results", doc3ScoreWithoutBoost);
        assertTrue("Document 3 should have positive score", doc3ScoreWithoutBoost > 0);

        // Document 5 "hello test document" should also match "hello"
        assertNotNull("Document 5 should be in results", doc5ScoreWithoutBoost);
        assertTrue("Document 5 should have positive score", doc5ScoreWithoutBoost > 0);

        logger.info("Without boost - Doc 3 score: {}, Doc 5 score: {}", doc3ScoreWithoutBoost, doc5ScoreWithoutBoost);

        // Query 2: Hybrid search WITH result boost on document "5"
        String queryWithBoost = """
            {
                "query": {
                    "hybrid": {
                        "queries": [
                            { "term": { "text": "hello" } },
                            { "term": { "text": "world" } }
                        ]
                    }
                },
                "ext": {
                    "result_boost": {
                        "boosts": [
                            { "document_id": "5", "factor": 10.0 }
                        ]
                    }
                }
            }
            """;

        Map<String, Object> resultsWithBoost = searchWithPipeline(TEST_INDEX, queryWithBoost, 5, RESPONSE_PROCESSOR_PIPELINE);

        // Verify we have results
        List<Map<String, Object>> hitsWithBoost = getHits(resultsWithBoost);
        assertFalse("Should have search results with boost", hitsWithBoost.isEmpty());

        // Find document "5" score in results with boost
        Float doc5ScoreWithBoost = getDocumentScore(hitsWithBoost, "5");
        assertNotNull("Document 5 should be in results with boost", doc5ScoreWithBoost);

        logger.info("With boost - Doc 5 score: {} (was: {})", doc5ScoreWithBoost, doc5ScoreWithoutBoost);

        // Verify boost was applied - score should be higher
        assertTrue(
            "Document 5 score with boost (" + doc5ScoreWithBoost + ") should be greater than without boost (" + doc5ScoreWithoutBoost + ")",
            doc5ScoreWithBoost > doc5ScoreWithoutBoost
        );

        // With multiplicative factor 10.0, boosted score should be approximately 10x original
        // (allowing for floating point precision)
        float expectedBoostedScore = doc5ScoreWithoutBoost * 10.0f;
        assertTrue(
            "Boosted score (" + doc5ScoreWithBoost + ") should be approximately 10x original (" + expectedBoostedScore + ")",
            Math.abs(doc5ScoreWithBoost - expectedBoostedScore) < 0.01f
        );
    }

    /**
     * Test additive boost type.
     */
    @SneakyThrows
    public void testResultBoost_whenAdditiveBoost_thenScoreIncreasedByFactor() {
        initializeTestIndex();

        createPipelineWithResultBoostResponseProcessor(RESPONSE_PROCESSOR_PIPELINE);

        // First get baseline score
        String queryWithoutBoost = """
            {
                "query": {
                    "hybrid": {
                        "queries": [
                            { "term": { "text": "hello" } },
                            { "term": { "text": "world" } }
                        ]
                    }
                }
            }
            """;

        Map<String, Object> resultsWithoutBoost = searchWithPipeline(TEST_INDEX, queryWithoutBoost, 5, RESPONSE_PROCESSOR_PIPELINE);
        List<Map<String, Object>> hitsWithoutBoost = getHits(resultsWithoutBoost);
        Float doc3ScoreWithoutBoost = getDocumentScore(hitsWithoutBoost, "3");

        // Query with additive boost
        String queryWithAdditiveBoost = """
            {
                "query": {
                    "hybrid": {
                        "queries": [
                            { "term": { "text": "hello" } },
                            { "term": { "text": "world" } }
                        ]
                    }
                },
                "ext": {
                    "result_boost": {
                        "boosts": [
                            { "document_id": "3", "factor": 2.5, "type": "additive" }
                        ]
                    }
                }
            }
            """;

        Map<String, Object> results = searchWithPipeline(TEST_INDEX, queryWithAdditiveBoost, 5, RESPONSE_PROCESSOR_PIPELINE);

        List<Map<String, Object>> hits = getHits(results);
        assertFalse("Should have search results", hits.isEmpty());

        // Document 3 with additive boost should have score = original + 2.5
        Float doc3ScoreWithBoost = getDocumentScore(hits, "3");
        assertNotNull("Document 3 should be in results", doc3ScoreWithBoost);

        logger.info("Additive boost - Doc 3 score: {} (was: {})", doc3ScoreWithBoost, doc3ScoreWithoutBoost);

        // Verify additive boost: new_score = original_score + factor
        float expectedAdditiveScore = doc3ScoreWithoutBoost + 2.5f;
        assertTrue(
            "Document 3 with additive boost 2.5 should have score ~" + expectedAdditiveScore + ", got: " + doc3ScoreWithBoost,
            Math.abs(doc3ScoreWithBoost - expectedAdditiveScore) < 0.01f
        );
    }

    /**
     * Test that result boost works correctly in multi-shard environments.
     * This is the key test that validates the SearchResponseProcessor approach.
     */
    @SneakyThrows
    public void testResultBoost_whenMultipleShards_thenBoostAppliedCorrectly() {
        // Create a multi-shard index
        initializeMultiShardIndex();

        // Create pipeline with response processor
        createPipelineWithResultBoostResponseProcessor(RESPONSE_PROCESSOR_PIPELINE);

        // Get baseline scores
        String queryWithoutBoost = """
            {
                "query": {
                    "hybrid": {
                        "queries": [
                            { "term": { "text": "hello" } },
                            { "term": { "text": "test" } }
                        ]
                    }
                }
            }
            """;

        Map<String, Object> resultsWithoutBoost = searchWithPipeline(MULTI_SHARD_INDEX, queryWithoutBoost, 10, RESPONSE_PROCESSOR_PIPELINE);
        List<Map<String, Object>> hitsWithoutBoost = getHits(resultsWithoutBoost);

        assertFalse("Should have results in multi-shard index", hitsWithoutBoost.isEmpty());

        // Find any document that matches
        Float doc1ScoreWithoutBoost = getDocumentScore(hitsWithoutBoost, "1");
        assertNotNull("Document 1 should be in results", doc1ScoreWithoutBoost);

        logger.info("Multi-shard without boost - Doc 1 score: {}", doc1ScoreWithoutBoost);

        // Now search with boost
        String queryWithBoost = """
            {
                "query": {
                    "hybrid": {
                        "queries": [
                            { "term": { "text": "hello" } },
                            { "term": { "text": "test" } }
                        ]
                    }
                },
                "ext": {
                    "result_boost": {
                        "boosts": [
                            { "document_id": "1", "factor": 5.0 }
                        ]
                    }
                }
            }
            """;

        Map<String, Object> resultsWithBoost = searchWithPipeline(MULTI_SHARD_INDEX, queryWithBoost, 10, RESPONSE_PROCESSOR_PIPELINE);
        List<Map<String, Object>> hitsWithBoost = getHits(resultsWithBoost);

        Float doc1ScoreWithBoost = getDocumentScore(hitsWithBoost, "1");
        assertNotNull("Document 1 should be in results with boost", doc1ScoreWithBoost);

        logger.info("Multi-shard with boost - Doc 1 score: {} (was: {})", doc1ScoreWithBoost, doc1ScoreWithoutBoost);

        // Verify boost was applied
        assertTrue(
            "Document 1 score with boost (" + doc1ScoreWithBoost + ") should be greater than without boost (" + doc1ScoreWithoutBoost + ")",
            doc1ScoreWithBoost > doc1ScoreWithoutBoost
        );

        // Verify multiplicative factor was applied correctly
        float expectedBoostedScore = doc1ScoreWithoutBoost * 5.0f;
        assertTrue(
            "Boosted score (" + doc1ScoreWithBoost + ") should be approximately 5x original (" + expectedBoostedScore + ")",
            Math.abs(doc1ScoreWithBoost - expectedBoostedScore) < 0.01f
        );
    }

    @SneakyThrows
    private void initializeTestIndex() {
        if (indexExists(TEST_INDEX)) {
            return;
        }

        // Create simple text index with 1 shard
        String indexMapping = """
            {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                },
                "mappings": {
                    "properties": {
                        "text": { "type": "text" }
                    }
                }
            }
            """;

        createIndexWithConfiguration(TEST_INDEX, indexMapping, "");

        // Add test documents
        addDocument(TEST_INDEX, "1", Map.of(TEXT_FIELD, "Welcome to the place"));
        addDocument(TEST_INDEX, "2", Map.of(TEXT_FIELD, "Hi there friend"));
        addDocument(TEST_INDEX, "3", Map.of(TEXT_FIELD, "Hello world"));
        addDocument(TEST_INDEX, "4", Map.of(TEXT_FIELD, "Greetings everyone"));
        addDocument(TEST_INDEX, "5", Map.of(TEXT_FIELD, "hello test document"));

        // Verify documents are indexed
        int docCount = getDocCount(TEST_INDEX);
        logger.info("Document count after indexing: {}", docCount);
        assertEquals("Expected 5 documents in index", 5, docCount);
    }

    @SneakyThrows
    private void initializeMultiShardIndex() {
        if (indexExists(MULTI_SHARD_INDEX)) {
            return;
        }

        // Create index with multiple shards to test distributed behavior
        String indexMapping = """
            {
                "settings": {
                    "number_of_shards": 3,
                    "number_of_replicas": 0
                },
                "mappings": {
                    "properties": {
                        "text": { "type": "text" }
                    }
                }
            }
            """;

        createIndexWithConfiguration(MULTI_SHARD_INDEX, indexMapping, "");

        // Add test documents - distributed across shards
        addDocument(MULTI_SHARD_INDEX, "1", Map.of(TEXT_FIELD, "hello world test"));
        addDocument(MULTI_SHARD_INDEX, "2", Map.of(TEXT_FIELD, "hello there friend"));
        addDocument(MULTI_SHARD_INDEX, "3", Map.of(TEXT_FIELD, "test document example"));
        addDocument(MULTI_SHARD_INDEX, "4", Map.of(TEXT_FIELD, "hello test sample"));
        addDocument(MULTI_SHARD_INDEX, "5", Map.of(TEXT_FIELD, "world peace hello"));
        addDocument(MULTI_SHARD_INDEX, "6", Map.of(TEXT_FIELD, "test hello world sample"));
        addDocument(MULTI_SHARD_INDEX, "7", Map.of(TEXT_FIELD, "another test document"));
        addDocument(MULTI_SHARD_INDEX, "8", Map.of(TEXT_FIELD, "hello goodbye test"));

        // Verify documents are indexed
        int docCount = getDocCount(MULTI_SHARD_INDEX);
        logger.info("Multi-shard document count after indexing: {}", docCount);
        assertEquals("Expected 8 documents in multi-shard index", 8, docCount);
    }

    /**
     * Create a search pipeline with only normalization processor.
     *
     * Note: NO explicit result_boost response processor is configured here!
     * The ResultBoostSystemFactory will auto-attach the processor when ext.result_boost
     * is present in the search request (zero-config pattern).
     */
    @SneakyThrows
    private void createPipelineWithResultBoostResponseProcessor(String pipelineName) {
        // Check if pipeline already exists
        try {
            Request getRequest = new Request("GET", "/_search/pipeline/" + pipelineName);
            client().performRequest(getRequest);
            // Pipeline exists, return
            return;
        } catch (Exception e) {
            // Pipeline doesn't exist, create it
        }

        // Pipeline with ONLY normalization processor - no result_boost response processor!
        // Result boost will be auto-attached via SystemGeneratedProcessor when ext.result_boost is present
        String pipelineConfig = """
            {
                "description": "Pipeline with normalization only - result boost auto-attached via SystemGeneratedProcessor",
                "phase_results_processors": [
                    {
                        "normalization-processor": {
                            "normalization": {
                                "technique": "%s"
                            },
                            "combination": {
                                "technique": "%s"
                            }
                        }
                    }
                ]
            }
            """.formatted(DEFAULT_NORMALIZATION_METHOD, DEFAULT_COMBINATION_METHOD);

        Request request = new Request("PUT", "/_search/pipeline/" + pipelineName);
        request.setJsonEntity(pipelineConfig);

        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        logger.info("Created search pipeline '{}' with normalization only (result boost auto-attached)", pipelineName);
    }

    @SneakyThrows
    private Map<String, Object> searchWithPipeline(String index, String query, int size, String pipeline) {
        Request request = new Request("POST", "/" + index + "/_search");
        request.setJsonEntity(query);
        request.addParameter("size", Integer.toString(size));
        request.addParameter("search_pipeline", pipeline);

        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        String responseBody = org.apache.hc.core5.http.io.entity.EntityUtils.toString(response.getEntity());
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getHits(Map<String, Object> searchResponse) {
        Map<String, Object> hitsWrapper = (Map<String, Object>) searchResponse.get("hits");
        if (hitsWrapper == null) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> hits = (List<Map<String, Object>>) hitsWrapper.get("hits");
        return hits != null ? hits : Collections.emptyList();
    }

    private Float getDocumentScore(List<Map<String, Object>> hits, String documentId) {
        for (Map<String, Object> hit : hits) {
            String id = (String) hit.get("_id");
            if (documentId.equals(id)) {
                Object score = hit.get("_score");
                if (score instanceof Number) {
                    return ((Number) score).floatValue();
                }
            }
        }
        return null;
    }

    private void addDocument(String index, String id, Map<String, Object> fields) throws IOException {
        Request request = new Request("PUT", "/" + index + "/_doc/" + id);
        request.addParameter("refresh", "true");
        request.setJsonEntity(toJsonString(fields));
        client().performRequest(request);
    }

    private String toJsonString(Map<String, Object> map) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (!first) sb.append(",");
            sb.append("\"").append(entry.getKey()).append("\":\"").append(entry.getValue()).append("\"");
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }
}
