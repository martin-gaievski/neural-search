/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import lombok.SneakyThrows;

/**
 * Integration tests for the fusion query type with shard-level RRF scoring.
 * Uses 3 shards and 300 documents for realistic distributed testing (~100 docs/shard).
 * Test data is loaded from a bulk JSON resource file with simple field types
 * (text, keyword, integer) — no vectors or ML models needed.
 *
 * Data distribution (300 docs, deterministic seed=42):
 *   - 5 categories: technology(60), science(60), devops(60), business(60), education(60)
 *   - 37 docs with "neural" in title (all in technology category)
 *   - 64 docs with "search" in title
 *   - Prices range from 10 to 500
 *
 * RRF parameters: k=60 (hardcoded), 2 sub-queries per test.
 * With 300 docs across 3 shards (~100 docs/shard avg):
 *   - Min possible score: 1/(60+300) ≈ 0.00278  (worst rank, single sub-query, all docs one shard)
 *   - Max possible score: 2/(60+1)   ≈ 0.03279  (best rank in both sub-queries)
 */
public class FusionQueryIT extends BaseNeuralSearchIT {

    private static final String TEST_INDEX = "test-fusion-index";
    private static final String TITLE_FIELD = "title";
    private static final String CATEGORY_FIELD = "category";
    private static final String PRICE_FIELD = "price";
    private static final int NUM_SHARDS = 3;
    private static final int TOTAL_DOCS = 300;

    // RRF score bounds for 2 sub-queries, k=60, up to 300 docs per shard (worst case)
    private static final double RRF_MIN_SCORE = 1.0 / (60 + TOTAL_DOCS);
    private static final double RRF_MAX_SCORE = 2.0 / (60 + 1);
    private static final double SCORE_DELTA = 0.0001;

    // Known data distribution (300 docs, deterministic seed=42)
    private static final int TECHNOLOGY_COUNT = 60;
    private static final int SCIENCE_COUNT = 60;
    private static final int DEVOPS_COUNT = 60;
    private static final int BUSINESS_COUNT = 60;
    private static final int EDUCATION_COUNT = 60;
    private static final int NEURAL_IN_TITLE_COUNT = 37;  // all are also technology

    private static final String BULK_DATA_RESOURCE = "processor/fusion_test_bulk_data.json";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
        initializeIndexIfNotExist();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    /**
     * Test basic fusion query with match("neural") + term(category="technology").
     *
     * Sub-query 1 - match "neural": 37 docs with "neural" in title
     * Sub-query 2 - term category=technology: 60 technology docs
     * Both-match docs: 37 docs (all neural docs are technology)
     * Union total: 60 docs (technology superset includes all neural docs)
     */
    @SneakyThrows
    public void testBasicFusionQuery_thenSuccessful() {
        String query = "{\n"
            + "  \"size\": 100,\n"
            + "  \"query\": {\n"
            + "    \"fusion\": {\n"
            + "      \"queries\": [\n"
            + "        {\"match\": {\""
            + TITLE_FIELD
            + "\": \"neural\"}},\n"
            + "        {\"term\": {\""
            + CATEGORY_FIELD
            + "\": \"technology\"}}\n"
            + "      ]\n"
            + "    }\n"
            + "  }\n"
            + "}";

        Map<String, Object> response = searchRaw(TEST_INDEX, query);

        // Total hits = technology category (20 docs, superset of all neural docs)
        int totalHits = extractTotalHitsValue(response);
        assertEquals("Total hits should be technology count (superset of neural docs)", TECHNOLOGY_COUNT, totalHits);

        int hitCount = getHitCount(response);
        assertEquals("Returned hits should equal total hits", totalHits, hitCount);

        // All doc ids should be unique
        List<String> ids = extractDocIds(response);
        assertEquals("All doc ids should be unique", Set.copyOf(ids).size(), ids.size());

        // Scores in descending order
        List<Double> scores = extractScores(response);
        assertTrue(
            "Scores should be in descending order",
            IntStream.range(0, scores.size() - 1).noneMatch(i -> scores.get(i) < scores.get(i + 1))
        );

        // All scores within valid RRF bounds
        for (Double score : scores) {
            assertTrue("Score " + score + " below RRF minimum", score >= RRF_MIN_SCORE - SCORE_DELTA);
            assertTrue("Score " + score + " above RRF maximum", score <= RRF_MAX_SCORE + SCORE_DELTA);
        }

        // Both-match docs (neural + technology, 13 docs) should score higher than
        // single-match docs (technology-only, 7 docs without neural in title)
        // Both-match docs get RRF contribution from both sub-queries
        double minBothMatchScore = Double.MAX_VALUE;
        double maxSingleMatchScore = 0;
        int bothMatchCount = 0;
        int singleMatchCount = 0;

        // Neural doc IDs (37 docs with "neural" in title, all technology)
        Set<String> neuralDocIds = new HashSet<>(
            Arrays.asList(
                "1",
                "6",
                "11",
                "21",
                "31",
                "41",
                "51",
                "56",
                "61",
                "71",
                "76",
                "81",
                "91",
                "101",
                "106",
                "111",
                "121",
                "131",
                "141",
                "151",
                "161",
                "171",
                "181",
                "191",
                "196",
                "201",
                "211",
                "221",
                "231",
                "241",
                "251",
                "256",
                "261",
                "271",
                "281",
                "291",
                "296"
            )
        );

        for (int i = 0; i < ids.size(); i++) {
            double score = scores.get(i);
            if (neuralDocIds.contains(ids.get(i))) {
                minBothMatchScore = Math.min(minBothMatchScore, score);
                bothMatchCount++;
            } else {
                maxSingleMatchScore = Math.max(maxSingleMatchScore, score);
                singleMatchCount++;
            }
        }

        assertTrue("Should have both-match docs", bothMatchCount > 0);
        assertTrue("Should have single-match docs", singleMatchCount > 0);
        assertTrue(
            "Both-match docs (min=" + minBothMatchScore + ") should score higher than single-match (max=" + maxSingleMatchScore + ")",
            minBothMatchScore > maxSingleMatchScore
        );
    }

    /**
     * Test fusion query with non-matching terms returns empty results.
     */
    @SneakyThrows
    public void testFusionQuery_withNoMatchingDocs_thenEmptyResults() {
        String query = "{\n"
            + "  \"query\": {\n"
            + "    \"fusion\": {\n"
            + "      \"queries\": [\n"
            + "        {\"term\": {\""
            + TITLE_FIELD
            + "\": \"zzzznonexistent\"}},\n"
            + "        {\"term\": {\""
            + CATEGORY_FIELD
            + "\": \"nonexistentcategory\"}}\n"
            + "      ]\n"
            + "    }\n"
            + "  }\n"
            + "}";

        Map<String, Object> response = search(TEST_INDEX, query, 10);
        assertEquals("Expected 0 hits for non-matching query", 0, getHitCount(response));
        assertEquals("Expected 0 total hits", 0, extractTotalHitsValue(response));
    }

    /**
     * Test RRF combination with disjoint results.
     * match "neural" (37 docs, all technology) + term category="science" (60 docs)
     * Both-match: 0 (no neural docs in science)
     * Union: 37 + 60 = 97 docs
     * All docs match only one sub-query, so all have single-query RRF scores.
     */
    @SneakyThrows
    public void testFusionQuery_withDisjointResults_thenAllSingleMatch() {
        String query = "{\n"
            + "  \"size\": 100,\n"
            + "  \"query\": {\n"
            + "    \"fusion\": {\n"
            + "      \"queries\": [\n"
            + "        {\"match\": {\""
            + TITLE_FIELD
            + "\": \"neural\"}},\n"
            + "        {\"term\": {\""
            + CATEGORY_FIELD
            + "\": \"science\"}}\n"
            + "      ]\n"
            + "    }\n"
            + "  }\n"
            + "}";

        Map<String, Object> response = searchRaw(TEST_INDEX, query);

        // Union: neural(13, all tech) + science(20) = 33 (disjoint sets)
        int totalHits = extractTotalHitsValue(response);
        assertEquals("Total hits should be neural + science (disjoint)", NEURAL_IN_TITLE_COUNT + SCIENCE_COUNT, totalHits);

        List<Double> scores = extractScores(response);
        // All scores are single-match (≤ 1/(60+1) for best single-query rank)
        double singleMatchMax = 1.0 / (60 + 1);
        for (Double score : scores) {
            assertTrue("Score " + score + " should not exceed single-match max " + singleMatchMax, score <= singleMatchMax + SCORE_DELTA);
            assertTrue("Score " + score + " below RRF minimum", score >= RRF_MIN_SCORE - SCORE_DELTA);
        }
    }

    /**
     * Test from/size pagination with fusion query.
     * match_all matches all 300 docs + term matches 60 technology docs.
     * Union = 300 docs (match_all covers everything).
     */
    @SneakyThrows
    public void testFusionQuery_withFromSize_thenPaginationWorks() {
        // First get first page (size 10)
        String queryAll = "{\n"
            + "  \"size\": 20,\n"
            + "  \"query\": {\n"
            + "    \"fusion\": {\n"
            + "      \"queries\": [\n"
            + "        {\"match_all\": {}},\n"
            + "        {\"term\": {\""
            + CATEGORY_FIELD
            + "\": \"technology\"}}\n"
            + "      ]\n"
            + "    }\n"
            + "  }\n"
            + "}";
        Map<String, Object> allResponse = searchRaw(TEST_INDEX, queryAll);
        int totalHits = extractTotalHitsValue(allResponse);
        assertEquals("match_all + term should return all docs", TOTAL_DOCS, totalHits);

        List<String> firstPageIds = extractDocIds(allResponse);

        // Get page 2 with from=5, size=5
        String queryPaged = "{\n"
            + "  \"from\": 5,\n"
            + "  \"size\": 5,\n"
            + "  \"query\": {\n"
            + "    \"fusion\": {\n"
            + "      \"queries\": [\n"
            + "        {\"match_all\": {}},\n"
            + "        {\"term\": {\""
            + CATEGORY_FIELD
            + "\": \"technology\"}}\n"
            + "      ]\n"
            + "    }\n"
            + "  }\n"
            + "}";
        Map<String, Object> pagedResponse = searchRaw(TEST_INDEX, queryPaged);
        assertEquals("Expected exactly 5 results", 5, getHitCount(pagedResponse));
        assertEquals("Total hits should match", TOTAL_DOCS, extractTotalHitsValue(pagedResponse));

        // Paged results should match positions 5-9 of full result set
        List<String> pagedIds = extractDocIds(pagedResponse);
        for (int i = 0; i < pagedIds.size(); i++) {
            assertEquals(
                "Paged doc at position " + i + " should match full results at offset position",
                firstPageIds.get(i + 5),
                pagedIds.get(i)
            );
        }
    }

    /**
     * Test sort by integer field with fusion query.
     * match_all + term covers all 300 docs.
     */
    @SneakyThrows
    public void testFusionQuery_withSortByField_thenSortedCorrectly() {
        String query = "{\n"
            + "  \"size\": 20,\n"
            + "  \"query\": {\n"
            + "    \"fusion\": {\n"
            + "      \"queries\": [\n"
            + "        {\"match_all\": {}},\n"
            + "        {\"term\": {\""
            + CATEGORY_FIELD
            + "\": \"technology\"}}\n"
            + "      ]\n"
            + "    }\n"
            + "  },\n"
            + "  \"sort\": [{\""
            + PRICE_FIELD
            + "\": {\"order\": \"desc\"}}]\n"
            + "}";

        Map<String, Object> response = searchRaw(TEST_INDEX, query);
        assertEquals("Total hits should match", TOTAL_DOCS, extractTotalHitsValue(response));

        // Verify prices are in descending order
        List<Map<String, Object>> hits = extractNestedHits(response);
        List<Integer> prices = new ArrayList<>();
        for (Map<String, Object> hit : hits) {
            Map<String, Object> source = (Map<String, Object>) hit.get("_source");
            if (source != null && source.get(PRICE_FIELD) != null) {
                prices.add((Integer) source.get(PRICE_FIELD));
            }
        }
        assertTrue("Expected at least 10 price values", prices.size() >= 10);
        assertTrue(
            "Prices should be in descending order",
            IntStream.range(0, prices.size() - 1).noneMatch(i -> prices.get(i) < prices.get(i + 1))
        );
    }

    /**
     * Test aggregation with fusion query.
     * match_all + match "search" covers all 300 docs (match_all matches everything).
     */
    @SneakyThrows
    public void testFusionQuery_withAggregation_thenAggregationWorks() {
        String query = "{\n"
            + "  \"size\": 0,\n"
            + "  \"query\": {\n"
            + "    \"fusion\": {\n"
            + "      \"queries\": [\n"
            + "        {\"match_all\": {}},\n"
            + "        {\"match\": {\""
            + TITLE_FIELD
            + "\": \"search\"}}\n"
            + "      ]\n"
            + "    }\n"
            + "  },\n"
            + "  \"aggs\": {\n"
            + "    \"categories\": {\n"
            + "      \"terms\": {\"field\": \""
            + CATEGORY_FIELD
            + "\", \"size\": 10}\n"
            + "    }\n"
            + "  }\n"
            + "}";

        Map<String, Object> response = searchRaw(TEST_INDEX, query);
        assertEquals("Total hits should match", TOTAL_DOCS, extractTotalHitsValue(response));

        // Verify aggregation buckets
        Map<String, Object> aggs = (Map<String, Object>) response.get("aggregations");
        assertNotNull("Aggregations should be present", aggs);
        Map<String, Object> categories = (Map<String, Object>) aggs.get("categories");
        assertNotNull("Categories aggregation should be present", categories);
        List<Map<String, Object>> buckets = (List<Map<String, Object>>) categories.get("buckets");
        assertNotNull("Buckets should be present", buckets);
        assertEquals("Should have 5 category buckets", 5, buckets.size());

        // Each category should have 60 docs (300 / 5 categories)
        for (Map<String, Object> bucket : buckets) {
            int docCount = ((Number) bucket.get("doc_count")).intValue();
            assertEquals("Each category should have 60 docs", 60, docCount);
        }
    }

    /**
     * Test profiler with fusion query — scores should be correct RRF scores even with profiling.
     */
    @SneakyThrows
    public void testFusionQuery_withProfileEnabled_thenProfileDataReturned() {
        String query = "{\n"
            + "  \"query\": {\n"
            + "    \"fusion\": {\n"
            + "      \"queries\": [\n"
            + "        {\"match\": {\""
            + TITLE_FIELD
            + "\": \"neural\"}},\n"
            + "        {\"term\": {\""
            + CATEGORY_FIELD
            + "\": \"technology\"}}\n"
            + "      ]\n"
            + "    }\n"
            + "  },\n"
            + "  \"profile\": true\n"
            + "}";

        Map<String, Object> response = searchRaw(TEST_INDEX, query);

        int hitCount = getHitCount(response);
        assertTrue("Expected at least 1 hit", hitCount >= 1);

        // Verify all returned scores are in valid RRF range (not raw BM25)
        List<Double> scores = extractScores(response);
        for (Double score : scores) {
            assertTrue("Score " + score + " below RRF minimum", score >= RRF_MIN_SCORE - SCORE_DELTA);
            assertTrue("Score " + score + " above RRF maximum (got raw BM25?)", score <= RRF_MAX_SCORE + SCORE_DELTA);
        }

        // Verify profile data structure
        assertNotNull("Profile data should be present", response.get("profile"));
        Map<String, Object> profile = (Map<String, Object>) response.get("profile");
        assertNotNull("Profile shards should be present", profile.get("shards"));

        List<Map<String, Object>> shards = (List<Map<String, Object>>) profile.get("shards");
        assertFalse("Profile shards should not be empty", shards.isEmpty());

        for (Map<String, Object> shard : shards) {
            List<Map<String, Object>> searches = (List<Map<String, Object>>) shard.get("searches");
            assertNotNull("Shard should have searches", searches);
            assertFalse("Shard searches should not be empty", searches.isEmpty());
        }
    }

    // ======================== Helper Methods ========================

    @SneakyThrows
    private void initializeIndexIfNotExist() {
        if (indexExists(TEST_INDEX)) {
            return;
        }
        // Create index with text, keyword, and integer fields — 3 shards
        createIndexWithConfiguration(
            TEST_INDEX,
            buildIndexConfiguration(
                List.of(),                 // no KNN fields
                Map.of(),                  // no nested fields
                List.of(PRICE_FIELD),      // integer fields
                List.of(CATEGORY_FIELD),   // keyword fields
                List.of(),                 // no date fields
                NUM_SHARDS
            ),
            ""
        );

        // Bulk ingest 100 documents from resource file
        String bulkPayload = Files.readString(Path.of(classLoader.getResource(BULK_DATA_RESOURCE).toURI()));
        Request bulkRequest = new Request("POST", "/" + TEST_INDEX + "/_bulk?refresh=true");
        bulkRequest.setJsonEntity(bulkPayload);
        Response bulkResponse = client().performRequest(bulkRequest);
        assertEquals(RestStatus.OK, RestStatus.fromCode(bulkResponse.getStatusLine().getStatusCode()));

        // Verify all docs ingested
        assertEquals(TOTAL_DOCS, getDocCount(TEST_INDEX));
    }

    /**
     * Execute a raw search request with full JSON body.
     */
    @SneakyThrows
    private Map<String, Object> searchRaw(String index, String query) {
        Request request = new Request("POST", "/" + index + "/_search");
        request.setJsonEntity(query);
        request.addParameter("search_type", "query_then_fetch");
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        String responseBody = EntityUtils.toString(response.getEntity());
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);
    }

    @SuppressWarnings("unchecked")
    private int extractTotalHitsValue(Map<String, Object> searchResponse) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponse.get("hits");
        Map<String, Object> total = (Map<String, Object>) hitsMap.get("total");
        return ((Number) total.get("value")).intValue();
    }

    @SuppressWarnings("unchecked")
    private List<Double> extractScores(Map<String, Object> searchResponse) {
        List<Double> scores = new ArrayList<>();
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponse.get("hits");
        List<Map<String, Object>> hitsList = (List<Map<String, Object>>) hitsMap.get("hits");
        for (Map<String, Object> hit : hitsList) {
            Object score = hit.get("_score");
            if (score != null) {
                scores.add(((Number) score).doubleValue());
            }
        }
        return scores;
    }

    @SuppressWarnings("unchecked")
    private List<String> extractDocIds(Map<String, Object> searchResponse) {
        List<String> ids = new ArrayList<>();
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponse.get("hits");
        List<Map<String, Object>> hitsList = (List<Map<String, Object>>) hitsMap.get("hits");
        for (Map<String, Object> hit : hitsList) {
            ids.add((String) hit.get("_id"));
        }
        return ids;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> extractNestedHits(Map<String, Object> searchResponse) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponse.get("hits");
        return (List<Map<String, Object>>) hitsMap.get("hits");
    }
}
