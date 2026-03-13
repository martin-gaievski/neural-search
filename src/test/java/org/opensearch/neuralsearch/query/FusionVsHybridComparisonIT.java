/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Locale;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

/**
 * Comparison test: Fusion query (shard-level RRF) vs Hybrid query + RRF processor (coordinator-level RRF).
 * Runs identical sub-queries through both approaches and compares ranking quality using:
 * - Overlap@K: percentage of docs in common at top-K
 * - Spearman's rank correlation
 * - NDCG@K against pre-computed ground truth from the JSON data
 *
 * For RRF, shard-level and coordinator-level should produce very similar rankings.
 * Differences arise only from shard-local rank assignment (docs distributed across 3 shards).
 */
@Log4j2
public class FusionVsHybridComparisonIT extends BaseNeuralSearchIT {

    private static final String TEST_INDEX = "test-comparison-index";
    private static final String TITLE_FIELD = "title";
    private static final String CATEGORY_FIELD = "category";
    private static final String PRICE_FIELD = "price";
    private static final int NUM_SHARDS = 3;
    private static final int TOTAL_DOCS = 300;
    private static final int RRF_K = 60;
    private static final String RRF_PIPELINE = RRF_SEARCH_PIPELINE;
    private static final String BULK_DATA_RESOURCE = "processor/fusion_test_bulk_data.json";

    // Parsed doc data for ground truth computation
    private List<Map<String, Object>> allDocs;

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
     * Compare fusion query vs hybrid+RRF for: match("neural") + term(category="technology")
     * Ground truth: 20 technology docs, 13 with "neural" in title (both-match).
     */
    @SneakyThrows
    public void testComparison_matchNeuralAndTermTechnology() {
        String fusionQuery = "{\n"
            + "  \"size\": 20,\n"
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

        String hybridQuery = "{\n"
            + "  \"size\": 20,\n"
            + "  \"query\": {\n"
            + "    \"hybrid\": {\n"
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

        // Compute ground truth: docs matching "neural" in title OR category=technology
        List<String> groundTruth = computeGroundTruthRRF(
            doc -> ((String) doc.get("title")).contains("neural"),
            doc -> "technology".equals(doc.get("category"))
        );

        compareAndAssert("matchNeural_termTechnology", fusionQuery, hybridQuery, groundTruth, 10);
        compareAndAssert("matchNeural_termTechnology", fusionQuery, hybridQuery, groundTruth, 20);
    }

    /**
     * Compare fusion query vs hybrid+RRF for: match("search") + term(category="science")
     * These are largely disjoint sets (search is mostly in technology titles, science is a different category).
     */
    @SneakyThrows
    public void testComparison_matchSearchAndTermScience() {
        String fusionQuery = "{\n"
            + "  \"size\": 20,\n"
            + "  \"query\": {\n"
            + "    \"fusion\": {\n"
            + "      \"queries\": [\n"
            + "        {\"match\": {\""
            + TITLE_FIELD
            + "\": \"search\"}},\n"
            + "        {\"term\": {\""
            + CATEGORY_FIELD
            + "\": \"science\"}}\n"
            + "      ]\n"
            + "    }\n"
            + "  }\n"
            + "}";

        String hybridQuery = "{\n"
            + "  \"size\": 20,\n"
            + "  \"query\": {\n"
            + "    \"hybrid\": {\n"
            + "      \"queries\": [\n"
            + "        {\"match\": {\""
            + TITLE_FIELD
            + "\": \"search\"}},\n"
            + "        {\"term\": {\""
            + CATEGORY_FIELD
            + "\": \"science\"}}\n"
            + "      ]\n"
            + "    }\n"
            + "  }\n"
            + "}";

        List<String> groundTruth = computeGroundTruthRRF(
            doc -> ((String) doc.get("title")).contains("search"),
            doc -> "science".equals(doc.get("category"))
        );

        compareAndAssert("matchSearch_termScience", fusionQuery, hybridQuery, groundTruth, 10);
        compareAndAssert("matchSearch_termScience", fusionQuery, hybridQuery, groundTruth, 20);
    }

    /**
     * Compare with range(price >= 450) + match("neural") — realistic e-commerce pattern.
     * Range matches ~34 docs (prices random 10-500, price >= 450), combined with focused
     * text search for "neural" (~37 docs). Both sets small enough for fair hybrid comparison.
     */
    @SneakyThrows
    public void testComparison_rangePriceAndMatchNeural() {
        String fusionQuery = "{\n"
            + "  \"size\": 20,\n"
            + "  \"query\": {\n"
            + "    \"fusion\": {\n"
            + "      \"queries\": [\n"
            + "        {\"range\": {\""
            + PRICE_FIELD
            + "\": {\"gte\": 450}}},\n"
            + "        {\"match\": {\""
            + TITLE_FIELD
            + "\": \"neural\"}}\n"
            + "      ]\n"
            + "    }\n"
            + "  }\n"
            + "}";

        String hybridQuery = "{\n"
            + "  \"size\": 20,\n"
            + "  \"query\": {\n"
            + "    \"hybrid\": {\n"
            + "      \"queries\": [\n"
            + "        {\"range\": {\""
            + PRICE_FIELD
            + "\": {\"gte\": 450}}},\n"
            + "        {\"match\": {\""
            + TITLE_FIELD
            + "\": \"neural\"}}\n"
            + "      ]\n"
            + "    }\n"
            + "  }\n"
            + "}";

        List<String> groundTruth = computeGroundTruthRRF(
            doc -> ((Number) doc.get("price")).intValue() >= 450,
            doc -> ((String) doc.get("title")).contains("neural")
        );

        compareAndAssert("rangePrice_matchNeural", fusionQuery, hybridQuery, groundTruth, 10);
        compareAndAssert("rangePrice_matchNeural", fusionQuery, hybridQuery, groundTruth, 20);
    }

    // ======================== Core Comparison Logic ========================

    @SneakyThrows
    private void compareAndAssert(String testName, String fusionQuery, String hybridQuery, List<String> groundTruth, int k) {
        // Run fusion query
        List<String> fusionIds = extractDocIds(searchRaw(TEST_INDEX, fusionQuery));

        // Run hybrid query with RRF pipeline
        List<String> hybridIds = extractDocIds(searchWithPipeline(TEST_INDEX, hybridQuery, RRF_PIPELINE));

        // Trim to K
        List<String> fusionTopK = fusionIds.subList(0, Math.min(k, fusionIds.size()));
        List<String> hybridTopK = hybridIds.subList(0, Math.min(k, hybridIds.size()));
        List<String> groundTruthTopK = groundTruth.subList(0, Math.min(k, groundTruth.size()));

        // Compute metrics
        double overlap = overlapAtK(fusionTopK, hybridTopK);
        double spearman = spearmanCorrelation(fusionTopK, hybridTopK);
        double ndcgFusion = ndcgAtK(fusionTopK, groundTruthTopK, k);
        double ndcgHybrid = ndcgAtK(hybridTopK, groundTruthTopK, k);

        // Log metrics
        log.info("=== {} @{} ===", testName, k);
        log.info("  Overlap@{}: {}", k, String.format(Locale.ROOT, "%.4f", overlap));
        log.info("  Spearman ρ: {}", String.format(Locale.ROOT, "%.4f", spearman));
        log.info("  NDCG@{} fusion:  {}", k, String.format(Locale.ROOT, "%.4f", ndcgFusion));
        log.info("  NDCG@{} hybrid:  {}", k, String.format(Locale.ROOT, "%.4f", ndcgHybrid));
        log.info("  NDCG difference: {}", String.format(Locale.ROOT, "%.4f", Math.abs(ndcgFusion - ndcgHybrid)));
        log.info("  Fusion top-{}: {}", k, fusionTopK);
        log.info("  Hybrid top-{}: {}", k, hybridTopK);
        log.info("  Ground truth top-{}: {}", k, groundTruthTopK);

        // Assertions — for RRF these should be very close
        assertTrue(testName + " Overlap@" + k + " should be >= 0.7, got " + overlap, overlap >= 0.7);
        // NDCG difference should be small — tightened based on 100-doc results
        assertTrue(
            testName + " NDCG difference should be < 0.15, got " + Math.abs(ndcgFusion - ndcgHybrid),
            Math.abs(ndcgFusion - ndcgHybrid) < 0.15
        );
    }

    // ======================== Ground Truth Computation ========================

    /**
     * Compute ideal single-shard RRF ranking from the document data.
     * For each sub-query predicate, identifies matching docs, assigns binary relevance (1/0),
     * sorts by relevance within each sub-query, assigns ranks, computes RRF scores.
     */
    private List<String> computeGroundTruthRRF(
        java.util.function.Predicate<Map<String, Object>> subQuery1Matcher,
        java.util.function.Predicate<Map<String, Object>> subQuery2Matcher
    ) {
        // Find matching docs for each sub-query
        List<String> sq1Matches = new ArrayList<>();
        List<String> sq2Matches = new ArrayList<>();

        for (Map<String, Object> doc : allDocs) {
            String id = (String) doc.get("id");
            if (subQuery1Matcher.test(doc)) sq1Matches.add(id);
            if (subQuery2Matcher.test(doc)) sq2Matches.add(id);
        }

        // Compute RRF scores (binary relevance = all matched docs get same score, rank by doc order)
        Map<String, Double> rrfScores = new HashMap<>();

        // Sub-query 1: assign ranks 1..N
        for (int rank = 0; rank < sq1Matches.size(); rank++) {
            rrfScores.merge(sq1Matches.get(rank), 1.0 / (RRF_K + rank + 1), Double::sum);
        }

        // Sub-query 2: assign ranks 1..N
        for (int rank = 0; rank < sq2Matches.size(); rank++) {
            rrfScores.merge(sq2Matches.get(rank), 1.0 / (RRF_K + rank + 1), Double::sum);
        }

        // Sort by RRF score desc, then by ID for tie-breaking
        List<Map.Entry<String, Double>> sorted = new ArrayList<>(rrfScores.entrySet());
        sorted.sort((a, b) -> {
            int cmp = Double.compare(b.getValue(), a.getValue());
            return cmp != 0 ? cmp : a.getKey().compareTo(b.getKey());
        });

        List<String> result = new ArrayList<>();
        for (Map.Entry<String, Double> entry : sorted) {
            result.add(entry.getKey());
        }
        return result;
    }

    // ======================== Metrics ========================

    /**
     * Overlap@K: fraction of docs in common between two ranked lists at position K.
     */
    private double overlapAtK(List<String> list1, List<String> list2) {
        Set<String> set1 = new HashSet<>(list1);
        Set<String> set2 = new HashSet<>(list2);
        set1.retainAll(set2);
        return (double) set1.size() / Math.max(list1.size(), list2.size());
    }

    /**
     * Spearman's rank correlation between two ranked lists.
     * Only considers docs that appear in both lists.
     */
    private double spearmanCorrelation(List<String> list1, List<String> list2) {
        // Find common docs
        Set<String> common = new HashSet<>(list1);
        common.retainAll(new HashSet<>(list2));
        if (common.size() < 2) return 0.0;

        // Map doc -> rank in each list
        Map<String, Integer> rank1 = new HashMap<>();
        Map<String, Integer> rank2 = new HashMap<>();
        for (int i = 0; i < list1.size(); i++)
            rank1.put(list1.get(i), i + 1);
        for (int i = 0; i < list2.size(); i++)
            rank2.put(list2.get(i), i + 1);

        // Compute Spearman's rho: 1 - 6*Σd²/(n*(n²-1))
        double sumD2 = 0;
        for (String doc : common) {
            double d = rank1.get(doc) - rank2.get(doc);
            sumD2 += d * d;
        }
        int n = common.size();
        return 1.0 - (6.0 * sumD2) / (n * ((long) n * n - 1));
    }

    /**
     * NDCG@K: Normalized Discounted Cumulative Gain.
     * Uses ground truth ranking to assign relevance: position in ground truth = relevance score.
     */
    private double ndcgAtK(List<String> results, List<String> groundTruth, int k) {
        // Assign relevance based on position in ground truth (higher position = more relevant)
        Map<String, Double> relevance = new HashMap<>();
        for (int i = 0; i < groundTruth.size(); i++) {
            // Relevance = N - rank (so top doc gets highest relevance)
            relevance.put(groundTruth.get(i), (double) (groundTruth.size() - i));
        }

        // DCG of actual results
        double dcg = 0;
        for (int i = 0; i < Math.min(k, results.size()); i++) {
            double rel = relevance.getOrDefault(results.get(i), 0.0);
            dcg += rel / (Math.log(i + 2) / Math.log(2)); // log2(i+2) since i is 0-based
        }

        // Ideal DCG (ground truth order)
        double idcg = 0;
        for (int i = 0; i < Math.min(k, groundTruth.size()); i++) {
            double rel = groundTruth.size() - i; // decreasing relevance
            idcg += rel / (Math.log(i + 2) / Math.log(2));
        }

        return idcg > 0 ? dcg / idcg : 0.0;
    }

    // ======================== Helper Methods ========================

    @SneakyThrows
    private void initializeIndexIfNotExist() {
        if (indexExists(TEST_INDEX)) {
            // Still need to parse docs for ground truth
            parseBulkData();
            return;
        }

        createIndexWithConfiguration(
            TEST_INDEX,
            buildIndexConfiguration(List.of(), Map.of(), List.of(PRICE_FIELD), List.of(CATEGORY_FIELD), List.of(), NUM_SHARDS),
            ""
        );

        String bulkPayload = Files.readString(Path.of(classLoader.getResource(BULK_DATA_RESOURCE).toURI()));
        Request bulkRequest = new Request("POST", "/" + TEST_INDEX + "/_bulk?refresh=true");
        bulkRequest.setJsonEntity(bulkPayload);
        Response bulkResponse = client().performRequest(bulkRequest);
        assertEquals(RestStatus.OK, RestStatus.fromCode(bulkResponse.getStatusLine().getStatusCode()));
        assertEquals(TOTAL_DOCS, getDocCount(TEST_INDEX));

        // Create RRF search pipeline for hybrid query
        createDefaultRRFSearchPipeline();

        parseBulkData();
    }

    @SneakyThrows
    private void parseBulkData() {
        allDocs = new ArrayList<>();
        String content = Files.readString(Path.of(classLoader.getResource(BULK_DATA_RESOURCE).toURI()));
        String[] lines = content.split("\n");
        for (int i = 0; i < lines.length - 1; i += 2) {
            // lines[i] = {"index":{"_id":"1"}}
            // lines[i+1] = {"title":"...","category":"...","price":...}
            Map<String, Object> meta = XContentHelper.convertToMap(XContentType.JSON.xContent(), lines[i], false);
            Map<String, Object> indexMeta = (Map<String, Object>) meta.get("index");
            String id = (String) indexMeta.get("_id");

            Map<String, Object> doc = XContentHelper.convertToMap(XContentType.JSON.xContent(), lines[i + 1], false);
            doc.put("id", id);
            allDocs.add(doc);
        }
    }

    @SneakyThrows
    private Map<String, Object> searchRaw(String index, String query) {
        Request request = new Request("POST", "/" + index + "/_search");
        request.setJsonEntity(query);
        request.addParameter("search_type", "query_then_fetch");
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    @SneakyThrows
    private Map<String, Object> searchWithPipeline(String index, String query, String pipeline) {
        Request request = new Request("POST", "/" + index + "/_search");
        request.setJsonEntity(query);
        request.addParameter("search_type", "query_then_fetch");
        request.addParameter("search_pipeline", pipeline);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
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
}
