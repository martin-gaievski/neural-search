/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import lombok.SneakyThrows;

/**
 * Discriminating integration tests for AGGREGATION MATCH-SET COVERAGE under hybrid {@code mode:"fused"} vs classic
 * hybrid. Answers one question empirically: does a fused/resolver hybrid compute aggregations over the FULL leg-union
 * match set (like classic) or only over the fused top-K window (a regression)?
 *
 * <p><b>Why this test can tell the difference (existing agg ITs cannot).</b> The classic {@code HybridQueryAggregationsIT}
 * uses ~5-7 docs, so window==union and it passes either way. Here we deliberately arrange <b>matching docs (30) ≫
 * rank_window_size (5)</b>, and place a "loser" bucket ({@code color:red}) on docs engineered to score BELOW the fused
 * window on every leg. Then:
 * <ul>
 *   <li>if aggregations see the full union  → {@code red} count = 5 (correct, classic behavior);</li>
 *   <li>if aggregations see only the window → {@code red} count = 0 (windowed regression).</li>
 * </ul>
 * {@code red==5} vs {@code red==0} is the smoking gun; {@code hits.total.value} (30 vs 5) is the corroborating signal.
 *
 * <p><b>Determinism on a multi-node cluster.</b> Legs are {@code function_score} scored by a numeric field ({@code s}),
 * NOT BM25 — so the fused top-K window is the 5 highest-{@code s} docs regardless of how the 3 shards are spread across
 * nodes (no per-shard IDF variance). Run with {@code -PnumNodes=2} (or 3) so the query+aggregation phase is genuinely
 * distributed and the single-shard {@code HybridAggregationProcessor.postProcess} short-circuit is NOT exercised.
 *
 * <p>Dataset (30 docs, ids 1..30), tiered so both legs rank the 5 blue docs into the window and never the red docs:
 * <pre>
 *   color:  ids 1-5  = "blue"  (5 docs, tier 3 — always in the fused top-5 window)
 *           ids 6-25 = "green" (20 docs, tier 2 — always outside the window)
 *           ids 26-30= "red"   (5 docs, tier 1 — lowest, guaranteed outside the window)
 *   s (int) = tier*1000 - id     (blue 2999..2995 > green 1994..1975 > red 974..970)
 *   rank_window_size = 5  => fused window = {1,2,3,4,5} = all blue
 * </pre>
 * Expected {@code terms(color)} if aggregations cover the union: {@code blue=5, green=20, red=5}, total=30.
 */
public class HybridQueryFusedModeAggregationsIT extends BaseNeuralSearchIT {

    private static final String INDEX = "test-fused-aggs-matchset";
    private static final String NORM_PIPELINE = "fused-aggs-norm-pipeline";
    private static final String COLOR_FIELD = "color";
    private static final String SCORE_FIELD = "s";
    private static final int RANK_WINDOW = 5;

    private String indexConfig() {
        return "{\"settings\":{\"number_of_shards\":3,\"number_of_replicas\":0,"
            + "\"index.search.default_pipeline\":\""
            + NORM_PIPELINE
            + "\"},"
            + "\"mappings\":{\"properties\":{\""
            + COLOR_FIELD
            + "\":{\"type\":\"keyword\"},\""
            + SCORE_FIELD
            + "\":{\"type\":\"integer\"}}}}";
    }

    @SneakyThrows
    private void ensureDataset() {
        createSearchPipeline(NORM_PIPELINE, "min_max", "arithmetic_mean", Map.of());
        if (indexExists(INDEX)) {
            return;
        }
        createIndex(INDEX, indexConfig());
        for (int id = 1; id <= 30; id++) {
            String color = id <= 5 ? "blue" : (id <= 25 ? "green" : "red");
            int tier = id <= 5 ? 3 : (id <= 25 ? 2 : 1);
            int s = tier * 1000 - id;
            String doc = "{\"" + COLOR_FIELD + "\":\"" + color + "\",\"" + SCORE_FIELD + "\":" + s + "}";
            Request request = new Request("PUT", "/" + INDEX + "/_doc/" + id + "?refresh=true");
            request.setJsonEntity(doc);
            Response response = client().performRequest(request);
            int code = response.getStatusLine().getStatusCode();
            assertTrue(
                "indexing doc " + id + " failed: " + code,
                code == RestStatus.OK.getStatus() || code == RestStatus.CREATED.getStatus()
            );
        }
    }

    /** A leg that matches ALL docs and scores each by the numeric field {@code s} (deterministic, shard-independent). */
    private String leg() {
        return "{\"function_score\":{\"query\":{\"match_all\":{}},"
            + "\"field_value_factor\":{\"field\":\""
            + SCORE_FIELD
            + "\",\"modifier\":\"none\",\"missing\":1}}}";
    }

    private String termsAgg() {
        return "\"aggregations\":{\"by_color\":{\"terms\":{\"field\":\"" + COLOR_FIELD + "\",\"size\":10}}}";
    }

    // ---------------------------------------------------------------------------------------------
    // Test 1 (ORACLE): classic hybrid aggregations cover the full leg-union. Establishes the baseline
    // that a correct hybrid reports blue=5, green=20, red=5, total=30.
    // ---------------------------------------------------------------------------------------------
    @SneakyThrows
    public void testClassicHybrid_aggregationsCoverFullUnion() {
        ensureDataset();
        String body = "{\"query\":{\"hybrid\":{\"queries\":[" + leg() + "," + leg() + "]}}," + termsAgg() + ",\"track_total_hits\":true}";
        Map<String, Object> resp = searchRaw(body, 10);
        Map<String, Integer> buckets = colorBuckets(resp);
        long total = totalHits(resp);
        logResult("CLASSIC", buckets, total, resp);

        assertEquals("classic: blue", Integer.valueOf(5), buckets.getOrDefault("blue", 0));
        assertEquals("classic: green (proves aggs see beyond the window)", Integer.valueOf(20), buckets.getOrDefault("green", 0));
        assertEquals("classic: red (the loser bucket outside the window)", Integer.valueOf(5), buckets.getOrDefault("red", 0));
        assertEquals("classic: total_hits", 30L, total);
    }

    // ---------------------------------------------------------------------------------------------
    // Test 2 (CORE REGRESSION TEST): fused hybrid must produce the SAME aggregations as classic — i.e.
    // the Tail filter puts the full union into the self-erased query.
    // ---------------------------------------------------------------------------------------------
    @SneakyThrows
    public void testFusedHybrid_aggregationsCoverFullUnion_notJustWindow() {
        ensureDataset();
        String body = "{\"query\":{\"hybrid\":{\"mode\":\"fused\",\"rank_window_size\":"
            + RANK_WINDOW
            + ",\"queries\":["
            + leg()
            + ","
            + leg()
            + "]}},"
            + termsAgg()
            + ",\"track_total_hits\":true}";
        Map<String, Object> resp = searchRaw(body, 10);
        Map<String, Integer> buckets = colorBuckets(resp);
        long total = totalHits(resp);
        logResult("FUSED", buckets, total, resp);

        // The decisive assertions: if fused windowed the aggregation, green/red would be 0 and total ~5.
        assertEquals("fused: blue", Integer.valueOf(5), buckets.getOrDefault("blue", 0));
        assertEquals("fused: green must equal classic (full union, NOT windowed)", Integer.valueOf(20), buckets.getOrDefault("green", 0));
        assertEquals("fused: red (loser bucket) must equal classic — the smoking gun", Integer.valueOf(5), buckets.getOrDefault("red", 0));
        assertEquals("fused: total_hits must be the full match set", 30L, total);
    }

    // ---------------------------------------------------------------------------------------------
    // Test 3: fused + track_total_hits:false. Code path: requiresExecutionTail (aggs present) is checked
    // BEFORE the track_total_hits branch, so the Tail must be retained and aggregations stay full
    // even though the user asked not to track totals.
    // ---------------------------------------------------------------------------------------------
    @SneakyThrows
    public void testFusedHybrid_trackTotalHitsFalse_aggregationsStillFull() {
        ensureDataset();
        String body = "{\"query\":{\"hybrid\":{\"mode\":\"fused\",\"rank_window_size\":"
            + RANK_WINDOW
            + ",\"queries\":["
            + leg()
            + ","
            + leg()
            + "]}},"
            + termsAgg()
            + ",\"track_total_hits\":false}";
        Map<String, Object> resp = searchRaw(body, 10);
        Map<String, Integer> buckets = colorBuckets(resp);
        logResult("FUSED+tth:false", buckets, -1, resp);

        assertEquals("fused tth:false: green must still be full union", Integer.valueOf(20), buckets.getOrDefault("green", 0));
        assertEquals("fused tth:false: red must still be full union", Integer.valueOf(5), buckets.getOrDefault("red", 0));
    }

    // ---------------------------------------------------------------------------------------------
    // Test 4 (CAVEAT DISCOVERY): fused + min_score>0. The Tail docs match only the non-scoring filter, so
    // they score 0. If min_score>0 drops them before aggregation, red collapses to 0 (aggregation
    // silently windows). This test records ACTUAL behavior — the predicted outcome is red=0.
    // ---------------------------------------------------------------------------------------------
    @SneakyThrows
    public void testFusedHybrid_minScorePositive_dropsTailFromAggregations() {
        ensureDataset();
        String body = "{\"query\":{\"hybrid\":{\"mode\":\"fused\",\"rank_window_size\":"
            + RANK_WINDOW
            + ",\"queries\":["
            + leg()
            + ","
            + leg()
            + "]}},"
            + termsAgg()
            + ",\"min_score\":0.0005,\"track_total_hits\":true}";
        Map<String, Object> resp = searchRaw(body, 10);
        Map<String, Integer> buckets = colorBuckets(resp);
        long total = totalHits(resp);
        logResult("FUSED+min_score", buckets, total, resp);

        // PREDICTION (from code read): score-0 Tail docs are filtered out by min_score, so the aggregation
        // shrinks to the fused window -> green=0, red=0, only blue survives. If this assertion FAILS with
        // red=5, that is the good-news finding that min_score does NOT window aggregations.
        assertEquals("fused min_score: blue (window) survives", Integer.valueOf(5), buckets.getOrDefault("blue", 0));
        assertEquals("fused min_score: green dropped (score-0 tail filtered)", Integer.valueOf(0), buckets.getOrDefault("green", 0));
        assertEquals("fused min_score: red dropped (score-0 tail filtered)", Integer.valueOf(0), buckets.getOrDefault("red", 0));
    }

    // ---------------------------------------------------------------------------------------------
    // Test 5 (DEPTH-INDEPENDENT TAIL): a fused hybrid NESTED inside a bool keeps the Tail when the request
    // carries an aggregation (the Tail decision is depth-independent), so the aggregation covers the FULL
    // leg-union — same as the top-level case — NOT just the fused window. Classic hybrid cannot be nested
    // at all (top-level-only guard), so this is new capability with correct (full-union) aggregations.
    // ---------------------------------------------------------------------------------------------
    @SneakyThrows
    public void testFusedHybrid_nestedInBool_aggregationsCoverFullUnion() {
        ensureDataset();
        String body = "{\"query\":{\"bool\":{\"must\":[{\"hybrid\":{\"mode\":\"fused\",\"rank_window_size\":"
            + RANK_WINDOW
            + ",\"fusion\":{\"normalization\":{\"technique\":\"min_max\"},"
            + "\"combination\":{\"technique\":\"arithmetic_mean\",\"parameters\":{\"weights\":[0.5,0.5]}}},"
            + "\"queries\":["
            + leg()
            + ","
            + leg()
            + "]}}]}},"
            + termsAgg()
            + ",\"track_total_hits\":true}";
        Map<String, Object> resp;
        try {
            resp = searchRaw(body, 10);
        } catch (Exception e) {
            System.out.println("RESULT[FUSED-nested]: THREW " + e.getClass().getSimpleName() + ": " + e.getMessage());
            throw e;
        }
        Map<String, Integer> buckets = colorBuckets(resp);
        long total = totalHits(resp);
        logResult("FUSED-nested", buckets, total, resp);

        // Depth-independent Tail: nested + aggregation keeps the Tail => aggregation covers the full union,
        // identical to the top-level case => blue=5, green=20, red=5, total=30.
        assertEquals("nested fused: blue", Integer.valueOf(5), buckets.getOrDefault("blue", 0));
        assertEquals("nested fused: green (full union via depth-independent Tail)", Integer.valueOf(20), buckets.getOrDefault("green", 0));
        assertEquals("nested fused: red (full union via depth-independent Tail)", Integer.valueOf(5), buckets.getOrDefault("red", 0));
        assertEquals("nested fused: total is the full union", 30L, total);
    }

    // ------------------------------------------------ helpers ------------------------------------------------

    @SneakyThrows
    private Map<String, Object> searchRaw(String jsonBody, int size) {
        Request request = new Request("POST", "/" + INDEX + "/_search");
        request.setJsonEntity(jsonBody);
        request.addParameter("size", Integer.toString(size));
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Integer> colorBuckets(Map<String, Object> resp) {
        Map<String, Integer> out = new LinkedHashMap<>();
        Map<String, Object> aggs = (Map<String, Object>) resp.get("aggregations");
        if (aggs == null) {
            return out;
        }
        Map<String, Object> byColor = (Map<String, Object>) aggs.get("by_color");
        List<Map<String, Object>> buckets = (List<Map<String, Object>>) byColor.get("buckets");
        for (Map<String, Object> b : buckets) {
            out.put((String) b.get("key"), ((Number) b.get("doc_count")).intValue());
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private long totalHits(Map<String, Object> resp) {
        Map<String, Object> hits = (Map<String, Object>) resp.get("hits");
        Object totalObj = hits.get("total");
        if (totalObj instanceof Map) {
            Object v = ((Map<String, Object>) totalObj).get("value");
            return v == null ? -1 : ((Number) v).longValue();
        }
        return -1;
    }

    @SuppressWarnings("unchecked")
    private int returnedHitCount(Map<String, Object> resp) {
        Map<String, Object> hits = (Map<String, Object>) resp.get("hits");
        List<Object> hitList = (List<Object>) hits.get("hits");
        return hitList == null ? 0 : hitList.size();
    }

    private void logResult(String label, Map<String, Integer> buckets, long total, Map<String, Object> resp) {
        List<String> ordered = new ArrayList<>();
        for (String c : List.of("blue", "green", "red")) {
            ordered.add(c + "=" + buckets.getOrDefault(c, 0));
        }
        System.out.println(
            "RESULT["
                + label
                + "]: buckets{"
                + String.join(", ", ordered)
                + "} total_hits="
                + total
                + " returned_hits="
                + returnedHitCount(resp)
        );
    }
}
