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
 * Investigates the NESTED fused/resolver hybrid case: a {@code mode:"fused"} hybrid wrapped inside another compound
 * query ({@code bool}, {@code dis_max}, {@code function_score}, or another fused hybrid). Classic hybrid CANNOT nest
 * (top-level-only guard in {@code HybridQueryPhaseSearcher} throws), so these behaviors have NO classic equivalent —
 * this is new capability the self-erase unlocks.
 *
 * <p><b>Depth-independent Tail (the fix):</b> {@code HybridFusionOrchestrator.buildFusedQuery} decides the Tail the
 * same way regardless of nesting depth. A nested hybrid that wants totals/aggregations/highlight keeps the Tail
 * ({@code bool.filter: bool{should: legs}}), so {@code total_hits} and aggregations cover the full leg-union at any
 * depth (previously nested was unconditionally Top-only, which silently windowed nested aggs/totals — see
 * {@code testFused_nestedInBool_outerAggregationCoversFullUnion}).
 *
 * <p><b>Ranking is still fuse-then-filter.</b> Only the fused-window Top clauses carry a non-zero score; Tail-only docs
 * score 0 and sort last. The enclosing {@code bool.filter} is a sibling clause and is NOT pushed into the legs before
 * fusion, so the window is the GLOBAL fused top-K and the enclosing filter intersects it at the query phase. To NARROW
 * retrieval, use the hybrid's OWN {@code filter} field (pushed into each leg). When the enclosing filter excludes the
 * fused window, the Tail still surfaces the matching docs as score-0 hits (so totals/aggs are correct) rather than
 * losing them — see {@code testFused_nestedInBool_restrictiveFilter_tailSurfacesFullUnion}.
 *
 * <p>Determinism: legs are {@code function_score} by numeric {@code s} (shard-independent window). Run multi-node with
 * {@code -PnumNodes=2}.
 *
 * <p>Dataset (30 docs, ids 1..30):
 * <pre>
 *   grp:   ids 1-5   = "A" (tier 3, highest s -> the global fused top-5 window)
 *          ids 6-30  = "B" (tiers 2/1, lower s -> never in the top-5 window)
 *   s = tier*1000 - id ; color alternates for a secondary filter.
 * </pre>
 */
public class HybridQueryFusedModeNestedIT extends BaseNeuralSearchIT {

    private static final String INDEX = "test-fused-nested";
    private static final String NORM_PIPELINE = "fused-nested-norm-pipeline";
    private static final String GRP_FIELD = "grp";
    private static final String SCORE_FIELD = "s";
    private static final int RANK_WINDOW = 5;

    private String indexConfig() {
        return "{\"settings\":{\"number_of_shards\":3,\"number_of_replicas\":0,"
            + "\"index.search.default_pipeline\":\""
            + NORM_PIPELINE
            + "\"},"
            + "\"mappings\":{\"properties\":{\""
            + GRP_FIELD
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
            // grp A = ids 1..5 (highest s, own the top-5 window). grp B = ids 6..30 (never in the window).
            String grp = id <= 5 ? "A" : "B";
            int tier = id <= 5 ? 3 : (id <= 17 ? 2 : 1);
            int s = tier * 1000 - id;
            String doc = "{\"" + GRP_FIELD + "\":\"" + grp + "\",\"" + SCORE_FIELD + "\":" + s + "}";
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

    /** A leg that matches ALL docs and scores by numeric {@code s} (deterministic, shard-independent). */
    private String leg() {
        return "{\"function_score\":{\"query\":{\"match_all\":{}},"
            + "\"field_value_factor\":{\"field\":\""
            + SCORE_FIELD
            + "\",\"modifier\":\"none\",\"missing\":1}}}";
    }

    private String fusedHybrid() {
        return "{\"hybrid\":{\"mode\":\"fused\",\"rank_window_size\":"
            + RANK_WINDOW
            + ",\"fusion\":{\"normalization\":{\"technique\":\"min_max\"},"
            + "\"combination\":{\"technique\":\"arithmetic_mean\",\"parameters\":{\"weights\":[0.5,0.5]}}},"
            + "\"queries\":["
            + leg()
            + ","
            + leg()
            + "]}}";
    }

    // ---------------------------------------------------------------------------------------------
    // Test 1 (BASELINE + TOP-LEVEL TAIL SEMANTICS): the SAME fused hybrid at TOP LEVEL has a Tail, so it
    // differs from the nested case. With size <= rank_window_size it returns exactly the fused window
    // (5 grp-A docs, scored). With size > rank_window_size it ALSO returns Tail docs (score 0) up to
    // size, ranked below the window — i.e. the full union is available as hits, not just the window.
    // This is the top-level contrast to the nested (Top-only, no-Tail) cases below.
    // ---------------------------------------------------------------------------------------------
    @SneakyThrows
    public void testFused_topLevel_windowThenTail() {
        ensureDataset();
        String body = "{\"query\":" + fusedHybrid() + "}";

        // size == rank_window_size (5): exactly the window, all grp A, ids 1..5.
        Map<String, Object> windowResp = searchRaw(body, RANK_WINDOW);
        List<String> windowIds = hitIds(windowResp);
        System.out.println("RESULT[top-level size=5]: ids=" + windowIds + " grp=" + grpCounts(windowResp));
        assertEquals("size<=window returns exactly the fused window", 5, windowIds.size());
        assertTrue("window is grp A (ids 1..5)", windowIds.stream().allMatch(id -> Integer.parseInt(id) <= 5));

        // size > rank_window_size (20): window (5 grp A, scored) + Tail (score-0 union docs) up to size.
        Map<String, Object> tailResp = searchRaw(body, 20);
        List<String> allIds = hitIds(tailResp);
        System.out.println("RESULT[top-level size=20]: count=" + allIds.size() + " grp=" + grpCounts(tailResp));
        assertEquals("size>window: top-level Tail exposes union docs beyond the window", 20, allIds.size());
        List<String> firstFive = allIds.subList(0, 5);
        assertTrue("the window (grp A) still ranks first", firstFive.stream().allMatch(id -> Integer.parseInt(id) <= 5));
    }

    // ---------------------------------------------------------------------------------------------
    // Test 2 (FUSE-THEN-FILTER, compatible filter): fused hybrid in bool.must with filter grp:A.
    // Window={1..5} all grp A, so the enclosing filter keeps all 5. Result = 5.
    // ---------------------------------------------------------------------------------------------
    @SneakyThrows
    public void testFused_nestedInBool_filterMatchesWindow_keepsAll() {
        ensureDataset();
        String body = "{\"query\":{\"bool\":{"
            + "\"must\":["
            + fusedHybrid()
            + "],"
            + "\"filter\":[{\"term\":{\""
            + GRP_FIELD
            + "\":\"A\"}}]}}}";
        Map<String, Object> resp = searchRaw(body, 20);
        List<String> ids = hitIds(resp);
        System.out.println("RESULT[nested filter=A(window)]: ids=" + ids + " grp=" + grpCounts(resp));
        assertEquals("filter grp:A intersects the all-A window -> keeps 5", 5, ids.size());
    }

    // ---------------------------------------------------------------------------------------------
    // Test 3 (DEPTH-INDEPENDENT TAIL + restrictive filter): fused hybrid in bool.must with filter grp:B.
    // With the depth-independent Tail decision, a nested hybrid that wants totals keeps the Tail, so it
    // matches the FULL leg-union (all 30 docs); the enclosing filter grp:B intersects that union -> the
    // 25 grp-B docs survive (window docs 1..5 are grp A, dropped by the filter). Ranking is still
    // fuse-then-filter (the fused-scored window is grp A and is filtered out, so the surviving grp-B hits
    // are score-0 Tail docs), but they are no longer LOST from the result and totals. Before the fix this
    // returned 0 (nested was Top-only); the fix makes totals/aggs correct at any depth.
    // ---------------------------------------------------------------------------------------------
    @SneakyThrows
    public void testFused_nestedInBool_restrictiveFilter_tailSurfacesFullUnion() {
        ensureDataset();
        String body = "{\"query\":{\"bool\":{"
            + "\"must\":["
            + fusedHybrid()
            + "],"
            + "\"filter\":[{\"term\":{\""
            + GRP_FIELD
            + "\":\"B\"}}]}},\"track_total_hits\":true}";
        Map<String, Object> resp = searchRaw(body, 30);
        List<String> ids = hitIds(resp);
        long total = totalHits(resp);
        System.out.println("RESULT[nested filter=B]: count=" + ids.size() + " total_hits=" + total + " grp=" + grpCounts(resp));
        // Depth-independent Tail: the hybrid matches the full union; filter grp:B keeps the 25 grp-B docs
        // (as score-0 Tail docs, since the fused window was all grp A). total_hits counts them.
        assertEquals("depth-independent Tail: filter(grpB) intersect full-union = 25 grp-B docs", 25, ids.size());
        assertEquals("all surviving hits are grp B", Integer.valueOf(25), grpCounts(resp).getOrDefault("B", 0));
        assertEquals("total_hits counts the full filtered union", 25L, total);
    }

    // ---------------------------------------------------------------------------------------------
    // Test 4 (DEPTH-INDEPENDENT AGG): outer terms(grp) agg over a nested fused hybrid. With the fix the
    // nested hybrid keeps the Tail (aggregations present), so the aggregation covers the FULL leg-union
    // (all 30 docs), NOT just the window. Before the fix this was windowed to {1..5} (A=5, B=0).
    // ---------------------------------------------------------------------------------------------
    @SneakyThrows
    public void testFused_nestedInBool_outerAggregationCoversFullUnion() {
        ensureDataset();
        String body = "{\"query\":{\"bool\":{\"must\":["
            + fusedHybrid()
            + "]}},"
            + "\"aggregations\":{\"by_grp\":{\"terms\":{\"field\":\""
            + GRP_FIELD
            + "\",\"size\":10}}},"
            + "\"track_total_hits\":true}";
        Map<String, Object> resp = searchRaw(body, 20);
        Map<String, Integer> grp = grpBuckets(resp);
        long total = totalHits(resp);
        System.out.println("RESULT[nested outer-agg]: buckets=" + grp + " total_hits=" + total);
        // Depth-independent Tail: aggregation covers the full union -> A=5, B=25, total=30.
        assertEquals("nested agg: A full", Integer.valueOf(5), grp.getOrDefault("A", 0));
        assertEquals("nested agg: B now covered by the depth-independent Tail", Integer.valueOf(25), grp.getOrDefault("B", 0));
        assertEquals("nested agg: total is the full union", 30L, total);
    }

    // ---------------------------------------------------------------------------------------------
    // Test 5 (dis_max / function_score wrapping + fusion-of-fusion): confirm self-erase composes when
    // wrapped in another compound and when a fused hybrid is nested in a fused hybrid. With the
    // depth-independent Tail, a nested fused in dis_max keeps the Tail (default totals wanted), so it
    // matches the full union (all 30 docs) and returns up to `size`, but the fused-scored window (grp A)
    // still ranks FIRST (Tail docs score 0). Asserts composition + window-ranks-first, not a count.
    // ---------------------------------------------------------------------------------------------
    @SneakyThrows
    public void testFused_nestedInDisMax_composesWindowRanksFirst() {
        ensureDataset();
        String body = "{\"query\":{\"dis_max\":{\"queries\":[" + fusedHybrid() + "]}}}";
        Map<String, Object> resp;
        try {
            resp = searchRaw(body, 20);
        } catch (Exception e) {
            System.out.println("RESULT[dis_max nested]: THREW " + e.getClass().getSimpleName() + ": " + e.getMessage());
            throw e;
        }
        List<String> ids = hitIds(resp);
        System.out.println("RESULT[dis_max nested]: ids=" + ids);
        assertFalse("dis_max over fused composes (self-erase resolved)", ids.isEmpty());
        // depth-independent Tail: the union is available beyond the window; the fused window (grp A) ranks first.
        List<String> firstFive = ids.subList(0, Math.min(5, ids.size()));
        assertTrue("fused window (grp A) ranks first", firstFive.stream().allMatch(id -> Integer.parseInt(id) <= 5));
    }

    @SneakyThrows
    public void testFused_hybridNestedInHybrid_fusionOfFusion() {
        ensureDataset();
        // Outer fused hybrid whose leg 1 is itself a fused hybrid, leg 2 a plain function_score.
        String inner = fusedHybrid();
        String body = "{\"query\":{\"hybrid\":{\"mode\":\"fused\",\"rank_window_size\":"
            + RANK_WINDOW
            + ",\"fusion\":{\"normalization\":{\"technique\":\"min_max\"},"
            + "\"combination\":{\"technique\":\"arithmetic_mean\",\"parameters\":{\"weights\":[0.5,0.5]}}},"
            + "\"queries\":["
            + inner
            + ","
            + leg()
            + "]}}}";
        Map<String, Object> resp;
        try {
            resp = searchRaw(body, 20);
        } catch (Exception e) {
            System.out.println("RESULT[fusion-of-fusion]: THREW " + e.getClass().getSimpleName() + ": " + e.getMessage());
            throw e;
        }
        List<String> ids = hitIds(resp);
        System.out.println("RESULT[fusion-of-fusion]: ids=" + ids + " grp=" + grpCounts(resp));
        assertFalse("fusion-of-fusion must return some hits", ids.isEmpty());
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
    private List<String> hitIds(Map<String, Object> resp) {
        List<String> out = new ArrayList<>();
        Map<String, Object> hits = (Map<String, Object>) resp.get("hits");
        List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits.get("hits");
        if (hitList != null) {
            for (Map<String, Object> h : hitList) {
                out.add((String) h.get("_id"));
            }
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Integer> grpBuckets(Map<String, Object> resp) {
        Map<String, Integer> out = new LinkedHashMap<>();
        Map<String, Object> aggs = (Map<String, Object>) resp.get("aggregations");
        if (aggs == null) {
            return out;
        }
        Map<String, Object> byGrp = (Map<String, Object>) aggs.get("by_grp");
        List<Map<String, Object>> buckets = (List<Map<String, Object>>) byGrp.get("buckets");
        for (Map<String, Object> b : buckets) {
            out.put((String) b.get("key"), ((Number) b.get("doc_count")).intValue());
        }
        return out;
    }

    private Map<String, Integer> grpCounts(Map<String, Object> resp) {
        Map<String, Integer> out = new LinkedHashMap<>();
        for (String id : hitIds(resp)) {
            String g = Integer.parseInt(id) <= 5 ? "A" : "B";
            out.merge(g, 1, Integer::sum);
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
}
