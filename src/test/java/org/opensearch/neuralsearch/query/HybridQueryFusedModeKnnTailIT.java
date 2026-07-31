/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

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
import org.opensearch.neuralsearch.processor.HybridFusedAggregationsResponseProcessor;

import lombok.SneakyThrows;

/**
 * Isolates the KNN-only-Tail-gap question for a TOP-LEVEL fused hybrid: a dense {@code knn} leg's Tail is materialized
 * from the ids the leg RETURNED (top {@code rank_window_size}, further capped by the query's {@code k}) — it is NOT
 * re-run as a real query ({@code HybridFusionOrchestrator.isMaterializableLeg} avoids a second HNSW walk). So a doc that
 * is a genuine vector neighbor but sits OUTSIDE the returned window, and is NOT matched by the lexical leg, is absent
 * from the Tail and therefore absent from aggregations / total_hits.
 *
 * <p><b>Empirical finding (this is a REGRESSION on multi-shard indices):</b> classic hybrid runs the KNN sub-query
 * in-place per shard, so its aggregation sees the <b>per-shard-k</b> union (which includes {@code red_far}, top-3 on
 * its own shard). Fused fires the KNN leg with {@code size=rank_window_size} and the coordinator reduces it to the
 * <b>global top-{@code rank_window_size}</b> before materializing the Tail, dropping {@code red_far} in that global
 * merge. So on a 2-shard index: classic counts {@code red=1} (total 5), fused counts {@code red=0} (total 4). The KNN
 * leg's aggregation contribution is therefore narrower under fused (global-top-window) than classic (per-shard-k union)
 * — a genuine undercount, distinct from the lexical case where the Tail re-runs the real query and covers the full
 * match set. (A single-shard index would hide this, since per-shard-k == global-k there.)
 *
 * <p><b>Construction (2-D vectors, single query vector [0,0]):</b>
 * <pre>
 *   blue1..blue3  vectors near [0,0]  (the 3 nearest neighbors)  color=blue  text="apple"
 *   red_far       vector far-ish [10,0] (4th nearest, a real neighbor but outside k=3/window=3) color=red text="banana"
 *   green_lexical vector very far [99,0]  color=green  text="apple"  (matched by the lexical leg, not by knn top-k)
 * </pre>
 * Lexical leg = {@code match text:apple} (hits blue1..3 + green_lexical, NOT red_far). KNN leg = nearest 3 to [0,0]
 * (blue1..3), with red_far as the 4th. So {@code red_far} is a KNN-only tail doc outside the window.
 *
 * <p>Run multi-node with {@code -PnumNodes=2}.
 */
public class HybridQueryFusedModeKnnTailIT extends BaseNeuralSearchIT {

    private static final String INDEX = "test-fused-knn-tail";
    private static final String NORM_PIPELINE = "fused-knn-tail-pipeline";
    private static final String VEC = "vec";
    private static final String COLOR = "color";
    private static final String TEXT = "text";
    private static final int RANK_WINDOW = 3;
    private static final int K = 3;

    @SneakyThrows
    private void ensureDataset() {
        createSearchPipeline(NORM_PIPELINE, "min_max", "arithmetic_mean", Map.of());
        if (indexExists(INDEX)) {
            return;
        }
        String cfg = "{\"settings\":{\"number_of_shards\":2,\"number_of_replicas\":0,\"index.knn\":true,"
            + "\"index.search.default_pipeline\":\""
            + NORM_PIPELINE
            + "\"},"
            + "\"mappings\":{\"properties\":{"
            + "\""
            + VEC
            + "\":{\"type\":\"knn_vector\",\"dimension\":2,"
            + "\"method\":{\"engine\":\"lucene\",\"space_type\":\"l2\",\"name\":\"hnsw\"}},"
            + "\""
            + COLOR
            + "\":{\"type\":\"keyword\"},"
            + "\""
            + TEXT
            + "\":{\"type\":\"text\"}}}}";
        createIndex(INDEX, cfg);

        // 3 nearest neighbors to [0,0] — blue, lexical "apple"
        indexDoc("blue1", new double[] { 0.0, 0.0 }, "blue", "apple");
        indexDoc("blue2", new double[] { 0.1, 0.0 }, "blue", "apple");
        indexDoc("blue3", new double[] { 0.2, 0.0 }, "blue", "apple");
        // 4th nearest — a REAL vector neighbor but outside k=3/window=3; red; NOT lexical ("banana")
        indexDoc("red_far", new double[] { 10.0, 0.0 }, "red", "banana");
        // far vector, but matched by the lexical leg ("apple"); green
        indexDoc("green_lexical", new double[] { 99.0, 0.0 }, "green", "apple");
    }

    @SneakyThrows
    private void indexDoc(String id, double[] vec, String color, String text) {
        String doc = "{\""
            + VEC
            + "\":["
            + vec[0]
            + ","
            + vec[1]
            + "],\""
            + COLOR
            + "\":\""
            + color
            + "\",\""
            + TEXT
            + "\":\""
            + text
            + "\"}";
        Request r = new Request("PUT", "/" + INDEX + "/_doc/" + id + "?refresh=true");
        r.setJsonEntity(doc);
        Response resp = client().performRequest(r);
        int code = resp.getStatusLine().getStatusCode();
        assertTrue("index " + id + " failed: " + code, code == RestStatus.OK.getStatus() || code == RestStatus.CREATED.getStatus());
    }

    private String lexicalLeg() {
        return "{\"match\":{\"" + TEXT + "\":\"apple\"}}";
    }

    private String knnLeg() {
        return "{\"knn\":{\"" + VEC + "\":{\"vector\":[0.0,0.0],\"k\":" + K + "}}}";
    }

    private String termsAgg() {
        return "\"aggregations\":{\"by_color\":{\"terms\":{\"field\":\"" + COLOR + "\",\"size\":10}}}";
    }

    // Baseline: what the KNN leg alone returns. NOTE: knn `k` is PER-SHARD. With 2 shards holding ~2-3 docs
    // each, red_far is top-3 ON ITS OWN SHARD, so it surfaces even though it is the 4th-nearest GLOBALLY.
    // This is the crux: the KNN leg's match set is the per-shard-k union (blue1..3 + red_far = 4), not the
    // global top-3. (Discovered empirically — a single-shard index would exclude red_far.)
    @SneakyThrows
    public void testKnnLegAlone_perShardKSurfacesRedFar() {
        ensureDataset();
        String body = "{\"query\":" + knnLeg() + "," + termsAgg() + ",\"track_total_hits\":true}";
        Map<String, Object> resp = searchRaw(body, 10);
        System.out.println("RESULT[knn-alone]: buckets=" + colorBuckets(resp) + " total=" + totalHits(resp));
        assertEquals("knn alone: the 3 blue neighbors", Integer.valueOf(3), colorBuckets(resp).getOrDefault("blue", 0));
        // per-shard k=3 surfaces red_far (top-3 on its shard) even though it is 4th globally.
        assertEquals("knn alone: red_far surfaces via per-shard k", Integer.valueOf(1), colorBuckets(resp).getOrDefault("red", 0));
    }

    // CLASSIC hybrid aggregation — the baseline. Classic runs the KNN sub-query IN-PLACE per shard, so its
    // aggregation collector sees the per-shard-k union INCLUDING red_far (top-3 on its shard). red_far is a
    // knn-only doc, not a lexical match, yet classic counts it: blue=3, green=1, red=1, total=5.
    @SneakyThrows
    public void testClassicHybrid_knnPlusLexical_countsRedFar() {
        ensureDataset();
        String body = "{\"query\":{\"hybrid\":{\"queries\":["
            + lexicalLeg()
            + ","
            + knnLeg()
            + "]}},"
            + termsAgg()
            + ",\"track_total_hits\":true}";
        Map<String, Object> resp = searchRaw(body, 10);
        Map<String, Integer> b = colorBuckets(resp);
        System.out.println("RESULT[CLASSIC knn+lex]: buckets=" + b + " total=" + totalHits(resp));
        assertEquals("classic: blue", Integer.valueOf(3), b.getOrDefault("blue", 0));
        assertEquals("classic: green (lexical)", Integer.valueOf(1), b.getOrDefault("green", 0));
        // classic aggregates the per-shard-k KNN union -> red_far IS counted.
        assertEquals("classic: red_far counted (per-shard-k KNN union)", Integer.valueOf(1), b.getOrDefault("red", 0));
        assertEquals("classic: total is the full union", 5L, totalHits(resp));
    }

    // FUSED hybrid aggregation — THE DIVERGENCE. Fused fires the KNN leg with size=rank_window_size and the
    // coordinator reduces it to the GLOBAL top-rank_window_size (blue1..3), dropping red_far in that global
    // merge; the Tail is then materialized from those returned ids only. So red_far is ABSENT from fused aggs
    // (blue=3, green=1, red=0, total=4) while classic counts it (red=1, total=5). This is a genuine fused
    // undercount of the KNN leg's aggregation contribution on a MULTI-SHARD index: fused aggregates the global
    // top-rank_window_size of the KNN leg, classic aggregates the per-shard-k union. Regression guard.
    @SneakyThrows
    public void testFusedHybrid_knnLeg_undercountsVsClassic() {
        ensureDataset();
        String body = "{\"query\":{\"hybrid\":{\"mode\":\"fused\",\"rank_window_size\":"
            + RANK_WINDOW
            + ",\"queries\":["
            + lexicalLeg()
            + ","
            + knnLeg()
            + "]}},"
            + termsAgg()
            + ",\"track_total_hits\":true}";
        Map<String, Object> resp = searchRaw(body, 10);
        Map<String, Integer> b = colorBuckets(resp);
        System.out.println("RESULT[FUSED knn+lex]: buckets=" + b + " total=" + totalHits(resp));
        assertEquals("fused: blue in union", Integer.valueOf(3), b.getOrDefault("blue", 0));
        assertEquals("fused: green (lexical) in union", Integer.valueOf(1), b.getOrDefault("green", 0));
        // red_far dropped in the coordinator global-top-rank_window_size reduction of the KNN leg -> absent from Tail.
        assertEquals(
            "fused: red_far ABSENT (global-top-window reduction) — diverges from classic red=1",
            Integer.valueOf(0),
            b.getOrDefault("red", 0)
        );
        assertEquals("fused: total undercounts vs classic (4 vs 5)", 4L, totalHits(resp));
    }

    // ---------------------------------------------------------------------------------------------
    // PROPOSAL PROBE (A): the "extra aggregation leg" shape, WRONG form — all sub-queries as SEPARATE
    // filter clauses = a CONJUNCTION (doc must match every leg). Far too narrow: red_far matches only
    // the knn leg, so an AND excludes it (and green_lexical too). Demonstrates why the shape matters.
    // ---------------------------------------------------------------------------------------------
    @SneakyThrows
    public void testAggLegProbe_allLegsAsSeparateFilters_isConjunctionTooNarrow() {
        ensureDataset();
        String body = "{\"size\":0,\"query\":{\"bool\":{\"filter\":["
            + lexicalLeg()
            + ","
            + knnLeg()
            + "]}},"
            + termsAgg()
            + ",\"track_total_hits\":true}";
        Map<String, Object> resp = searchRaw(body, 0);
        Map<String, Integer> b = colorBuckets(resp);
        System.out.println("RESULT[agg-leg AND(wrong)]: buckets=" + b + " total=" + totalHits(resp));
        // conjunction: only docs matching BOTH legs (the 3 blue) survive.
        assertEquals("AND form: red_far excluded (matches only knn leg)", Integer.valueOf(0), b.getOrDefault("red", 0));
        assertEquals("AND form: green_lexical excluded (matches only lexical leg)", Integer.valueOf(0), b.getOrDefault("green", 0));
    }

    // ---------------------------------------------------------------------------------------------
    // PROPOSAL PROBE (B): the "extra aggregation leg" shape, CORRECT form — ONE filter clause wrapping
    // a should-disjunction of all legs, with size:0 + aggregations + track_total_hits. THE KEY QUESTION:
    // does this recapture red_far (the doc fused currently undercounts)?
    //
    // It should, because the knn sub-query executes IN-PLACE PER SHARD here (per-shard k=3 -> red_far is
    // top-3 on its own shard), exactly like classic hybrid — rather than being reconstructed from the
    // coordinator's globally-reduced top-rank_window_size ids. If this yields red=1/total=5 (matching
    // classic) then the extra-agg-leg proposal is mechanically sound.
    // ---------------------------------------------------------------------------------------------
    @SneakyThrows
    public void testAggLegProbe_unionInsideFilter_recapturesMissedKnnDoc() {
        ensureDataset();
        String body = "{\"size\":0,\"query\":{\"bool\":{\"filter\":[{\"bool\":{\"should\":["
            + lexicalLeg()
            + ","
            + knnLeg()
            + "]}}]}},"
            + termsAgg()
            + ",\"track_total_hits\":true}";
        Map<String, Object> resp = searchRaw(body, 0);
        Map<String, Integer> b = colorBuckets(resp);
        System.out.println("RESULT[agg-leg OR-in-filter(proposed)]: buckets=" + b + " total=" + totalHits(resp));
        // The proposal's payoff: per-shard knn execution restores the full union, matching classic.
        assertEquals("proposed agg leg: blue", Integer.valueOf(3), b.getOrDefault("blue", 0));
        assertEquals("proposed agg leg: green (lexical)", Integer.valueOf(1), b.getOrDefault("green", 0));
        assertEquals("proposed agg leg RECAPTURES red_far (per-shard knn, like classic)", Integer.valueOf(1), b.getOrDefault("red", 0));
        assertEquals("proposed agg leg: total matches classic (5)", 5L, totalHits(resp));
    }

    // ---------------------------------------------------------------------------------------------
    // THE FIX (aggregation leg): with the agg leg implemented, a top-level fused hybrid carrying an
    // aggregation must now report the TRUE leg union — identical to classic — instead of undercounting
    // the KNN leg. Expected: blue=3, green=1, red=1, total=5 (was blue=3, green=1, total=4).
    // ---------------------------------------------------------------------------------------------
    @SneakyThrows
    public void testFusedHybrid_withAggregationLeg_matchesClassicExactly() {
        ensureDataset();
        // System-generated processors are OFF by default; the aggregation-leg swap rides one, so enable its factory
        // (same deployment prerequisite as the fused profiler processor).
        updateClusterSettings("cluster.search.enabled_system_generated_factories", List.of(HybridFusedAggregationsResponseProcessor.TYPE));
        try {
            String fusedBody = "{\"query\":{\"hybrid\":{\"mode\":\"fused\",\"rank_window_size\":"
                + RANK_WINDOW
                + ",\"queries\":["
                + lexicalLeg()
                + ","
                + knnLeg()
                + "]}},"
                + termsAgg()
                + ",\"track_total_hits\":true}";
            Map<String, Object> fused = searchRaw(fusedBody, 10);
            Map<String, Integer> fb = colorBuckets(fused);
            long fusedTotal = totalHits(fused);

            String classicBody = "{\"query\":{\"hybrid\":{\"queries\":["
                + lexicalLeg()
                + ","
                + knnLeg()
                + "]}},"
                + termsAgg()
                + ",\"track_total_hits\":true}";
            Map<String, Object> classic = searchRaw(classicBody, 10);
            Map<String, Integer> cb = colorBuckets(classic);
            long classicTotal = totalHits(classic);

            System.out.println("RESULT[AGGLEG fused]: buckets=" + fb + " total=" + fusedTotal);
            System.out.println("RESULT[AGGLEG classic]: buckets=" + cb + " total=" + classicTotal);

            // THE PROOF: fused aggregations now equal classic, including the previously-missed KNN-only doc.
            assertEquals("agg leg: red_far recaptured (was 0)", Integer.valueOf(1), fb.getOrDefault("red", 0));
            assertEquals("agg leg: blue matches classic", cb.getOrDefault("blue", 0), fb.getOrDefault("blue", 0));
            assertEquals("agg leg: green matches classic", cb.getOrDefault("green", 0), fb.getOrDefault("green", 0));
            assertEquals("agg leg: red matches classic", cb.getOrDefault("red", 0), fb.getOrDefault("red", 0));
            assertEquals("agg leg: total_hits matches classic", classicTotal, fusedTotal);
        } finally {
            updateClusterSettings("cluster.search.enabled_system_generated_factories", List.of());
        }
    }

    // ---------------------------------------------------------------------------------------------
    // RELEVANCE GUARD: the aggregation leg must NOT change ranking. It is a size:0, non-scoring side
    // query, so the SCORED docs (the fused Top window) must be identical with and without it.
    //
    // NOTE on shape: a request WITH aggregations also retains the Tail (pre-existing behavior, unrelated
    // to the agg leg), which appends score-0 union docs BELOW the window. So we compare the SCORED
    // PREFIX (score > 0) — that is the ranking — not the raw hit list length.
    // ---------------------------------------------------------------------------------------------
    @SneakyThrows
    public void testAggregationLeg_doesNotChangeRankingOrScores() {
        ensureDataset();
        String withAgg = "{\"query\":{\"hybrid\":{\"mode\":\"fused\",\"rank_window_size\":"
            + RANK_WINDOW
            + ",\"queries\":["
            + lexicalLeg()
            + ","
            + knnLeg()
            + "]}},"
            + termsAgg()
            + ",\"track_total_hits\":false}";
        String withoutAgg = "{\"query\":{\"hybrid\":{\"mode\":\"fused\",\"rank_window_size\":"
            + RANK_WINDOW
            + ",\"queries\":["
            + lexicalLeg()
            + ","
            + knnLeg()
            + "]}},\"track_total_hits\":false}";

        Map<String, Object> a = searchRaw(withAgg, 10);
        Map<String, Object> b = searchRaw(withoutAgg, 10);
        List<String> scoredWith = scoredPrefixIds(a);
        List<String> scoredWithout = scoredPrefixIds(b);
        List<Double> scoresWith = scoredPrefixScores(a);
        List<Double> scoresWithout = scoredPrefixScores(b);
        System.out.println("RESULT[AGGLEG ranking with-agg]: scoredIds=" + scoredWith + " scores=" + scoresWith);
        System.out.println("RESULT[AGGLEG ranking no-agg]: scoredIds=" + scoredWithout + " scores=" + scoresWithout);

        assertEquals("agg leg must not change the scored (fused window) ranking", scoredWithout, scoredWith);
        for (int i = 0; i < scoresWithout.size(); i++) {
            assertEquals("agg leg must not change fused scores at rank " + i, scoresWithout.get(i), scoresWith.get(i), 1e-9);
        }
    }

    /** Ids of hits with score &gt; 0 — i.e. the fused Top window (Tail docs score exactly 0 and sort below). */
    private List<String> scoredPrefixIds(Map<String, Object> resp) {
        java.util.List<String> ids = new java.util.ArrayList<>();
        List<String> all = hitIds(resp);
        List<Double> scores = hitScores(resp);
        for (int i = 0; i < all.size(); i++) {
            if (scores.get(i) > 0.0) {
                ids.add(all.get(i));
            }
        }
        return ids;
    }

    private List<Double> scoredPrefixScores(Map<String, Object> resp) {
        java.util.List<Double> out = new java.util.ArrayList<>();
        for (Double s : hitScores(resp)) {
            if (s > 0.0) {
                out.add(s);
            }
        }
        return out;
    }

    // ------------------------------------------------ helpers ------------------------------------------------

    @SuppressWarnings("unchecked")
    private List<String> hitIds(Map<String, Object> resp) {
        java.util.List<String> out = new java.util.ArrayList<>();
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
    private List<Double> hitScores(Map<String, Object> resp) {
        java.util.List<Double> out = new java.util.ArrayList<>();
        Map<String, Object> hits = (Map<String, Object>) resp.get("hits");
        List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits.get("hits");
        if (hitList != null) {
            for (Map<String, Object> h : hitList) {
                Object s = h.get("_score");
                out.add(s == null ? 0.0 : ((Number) s).doubleValue());
            }
        }
        return out;
    }

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
        for (Map<String, Object> bk : buckets) {
            out.put((String) bk.get("key"), ((Number) bk.get("doc_count")).intValue());
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private long totalHits(Map<String, Object> resp) {
        Map<String, Object> hits = (Map<String, Object>) resp.get("hits");
        Object t = hits.get("total");
        if (t instanceof Map) {
            Object v = ((Map<String, Object>) t).get("value");
            return v == null ? -1 : ((Number) v).longValue();
        }
        return -1;
    }
}
