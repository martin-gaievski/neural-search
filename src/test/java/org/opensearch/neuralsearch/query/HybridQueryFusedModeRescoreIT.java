/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import lombok.SneakyThrows;

/**
 * Runtime spike: does a STANDARD top-level {@code rescore} block work on a hybrid query in {@code mode: "fused"} and
 * deliver banded promotion with ZERO plugin code?
 *
 * <p>Mechanism under test: the fused-mode query self-erases at the coordinator into a standard {@code bool} query
 * (constant_score(ids)^fusedScore Top clauses + non-scoring Tail filter). Because the shard sees a plain query, the
 * stock query-phase rescorer should apply on top of the FUSED scores — a {@code constant_score(filter)^BAND} rescore
 * query with {@code score_mode: total} then adds a large flat band to any fused hit matching the promotion filter,
 * lifting it above every organic hit while fused order decides ranking within a band.
 */
public class HybridQueryFusedModeRescoreIT extends BaseNeuralSearchIT {

    private static final String TEXT_FIELD = "text";
    private static final String TITLE_FIELD = "title";
    private static final String CAMPAIGN_FIELD = "campaign_id";
    private static final String INDEX_NAME = "test-hybrid-fused-rescore";
    private static final String NORM_PIPELINE = "fused-rescore-norm-pipeline";

    private static final String PROMOTED_SUMMER_ID = "promoted-low";
    private static final String PROMOTED_FALL_ID = "promoted-fall";

    private String indexConfig(String pipelineId) {
        return "{\"settings\":{\"number_of_shards\":3,\"number_of_replicas\":0,"
            + "\"index.search.default_pipeline\":\""
            + pipelineId
            + "\"},"
            + "\"mappings\":{\"properties\":{"
            + "\""
            + TEXT_FIELD
            + "\":{\"type\":\"text\"},"
            + "\""
            + TITLE_FIELD
            + "\":{\"type\":\"text\"},"
            + "\""
            + CAMPAIGN_FIELD
            + "\":{\"type\":\"keyword\"}}}}";
    }

    @SneakyThrows
    private void prepareIndex() {
        createSearchPipeline(NORM_PIPELINE, "min_max", "arithmetic_mean", Map.of());
        if (indexExists(INDEX_NAME) == false) {
            createIndex(INDEX_NAME, indexConfig(NORM_PIPELINE));
            // organic docs: strong relevance to the query terms, no campaign field
            indexDoc("organic-best", "hello hello hello", "place place place", null);
            indexDoc("organic-mid", "hello hello", "place", null);
            indexDoc("organic-low", "hello", "nothing here at all", null);
            // promoted docs: LOW relevance (single query term buried in long filler) but carry a campaign
            indexDoc(
                PROMOTED_SUMMER_ID,
                "hello buried in a very long passage of entirely unrelated filler words about gardening and weather patterns",
                null,
                "SUMMER"
            );
            indexDoc(
                PROMOTED_FALL_ID,
                "hello also buried deep within another long passage of unrelated filler text about cooking and travel notes",
                null,
                "FALL"
            );
            // matches neither leg
            indexDoc("no-match", "nothing relevant whatsoever", "different words", null);
        }
    }

    /** Two-leg fused hybrid: match leg on {@code text} + second match leg on {@code title} (another field). */
    private String fusedQueryJson() {
        return "\"query\":{\"hybrid\":{\"mode\":\"fused\",\"queries\":["
            + "{\"match\":{\""
            + TEXT_FIELD
            + "\":\"hello\"}},"
            + "{\"match\":{\""
            + TITLE_FIELD
            + "\":\"place\"}}]}}";
    }

    private static final String SINGLE_TIER_RESCORE = "\"rescore\":{\"window_size\":100,\"query\":{"
        + "\"rescore_query\":{\"constant_score\":{\"filter\":{\"term\":{\"campaign_id\":\"SUMMER\"}},\"boost\":100}},"
        + "\"query_weight\":1,\"rescore_query_weight\":1,\"score_mode\":\"total\"}}";

    private static final String TWO_TIER_DISMAX_RESCORE = "\"rescore\":{\"window_size\":100,\"query\":{"
        + "\"rescore_query\":{\"dis_max\":{\"tie_breaker\":0,\"queries\":["
        + "{\"constant_score\":{\"filter\":{\"term\":{\"campaign_id\":\"SUMMER\"}},\"boost\":20}},"
        + "{\"constant_score\":{\"filter\":{\"term\":{\"campaign_id\":\"FALL\"}},\"boost\":10}}]}},"
        + "\"query_weight\":1,\"rescore_query_weight\":1,\"score_mode\":\"total\"}}";

    /**
     * Single-band promotion: {@code constant_score(term campaign_id=SUMMER)^100, score_mode:total} on top of the fused
     * query. Expect: request accepted, promoted-low is hit #1 with _score > 100 (band + fused), the remaining docs keep
     * their relative fused order below, and total hits are unchanged vs the same fused query without rescore.
     */
    @SneakyThrows
    public void testFusedMode_whenConstantScoreRescore_thenPromotedDocBandsToTop() {
        prepareIndex();

        Map<String, Object> baseline = searchWithRawBody("{" + fusedQueryJson() + "}");
        Map<String, Object> rescored = searchWithRawBody("{" + fusedQueryJson() + "," + SINGLE_TIER_RESCORE + "}");

        List<Map<String, Object>> baselineHits = getNestedHits(baseline);
        List<Map<String, Object>> rescoredHits = getNestedHits(rescored);
        logHits("BASELINE_NO_RESCORE", baselineHits);
        logHits("SINGLE_TIER_RESCORE", rescoredHits);

        // (a) request succeeded (searchWithRawBody asserts 200); sanity: fused legs matched 5 of the 6 docs
        assertEquals(5, getHitCount(baseline));
        // promoted doc must NOT already be first organically, else the promotion assertion is vacuous
        assertNotEquals(PROMOTED_SUMMER_ID, baselineHits.get(0).get("_id"));

        // (b) promoted-low is hit #1
        assertEquals(PROMOTED_SUMMER_ID, rescoredHits.get(0).get("_id"));
        // (c) band applied ON TOP of the fused score: _score > 100
        double topScore = score(rescoredHits.get(0));
        assertTrue("promoted doc score must exceed the 100 band, got " + topScore, topScore > 100.0);

        // (d) the remaining docs keep their relative fused order below the promoted doc
        List<String> baselineOrderWithoutPromoted = idsExcluding(baselineHits, PROMOTED_SUMMER_ID);
        List<String> rescoredOrderBelowTop = idsExcluding(rescoredHits, PROMOTED_SUMMER_ID);
        assertEquals("non-promoted docs must keep relative fused order", baselineOrderWithoutPromoted, rescoredOrderBelowTop);

        // (e) total hits unchanged by rescore
        assertEquals(getTotalHits(baseline), getTotalHits(rescored));
    }

    /**
     * Two-tier promotion via a {@code dis_max} rescore query (tie_breaker 0 = first-match-wins): SUMMER band 20, FALL
     * band 10. Expect the SUMMER doc #1, the FALL doc #2, organic docs below in fused order — multi-tier promotion with
     * zero plugin code.
     */
    @SneakyThrows
    public void testFusedMode_whenTwoTierDisMaxRescore_thenTiersRankAboveOrganic() {
        prepareIndex();

        Map<String, Object> baseline = searchWithRawBody("{" + fusedQueryJson() + "}");
        Map<String, Object> rescored = searchWithRawBody("{" + fusedQueryJson() + "," + TWO_TIER_DISMAX_RESCORE + "}");

        List<Map<String, Object>> baselineHits = getNestedHits(baseline);
        List<Map<String, Object>> rescoredHits = getNestedHits(rescored);
        logHits("BASELINE_NO_RESCORE", baselineHits);
        logHits("TWO_TIER_DISMAX_RESCORE", rescoredHits);

        assertEquals(5, getHitCount(baseline));

        // tier 1: SUMMER doc first, banded above 20
        assertEquals(PROMOTED_SUMMER_ID, rescoredHits.get(0).get("_id"));
        assertTrue("SUMMER tier score must exceed its 20 band", score(rescoredHits.get(0)) > 20.0);
        // tier 2: FALL doc second, banded above 10 but below the SUMMER tier
        assertEquals(PROMOTED_FALL_ID, rescoredHits.get(1).get("_id"));
        double fallScore = score(rescoredHits.get(1));
        assertTrue("FALL tier score must exceed its 10 band", fallScore > 10.0);
        assertTrue("FALL tier must stay below the SUMMER tier", fallScore < score(rescoredHits.get(0)));

        // organic docs below the tiers, still in fused order
        List<String> baselineOrganicOrder = idsExcluding(baselineHits, PROMOTED_SUMMER_ID, PROMOTED_FALL_ID);
        List<String> rescoredOrganicOrder = idsExcluding(rescoredHits, PROMOTED_SUMMER_ID, PROMOTED_FALL_ID);
        assertEquals("organic docs must keep relative fused order below the tiers", baselineOrganicOrder, rescoredOrganicOrder);
        for (int i = 2; i < rescoredHits.size(); i++) {
            assertTrue("organic docs must score below both tiers", score(rescoredHits.get(i)) < fallScore);
        }

        assertEquals(getTotalHits(baseline), getTotalHits(rescored));
    }

    // ---------------------------------------------------------------------------------------------
    // helpers (mirroring HybridQueryFusedModeIT conventions)
    // ---------------------------------------------------------------------------------------------

    @SneakyThrows
    private void indexDoc(String id, String text, String title, String campaign) {
        StringBuilder doc = new StringBuilder("{\"").append(TEXT_FIELD).append("\":\"").append(text).append("\"");
        if (title != null) {
            doc.append(",\"").append(TITLE_FIELD).append("\":\"").append(title).append("\"");
        }
        if (campaign != null) {
            doc.append(",\"").append(CAMPAIGN_FIELD).append("\":\"").append(campaign).append("\"");
        }
        doc.append("}");
        Request request = new Request("PUT", "/" + INDEX_NAME + "/_doc/" + id + "?refresh=true");
        request.setJsonEntity(doc.toString());
        Response response = client().performRequest(request);
        int code = response.getStatusLine().getStatusCode();
        assertTrue("indexing doc failed: " + code, code == RestStatus.OK.getStatus() || code == RestStatus.CREATED.getStatus());
    }

    @SneakyThrows
    private Map<String, Object> searchWithRawBody(String jsonBody) {
        Request request = new Request("POST", "/" + INDEX_NAME + "/_search");
        request.setJsonEntity(jsonBody);
        request.addParameter("size", "10");
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getNestedHits(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        return (List<Map<String, Object>>) hitsMap.get("hits");
    }

    @SuppressWarnings("unchecked")
    private int getTotalHits(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        Map<String, Object> total = (Map<String, Object>) hitsMap.get("total");
        return ((Number) total.get("value")).intValue();
    }

    private double score(Map<String, Object> hit) {
        return ((Number) hit.get("_score")).doubleValue();
    }

    private List<String> idsExcluding(List<Map<String, Object>> hits, String... excludedIds) {
        List<String> excluded = List.of(excludedIds);
        return hits.stream().map(hit -> (String) hit.get("_id")).filter(id -> excluded.contains(id) == false).collect(Collectors.toList());
    }

    private void logHits(String label, List<Map<String, Object>> hits) {
        List<String> rendered = new ArrayList<>();
        for (Map<String, Object> hit : hits) {
            rendered.add(hit.get("_id") + "=" + hit.get("_score"));
        }
        System.out.println("HITS[" + label + "]=" + rendered);
    }
}
