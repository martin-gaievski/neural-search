/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hc.core5.http.HttpHeaders;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import lombok.SneakyThrows;

/**
 * Document-level security applied to a fused (in-query fusion) hybrid query, on both of its paths.
 *
 * <p>The scenario is neural-search#1303: a role with a DLS rule, an internal user mapped to it, a hybrid query run as
 * that user. Classic hybrid needed shard-side unwrapping (neural-search#1432) and later a security-side fix
 * (security#6416) because the DLS machinery wraps the shard-level query around {@code HybridQuery}. The fused mode's
 * exposure is different and this class pins it down: the legs run as ordinary searches under the user's context (DLS
 * applies to each leg at the shard), and round 2 — the self-erased bool, or {@code match_none} on the fast path — is
 * not a {@code HybridQuery} at all, so the wrap that broke #1303 has nothing to break. When the security plugin (or a
 * user) attaches a filter through {@code HybridQueryBuilder#filter}, it is pushed into every sub-query before the
 * rewrite builds the legs, so the legs inherit it.
 *
 * <p>What must hold, and what each test asserts: a DLS-restricted user must never see a document outside the DLS
 * scope — not in the hits (including the fast path's coordinator-assembled page), not in aggregations, and not in
 * {@code hits.total} (the leg-derived count must count only what the user may see). The all-ties test doubles as the
 * fast-path liveness proof: its {@code _id}-ordered page can only be produced by the fast path, so the assertion
 * passing shows the leak check really exercised the assembled page.
 *
 * @see <a href="https://github.com/opensearch-project/neural-search/issues/1303">neural-search#1303</a>
 * @see <a href="https://github.com/opensearch-project/security/pull/6416">security#6416</a>
 * @see <a href="https://github.com/opensearch-project/neural-search/pull/1957">neural-search#1957 (classic-hybrid companion IT)</a>
 */
public class HybridQueryFusedModeDlsIT extends BaseNeuralSearchIT {

    private static final String INDEX_NAME = "fused-hybrid-dls-test";
    private static final String PIPELINE_NAME = "fused-hybrid-dls-classic-pipeline";
    private static final String ROLE_NAME = "fused_hybrid_dls_test_role";
    private static final String USER_NAME = "fused_hybrid_dls_test_user";
    private static final String USER_PASSWORD = "FusedHybridDlsTest1!";
    private static final String DLS_QUERY_JSON = "{\\\"term\\\":{\\\"access\\\":\\\"allowed\\\"}}";

    /** Ids chosen so the allowed set's lexicographic order ("1","10","11","12","2","3") differs from insertion order. */
    private static final Set<String> ALLOWED_IDS = Set.of("1", "2", "3", "10", "11", "12");
    private static final Set<String> BLOCKED_IDS = Set.of("4", "5", "6", "7", "8", "9");
    private static final Set<String> ALL_IDS = Set.of("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12");
    /** Spans both sides of the DLS line, for the filter-composition test. */
    private static final Set<String> GROUP_ONE_IDS = Set.of("1", "2", "10", "4", "5");

    @BeforeClass
    public static void requireSecurityPlugin() {
        assumeTrue("requires the Security plugin", isSecurityPluginEnabled());
    }

    private static boolean isSecurityPluginEnabled() {
        return Boolean.parseBoolean(System.getProperty("security.enabled", "false"));
    }

    @Before
    @SneakyThrows
    public void setUpDlsResources() {
        createIndexAndDocuments();
        assertOk(adminRequest("PUT", "/_search/pipeline/" + PIPELINE_NAME, """
            {
              "phase_results_processors": [
                {
                  "normalization-processor": {
                    "normalization": { "technique": "min_max" },
                    "combination": { "technique": "arithmetic_mean" }
                  }
                }
              ]
            }
            """));
        assertCreatedOrOk(adminRequest("PUT", "/_plugins/_security/api/roles/" + ROLE_NAME, String.format(Locale.ROOT, """
            {
              "cluster_permissions": ["indices:data/read/msearch"],
              "index_permissions": [
                {
                  "index_patterns": ["%s"],
                  "allowed_actions": ["read"],
                  "dls": "%s"
                }
              ]
            }
            """, INDEX_NAME, DLS_QUERY_JSON)));
        assertCreatedOrOk(
            adminRequest(
                "PUT",
                "/_plugins/_security/api/internalusers/" + USER_NAME,
                String.format(Locale.ROOT, "{\"password\": \"%s\", \"opendistro_security_roles\": [\"%s\"]}", USER_PASSWORD, ROLE_NAME)
            )
        );
    }

    @After
    @SneakyThrows
    public void cleanUpDlsResources() {
        if (isSecurityPluginEnabled() == false) {
            return;
        }
        for (String endpoint : List.of(
            "/_plugins/_security/api/internalusers/" + USER_NAME,
            "/_plugins/_security/api/roles/" + ROLE_NAME,
            "/_search/pipeline/" + PIPELINE_NAME,
            "/" + INDEX_NAME
        )) {
            try {
                client().performRequest(new Request("DELETE", endpoint));
            } catch (ResponseException e) {
                if (e.getResponse().getStatusLine().getStatusCode() != RestStatus.NOT_FOUND.getStatus()) {
                    throw e;
                }
            }
        }
    }

    /**
     * The #1303 regression control: classic hybrid through the normalization pipeline, as admin and as the DLS user.
     * On stacks whose Security plugin carries security#6416 (3.9+) or whose wrap shape neural-search#1432 recognizes,
     * the user gets clean DLS-scoped results; on stacks in between (Security 3.8's wrap shape), the request fails with
     * the exact #1303 signature. Both outcomes are asserted so the test documents the classic path's state on whatever
     * stack it runs — the fused-mode tests below must pass on all of them.
     */
    @SneakyThrows
    public void testClassicHybrid_whenDlsRestrictedUser_thenScopedResultsOrTheKnown1303Failure() {
        String request = """
            {
              "size": 20,
              "query": {
                "hybrid": {
                  "queries": [
                    { "match": { "text": "hello" } },
                    { "match": { "text": "place" } }
                  ]
                }
              }
            }
            """;
        assertHitIdSet(search(request, false, true), ALL_IDS);
        try {
            assertHitIdSet(search(request, true, true), ALLOWED_IDS);
            logger.info("classic hybrid + DLS: clean on this stack (post-#6416 or #1432-recognized wrap shape)");
        } catch (Exception e) {
            assertTrue("expected a ResponseException, got " + e.getClass(), e instanceof ResponseException);
            String body = new String(((ResponseException) e).getResponse().getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
            assertTrue(
                "expected the #1303 signature, got: " + body,
                body.contains("cannot be cast to class org.opensearch.neuralsearch.query.HybridQuery")
                    || body.contains("hybrid query must be a top level query")
            );
            logger.info("classic hybrid + DLS: reproduces #1303 on this stack (Security wrap shape not recognized)");
        }
    }

    /**
     * Fused mode on the two-round path (aggregations and exact totals both force round 2): the page, the aggregation
     * buckets, and the exact total all stay inside the DLS scope.
     */
    @SneakyThrows
    public void testFusedTwoRounds_whenDlsRestrictedUser_thenHitsAggregationsAndTotalStayInScope() {
        String request = fusedBody(
            20,
            "\"size\": 20, \"track_total_hits\": true, \"aggs\": {\"by_access\": {\"terms\": {\"field\": \"access\"}}}",
            "{ \"match\": { \"text\": \"hello\" } }",
            "{ \"match\": { \"text\": \"place\" } }"
        );

        Map<String, Object> admin = search(request, false, false);
        assertHitIdSet(admin, ALL_IDS);
        assertEquals(Map.of("value", 12, "relation", "eq"), hits(admin).get("total"));
        assertEquals(Map.of("allowed", 6, "blocked", 6), accessBuckets(admin));

        Map<String, Object> restricted = search(request, true, false);
        assertHitIdSet(restricted, ALLOWED_IDS);
        assertEquals(Map.of("value", 6, "relation", "eq"), hits(restricted).get("total"));
        assertEquals(Map.of("allowed", 6), accessBuckets(restricted));
    }

    /**
     * The fast path: every fused score ties (both legs are constant-scoring term queries), the shape is eligible, so the
     * page is assembled on the coordinator from the legs' hits — the surface where a leg escaping DLS would leak straight
     * to the client. The page must hold exactly the allowed documents, in the fast path's {@code _id} order — which is
     * also the proof the fast path ran, since round 2 cannot produce that order here — and ids and scores must match the
     * two-round control. The control is forced with {@code track_total_hits: true} (exact totals always take round 2),
     * not the usual {@code profile: true} twin: the Security plugin rejects profiling under DLS outright. The control's
     * own page comes back in round 2's Lucene doc-id order — the two orders differing is the proof both paths ran.
     */
    @SneakyThrows
    public void testFusedFastPath_whenDlsRestrictedUserAndAllScoresTie_thenPageHasOnlyAllowedDocumentsInIdOrder() {
        String extras = "\"size\": 6, \"track_total_hits\": false, \"_source\": true";
        String legOne = "{ \"term\": { \"tag\": \"same\" } }";
        String legTwo = "{ \"term\": { \"tag2\": \"same\" } }";

        String controlExtras = "\"size\": 6, \"track_total_hits\": true, \"_source\": true";

        Map<String, Object> fast = search(fusedBody(10, extras, legOne, legTwo), true, false);
        Map<String, Object> control = search(fusedBody(10, controlExtras, legOne, legTwo), true, false);

        List<String> fastIds = hitIds(fast);
        assertEquals("no document outside the DLS scope on the assembled page", ALLOWED_IDS, Set.copyOf(fastIds));
        assertEquals("the fast path's _id tie order — round 2 cannot produce this", List.of("1", "10", "11", "12", "2", "3"), fastIds);
        assertEquals(
            "round 2's doc-id tie order — the fast path cannot produce this",
            List.of("1", "2", "3", "10", "11", "12"),
            hitIds(control)
        );
        assertEquals(
            "the exact DLS-visible count, counted by round 2's Tail",
            Map.of("value", 6, "relation", "eq"),
            hits(control).get("total")
        );

        // Scores: the fast path's page carries the coordinator's fused scores untouched; round 2's come from the shard
        // executing the substitute under whatever wrap the Security plugin applies. In lucene-level DLS mode the wrap is
        // a bool whose scoring constant_score(dls) clause shifts every visible document's score by a constant (+1.0
        // here) — a platform-wide effect on any scored query under DLS, not a fused artifact. Measure that shift on a
        // plain term query and assert round 2 differs from the fast path by exactly it; on a stack whose Security
        // plugin does not shift scores the measured shift is 0 and the two paths' scores are identical.
        double dlsShift = uniformScore(searchPlainTerm(true)) - uniformScore(searchPlainTerm(false));
        double fastScore = uniformScore(fast);
        double controlScore = uniformScore(control);
        assertEquals("all-ties fused score is the zero-range min_max convention", 1.0, fastScore, 0.0001);
        assertEquals("round 2 differs from the fast path by exactly the DLS score shift", dlsShift, controlScore - fastScore, 0.0001);
        assertNoFailedShards(fast);
        assertNoFailedShards(control);
    }

    /** A filter on the hybrid itself composes with DLS: the user sees the intersection, the admin the filter alone. */
    @SneakyThrows
    public void testFusedWithHybridFilter_whenDlsRestrictedUser_thenFiltersCompose() {
        String request = fusedBody(
            10,
            "\"size\": 10, \"track_total_hits\": false",
            "{ \"match\": { \"text\": \"hello\" } }",
            "{ \"match\": { \"text\": \"place\" } }"
        ).replace("\"queries\":", "\"filter\": { \"term\": { \"group\": \"g1\" } }, \"queries\":");

        assertHitIdSet(search(request, false, false), GROUP_ONE_IDS);
        Set<String> allowedInGroupOne = GROUP_ONE_IDS.stream().filter(ALLOWED_IDS::contains).collect(Collectors.toSet());
        assertHitIdSet(search(request, true, false), allowedInGroupOne);
    }

    /**
     * The leg-derived total under DLS. Threshold 8 with window 6: the admin's legs count 8 visible matches, prove the
     * threshold, and the fast path reports the derived {@code gte} total. The user's legs can only ever count the 6
     * visible documents — the threshold is unprovable, the request falls back to the two-round build, and the Tail's
     * count is the exact DLS-visible 6. Wrong behavior here would be a count leak: a total acknowledging documents the
     * user cannot see.
     */
    @SneakyThrows
    public void testFusedDerivedTotal_whenDlsRestrictedUser_thenTotalCountsOnlyVisibleDocuments() {
        String request = fusedBody(
            6,
            "\"size\": 3, \"track_total_hits\": 8, \"_source\": true",
            "{ \"match\": { \"text\": \"hello\" } }",
            "{ \"match\": { \"text\": \"place\" } }"
        );

        Map<String, Object> admin = search(request, false, false);
        assertEquals(Map.of("value", 8, "relation", "gte"), hits(admin).get("total"));

        Map<String, Object> restricted = search(request, true, false);
        assertEquals(Map.of("value", 6, "relation", "eq"), hits(restricted).get("total"));
        assertTrue("hits stay inside the DLS scope", ALLOWED_IDS.containsAll(hitIds(restricted)));
    }

    private String fusedBody(int windowSize, String extras, String... legs) {
        return String.format(Locale.ROOT, """
            {
              %s,
              "query": {
                "hybrid": {
                  "fusion": {
                    "window_size": %d,
                    "normalization": { "technique": "min_max" },
                    "combination": { "technique": "arithmetic_mean" }
                  },
                  "queries": [%s]
                }
              }
            }
            """, extras, windowSize, String.join(",", legs));
    }

    @SneakyThrows
    private void createIndexAndDocuments() {
        try {
            client().performRequest(new Request("GET", "/" + INDEX_NAME));
            return;
        } catch (ResponseException e) {
            // absent — create it
        }
        assertOk(adminRequest("PUT", "/" + INDEX_NAME, """
            {
              "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
              "mappings": {
                "properties": {
                  "access": { "type": "keyword" },
                  "group": { "type": "keyword" },
                  "tag": { "type": "keyword" },
                  "tag2": { "type": "keyword" },
                  "text": { "type": "text" }
                }
              }
            }
            """));
        for (int i = 1; i <= 12; i++) {
            String id = String.valueOf(i);
            String access = ALLOWED_IDS.contains(id) ? "allowed" : "blocked";
            String group = GROUP_ONE_IDS.contains(id) ? "g1" : "g2";
            // distinct lengths give every document a distinct BM25 score, so non-tie tests order deterministically
            String text = "hello place" + " filler".repeat(i);
            assertCreatedOrOk(
                adminRequest(
                    "PUT",
                    "/" + INDEX_NAME + "/_doc/" + id,
                    String.format(
                        Locale.ROOT,
                        "{\"access\": \"%s\", \"group\": \"%s\", \"tag\": \"same\", \"tag2\": \"same\", \"text\": \"%s\"}",
                        access,
                        group,
                        text
                    )
                )
            );
        }
        assertOk(client().performRequest(new Request("POST", "/" + INDEX_NAME + "/_refresh")));
    }

    @SneakyThrows
    private Map<String, Object> search(String requestBody, boolean asDlsUser, boolean withPipeline) {
        Request request = new Request("POST", "/" + INDEX_NAME + "/_search");
        if (withPipeline) {
            request.addParameter("search_pipeline", PIPELINE_NAME);
        }
        request.setJsonEntity(requestBody);
        if (asDlsUser) {
            RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
            String credentials = USER_NAME + ":" + USER_PASSWORD;
            options.addHeader(
                HttpHeaders.AUTHORIZATION,
                "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8))
            );
            request.setOptions(options.build());
        }
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        return responseAsMap(response);
    }

    private void assertOk(Response response) {
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
    }

    private void assertCreatedOrOk(Response response) {
        int status = response.getStatusLine().getStatusCode();
        assertTrue(
            "expected 200 or 201 but got " + status,
            status == RestStatus.OK.getStatus() || status == RestStatus.CREATED.getStatus()
        );
    }

    private Response adminRequest(String method, String endpoint, String body) throws IOException {
        Request request = new Request(method, endpoint);
        request.setJsonEntity(body);
        return client().performRequest(request);
    }

    private void assertHitIdSet(Map<String, Object> response, Set<String> expectedIds) {
        assertNoFailedShards(response);
        List<String> ids = hitIds(response);
        assertEquals("no duplicate documents", ids.size(), Set.copyOf(ids).size());
        assertEquals(expectedIds, Set.copyOf(ids));
    }

    private void assertNoFailedShards(Map<String, Object> response) {
        assertEquals(0, ((Number) asMap(response.get("_shards")).get("failed")).intValue());
    }

    private Map<String, Object> hits(Map<String, Object> response) {
        return asMap(response.get("hits"));
    }

    private List<String> hitIds(Map<String, Object> response) {
        return asList(hits(response).get("hits")).stream().map(hit -> (String) hit.get("_id")).collect(Collectors.toList());
    }

    /** A plain (non-hybrid) constant-scoring query, to measure what DLS alone does to a shard-computed score. */
    @SneakyThrows
    private Map<String, Object> searchPlainTerm(boolean asDlsUser) {
        return search("{\"size\": 1, \"query\": { \"term\": { \"tag\": \"same\" } }}", asDlsUser, false);
    }

    /** Every hit in these responses ties at one score; return it, asserting the uniformity. */
    private double uniformScore(Map<String, Object> response) {
        Set<Object> distinct = asList(hits(response).get("hits")).stream().map(hit -> hit.get("_score")).collect(Collectors.toSet());
        assertEquals("expected one uniform score, got " + distinct, 1, distinct.size());
        return ((Number) distinct.iterator().next()).doubleValue();
    }

    private Map<String, Integer> accessBuckets(Map<String, Object> response) {
        List<Map<String, Object>> buckets = asList(asMap(asMap(response.get("aggregations")).get("by_access")).get("buckets"));
        return buckets.stream()
            .collect(Collectors.toMap(bucket -> (String) bucket.get("key"), bucket -> ((Number) bucket.get("doc_count")).intValue()));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object value) {
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> asList(Object value) {
        return (List<Map<String, Object>>) value;
    }
}
