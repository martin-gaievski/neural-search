/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import lombok.SneakyThrows;

/**
 * Integration tests for the hybrid query {@code mode: "fused"} path (Option P): the query self-erases at the
 * coordinator, reads its normalization/combination config from the attached search pipeline (same source as classic
 * hybrid), and returns fused results — WITHOUT the phase-results normalization processor running on the results.
 *
 * <p>Uses two lexical legs on a plain text index so the mechanism (pipeline-read + coordinator fusion + self-erase) is
 * exercised end-to-end without KNN/model plumbing.
 *
 * <p>Config source: {@code index.search.default_pipeline} is the ONLY source that works today with no core change —
 * the pipeline id lives in cluster state and survives to the coordinator rewrite. The other two sources are pre-req
 * core gaps (both verified below/here):
 * <ul>
 *   <li>The {@code ?search_pipeline=} URL param reads null at rewrite: core wraps the resolved request through the
 *       {@code SearchRequest} copy constructor, which does not copy the {@code pipeline} field.</li>
 *   <li>The inline-body {@code search_pipeline} object is drained to empty at rewrite: core's {@code resolvePipeline}
 *       builds the ad-hoc pipeline (before query rewrite) via {@code ConfigurationUtils.readOptionalList}, which
 *       {@code remove()}s {@code phase_results_processors} from the same live source map.</li>
 * </ul>
 * Both need a small core change; {@code index.search.default_pipeline} needs none.
 */
public class HybridQueryFusedModeIT extends BaseNeuralSearchIT {

    private static final String TEXT_FIELD = "text";
    private static final String TITLE_FIELD = "title";
    private static final String NESTED_PATH = "user";
    private static final String INDEX_WITH_DEFAULT_NORM = "test-hybrid-fused-default-norm";
    private static final String INDEX_WITH_DEFAULT_RRF = "test-hybrid-fused-default-rrf";
    private static final String INDEX_NO_PIPELINE = "test-hybrid-fused-no-pipeline";
    private static final String INDEX_NESTED = "test-hybrid-fused-nested-innerhits";
    private static final String NORM_PIPELINE = "fused-mode-norm-pipeline";
    private static final String RRF_PIPELINE = "fused-mode-rrf-pipeline";

    private String indexConfigWithDefaultPipeline(String pipelineId) {
        return "{\"settings\":{\"number_of_shards\":3,\"number_of_replicas\":0"
            + (pipelineId == null ? "" : ",\"index.search.default_pipeline\":\"" + pipelineId + "\"")
            + "},\"mappings\":{\"properties\":{\""
            + TEXT_FIELD
            + "\":{\"type\":\"text\"}}}}";
    }

    @SneakyThrows
    private void addFourDocs(String index) {
        addDocument(index, "1", TEXT_FIELD, "hello world hello", null, null);
        addDocument(index, "2", TEXT_FIELD, "hello there place", null, null);
        addDocument(index, "3", TEXT_FIELD, "welcome to the place", null, null);
        addDocument(index, "4", TEXT_FIELD, "nothing relevant at all", null, null);
    }

    private HybridQueryBuilder fusedTwoLegQuery() {
        HybridQueryBuilder fused = new HybridQueryBuilder().mode(HybridQueryBuilder.Mode.FUSED);
        fused.add(new MatchQueryBuilder(TEXT_FIELD, "hello"));
        fused.add(new TermQueryBuilder(TEXT_FIELD, "place"));
        return fused;
    }

    @SneakyThrows
    public void testFusedMode_whenIndexDefaultNormalizationPipeline_thenReadsConfigAndFuses() {
        // classic normalization pipeline (min_max + arithmetic_mean), attached as the index default — UNCHANGED from
        // what an existing hybrid user has today. The fused query adds ONE token and reads this config at rewrite.
        createSearchPipeline(NORM_PIPELINE, "min_max", "arithmetic_mean", Map.of());
        if (indexExists(INDEX_WITH_DEFAULT_NORM) == false) {
            createIndex(INDEX_WITH_DEFAULT_NORM, indexConfigWithDefaultPipeline(NORM_PIPELINE));
            addFourDocs(INDEX_WITH_DEFAULT_NORM);
        }

        Map<String, Object> response = search(INDEX_WITH_DEFAULT_NORM, fusedTwoLegQuery(), 10);

        // docs 1 (hello x2), 2 (hello + place), 3 (place) match at least one leg; doc 4 matches neither.
        assertEquals(3, getHitCount(response));
        List<Map<String, Object>> hits = getNestedHits(response);
        // doc 2 matches BOTH legs -> should rank first under min_max + arithmetic mean.
        assertEquals("2", hits.get(0).get("_id"));
        // scores are fused and descending, all > 0 for a matched doc.
        double prev = Double.MAX_VALUE;
        for (Map<String, Object> hit : hits) {
            double score = ((Number) hit.get("_score")).doubleValue();
            assertTrue("scores must be descending", score <= prev);
            assertTrue("fused score must be > 0 for a matched doc", score > 0.0);
            prev = score;
        }
    }

    @SneakyThrows
    public void testFusedMode_whenIndexDefaultRrfScoreRankerPipeline_thenFusesWithRrf() {
        createRRFSearchPipeline(RRF_PIPELINE, java.util.Arrays.asList(), false);
        if (indexExists(INDEX_WITH_DEFAULT_RRF) == false) {
            createIndex(INDEX_WITH_DEFAULT_RRF, indexConfigWithDefaultPipeline(RRF_PIPELINE));
            addFourDocs(INDEX_WITH_DEFAULT_RRF);
        }

        Map<String, Object> response = search(INDEX_WITH_DEFAULT_RRF, fusedTwoLegQuery(), 10);
        assertEquals(3, getHitCount(response));
        // doc 2 matches both legs -> top under RRF too.
        assertEquals("2", getNestedHits(response).get(0).get("_id"));
    }

    @SneakyThrows
    public void testFusedMode_whenNoPipeline_thenFailsFast() {
        if (indexExists(INDEX_NO_PIPELINE) == false) {
            createIndex(INDEX_NO_PIPELINE, indexConfigWithDefaultPipeline(null));
            addFourDocs(INDEX_NO_PIPELINE);
        }
        ResponseException e = expectThrows(ResponseException.class, () -> search(INDEX_NO_PIPELINE, fusedTwoLegQuery(), 10));
        assertTrue(e.getMessage().contains("requires a normalization or score-ranker processor") || e.getMessage().contains("mode=fused"));
    }

    @SneakyThrows
    public void testPipelineMode_whenModeOmitted_thenClassicBehaviorUnchanged() {
        createSearchPipeline(NORM_PIPELINE + "-classic", "min_max", "arithmetic_mean", Map.of());
        if (indexExists(INDEX_NO_PIPELINE) == false) {
            createIndex(INDEX_NO_PIPELINE, indexConfigWithDefaultPipeline(null));
            addFourDocs(INDEX_NO_PIPELINE);
        }

        // Classic hybrid (no mode) still uses the phase-results normalization pipeline via the named ?search_pipeline=.
        HybridQueryBuilder classic = new HybridQueryBuilder();
        classic.add(new MatchQueryBuilder(TEXT_FIELD, "hello"));
        classic.add(new TermQueryBuilder(TEXT_FIELD, "place"));

        Map<String, Object> response = search(
            INDEX_NO_PIPELINE,
            classic,
            null,
            10,
            Map.of("search_pipeline", NORM_PIPELINE + "-classic"),
            null
        );
        assertEquals(3, getHitCount(response));
    }

    /**
     * PRE-REQ GAP (verified): the inline-body pipeline form does NOT reach fused mode. Core's {@code resolvePipeline}
     * runs before query rewrite and builds the ad-hoc pipeline via {@code PipelineWithMetrics.create}, whose
     * {@code ConfigurationUtils.readOptionalList(config, "phase_results_processors")} REMOVES the key from the same
     * live {@code searchPipelineSource} map. So at rewrite the map is drained to empty and the fused resolver finds no
     * config -> fail-fast 400. This test pins the current behavior; when the core fix lands (see the design doc), flip
     * it to assert successful fusion.
     */
    @SneakyThrows
    public void testFusedMode_whenInlineBodyPipeline_thenFailsFastUntilCoreFix() {
        if (indexExists(INDEX_NO_PIPELINE) == false) {
            createIndex(INDEX_NO_PIPELINE, indexConfigWithDefaultPipeline(null));
            addFourDocs(INDEX_NO_PIPELINE);
        }
        String body = "{"
            + "\"search_pipeline\": { \"phase_results_processors\": [ { \"normalization-processor\": {"
            + "  \"normalization\": { \"technique\": \"min_max\" },"
            + "  \"combination\": { \"technique\": \"arithmetic_mean\" } } } ] },"
            + "\"query\": { \"hybrid\": { \"mode\": \"fused\", \"queries\": ["
            + "  { \"match\": { \""
            + TEXT_FIELD
            + "\": \"hello\" } },"
            + "  { \"term\": { \""
            + TEXT_FIELD
            + "\": \"place\" } } ] } } }";

        ResponseException e = expectThrows(ResponseException.class, () -> searchWithRawBody(INDEX_NO_PIPELINE, body, 10));
        assertTrue(e.getMessage().contains("requires a normalization or score-ranker processor") || e.getMessage().contains("mode=fused"));
    }

    @SneakyThrows
    private Map<String, Object> searchWithRawBody(String index, String jsonBody, int resultSize) {
        Request request = new Request("POST", "/" + index + "/_search");
        request.setJsonEntity(jsonBody);
        request.addParameter("size", Integer.toString(resultSize));
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    /**
     * Verifies that leg-level {@code inner_hits} survive the fused-mode self-erase. A {@code nested} sub-query declaring
     * inner_hits is fused with a lexical leg; the returned parent hits must carry the nested inner_hits. Without the
     * {@code HybridFusionQuery.extractInnerHitBuilders} override (+ Tail retention when a leg has inner_hits) the
     * self-erased query silently drops them, since the coordinator replaces the {@code hybrid} builder with
     * {@code HybridFusionQuery} before the shard extracts inner_hits.
     */
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public void testFusedMode_whenLegHasNestedInnerHits_thenInnerHitsReturned() {
        createSearchPipeline(NORM_PIPELINE + "-nested", "min_max", "arithmetic_mean", Map.of());
        if (indexExists(INDEX_NESTED) == false) {
            createIndex(INDEX_NESTED, indexConfigNested(NORM_PIPELINE + "-nested"));
            indexNestedDoc(INDEX_NESTED, "1", "hello world", "alice", "bob");
            indexNestedDoc(INDEX_NESTED, "2", "hello there", "alice", "carol");
            indexNestedDoc(INDEX_NESTED, "3", "welcome place", "dave");
        }

        // mode:fused hybrid — leg 1 lexical (title:hello), leg 2 nested (user.name:alice) with inner_hits.
        // docs 1 and 2 match both legs; doc 3 matches neither.
        String body = "{\"query\":{\"hybrid\":{\"mode\":\"fused\",\"queries\":["
            + "{\"match\":{\""
            + TITLE_FIELD
            + "\":\"hello\"}},"
            + "{\"nested\":{\"path\":\""
            + NESTED_PATH
            + "\",\"query\":{\"match\":{\""
            + NESTED_PATH
            + ".name\":\"alice\"}},"
            + "\"inner_hits\":{}}}"
            + "]}}}";

        Map<String, Object> response = searchWithRawBody(INDEX_NESTED, body, 10);

        List<Map<String, Object>> hits = getNestedHits(response);
        assertFalse("expected fused hits", hits.isEmpty());
        int hitsWithInnerHits = 0;
        for (Map<String, Object> hit : hits) {
            String id = (String) hit.get("_id");
            if (id.equals("1") == false && id.equals("2") == false) {
                continue;
            }
            Map<String, Object> innerHits = (Map<String, Object>) hit.get("inner_hits");
            assertNotNull("hit " + id + " must carry inner_hits", innerHits);
            Map<String, Object> userInner = (Map<String, Object>) innerHits.get(NESTED_PATH);
            assertNotNull("hit " + id + " must have '" + NESTED_PATH + "' inner_hits", userInner);
            List<Map<String, Object>> innerHitList = getNestedHits(userInner);
            assertFalse("hit " + id + " inner_hits must be non-empty", innerHitList.isEmpty());
            Map<String, Object> src = (Map<String, Object>) innerHitList.get(0).get("_source");
            assertEquals("matched nested doc must be the alice comment", "alice", src.get("name"));
            hitsWithInnerHits++;
        }
        assertEquals("both doc 1 and doc 2 must return inner_hits under fused mode", 2, hitsWithInnerHits);
    }

    private String indexConfigNested(String pipelineId) {
        return "{\"settings\":{\"number_of_shards\":3,\"number_of_replicas\":0,"
            + "\"index.search.default_pipeline\":\""
            + pipelineId
            + "\"},"
            + "\"mappings\":{\"properties\":{"
            + "\""
            + TITLE_FIELD
            + "\":{\"type\":\"text\"},"
            + "\""
            + NESTED_PATH
            + "\":{\"type\":\"nested\",\"properties\":{\"name\":{\"type\":\"text\"}}}}}}";
    }

    @SneakyThrows
    private void indexNestedDoc(String index, String id, String title, String... names) {
        StringBuilder users = new StringBuilder();
        for (int i = 0; i < names.length; i++) {
            if (i > 0) {
                users.append(",");
            }
            users.append("{\"name\":\"").append(names[i]).append("\"}");
        }
        String doc = "{\"" + TITLE_FIELD + "\":\"" + title + "\",\"" + NESTED_PATH + "\":[" + users + "]}";
        Request request = new Request("PUT", "/" + index + "/_doc/" + id + "?refresh=true");
        request.setJsonEntity(doc);
        Response response = client().performRequest(request);
        int code = response.getStatusLine().getStatusCode();
        assertTrue("indexing nested doc failed: " + code, code == RestStatus.OK.getStatus() || code == RestStatus.CREATED.getStatus());
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getNestedHits(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        return (List<Map<String, Object>>) hitsMap.get("hits");
    }
}
