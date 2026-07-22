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
import org.opensearch.client.ResponseException;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.processor.HybridFusedProfileResponseProcessor;

import lombok.SneakyThrows;

/**
 * Integration tests for the hybrid query {@code mode: "fused"} path (Option P): the query self-erases at the
 * coordinator, reads its normalization/combination config from the attached search pipeline (same source as classic
 * hybrid), and returns fused results — WITHOUT the phase-results normalization processor running on the results.
 *
 * <p>Uses two lexical legs on a plain text index so the mechanism (pipeline-read + coordinator fusion + self-erase) is
 * exercised end-to-end without KNN/model plumbing.
 *
 * <p>Config source: all three attach forms now reach fused mode. {@code index.search.default_pipeline} always worked
 * with no core change (the id lives in cluster state and survives to the coordinator rewrite). The other two were
 * pre-req core gaps until <a href="https://github.com/opensearch-project/OpenSearch/pull/22501">core PR #22501</a>
 * (merged, in 3.8) fixed both:
 * <ul>
 *   <li>The {@code ?search_pipeline=} URL param used to read null at rewrite because core wrapped the resolved request
 *       through the {@code SearchRequest} copy constructor, which did not copy the {@code pipeline} field. The fix
 *       preserves it in the {@code PipelinedRequest} constructor.</li>
 *   <li>The inline-body {@code search_pipeline} object used to be drained to empty at rewrite because core's
 *       {@code resolvePipeline} built the ad-hoc pipeline (before query rewrite) via
 *       {@code ConfigurationUtils.readOptionalList}, which {@code remove()}d {@code phase_results_processors} from the
 *       same live source map. The fix builds the ad-hoc pipeline from a deep copy, leaving the source map intact.</li>
 * </ul>
 * The plugin's {@code FusionConfigResolver} already reads all three sources, so no plugin change was needed — the core
 * fix flows straight through. NOTE: requires a core build that includes #22501 (3.8.0-SNAPSHOT from 2026-07-22 or later).
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
     * Inline-body {@code search_pipeline} form now reaches fused mode after core fix
     * <a href="https://github.com/opensearch-project/OpenSearch/pull/22501">#22501</a> (merged, in 3.8): core's
     * {@code resolvePipeline} previously built the ad-hoc pipeline via
     * {@code ConfigurationUtils.readOptionalList(config, "phase_results_processors")}, which {@code remove()}d the key
     * from the same live {@code searchPipelineSource} map, so at rewrite the map was drained to empty and the fused
     * resolver found no config (fail-fast 400). The fix builds the ad-hoc pipeline from a {@code deepCopyConfig(...)}
     * of the source, leaving the request's {@code searchPipelineSource} intact for later readers — so at rewrite the
     * fused resolver reads the min_max + arithmetic_mean config and fuses. (Was
     * {@code testFusedMode_whenInlineBodyPipeline_thenFailsFastUntilCoreFix}, flipped when the fix landed.)
     */
    @SneakyThrows
    public void testFusedMode_whenInlineBodyPipeline_thenReadsConfigAndFuses() {
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

        Map<String, Object> response = searchWithRawBody(INDEX_NO_PIPELINE, body, 10);
        assertEquals(3, getHitCount(response));
        // doc 2 matches BOTH legs -> ranks first under min_max + arithmetic_mean, same as the index-default path.
        assertEquals("2", getNestedHits(response).get(0).get("_id"));
    }

    /**
     * URL-param ({@code ?search_pipeline=<name>}) form now reaches fused mode after core fix
     * <a href="https://github.com/opensearch-project/OpenSearch/pull/22501">#22501</a> (merged, in 3.8): core wraps the
     * resolved request through the {@code PipelinedRequest} constructor, which now preserves the {@code pipeline} id via
     * {@code this.pipeline(transformedRequest.pipeline())} (previously the {@code SearchRequest} copy constructor dropped
     * it, so {@code searchRequest.pipeline()} read null at rewrite → fail-fast 400). With the id preserved, the fused
     * resolver looks the named pipeline's config up in cluster-state metadata and fuses.
     */
    @SneakyThrows
    public void testFusedMode_whenUrlParamPipeline_thenReadsConfigAndFuses() {
        createSearchPipeline(NORM_PIPELINE + "-urlparam", "min_max", "arithmetic_mean", Map.of());
        if (indexExists(INDEX_NO_PIPELINE) == false) {
            createIndex(INDEX_NO_PIPELINE, indexConfigWithDefaultPipeline(null));
            addFourDocs(INDEX_NO_PIPELINE);
        }

        Map<String, Object> response = search(
            INDEX_NO_PIPELINE,
            fusedTwoLegQuery(),
            null,
            10,
            Map.of("search_pipeline", NORM_PIPELINE + "-urlparam"),
            null
        );
        assertEquals(3, getHitCount(response));
        // doc 2 matches BOTH legs -> ranks first under min_max + arithmetic_mean, same as the index-default path.
        assertEquals("2", getNestedHits(response).get(0).get("_id"));
    }

    /**
     * Profiler fix: with {@code mode:"fused"} the outer shard still profiles the SELF-ERASED query (constant_score Top +
     * Tail, never a {@code HybridQuery}), but the fix additionally profiles each sub-query leg and merges those per-leg
     * profiles into the response under {@code [fused_leg_N]}-namespaced shard entries. So the response now carries the
     * real per-sub-query scoring the self-erased outer query cannot show. Classic hybrid, for reference, profiles the
     * real {@code HybridQuery} with per-sub-query children.
     */
    @SneakyThrows
    public void testFusedMode_profiler_surfacesPerLegSubQueryProfiles() {
        createSearchPipeline(NORM_PIPELINE, "min_max", "arithmetic_mean", Map.of());
        if (indexExists(INDEX_WITH_DEFAULT_NORM) == false) {
            createIndex(INDEX_WITH_DEFAULT_NORM, indexConfigWithDefaultPipeline(NORM_PIPELINE));
            addFourDocs(INDEX_WITH_DEFAULT_NORM);
        }

        // System-generated processors are off by default; enable the fused-profile factory (mirrors how the semantic
        // highlighting ITs enable theirs). This is the real deployment prerequisite for the fused profiler fix.
        updateClusterSettings("cluster.search.enabled_system_generated_factories", List.of(HybridFusedProfileResponseProcessor.TYPE));
        try {
            String fusedBody = "{\"profile\":true,\"query\":{\"hybrid\":{\"mode\":\"fused\",\"queries\":["
                + "{\"match\":{\""
                + TEXT_FIELD
                + "\":\"hello\"}},{\"term\":{\""
                + TEXT_FIELD
                + "\":\"place\"}}]}}}";
            Map<String, Object> fusedResp = searchWithRawBody(INDEX_WITH_DEFAULT_NORM, fusedBody, 10);
            Map<String, List<String>> typesByShard = collectProfileTypesByShard(fusedResp);
            List<String> legShardIds = typesByShard.keySet()
                .stream()
                .filter(id -> id.startsWith("[fused_leg_"))
                .collect(Collectors.toList());
            List<String> outerTypes = typesByShard.entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith("[fused_leg_") == false)
                .flatMap(e -> e.getValue().stream())
                .collect(Collectors.toList());
            List<String> legTypes = legShardIds.stream().flatMap(id -> typesByShard.get(id).stream()).collect(Collectors.toList());
            System.out.println("PROFILE_LEG_SHARD_IDS=" + legShardIds);
            System.out.println("PROFILE_OUTER_TYPES=" + outerTypes);
            System.out.println("PROFILE_LEG_TYPES=" + legTypes);

            // Outer request still profiles the self-erased query: constant_score Top clauses, NO HybridQuery.
            assertTrue("outer profile should show ConstantScoreQuery (self-erased Top)", outerTypes.contains("ConstantScoreQuery"));
            assertFalse("outer profile must NOT contain HybridQuery (the query self-erased away)", outerTypes.contains("HybridQuery"));
            // THE FIX: per-leg sub-query profiles are now merged in under [fused_leg_N] shard entries, carrying the real
            // sub-query scoring (TermQuery for the 'place' term leg and the analyzed 'hello' match leg).
            assertFalse("fused profile must now include per-leg sub-query profiles", legShardIds.isEmpty());
            assertTrue("leg profiles must contain the real sub-query scoring (TermQuery)", legTypes.contains("TermQuery"));

            String classicBody = "{\"profile\":true,\"query\":{\"hybrid\":{\"queries\":["
                + "{\"match\":{\""
                + TEXT_FIELD
                + "\":\"hello\"}},{\"term\":{\""
                + TEXT_FIELD
                + "\":\"place\"}}]}}}";
            Map<String, Object> classicResp = searchWithRawBody(INDEX_WITH_DEFAULT_NORM, classicBody, 10);
            List<String> classicTypes = collectProfileTypesByShard(classicResp).values()
                .stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());
            assertTrue("classic profile should contain HybridQuery", classicTypes.contains("HybridQuery"));
        } finally {
            updateClusterSettings("cluster.search.enabled_system_generated_factories", List.of());
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, List<String>> collectProfileTypesByShard(Map<String, Object> response) {
        Map<String, List<String>> byShard = new java.util.LinkedHashMap<>();
        Map<String, Object> profile = (Map<String, Object>) response.get("profile");
        if (profile == null) {
            return byShard;
        }
        List<Map<String, Object>> shards = (List<Map<String, Object>>) profile.get("shards");
        for (Map<String, Object> shard : shards) {
            String shardId = (String) shard.get("id");
            List<String> types = new ArrayList<>();
            List<Map<String, Object>> searches = (List<Map<String, Object>>) shard.get("searches");
            if (searches != null) {
                for (Map<String, Object> search : searches) {
                    List<Map<String, Object>> queries = (List<Map<String, Object>>) search.get("query");
                    if (queries == null) {
                        continue;
                    }
                    for (Map<String, Object> queryNode : queries) {
                        collectTypesRecursive(queryNode, types);
                    }
                }
            }
            byShard.put(shardId, types);
        }
        return byShard;
    }

    @SuppressWarnings("unchecked")
    private void collectTypesRecursive(Map<String, Object> queryNode, List<String> out) {
        Object type = queryNode.get("type");
        if (type != null) {
            out.add((String) type);
        }
        List<Map<String, Object>> children = (List<Map<String, Object>>) queryNode.get("children");
        if (children != null) {
            for (Map<String, Object> child : children) {
                collectTypesRecursive(child, out);
            }
        }
    }

    /**
     * Option X precedence step 1: an inline {@code fusion} block on the query body supplies the fusion config with NO
     * pipeline attached anywhere (no index default, no param, no inline search_pipeline). This is the co-located
     * config shape and must produce the same fusion as the pipeline-config path.
     */
    @SneakyThrows
    public void testFusedMode_whenInlineFusionBlock_thenFusesWithoutAnyPipeline() {
        if (indexExists(INDEX_NO_PIPELINE) == false) {
            createIndex(INDEX_NO_PIPELINE, indexConfigWithDefaultPipeline(null));
            addFourDocs(INDEX_NO_PIPELINE);
        }
        String body = "{\"query\":{\"hybrid\":{\"mode\":\"fused\","
            + "\"fusion\":{\"normalization\":{\"technique\":\"min_max\"},"
            + "\"combination\":{\"technique\":\"arithmetic_mean\",\"parameters\":{\"weights\":[0.5,0.5]}}},"
            + "\"queries\":[{\"match\":{\""
            + TEXT_FIELD
            + "\":\"hello\"}},{\"term\":{\""
            + TEXT_FIELD
            + "\":\"place\"}}]}}}";

        Map<String, Object> response = searchWithRawBody(INDEX_NO_PIPELINE, body, 10);
        assertEquals(3, getHitCount(response));
        // doc 2 matches both legs -> top under min_max + arithmetic_mean, same as the pipeline-config path.
        assertEquals("2", getNestedHits(response).get(0).get("_id"));
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
