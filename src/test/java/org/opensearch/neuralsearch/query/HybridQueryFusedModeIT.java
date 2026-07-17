/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.List;
import java.util.Map;

import org.opensearch.client.ResponseException;
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
 * <p>Config source: these tests use {@code index.search.default_pipeline}, the primary zero-migration case — the
 * pipeline id lives in cluster state and survives to the coordinator rewrite. The named {@code ?search_pipeline=}
 * param is NOT covered here: core wraps the resolved request through the {@code SearchRequest} copy constructor, which
 * does not copy the {@code pipeline} field, so {@code searchRequest.pipeline()} reads null at rewrite time. Reading the
 * named param requires either the (package-private) constructed-pipeline accessor or a small core change; the
 * index-default and inline-body sources need neither.
 */
public class HybridQueryFusedModeIT extends BaseNeuralSearchIT {

    private static final String TEXT_FIELD = "text";
    private static final String INDEX_WITH_DEFAULT_NORM = "test-hybrid-fused-default-norm";
    private static final String INDEX_WITH_DEFAULT_RRF = "test-hybrid-fused-default-rrf";
    private static final String INDEX_NO_PIPELINE = "test-hybrid-fused-no-pipeline";
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

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getNestedHits(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        return (List<Map<String, Object>>) hitsMap.get("hits");
    }
}
