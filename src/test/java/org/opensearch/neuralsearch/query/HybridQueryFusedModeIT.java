/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getNestedHits;

import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import lombok.SneakyThrows;

/**
 * End-to-end integration test for the resolver (fused) mode of the {@code hybrid} query — the first working slice
 * (min_max normalization + arithmetic_mean combination, top-level). Exercises the full coordinator flow: parse the
 * {@code fusion} parameter, fan the legs out as a MultiSearch, fuse on the coordinator via the shared fusion core, and
 * self-erase into a standard query that returns fused results.
 *
 * <p>Happy path only for this PR; broader coverage (nested, RRF, aggregations, explain/profiler, min_score, more
 * technique pairs) is scoped to later PRs.
 */
public class HybridQueryFusedModeIT extends BaseNeuralSearchIT {

    private static final String TEXT_FIELD = "text";
    /** Own index: the PIT test mutates the doc set mid-test, which would break the exact hit counts asserted above. */
    private static final String INDEX_FOR_PIT = "test-hybrid-fused-pit";
    private static final String RANK_FIELD = "rank";
    /** Documents in the PIT index. The fused window is bound to this count so a post-PIT doc can only enter it by
     *  evicting a real one — which is what lets this test detect legs that ignore the PIT. */
    private static final int PIT_DOCS = 3;
    private static final String INDEX_WITH_DEFAULT_NORM = "test-hybrid-fused-default-norm";
    private static final String INDEX_NO_PIPELINE = "test-hybrid-fused-inline-config";
    private static final String NORM_PIPELINE = "fused-mode-norm-pipeline";

    private String indexConfigWithDefaultPipeline(String pipelineId) {
        return "{\"settings\":{\"number_of_shards\":3,\"number_of_replicas\":0,\"index.search.default_pipeline\":\""
            + pipelineId
            + "\"},\"mappings\":{\"properties\":{\""
            + TEXT_FIELD
            + "\":{\"type\":\"text\"}}}}";
    }

    private String indexConfigWithoutPipeline() {
        return "{\"settings\":{\"number_of_shards\":3,\"number_of_replicas\":0},\"mappings\":{\"properties\":{\""
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

    /**
     * A fused two-leg hybrid query. Presence of the {@code fusion} block enables the resolver; {@code source: pipeline}
     * tells it to read the normalization/combination config from the attached search pipeline (here, the index default)
     * — the same config an existing classic-hybrid user already has.
     */
    private HybridQueryBuilder fusedTwoLegQuery() {
        HybridQueryBuilder fused = new HybridQueryBuilder().fusion(Map.of("source", "pipeline"));
        fused.add(new MatchQueryBuilder(TEXT_FIELD, "hello"));
        fused.add(new TermQueryBuilder(TEXT_FIELD, "place"));
        return fused;
    }

    /**
     * The same two-leg fused query, but with the fusion config supplied <b>inline</b> on the query body instead of read
     * from a pipeline. An inline {@code normalization}/{@code combination} block enables the resolver and takes
     * precedence over any attached pipeline — so this needs no {@code index.search.default_pipeline} at all.
     */
    private HybridQueryBuilder fusedTwoLegInlineConfigQuery() {
        HybridQueryBuilder fused = new HybridQueryBuilder().fusion(
            Map.of("normalization", Map.of("technique", "min_max"), "combination", Map.of("technique", "arithmetic_mean"))
        );
        fused.add(new MatchQueryBuilder(TEXT_FIELD, "hello"));
        fused.add(new TermQueryBuilder(TEXT_FIELD, "place"));
        return fused;
    }

    @SneakyThrows
    public void testFusedMode_whenIndexDefaultNormalizationPipeline_thenFusesMinMaxArithmeticMean() {
        // Classic min_max + arithmetic_mean normalization pipeline, attached as the index default — unchanged from what
        // an existing hybrid user has today. The fused query reads this config at coordinator rewrite and self-erases.
        createSearchPipeline(NORM_PIPELINE, "min_max", "arithmetic_mean", Map.of());
        if (indexExists(INDEX_WITH_DEFAULT_NORM) == false) {
            createIndex(INDEX_WITH_DEFAULT_NORM, indexConfigWithDefaultPipeline(NORM_PIPELINE));
            addFourDocs(INDEX_WITH_DEFAULT_NORM);
        }

        Map<String, Object> response = search(INDEX_WITH_DEFAULT_NORM, fusedTwoLegQuery(), 10);

        // docs 1 (hello x2), 2 (hello + place), 3 (place) match at least one leg; doc 4 matches neither.
        assertEquals(3, getHitCount(response));
        List<Map<String, Object>> hits = getNestedHits(response);
        // doc 2 matches BOTH legs -> ranks first under min_max + arithmetic mean.
        assertEquals("2", hits.get(0).get("_id"));
        // scores are fused, strictly positive for a matched doc, and in descending order.
        double previous = Double.MAX_VALUE;
        for (Map<String, Object> hit : hits) {
            double score = ((Number) hit.get("_score")).doubleValue();
            assertTrue("fused scores must be descending", score <= previous);
            assertTrue("fused score must be > 0 for a matched doc", score > 0.0);
            previous = score;
        }
    }

    @SneakyThrows
    public void testFusedMode_whenInlineNormalizationConfig_thenFusesWithoutAnyPipeline() {
        // Resolver (fused) mode driven entirely by an inline `fusion` block — no search pipeline, no index default. This
        // exercises the FusionSpec.fromInlineFusion path (distinct from the pipeline-resolution path above), proving the
        // config can travel on the query body alone.
        if (indexExists(INDEX_NO_PIPELINE) == false) {
            createIndex(INDEX_NO_PIPELINE, indexConfigWithoutPipeline());
            addFourDocs(INDEX_NO_PIPELINE);
        }

        Map<String, Object> response = search(INDEX_NO_PIPELINE, fusedTwoLegInlineConfigQuery(), 10);

        // Same corpus/legs as the pipeline test: docs 1,2,3 match at least one leg; doc 4 matches neither.
        assertEquals(3, getHitCount(response));
        List<Map<String, Object>> hits = getNestedHits(response);
        // doc 2 matches BOTH legs -> ranks first under min_max + arithmetic mean, identical to the pipeline-config path.
        assertEquals("2", hits.get(0).get("_id"));
        double previous = Double.MAX_VALUE;
        for (Map<String, Object> hit : hits) {
            double score = ((Number) hit.get("_score")).doubleValue();
            assertTrue("fused scores must be descending", score <= previous);
            assertTrue("fused score must be > 0 for a matched doc", score > 0.0);
            previous = score;
        }
    }

    /**
     * A user-supplied point-in-time must be honored by the whole fused flow, legs included.
     *
     * <p>Fused mode opens N leg searches plus the round-2 self-erased query, so without a shared view those are N+1
     * independent reader instants and a concurrently indexed document can appear in some of them but not others. Passing
     * the request's PIT down to every leg makes them all read one immutable snapshot.
     *
     * <p>The probe: take a PIT, then index a document that ranks ABOVE everything already there. Through the PIT it must
     * stay invisible, while a live search sees it.
     *
     * <p>Three details make this a real regression guard rather than a tautology:
     * <ul>
     *   <li>Both legs score by a numeric field via {@code function_score}, so the fused order is exactly the field order —
     *       deterministic and shard-independent, unlike BM25 (whose min_max floor can actually sink a newly added short
     *       document to the BOTTOM of a leg, which would hide the defect entirely).</li>
     *   <li>{@code window_size} is bound to the existing document count, so a document that should be invisible can only
     *       enter the window by evicting a real one.</li>
     *   <li>The query is Top-only ({@code track_total_hits:false}); with the Tail present the legs would be re-matched
     *       directly and return the real documents regardless of what the Top holds, masking the defect.</li>
     * </ul>
     * Top-only, the returned hits ARE the fused window: if the legs ignored the PIT they would rank the new document in,
     * round-2 would read the PIT, fail to match that id, and the request would return FEWER hits than the window holds.
     * Verified by mutation — removing the PIT passthrough from the legs makes this test fail with 2 hits instead of 3.
     */
    @SneakyThrows
    public void testFusedMode_whenPointInTimeSupplied_thenLegsAndRoundTwoShareOneSnapshot() {
        if (indexExists(INDEX_FOR_PIT) == false) {
            createIndex(INDEX_FOR_PIT, indexConfigWithRankField());
            for (int id = 1; id <= PIT_DOCS; id++) {
                indexRankedDoc(id, id * 10);
            }
        }
        String pitId = createPointInTime(INDEX_FOR_PIT);
        try {
            assertEquals("the Top-only fused window holds every document", PIT_DOCS, getHitCount(searchWithPit(pitId)));

            // A document that outranks everything present, so live legs would put it at the head of the window.
            indexRankedDoc(PIT_DOCS + 1, 100_000);

            Map<String, Object> throughPit = searchWithPit(pitId);
            assertEquals(
                "PIT snapshot must not see the doc indexed after it was taken, and no window slot may be lost to it",
                PIT_DOCS,
                getHitCount(throughPit)
            );
            for (Map<String, Object> hit : getNestedHits(throughPit)) {
                assertNotEquals("the post-PIT doc must not leak into the fused window", String.valueOf(PIT_DOCS + 1), hit.get("_id"));
            }

            // Sanity: a live search does rank it first, so the assertions above are about the snapshot — not about the
            // document failing to index or to match the legs.
            List<Map<String, Object>> liveHits = getNestedHits(searchLive());
            assertEquals("a live search ranks the new doc first", String.valueOf(PIT_DOCS + 1), liveHits.get(0).get("_id"));
        } finally {
            deletePointInTime(pitId);
        }
    }

    private String indexConfigWithRankField() {
        return "{\"settings\":{\"number_of_shards\":3,\"number_of_replicas\":0},\"mappings\":{\"properties\":{\""
            + RANK_FIELD
            + "\":{\"type\":\"integer\"}}}}";
    }

    @SneakyThrows
    private void indexRankedDoc(int id, int rank) {
        Request request = new Request("PUT", "/" + INDEX_FOR_PIT + "/_doc/" + id + "?refresh=true");
        request.setJsonEntity("{\"" + RANK_FIELD + "\":" + rank + "}");
        Response response = client().performRequest(request);
        int code = response.getStatusLine().getStatusCode();
        assertTrue("indexing doc " + id + " failed: " + code, code == RestStatus.OK.getStatus() || code == RestStatus.CREATED.getStatus());
    }

    /** Open a point-in-time over the index and return its id. */
    @SneakyThrows
    private String createPointInTime(String index) {
        Request request = new Request("POST", "/" + index + "/_search/point_in_time?keep_alive=5m");
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        Map<String, Object> body = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(response.getEntity()),
            false
        );
        String pitId = (String) body.get("pit_id");
        assertNotNull("pit_id must be returned", pitId);
        return pitId;
    }

    @SneakyThrows
    private void deletePointInTime(String pitId) {
        Request request = new Request("DELETE", "/_search/point_in_time");
        request.setJsonEntity("{\"pit_id\":[\"" + pitId + "\"]}");
        client().performRequest(request);
    }

    /** Two legs that both score by the numeric rank field, so the fused order is exactly the rank order. */
    private String rankedFusedQuery() {
        String leg = "{\"function_score\":{\"query\":{\"match_all\":{}},\"field_value_factor\":{\"field\":\""
            + RANK_FIELD
            + "\",\"modifier\":\"none\",\"missing\":1}}}";
        return "{\"hybrid\":{\"fusion\":{\"window_size\":"
            + PIT_DOCS
            + ",\"normalization\":{\"technique\":\"min_max\"},"
            + "\"combination\":{\"technique\":\"arithmetic_mean\"}},"
            + "\"queries\":["
            + leg
            + ","
            + leg
            + "]}}";
    }

    /**
     * Fused search against a PIT, Top-only. The index deliberately does NOT appear in the path: a PIT already pins its own
     * indices and core rejects a PIT request that also names them.
     */
    @SneakyThrows
    private Map<String, Object> searchWithPit(String pitId) {
        return searchRaw(
            "/_search",
            "{\"pit\":{\"id\":\"" + pitId + "\",\"keep_alive\":\"5m\"},\"track_total_hits\":false,\"query\":" + rankedFusedQuery() + "}"
        );
    }

    /** The same fused query without a PIT, for the live-visibility contrast. */
    @SneakyThrows
    private Map<String, Object> searchLive() {
        return searchRaw("/" + INDEX_FOR_PIT + "/_search", "{\"track_total_hits\":false,\"query\":" + rankedFusedQuery() + "}");
    }

    @SneakyThrows
    private Map<String, Object> searchRaw(String endpoint, String jsonBody) {
        Request request = new Request("POST", endpoint);
        request.addParameter("size", "10");
        request.setJsonEntity(jsonBody);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }
}
