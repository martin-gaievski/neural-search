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
 * End-to-end coverage of how a fused/resolver hybrid renders an ANN ({@code knn}/{@code neural}) leg in the Tail.
 *
 * <p>An ANN leg's Lucene match set IS the top-k it returned, so re-running it in the Tail would only re-walk the HNSW
 * graph to recount what the fan-out already retrieved. The coordinator therefore replaces such a leg with a direct address
 * of its returned hits — and because the Tail is a {@code filter}, that address decides the match set that
 * {@code total_hits}, every aggregation, and the score-0 region of the hit list are computed from.
 *
 * <p>Addressed by {@code _id} alone, that set silently grew: on a multi-index search every same-{@code _id} document in a
 * sibling index passed the filter, so the numbers the Tail exists to make correct were the ones it inflated. This is not
 * the Top's identity bug and was not fixed by qualifying the Top — the Tail is built from the raw leg hits, never from the
 * ranked window's indices — so it needs its own end-to-end proof.
 *
 * <p>Dataset: two indices with deliberately COLLIDING ids 1..3. Only {@code index-a}'s documents carry a vector, so the
 * ANN leg's complete match set is exactly those three documents — nothing is truncated by {@code k} or by the leg's
 * {@code size} ({@code window_size} is 10 against 3 returned hits), which keeps this test about <i>addressing</i> and not
 * about materialization depth. {@code index-b}'s documents exist, share the ids, and must stay out of the match set.
 */
public class HybridQueryFusedModeKnnTailIT extends BaseNeuralSearchIT {

    private static final String INDEX_A = "test-fused-knn-tail-a";
    private static final String INDEX_B = "test-fused-knn-tail-b";
    private static final String NORM_PIPELINE = "fused-knn-tail-norm-pipeline";
    private static final String OWNER_FIELD = "owner";
    private static final String SCORE_FIELD = "s";
    private static final String VECTOR_FIELD = "vec";
    private static final String OWNER_A = "owner-a";
    private static final String OWNER_B = "owner-b";
    private static final int COLLIDING_IDS = 3;
    private static final int WINDOW_SIZE = 10;

    private String indexConfig() {
        return "{\"settings\":{\"index\":{\"knn\":true,\"number_of_shards\":1,\"number_of_replicas\":0,"
            + "\"search.default_pipeline\":\""
            + NORM_PIPELINE
            + "\"}},"
            + "\"mappings\":{\"properties\":{\""
            + OWNER_FIELD
            + "\":{\"type\":\"keyword\"},\""
            + SCORE_FIELD
            + "\":{\"type\":\"integer\"},\""
            + VECTOR_FIELD
            + "\":{\"type\":\"knn_vector\",\"dimension\":2,"
            + "\"method\":{\"name\":\"hnsw\",\"space_type\":\"l2\",\"engine\":\"lucene\"}}}}}";
    }

    /**
     * Both indices hold ids 1..3 with the same mapping, but only index-a's documents carry a vector — so the ANN leg
     * retrieves index-a's three documents and nothing else, while index-b's three same-{@code _id} documents are exactly
     * the ones a bare {@code _id} address would drag in.
     */
    @SneakyThrows
    private void ensureDataset() {
        createSearchPipeline(NORM_PIPELINE, "min_max", "arithmetic_mean", Map.of());
        if (indexExists(INDEX_A) && indexExists(INDEX_B)) {
            return;
        }
        for (String index : List.of(INDEX_A, INDEX_B)) {
            if (indexExists(index) == false) {
                createIndex(index, indexConfig());
            }
            boolean withVector = INDEX_A.equals(index);
            for (int id = 1; id <= COLLIDING_IDS; id++) {
                String owner = withVector ? OWNER_A : OWNER_B;
                int s = (withVector ? 100 : 50) - id;
                String vector = withVector ? ",\"" + VECTOR_FIELD + "\":[1." + id + ",1.0]" : "";
                Request request = new Request("PUT", "/" + index + "/_doc/" + id + "?refresh=true");
                request.setJsonEntity("{\"" + OWNER_FIELD + "\":\"" + owner + "\",\"" + SCORE_FIELD + "\":" + s + vector + "}");
                Response response = client().performRequest(request);
                int code = response.getStatusLine().getStatusCode();
                assertTrue(
                    "indexing " + index + "/" + id + " failed: " + code,
                    code == RestStatus.OK.getStatus() || code == RestStatus.CREATED.getStatus()
                );
            }
        }
    }

    /** A materializable leg: {@code knn} is replaced in the Tail by an address of the hits it returned. */
    private String knnLeg(String filter) {
        return "{\"knn\":{\"" + VECTOR_FIELD + "\":{\"vector\":[1.1,1.0],\"k\":" + WINDOW_SIZE + filter + "}}}";
    }

    /** A non-materializable leg, kept as the real query in the Tail. Matches index-a's documents only. */
    private String ownerLeg() {
        return "{\"function_score\":{\"query\":{\"term\":{\""
            + OWNER_FIELD
            + "\":\""
            + OWNER_A
            + "\"}},\"field_value_factor\":{\"field\":\""
            + SCORE_FIELD
            + "\",\"modifier\":\"none\",\"missing\":1}}}";
    }

    private String fusedHybrid(String... legs) {
        return "{\"hybrid\":{\"fusion\":{\"window_size\":"
            + WINDOW_SIZE
            + ",\"normalization\":{\"technique\":\"min_max\"},"
            + "\"combination\":{\"technique\":\"arithmetic_mean\"}},"
            + "\"queries\":["
            + String.join(",", legs)
            + "]}}";
    }

    /**
     * The regression this class exists for. Both legs match index-a's three documents and nothing else, so the fused
     * match set is those three. While the materialized ANN leg was addressed by {@code _id} alone, index-b's three
     * same-{@code _id} documents also passed the Tail filter: {@code total_hits} read 6 instead of 3, the {@code owner}
     * aggregation reported a bucket for an index no leg had matched, and the user got three score-0 hits from it.
     */
    @SneakyThrows
    public void testFusedKnnTail_whenIdsCollideAcrossIndices_thenSiblingDocsStayOutOfTheMatchSet() {
        ensureDataset();
        String body = "{\"query\":"
            + fusedHybrid(knnLeg(""), ownerLeg())
            + ",\"aggregations\":{\"by_owner\":{\"terms\":{\"field\":\""
            + OWNER_FIELD
            + "\",\"size\":10}}},\"track_total_hits\":true}";

        Map<String, Object> response = searchRaw(body, 20);

        assertEquals("only the documents the legs matched are hits", COLLIDING_IDS, hits(response).size());
        assertEquals("total_hits counts the legs' match set, not their _id twins", (long) COLLIDING_IDS, totalHits(response));
        for (Map<String, Object> hit : hits(response)) {
            assertEquals("a hit from an index no leg matched means the Tail was addressed by _id alone", INDEX_A, hit.get("_index"));
        }
        Map<String, Integer> buckets = ownerBuckets(response);
        assertEquals("the aggregation covers index-a's documents", Integer.valueOf(COLLIDING_IDS), buckets.getOrDefault(OWNER_A, 0));
        assertEquals("and counts nothing from index-b", Integer.valueOf(0), buckets.getOrDefault(OWNER_B, 0));
    }

    /**
     * An ANN leg that matched nothing must keep matching nothing. Its Tail clause is an explicit {@code match_none}: a leg
     * rendered as an empty {@code bool{should: []}} would compile to {@code MatchAllDocsQuery} and make the Tail match
     * every document in both indices — so {@code total_hits} and the aggregation would report the whole corpus instead of
     * the one leg that actually matched. The {@code filter} on the kNN leg matches no owner, so the leg returns zero hits
     * while the second leg still fuses normally.
     */
    @SneakyThrows
    public void testFusedKnnTail_whenAnnLegMatchesNothing_thenTailDoesNotFallBackToMatchAll() {
        ensureDataset();
        String noOwnerFilter = ",\"filter\":{\"term\":{\"" + OWNER_FIELD + "\":\"owner-nobody\"}}";
        String body = "{\"query\":"
            + fusedHybrid(knnLeg(noOwnerFilter), ownerLeg())
            + ",\"aggregations\":{\"by_owner\":{\"terms\":{\"field\":\""
            + OWNER_FIELD
            + "\",\"size\":10}}},\"track_total_hits\":true}";

        Map<String, Object> response = searchRaw(body, 20);

        assertEquals("an empty ANN leg contributes nothing, it does not open the Tail up", COLLIDING_IDS, hits(response).size());
        assertEquals("total_hits stays the surviving leg's match set", (long) COLLIDING_IDS, totalHits(response));
        Map<String, Integer> buckets = ownerBuckets(response);
        assertEquals(Integer.valueOf(COLLIDING_IDS), buckets.getOrDefault(OWNER_A, 0));
        assertEquals("an empty ANN leg must not turn into match_all", Integer.valueOf(0), buckets.getOrDefault(OWNER_B, 0));
    }

    /**
     * Scores must still be per-document with the ANN leg materialized: the fused window is index-a's documents, and the
     * closest vector to the query must rank first. Guards against the qualification narrowing the Tail so far that it
     * filters out the Top it is supposed to accompany — every window document has to survive its own Tail.
     */
    @SneakyThrows
    public void testFusedKnnTail_whenAnnLegIsMaterialized_thenWindowDocsSurviveTheirOwnTail() {
        ensureDataset();
        String body = "{\"query\":" + fusedHybrid(knnLeg(""), ownerLeg()) + ",\"track_total_hits\":true}";

        Map<String, Object> response = searchRaw(body, 20);

        Map<String, Double> scoreById = new LinkedHashMap<>();
        for (Map<String, Object> hit : hits(response)) {
            scoreById.put((String) hit.get("_id"), ((Number) hit.get("_score")).doubleValue());
        }
        assertEquals("every window document is returned", COLLIDING_IDS, scoreById.size());
        // The query vector is index-a's doc 2, so it is the nearest neighbour and the highest raw ANN score.
        assertTrue(
            "the document nearest the query vector must outrank the farthest, got " + scoreById,
            scoreById.get("2") > scoreById.get("3")
        );
    }

    // ------------------------------------------------ helpers ------------------------------------------------

    @SneakyThrows
    private Map<String, Object> searchRaw(String jsonBody, int size) {
        Request request = new Request("POST", "/" + INDEX_A + "," + INDEX_B + "/_search");
        request.setJsonEntity(jsonBody);
        request.addParameter("size", Integer.toString(size));
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> hits(Map<String, Object> response) {
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits.get("hits");
        return hitList == null ? new ArrayList<>() : hitList;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Integer> ownerBuckets(Map<String, Object> response) {
        Map<String, Integer> out = new LinkedHashMap<>();
        Map<String, Object> aggregations = (Map<String, Object>) response.get("aggregations");
        if (aggregations == null) {
            return out;
        }
        Map<String, Object> byOwner = (Map<String, Object>) aggregations.get("by_owner");
        for (Map<String, Object> bucket : (List<Map<String, Object>>) byOwner.get("buckets")) {
            out.put((String) bucket.get("key"), ((Number) bucket.get("doc_count")).intValue());
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private long totalHits(Map<String, Object> response) {
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        Object total = hits.get("total");
        if (total instanceof Map) {
            Object value = ((Map<String, Object>) total).get("value");
            return value == null ? -1 : ((Number) value).longValue();
        }
        return -1;
    }
}
