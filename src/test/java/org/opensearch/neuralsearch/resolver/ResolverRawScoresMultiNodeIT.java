/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.resolver;

import com.google.common.collect.ImmutableList;
import lombok.SneakyThrows;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicHeader;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import java.util.List;
import java.util.Map;

import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_USER_AGENT;

/**
 * POC — proves the resolver query delivers RAW per-leg sub-query scores on every returned hit, with NO search
 * pipeline and NO customer-managed response processor (the opt-in is a single query-body field
 * {@code "sub_query_scores": true}), and that it is correct on a MULTI-NODE / multi-shard cluster.
 *
 * <p>Two delivery mechanisms are exercised, both keyed by the transport-stable {@code _id}:
 * <ul>
 *   <li><b>Fast path</b> (coordinator + per_shard) — raw scores attached in the coordinator's fabricated response
 *       ({@code ResolverOrchestrator.fabricateFastPathResponse}); no data-node involvement.</li>
 *   <li><b>Standard path</b> (stage B, forced here by aggregations) — raw scores carried inside the serialized
 *       {@code RankDocsQueryBuilder} / {@code RawScoreCarryingQuery} and attached at the DATA-NODE fetch by
 *       {@code RawSubQueryScoresFetchSubPhase}.</li>
 * </ul>
 * Run with {@code -PnumNodes=3} so a 3-shard index spreads across data nodes; asserting the field on every hit
 * therefore covers hits served by remote data nodes.
 */
public class ResolverRawScoresMultiNodeIT extends BaseNeuralSearchIT {

    private static final String TITLE = "title";
    private static final String BODY = "body";
    private static final String FIELD = "sub_query_scores";

    /** Fast path (coordinator collection): plain top-K, track_total_hits:false -> fast path eligible. */
    @SneakyThrows
    public void testRawScores_fastPath_coordinator_multiShard() {
        String index = initIndex("resolver-rawscores-fastcoord");
        String body = "{\"size\":10,\"track_total_hits\":false,\"query\":{" + resolverRrf(true) + "}}";
        assertRawScoresOnEveryLegHit(searchNoPipeline(index, body));
    }

    /** Fast path with per_shard fanout collection (min_max + arithmetic_mean, collection:per_shard). */
    @SneakyThrows
    public void testRawScores_fastPath_perShardFanout_multiShard() {
        String index = initIndex("resolver-rawscores-pershard");
        String body = "{\"size\":10,\"track_total_hits\":false,\"query\":{" + resolverPerShard() + "}}";
        assertRawScoresOnEveryLegHit(searchNoPipeline(index, body));
    }

    /** Standard (shard-fanout / stage-B) path: aggregations force the Tail path -> hits fetched on data nodes. */
    @SneakyThrows
    public void testRawScores_standardPath_shardFanout_multiShard() {
        String index = initIndex("resolver-rawscores-standard");
        String body = "{\"size\":10,\"query\":{" + resolverRrf(true) + "},\"aggs\":{\"by_title\":{\"terms\":{\"field\":\"_index\"}}}}";
        assertRawScoresOnEveryLegHit(searchNoPipeline(index, body));
    }

    /** Control: without the opt-in, the field must be ABSENT (proves the assertion is non-vacuous + zero default cost). */
    @SneakyThrows
    public void testRawScores_absent_whenNotOptedIn() {
        String index = initIndex("resolver-rawscores-absent");
        String body = "{\"size\":10,\"track_total_hits\":false,\"query\":{" + resolverRrf(false) + "}}";
        for (Map<String, Object> hit : readHits(searchNoPipeline(index, body))) {
            @SuppressWarnings("unchecked")
            Map<String, Object> fields = (Map<String, Object>) hit.get("fields");
            boolean hasField = fields != null && fields.containsKey(FIELD);
            assertFalse("hit " + hit.get("_id") + " must NOT carry raw scores without the opt-in", hasField);
        }
    }

    // --- shared assertions / helpers ---

    private void assertRawScoresOnEveryLegHit(Map<String, Object> response) {
        List<Map<String, Object>> hits = readHits(response);
        assertFalse("expected non-empty hits", hits.isEmpty());
        int both = 0, titleOnly = 0, bodyOnly = 0;
        for (Map<String, Object> hit : hits) {
            String id = (String) hit.get("_id");
            if ("d_none".equals(id)) {
                fail("d_none matches neither leg and must not appear");
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> fields = (Map<String, Object>) hit.get("fields");
            assertNotNull("hit " + id + " is MISSING the raw-scores field (multi-node attach failed)", fields);
            assertTrue("hit " + id + " has no " + FIELD, fields.containsKey(FIELD));
            @SuppressWarnings("unchecked")
            List<Object> raw = (List<Object>) fields.get(FIELD);
            assertEquals("one raw score per leg (2 legs)", 2, raw.size());
            double leg1 = toDouble(raw.get(0)); // title:apple raw
            double leg2 = toDouble(raw.get(1)); // body:banana raw
            switch (id) {
                case "d_both" -> {
                    assertTrue("d_both leg1 real", leg1 > 0.0);
                    assertTrue("d_both leg2 real", leg2 > 0.0);
                    both++;
                }
                case "d_title" -> {
                    assertTrue("d_title leg1 real", leg1 > 0.0);
                    assertTrue("d_title leg2 NaN", Double.isNaN(leg2));
                    titleOnly++;
                }
                case "d_body" -> {
                    assertTrue("d_body leg1 NaN", Double.isNaN(leg1));
                    assertTrue("d_body leg2 real", leg2 > 0.0);
                    bodyOnly++;
                }
                default -> assertTrue("hit " + id + " must have >=1 real leg score", leg1 > 0.0 || leg2 > 0.0);
            }
        }
        assertEquals("d_both present once", 1, both);
        assertEquals("d_title present once", 1, titleOnly);
        assertEquals("d_body present once", 1, bodyOnly);
    }

    private double toDouble(Object o) {
        // NaN/Infinity serialize as JSON strings ("NaN"), not numbers.
        return o instanceof Number n ? n.doubleValue() : Double.parseDouble(String.valueOf(o));
    }

    private String resolverRrf(boolean subQueryScores) {
        return "\"resolver\":{\"queries\":[{\"match\":{\""
            + TITLE
            + "\":\"apple\"}},{\"match\":{\""
            + BODY
            + "\":\"banana\"}}],"
            + "\"technique\":\"rrf\",\"rank_constant\":60,\"rank_window_size\":100,\"sub_query_scores\":"
            + subQueryScores
            + "}";
    }

    private String resolverPerShard() {
        return "\"resolver\":{\"queries\":[{\"match\":{\""
            + TITLE
            + "\":\"apple\"}},{\"match\":{\""
            + BODY
            + "\":\"banana\"}}],"
            + "\"combination\":{\"technique\":\"arithmetic_mean\"},\"normalization\":{\"technique\":\"min_max\"},"
            + "\"collection\":\"per_shard\",\"rank_window_size\":100,\"sub_query_scores\":true}";
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> readHits(Map<String, Object> response) {
        Map<String, Object> hitsMap = (Map<String, Object>) response.get("hits");
        return (List<Map<String, Object>>) hitsMap.get("hits");
    }

    @SneakyThrows
    private Map<String, Object> searchNoPipeline(String index, String body) {
        Response response = makeRequest(
            client(),
            "POST",
            "/" + index + "/_search",
            Map.of(),
            toHttpEntity(body),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    /** Create an isolated 3-shard index (each test gets its own to avoid cross-test state) and return its name. */
    @SneakyThrows
    private String initIndex(String index) {
        if (indexExists(index)) {
            return index;
        }
        String mapping = "{"
            + "\"settings\":{\"index\":{\"number_of_shards\":3,\"number_of_replicas\":0}},"
            + "\"mappings\":{\"properties\":{\"title\":{\"type\":\"text\"},\"body\":{\"type\":\"text\"}}}"
            + "}";
        createIndex(index, mapping);
        ingestDocument(index, "{\"title\":\"apple apple apple\",\"body\":\"banana banana banana\"}", "d_both");
        ingestDocument(index, "{\"title\":\"apple pie recipe\",\"body\":\"fresh grape juice\"}", "d_title");
        ingestDocument(index, "{\"title\":\"classic cherry tart\",\"body\":\"banana milk smoothie\"}", "d_body");
        ingestDocument(index, "{\"title\":\"cherry chocolate cake\",\"body\":\"grape jam jar\"}", "d_none");
        makeRequest(
            client(),
            "POST",
            "/" + index + "/_refresh",
            Map.of(),
            null,
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        return index;
    }
}
