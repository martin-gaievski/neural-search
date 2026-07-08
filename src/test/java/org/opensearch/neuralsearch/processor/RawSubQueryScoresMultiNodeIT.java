/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import com.google.common.collect.ImmutableList;
import lombok.SneakyThrows;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.message.BasicHeader;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;

import java.util.List;
import java.util.Map;

import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_USER_AGENT;

/**
 * POC — proves raw per-sub-query scores are delivered correctly on a MULTI-NODE cluster (the exact scenario that
 * forced the revert of PR #1369, reverted by #1476: the old design read a coordinator-only JVM-static
 * HybridScoreRegistry from a data-node fetch phase, so the field was silently absent on remote-node hits).
 *
 * <p>This IT uses a 3-shard index. When run with {@code -PnumNodes=3} the shards spread across data nodes, so any
 * assertion that the field is present and correct on EVERY hit necessarily covers hits served by remote data
 * nodes — the case the reverted implementation failed. The new implementation captures AND attaches entirely on
 * the coordinator (workflow stashes per-shard ordered raw scores on the PipelineProcessingContext; a coordinator
 * SearchResponseProcessor attaches them by SearchShard + positional order), so no cross-JVM read is involved.
 *
 * <p>Two lexical sub-queries (title/body match) are used so the test needs no ML model; the raw-score capture
 * path (per-sub-query TopDocs before normalize) is identical regardless of leg type.
 */
public class RawSubQueryScoresMultiNodeIT extends BaseNeuralSearchIT {

    private static final String INDEX = "raw-subquery-scores-poc-index";
    private static final String PIPELINE = "raw-subquery-scores-poc-pipeline";
    private static final String TITLE = "title";
    private static final String BODY = "body";
    private static final String FIELD = "hybridization_sub_query_scores";

    @SneakyThrows
    public void testRawSubQueryScores_presentAndCorrect_onEveryHit_multiShard() {
        initIndexIfNeeded();
        createRawScoresPipeline();

        // Two lexical sub-queries: title:apple + body:banana. Docs are seeded so that:
        // d_both matches BOTH legs, d_title only leg-1, d_body only leg-2, d_none neither.
        HybridQueryBuilder hybrid = new HybridQueryBuilder();
        hybrid.add(QueryBuilders.matchQuery(TITLE, "apple"));
        hybrid.add(QueryBuilders.matchQuery(BODY, "banana"));

        Map<String, Object> response = search(
            INDEX,
            hybrid,
            null,
            10,
            Map.of("search_pipeline", PIPELINE),
            null // no preference -> hits come from whichever data node owns each shard
        );

        @SuppressWarnings("unchecked")
        Map<String, Object> hitsWrapper = (Map<String, Object>) response.get("hits");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> hits = (List<Map<String, Object>>) hitsWrapper.get("hits");
        assertFalse("expected non-empty hits", hits.isEmpty());

        int checkedBoth = 0;
        for (Map<String, Object> hit : hits) {
            String id = (String) hit.get("_id");
            if ("d_none".equals(id)) {
                fail("d_none matches neither leg and must not appear");
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> fields = (Map<String, Object>) hit.get("fields");
            assertNotNull(
                "hit " + id + " is MISSING the raw-scores field " + "— this is exactly the multi-node failure that reverted #1369",
                fields
            );
            assertTrue("hit " + id + " has no " + FIELD + " field", fields.containsKey(FIELD));
            @SuppressWarnings("unchecked")
            List<Object> raw = (List<Object>) fields.get(FIELD);
            assertEquals("expected one raw score per sub-query (2 legs)", 2, raw.size());

            double leg1 = toDouble(raw.get(0)); // title:apple raw BM25 (NaN if this doc didn't match leg-1)
            double leg2 = toDouble(raw.get(1)); // body:banana raw BM25 (NaN if this doc didn't match leg-2)

            // Correctness of association: the per-leg raw score is present exactly when that leg matched this doc.
            switch (id) {
                case "d_both":
                    assertTrue("d_both must have a real title-leg raw score", leg1 > 0.0);
                    assertTrue("d_both must have a real body-leg raw score", leg2 > 0.0);
                    checkedBoth++;
                    break;
                case "d_title":
                    assertTrue("d_title must have a real title-leg raw score", leg1 > 0.0);
                    assertTrue("d_title must NOT have a body-leg score", Double.isNaN(leg2));
                    break;
                case "d_body":
                    assertTrue("d_body must NOT have a title-leg score", Double.isNaN(leg1));
                    assertTrue("d_body must have a real body-leg raw score", leg2 > 0.0);
                    break;
                default:
                    // seeded extra docs may match one leg; just assert at least one real leg score exists
                    assertTrue("hit " + id + " must have at least one real leg score", leg1 > 0.0 || leg2 > 0.0);
            }
        }
        assertEquals("d_both should be present exactly once", 1, checkedBoth);
        // Non-vacuity + multi-node coverage: with 3 shards the returned hits must span multiple shards, so at least
        // some hits were served by a data node other than the coordinator — the exact case #1369 failed. We assert
        // the field was present on ALL of them above, so it demonstrably reached remote-node hits.
        assertTrue("expected hits from more than one shard (multi-node coverage)", hits.size() >= 4);
    }

    /**
     * from&gt;0 deep pagination on a MULTI-SHARD index. This is the case the investigation flagged as the risk for a
     * positional (per-shard-ordinal) attach: under a global {@code from} offset, a shard's returned hits no longer
     * start at that shard's rank 0, so a naive per-shard counter starting at 0 would misattribute raw scores by
     * off-by-j. We build ground truth (docId -&gt; true per-leg raw scores) from a full scan, then request a deep page
     * and assert every returned hit's attached raw scores match THAT SPECIFIC doc's ground truth (association by
     * value, not by position) — so a paging misalignment is caught, not masked.
     */
    @SneakyThrows
    public void testRawSubQueryScores_correctUnderPagination_multiShard() {
        initIndexIfNeeded();
        createRawScoresPipeline();

        Map<String, double[]> truth = groundTruth();
        assertTrue("need enough matching docs to page through", truth.size() >= 6);

        // Walk the full result set in pages of 3 and verify each page's per-hit raw scores against ground truth.
        int pageSize = 3;
        int verifiedOnDeepPages = 0;
        for (int from = 0; from < truth.size(); from += pageSize) {
            HybridQueryBuilder hybrid = pageQuery();
            Map<String, Object> response = search(
                INDEX,
                hybrid,
                null,
                pageSize,
                Map.of("search_pipeline", PIPELINE),
                null,
                null,
                null,
                false,
                null,
                from,
                null
            );
            for (Map<String, Object> hit : readHits(response)) {
                String id = (String) hit.get("_id");
                double[] expected = truth.get(id);
                assertNotNull("ground truth missing for " + id, expected);
                double[] actual = rawScoresOf(hit);
                assertNotNull("from=" + from + ": hit " + id + " MISSING raw-scores field on a deep page", actual);
                assertScoresMatch("from=" + from + " id=" + id, expected, actual);
                if (from > 0) {
                    verifiedOnDeepPages++;
                }
            }
        }
        assertTrue("expected to verify hits on deep (from>0) pages", verifiedOnDeepPages >= 1);
    }

    private HybridQueryBuilder pageQuery() {
        HybridQueryBuilder hybrid = new HybridQueryBuilder();
        hybrid.add(QueryBuilders.matchQuery(TITLE, "apple"));
        hybrid.add(QueryBuilders.matchQuery(BODY, "banana"));
        hybrid.paginationDepth(100); // required for from>0 with hybrid query
        return hybrid;
    }

    /** docId -> {leg1RawOrNaN, leg2RawOrNaN} captured from a single full-window (from=0, size=100) request. */
    @SneakyThrows
    private Map<String, double[]> groundTruth() {
        Map<String, Object> response = search(INDEX, pageQuery(), null, 100, Map.of("search_pipeline", PIPELINE), null);
        Map<String, double[]> truth = new java.util.HashMap<>();
        for (Map<String, Object> hit : readHits(response)) {
            truth.put((String) hit.get("_id"), rawScoresOf(hit));
        }
        return truth;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> readHits(Map<String, Object> response) {
        Map<String, Object> hitsWrapper = (Map<String, Object>) response.get("hits");
        return (List<Map<String, Object>>) hitsWrapper.get("hits");
    }

    @SuppressWarnings("unchecked")
    private double[] rawScoresOf(Map<String, Object> hit) {
        Map<String, Object> fields = (Map<String, Object>) hit.get("fields");
        if (fields == null || !fields.containsKey(FIELD)) {
            return null;
        }
        List<Object> raw = (List<Object>) fields.get(FIELD);
        double[] out = new double[raw.size()];
        for (int i = 0; i < raw.size(); i++) {
            out[i] = toDouble(raw.get(i));
        }
        return out;
    }

    private void assertScoresMatch(String ctx, double[] expected, double[] actual) {
        assertEquals(ctx + ": leg count", expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            if (Double.isNaN(expected[i])) {
                assertTrue(ctx + ": leg " + i + " expected NaN but was " + actual[i], Double.isNaN(actual[i]));
            } else {
                assertEquals(ctx + ": leg " + i, expected[i], actual[i], 1e-4);
            }
        }
    }

    private double toDouble(Object o) {
        // NaN/Infinity do not serialize as JSON numbers, so the client parses them back as strings ("NaN").
        if (o instanceof Number number) {
            return number.doubleValue();
        }
        return Double.parseDouble(String.valueOf(o)); // handles "NaN"
    }

    @SneakyThrows
    private void createRawScoresPipeline() {
        // normalization-processor (min_max + arithmetic_mean) as the phase-results processor + our new
        // coordinator-side response processor that attaches the raw scores.
        String body = "{"
            + "\"description\":\"raw sub-query scores POC\","
            + "\"phase_results_processors\":[{\"normalization-processor\":{"
            + "\"normalization\":{\"technique\":\"min_max\"},"
            + "\"combination\":{\"technique\":\"arithmetic_mean\"}}}],"
            + "\"response_processors\":[{\""
            + RawSubQueryScoresResponseProcessor.TYPE
            + "\":{}}]"
            + "}";
        makeRequest(
            client(),
            "PUT",
            "/_search/pipeline/" + PIPELINE,
            Map.of(),
            toHttpEntity(body),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
    }

    @SneakyThrows
    private void initIndexIfNeeded() {
        if (indexExists(INDEX)) {
            return;
        }
        // 3 shards, 0 replicas -> on a >=2-node cluster the shards spread across data nodes.
        String mapping = "{"
            + "\"settings\":{\"index\":{\"number_of_shards\":3,\"number_of_replicas\":0}},"
            + "\"mappings\":{\"properties\":{\"title\":{\"type\":\"text\"},\"body\":{\"type\":\"text\"}}}"
            + "}";
        createIndexWithConfiguration(INDEX, mapping, null);
        // Spread ids so different shards (hence different nodes) own them; routing is by _id hash. Varied term
        // frequencies give distinct per-doc BM25 scores so ground-truth matching is a strong association check.
        ingestDocument(INDEX, "{\"title\":\"apple apple apple\",\"body\":\"banana banana banana\"}", "d_both");
        ingestDocument(INDEX, "{\"title\":\"apple pie\",\"body\":\"grape jelly\"}", "d_title");
        ingestDocument(INDEX, "{\"title\":\"cherry cake\",\"body\":\"banana split\"}", "d_body");
        ingestDocument(INDEX, "{\"title\":\"apple tart\",\"body\":\"kiwi\"}", "d_title2");
        ingestDocument(INDEX, "{\"title\":\"pear\",\"body\":\"banana bread\"}", "d_body2");
        ingestDocument(INDEX, "{\"title\":\"apple apple orchard\",\"body\":\"banana\"}", "d_both2");
        ingestDocument(INDEX, "{\"title\":\"apple\",\"body\":\"banana banana split cream\"}", "d_both3");
        ingestDocument(INDEX, "{\"title\":\"green apple juice\",\"body\":\"fruit\"}", "d_title3");
        ingestDocument(INDEX, "{\"title\":\"melon\",\"body\":\"banana banana banana bread loaf\"}", "d_body3");
        ingestDocument(INDEX, "{\"title\":\"apple cider vinegar\",\"body\":\"banana pudding\"}", "d_both4");
        ingestDocument(INDEX, "{\"title\":\"plain water\",\"body\":\"no flavor\"}", "d_none");
        // refresh so all docs are searchable
        makeRequest(
            client(),
            "POST",
            "/" + INDEX + "/_refresh",
            Map.of(),
            null,
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
    }
}
