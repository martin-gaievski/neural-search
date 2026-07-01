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
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import java.util.List;
import java.util.Map;

import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_USER_AGENT;

/**
 * End-to-end demonstration of the Resolver framework POC (Phase 1, RRF).
 *
 * <p>Runs a single {@code resolver} query over a 3-shard index through a search pipeline containing
 * the {@code resolver} request processor. The processor fires the two legs (match on {@code title},
 * match on {@code body}) as a parallel MultiSearch, fuses them with coordinator-level RRF, and
 * rewrites the request into a standard scored query. Because fusion happens at the coordinator, the
 * document that matches BOTH legs ranks first regardless of shard placement.
 */
public class ResolverProcessorIT extends BaseNeuralSearchIT {

    private static final String INDEX = "resolver-poc-index";
    private static final String PIPELINE = "resolver-poc-pipeline";
    private static final String TITLE = "title";
    private static final String BODY = "body";
    private static final String RESCORE_INDEX = "resolver-poc-rescore-index";
    private static final String RANKDOCS_INDEX = "resolver-poc-rankdocs-index";

    @SneakyThrows
    public void testResolverRrf_whenDocMatchesBothLegs_thenRanksFirst() {
        initIndexIfNeeded();
        createResolverPipeline(PIPELINE);

        // Two legs: lexical match on title:"apple" and body:"banana".
        // d_both matches both legs; d_title only leg 1; d_body only leg 2; d_none matches neither.
        ResolverQueryBuilder resolver = new ResolverQueryBuilder(
            List.of(new MatchQueryBuilder(TITLE, "apple"), new MatchQueryBuilder(BODY, "banana")),
            ResolverQueryBuilder.TECHNIQUE_RRF,
            60,
            100
        );

        Map<String, Object> response = search(INDEX, resolver, null, 10, Map.of("search_pipeline", PIPELINE), null);

        List<Map<String, Object>> hits = readHits(response);
        List<String> ids = hits.stream().map(hit -> (String) hit.get("_id")).toList();

        // Union of the two legs is exactly {d_both, d_title, d_body}; d_none is in neither leg.
        assertEquals(3, ids.size());
        assertTrue(ids.contains("d_both"));
        assertTrue(ids.contains("d_title"));
        assertTrue(ids.contains("d_body"));
        assertFalse(ids.contains("d_none"));

        // Coordinator-level RRF: the doc in BOTH legs accumulates two contributions -> ranked first.
        assertEquals("d_both", ids.get(0));

        // Final result is a standard scored query, so scores must be in descending order.
        List<Double> scores = hits.stream().map(hit -> ((Number) hit.get("_score")).doubleValue()).toList();
        for (int i = 0; i < scores.size() - 1; i++) {
            assertTrue("resolver scores must be descending", scores.get(i) >= scores.get(i + 1));
        }
    }

    @SneakyThrows
    private void initIndexIfNeeded() {
        if (indexExists(INDEX)) {
            return;
        }
        String mapping = "{"
            + "\"settings\":{\"index\":{\"number_of_shards\":3,\"number_of_replicas\":0}},"
            + "\"mappings\":{\"properties\":{\"title\":{\"type\":\"text\"},\"body\":{\"type\":\"text\"}}}"
            + "}";
        createIndex(INDEX, mapping);
        ingestDocument(INDEX, "{\"title\":\"apple pie recipe\",\"body\":\"banana bread loaf\"}", "d_both");
        ingestDocument(INDEX, "{\"title\":\"apple orchard tour\",\"body\":\"fresh grape juice\"}", "d_title");
        ingestDocument(INDEX, "{\"title\":\"classic cherry tart\",\"body\":\"banana milk smoothie\"}", "d_body");
        ingestDocument(INDEX, "{\"title\":\"cherry chocolate cake\",\"body\":\"grape jam jar\"}", "d_none");
    }

    private void createResolverPipeline(final String pipelineName) throws Exception {
        makeRequest(
            client(),
            "PUT",
            "/_search/pipeline/" + pipelineName,
            null,
            toHttpEntity("{\"request_processors\":[{\"resolver\":{}}]}"),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> readHits(final Map<String, Object> response) {
        Map<String, Object> hitsMap = (Map<String, Object>) response.get("hits");
        return (List<Map<String, Object>>) hitsMap.get("hits");
    }

    /**
     * Verifies the claim that the resolver supports COMBINED rescore via the standard OpenSearch
     * top-level {@code rescore} element: after the resolver self-erases into a standard query, core
     * applies the rescore to the fused (RRF) scores. Uses a single shard so the baseline RRF order
     * is deterministic.
     */
    @SneakyThrows
    public void testResolverRrf_withStandardRescore_thenFusedRankingIsRescored() {
        initRescoreIndexIfNeeded();
        createResolverPipeline(PIPELINE);

        // Same resolver in both requests: RRF over match(title:apple) + match(body:banana).
        String resolver = "\"resolver\":{\"queries\":[{\"match\":{\"title\":\"apple\"}},{\"match\":{\"body\":\"banana\"}}],"
            + "\"technique\":\"rrf\",\"rank_constant\":60,\"rank_window_size\":100}";

        // Baseline (no rescore): d_rrf_leader is top of both legs -> highest RRF -> ranks first.
        String baselineBody = "{\"size\":10,\"query\":{" + resolver + "}}";
        List<String> baselineIds = ids(searchRaw(RESCORE_INDEX, baselineBody));
        assertFalse(baselineIds.isEmpty());
        assertEquals("d_rrf_leader", baselineIds.get(0));

        // Same resolver + STANDARD OpenSearch top-level rescore (match_phrase on content).
        // Only d_phrase_winner contains the phrase, so the combined rescore should lift it to #1.
        String rescoreBody = "{\"size\":10,\"query\":{"
            + resolver
            + "},\"rescore\":{\"window_size\":50,\"query\":{"
            + "\"rescore_query\":{\"match_phrase\":{\"content\":\"open source search\"}},"
            + "\"query_weight\":0.6,\"rescore_query_weight\":1.4,\"score_mode\":\"total\"}}}";
        List<String> rescoredIds = ids(searchRaw(RESCORE_INDEX, rescoreBody));
        assertFalse(rescoredIds.isEmpty());

        // Combined rescore altered the fused ranking: the phrase doc is now first.
        assertEquals("d_phrase_winner", rescoredIds.get(0));
        assertNotEquals(baselineIds.get(0), rescoredIds.get(0));
    }

    @SneakyThrows
    private void initRescoreIndexIfNeeded() {
        if (indexExists(RESCORE_INDEX)) {
            return;
        }
        // Single shard so RRF ranks (and thus the baseline order) are deterministic.
        String mapping = "{"
            + "\"settings\":{\"index\":{\"number_of_shards\":1,\"number_of_replicas\":0}},"
            + "\"mappings\":{\"properties\":{\"title\":{\"type\":\"text\"},\"body\":{\"type\":\"text\"},\"content\":{\"type\":\"text\"}}}"
            + "}";
        createIndex(RESCORE_INDEX, mapping);
        // Strongest in both legs -> RRF #1 without rescore. No rescore phrase.
        ingestDocument(
            RESCORE_INDEX,
            "{\"title\":\"apple apple apple\",\"body\":\"banana banana banana\",\"content\":\"reference notes about databases and indexes\"}",
            "d_rrf_leader"
        );
        // In both legs but weaker (RRF #2), and the only doc containing the rescore phrase.
        ingestDocument(
            RESCORE_INDEX,
            "{\"title\":\"apple\",\"body\":\"banana\",\"content\":\"a practical guide to open source search engines\"}",
            "d_phrase_winner"
        );
        // Leg 1 only, no phrase.
        ingestDocument(
            RESCORE_INDEX,
            "{\"title\":\"apple pie\",\"body\":\"grape jelly\",\"content\":\"unrelated cooking notes\"}",
            "d_filler"
        );
    }

    @SneakyThrows
    private Map<String, Object> searchRaw(final String index, final String body) {
        Response response = makeRequest(
            client(),
            "POST",
            "/" + index + "/_search",
            Map.of("search_pipeline", PIPELINE),
            toHttpEntity(body),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    private List<String> ids(final Map<String, Object> response) {
        return readHits(response).stream().map(hit -> (String) hit.get("_id")).toList();
    }

    // ---------------------------------------------------------------------------------------------
    // RankDocsQuery (Top + Tail) verification: which claimed improvements actually land.
    // Legs = match(title:apple) + match(body:banana). Leg-matching docs = r1, r2, r3 (r4 matches
    // neither). Single shard for deterministic RRF. rank_window_size=1 makes the fused window (1
    // doc) smaller than the match set (3) so the Tail's effect is observable.
    // ---------------------------------------------------------------------------------------------

    /** CLAIM: total hits cover ALL matches (via the Tail), not just the fused window. */
    @SneakyThrows
    public void testRankDocs_totalHits_coversAllMatchesNotJustWindow() {
        initRankDocsIndexIfNeeded();
        createResolverPipeline(PIPELINE);
        // rank_window_size=1 -> fused window is a single doc; the Tail should still surface all 3 matches.
        String body = "{\"size\":1,\"query\":{" + resolverFragment(1) + "}}";
        Map<String, Object> response = searchRaw(RANKDOCS_INDEX, body);
        assertEquals(3, totalHits(response)); // r1, r2, r3 (r4 excluded); NOT 1 (the window)
        assertEquals("r1", ids(response).get(0)); // the RRF leader (matches both legs) is returned/top
    }

    /** CLAIM: aggregations run over ALL matches (via the Tail), not just the fused window. */
    @SneakyThrows
    public void testRankDocs_aggregations_coverAllMatchesNotJustWindow() {
        initRankDocsIndexIfNeeded();
        createResolverPipeline(PIPELINE);
        String body = "{\"size\":1,\"query\":{"
            + resolverFragment(1)
            + "},\"aggs\":{\"by_category\":{\"terms\":{\"field\":\"category\"}}}}";
        Map<String, Object> response = searchRaw(RANKDOCS_INDEX, body);

        @SuppressWarnings("unchecked")
        Map<String, Object> aggs = (Map<String, Object>) response.get("aggregations");
        assertNotNull(aggs);
        @SuppressWarnings("unchecked")
        Map<String, Object> byCategory = (Map<String, Object>) aggs.get("by_category");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> buckets = (List<Map<String, Object>>) byCategory.get("buckets");
        int total = 0;
        int a = 0;
        int b = 0;
        for (Map<String, Object> bucket : buckets) {
            int docCount = ((Number) bucket.get("doc_count")).intValue();
            total += docCount;
            if ("A".equals(bucket.get("key"))) {
                a = docCount;
            } else if ("B".equals(bucket.get("key"))) {
                b = docCount;
            }
        }
        // All 3 matches are aggregated even though the fused window was 1: A={r1,r2}=2, B={r3}=1.
        assertEquals(3, total);
        assertEquals(2, a);
        assertEquals(1, b);
    }

    /** CLAIM: highlighting works because the Tail exposes the sub-queries' terms. */
    @SneakyThrows
    public void testRankDocs_highlightOnSubQueryTerms() {
        initRankDocsIndexIfNeeded();
        createResolverPipeline(PIPELINE);
        String body = "{\"size\":10,\"query\":{" + resolverFragment(10) + "},\"highlight\":{\"fields\":{\"title\":{}}}}";
        Map<String, Object> response = searchRaw(RANKDOCS_INDEX, body);

        Map<String, Object> topHit = readHits(response).get(0);
        assertEquals("r1", topHit.get("_id"));
        @SuppressWarnings("unchecked")
        Map<String, Object> highlight = (Map<String, Object>) topHit.get("highlight");
        assertNotNull("expected a highlight section for the top hit", highlight);
        @SuppressWarnings("unchecked")
        List<String> titleHighlights = (List<String>) highlight.get("title");
        assertNotNull("expected title highlights", titleHighlights);
        assertFalse(titleHighlights.isEmpty());
        assertTrue("expected the 'apple' term highlighted", titleHighlights.get(0).contains("<em>apple</em>"));
    }

    /** CLAIM: explain works; capture what the breakdown actually contains. */
    @SneakyThrows
    public void testRankDocs_explainIsPresentAndConsistent() {
        initRankDocsIndexIfNeeded();
        createResolverPipeline(PIPELINE);
        String body = "{\"size\":3,\"explain\":true,\"query\":{" + resolverFragment(10) + "}}";
        Map<String, Object> response = searchRaw(RANKDOCS_INDEX, body);

        Map<String, Object> topHit = readHits(response).get(0);
        @SuppressWarnings("unchecked")
        Map<String, Object> explanation = (Map<String, Object>) topHit.get("_explanation");
        assertNotNull("explain must be present", explanation);
        // Log the full structure so we can report exactly how rich the breakdown is.
        logger.info("RANKDOCS_EXPLAIN {}", explanation);
        double explainValue = ((Number) explanation.get("value")).doubleValue();
        double score = ((Number) topHit.get("_score")).doubleValue();
        assertEquals(score, explainValue, 0.0001d);
    }

    /**
     * CLAIM: conditional Tail — a plain top-K query with track_total_hits:false skips the Tail
     * (returns only the fused window), while the default keeps the Tail (all leg matches present).
     * window=1: legs match r1,r2,r3; RRF leader is r1.
     */
    @SneakyThrows
    public void testRankDocs_conditionalTail_plainTopKSkipsTail() {
        initRankDocsIndexIfNeeded();
        createResolverPipeline(PIPELINE);
        // Default (no aggs/explain/highlight, default track_total_hits) -> Tail ON -> all 3 leg matches.
        String withTail = "{\"size\":10,\"query\":{" + resolverFragment(1) + "}}";
        // track_total_hits:false + plain top-K -> Tail OFF -> only the single windowed doc.
        String noTail = "{\"size\":10,\"track_total_hits\":false,\"query\":{" + resolverFragment(1) + "}}";
        int tailOn = readHits(searchRaw(RANKDOCS_INDEX, withTail)).size();
        int tailOff = readHits(searchRaw(RANKDOCS_INDEX, noTail)).size();
        assertEquals("Tail ON: all leg matches present", 3, tailOn);
        assertEquals("Tail OFF: only the fused window", 1, tailOff);
    }

    private String resolverFragment(final int rankWindowSize) {
        return "\"resolver\":{\"queries\":[{\"match\":{\"title\":\"apple\"}},{\"match\":{\"body\":\"banana\"}}],"
            + "\"technique\":\"rrf\",\"rank_constant\":60,\"rank_window_size\":"
            + rankWindowSize
            + "}";
    }

    @SneakyThrows
    private void initRankDocsIndexIfNeeded() {
        if (indexExists(RANKDOCS_INDEX)) {
            return;
        }
        String mapping = "{"
            + "\"settings\":{\"index\":{\"number_of_shards\":1,\"number_of_replicas\":0}},"
            + "\"mappings\":{\"properties\":{"
            + "\"title\":{\"type\":\"text\"},\"body\":{\"type\":\"text\"},"
            + "\"content\":{\"type\":\"text\"},\"category\":{\"type\":\"keyword\"}}}"
            + "}";
        createIndex(RANKDOCS_INDEX, mapping);
        // r1 matches both legs (RRF leader); r2 leg1 only; r3 leg2 only; r4 neither.
        ingestDocument(
            RANKDOCS_INDEX,
            "{\"title\":\"apple\",\"body\":\"banana\",\"content\":\"open source search\",\"category\":\"A\"}",
            "r1"
        );
        ingestDocument(
            RANKDOCS_INDEX,
            "{\"title\":\"apple pie\",\"body\":\"grape\",\"content\":\"cooking notes\",\"category\":\"A\"}",
            "r2"
        );
        ingestDocument(
            RANKDOCS_INDEX,
            "{\"title\":\"cherry\",\"body\":\"banana split\",\"content\":\"dessert menu\",\"category\":\"B\"}",
            "r3"
        );
        ingestDocument(RANKDOCS_INDEX, "{\"title\":\"durian\",\"body\":\"kiwi\",\"content\":\"tropical fruit\",\"category\":\"B\"}", "r4");
    }

    @SuppressWarnings("unchecked")
    private int totalHits(final Map<String, Object> response) {
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        Map<String, Object> total = (Map<String, Object>) hits.get("total");
        return ((Number) total.get("value")).intValue();
    }
}
