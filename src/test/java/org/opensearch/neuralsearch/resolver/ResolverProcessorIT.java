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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_USER_AGENT;

/**
 * End-to-end demonstration of the Resolver framework POC (Phase 1).
 *
 * <p>Runs {@code resolver} queries with NO search pipeline — the {@code ResolverActionFilter}
 * intercepts them on the coordinator, fires the legs as a parallel MultiSearch, fuses them (RRF or
 * min_max + arithmetic_mean), and rewrites the request into a standard scored query. Covers top-level
 * and nested / multi-marker placement, filter push-down, combined rescore, and the RankDocsQuery
 * (Top + conditional Tail) recoveries. Because fusion happens at the coordinator, the document that
 * matches BOTH legs ranks first regardless of shard placement.
 */
public class ResolverProcessorIT extends BaseNeuralSearchIT {

    private static final String INDEX = "resolver-poc-index";
    private static final String TITLE = "title";
    private static final String BODY = "body";
    private static final String RESCORE_INDEX = "resolver-poc-rescore-index";
    private static final String RANKDOCS_INDEX = "resolver-poc-rankdocs-index";
    private static final String NESTED_INDEX = "resolver-poc-nested-index";
    private static final String SHOP_INDEX = "resolver-poc-shop-index";
    private static final String DIVERGE_INDEX = "resolver-poc-diverge-index";
    private static final String PERSHARD_INDEX = "resolver-poc-pershard-index";
    private static final String PERSHARD_PIPELINE = "resolver-poc-mmam-pipeline";

    @SneakyThrows
    public void testResolverRrf_whenDocMatchesBothLegs_thenRanksFirst() {
        initIndexIfNeeded();

        // Two legs: lexical match on title:"apple" and body:"banana".
        // d_both matches both legs; d_title only leg 1; d_body only leg 2; d_none matches neither.
        ResolverQueryBuilder resolver = new ResolverQueryBuilder(
            List.of(new MatchQueryBuilder(TITLE, "apple"), new MatchQueryBuilder(BODY, "banana")),
            ResolverQueryBuilder.TECHNIQUE_RRF,
            60,
            100
        );

        Map<String, Object> response = search(INDEX, resolver, null, 10, Map.of(), null);

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

    /**
     * CLAIM (pipeline-free): a resolver query works with NO search pipeline — the ActionFilter
     * (`ResolverActionFilter`) intercepts and orchestrates it. Same fusion result as the pipeline path.
     */
    @SneakyThrows
    public void testResolver_worksWithoutSearchPipeline() {
        initIndexIfNeeded();
        // Intentionally do NOT create or reference any search pipeline.
        ResolverQueryBuilder resolver = new ResolverQueryBuilder(
            List.of(new MatchQueryBuilder(TITLE, "apple"), new MatchQueryBuilder(BODY, "banana")),
            ResolverQueryBuilder.TECHNIQUE_RRF,
            60,
            100
        );
        // Empty request params => no search_pipeline. The ActionFilter handles the resolver query.
        Map<String, Object> response = search(INDEX, resolver, null, 10, Map.of(), null);
        List<String> ids = ids(response);
        assertEquals(3, ids.size());
        assertEquals("d_both", ids.get(0));
        assertFalse(ids.contains("d_none"));
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
            Map.of(),
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
        // Default (no aggs/explain/highlight, default track_total_hits) -> Tail ON -> all 3 leg matches.
        String withTail = "{\"size\":10,\"query\":{" + resolverFragment(1) + "}}";
        // track_total_hits:false + plain top-K -> Tail OFF -> only the single windowed doc.
        String noTail = "{\"size\":10,\"track_total_hits\":false,\"query\":{" + resolverFragment(1) + "}}";
        int tailOn = readHits(searchRaw(RANKDOCS_INDEX, withTail)).size();
        int tailOff = readHits(searchRaw(RANKDOCS_INDEX, noTail)).size();
        assertEquals("Tail ON: all leg matches present", 3, tailOn);
        assertEquals("Tail OFF: only the fused window", 1, tailOff);
    }

    /**
     * CLAIM (Option B): the resolver also supports **min_max normalization + arithmetic_mean combination**,
     * selected via the `normalization`/`combination` objects — pipeline-free. Happy path: the doc strongest
     * in BOTH legs normalizes to ~1.0 in each leg, giving the highest arithmetic mean, so it ranks first.
     */
    @SneakyThrows
    public void testResolver_minMaxArithmeticMean_happyPath() {
        initRescoreIndexIfNeeded();
        String body = "{\"size\":10,\"query\":{\"resolver\":{"
            + "\"queries\":[{\"match\":{\"title\":\"apple\"}},{\"match\":{\"body\":\"banana\"}}],"
            + "\"rank_window_size\":100,"
            + "\"normalization\":{\"technique\":\"min_max\"},"
            + "\"combination\":{\"technique\":\"arithmetic_mean\"}"
            + "}}}";
        Map<String, Object> response = searchNoPipeline(RESCORE_INDEX, body);
        List<Map<String, Object>> hits = readHits(response);
        List<String> ids = hits.stream().map(hit -> (String) hit.get("_id")).toList();

        // d_rrf_leader has the strongest title+body match (highest TF) -> ~1.0 in both legs -> top mean.
        assertEquals("d_rrf_leader", ids.get(0));
        assertTrue(ids.contains("d_phrase_winner")); // both legs, weaker
        assertTrue(ids.contains("d_filler"));         // leg 1 only, still present

        // Self-erased into a standard scored query -> scores are in descending order.
        double previous = Double.MAX_VALUE;
        for (Map<String, Object> hit : hits) {
            double score = ((Number) hit.get("_score")).doubleValue();
            assertTrue(score <= previous);
            previous = score;
        }
    }

    /**
     * CLAIM: RRF and min_max+arithmetic_mean are genuinely different techniques (not silently the same).
     * Decisive setup (single shard), two legs — match(title:apple), match(body:banana) — three docs:
     * <ul>
     *   <li>{@code strong_title} — leg-1 top (title apple x3), absent from leg 2</li>
     *   <li>{@code strong_body}  — leg-2 top (body banana x3), absent from leg 1</li>
     *   <li>{@code both_mid}     — matches BOTH legs but is the minimum score in each</li>
     * </ul>
     * RRF rewards multi-leg presence (sum of reciprocal ranks) -> {@code both_mid} wins. min_max+AM
     * rewards normalized score (mean over matched legs): a single-leg leader normalizes to 1.0 and beats
     * {@code both_mid}, whose per-leg scores are the minimum (~0.001) in each leg -> {@code both_mid} last.
     * The orderings FLIP.
     */
    @SneakyThrows
    public void testResolver_rrfVsMinMaxArithmeticMean_orderingsDiffer() {
        initDivergeIndexIfNeeded();
        String legs = "\"queries\":[{\"match\":{\"title\":\"apple\"}},{\"match\":{\"body\":\"banana\"}}]";
        String rrfBody = "{\"size\":10,\"query\":{\"resolver\":{"
            + legs
            + ",\"combination\":{\"technique\":\"rrf\",\"parameters\":{\"rank_constant\":60}}}}}";
        String amBody = "{\"size\":10,\"query\":{\"resolver\":{"
            + legs
            + ",\"normalization\":{\"technique\":\"min_max\"},\"combination\":{\"technique\":\"arithmetic_mean\"}}}}";

        List<String> rrfIds = ids(searchNoPipeline(DIVERGE_INDEX, rrfBody));
        List<String> amIds = ids(searchNoPipeline(DIVERGE_INDEX, amBody));

        // RRF: the both-legs doc wins on summed reciprocal ranks.
        assertEquals("both_mid", rrfIds.get(0));
        // min_max+AM: a single-leg leader (normalized 1.0) wins; the both-legs doc is demoted to last.
        assertNotEquals("both_mid", amIds.get(0));
        assertEquals("both_mid", amIds.get(amIds.size() - 1));
        // The two techniques genuinely disagree.
        assertNotEquals(rrfIds, amIds);
    }

    @SneakyThrows
    private void initDivergeIndexIfNeeded() {
        if (indexExists(DIVERGE_INDEX)) {
            return;
        }
        // Single shard for deterministic BM25/RRF.
        String mapping = "{"
            + "\"settings\":{\"index\":{\"number_of_shards\":1,\"number_of_replicas\":0}},"
            + "\"mappings\":{\"properties\":{\"title\":{\"type\":\"text\"},\"body\":{\"type\":\"text\"}}}"
            + "}";
        createIndex(DIVERGE_INDEX, mapping);
        // leg 1 (title:apple) top, absent from leg 2
        ingestDocument(DIVERGE_INDEX, "{\"title\":\"apple apple apple\",\"body\":\"grape jam\"}", "strong_title");
        // leg 2 (body:banana) top, absent from leg 1
        ingestDocument(DIVERGE_INDEX, "{\"title\":\"cherry pie\",\"body\":\"banana banana banana\"}", "strong_body");
        // matches BOTH legs but is the minimum score in each
        ingestDocument(DIVERGE_INDEX, "{\"title\":\"apple\",\"body\":\"banana\"}", "both_mid");
    }

    private String resolverFragment(final int rankWindowSize) {
        return "\"resolver\":{\"queries\":[{\"match\":{\"title\":\"apple\"}},{\"match\":{\"body\":\"banana\"}}],"
            + "\"technique\":\"rrf\",\"rank_constant\":60,\"rank_window_size\":"
            + rankWindowSize
            + "}";
    }

    /**
     * CLAIM: accurate total-hits can be derived from the legs' OWN totals (id set-union) — no Tail — when every
     * leg's full match set is retrieved. rank_window_size=100 >> the leg match counts (2 each), so the union
     * {r1,r2,r3}=3 is computed from stage-A alone and patched onto the response (Tail skipped).
     */
    @SneakyThrows
    public void testRankDocs_totalHits_fromLegUnion_whenFullyRetrieved() {
        initRankDocsIndexIfNeeded();
        String body = "{\"size\":10,\"query\":{" + resolverFragment(100) + "}}";
        Map<String, Object> response = searchNoPipeline(RANKDOCS_INDEX, body);
        assertEquals(3, totalHits(response));      // union {r1,r2,r3} — from the legs, not a Tail re-run
        assertEquals("r1", ids(response).get(0));  // RRF leader (matches both legs)
        assertFalse(ids(response).contains("r4")); // matches neither leg
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

    // ---------------------------------------------------------------------------------------------
    // Nested placement: a resolver marker inside a bool tree, resolved by the ActionFilter via
    // recursive search-and-replace + filter push-down. Possible because the resolver self-erases into
    // a standard query — the hybrid query, which must be top-level, cannot be nested like this.
    // ---------------------------------------------------------------------------------------------

    /** CLAIM: a resolver nested inside bool.must works with NO pipeline and fusion is preserved. */
    @SneakyThrows
    public void testResolver_nestedInBoolMust_noFilter_thenFuses() {
        initNestedIndexIfNeeded();
        // bool { must: [ resolver ] } -- no search pipeline; the ActionFilter resolves the nested marker.
        String body = "{\"size\":10,\"query\":{\"bool\":{\"must\":[{" + resolverFragment(100) + "}]}}}";
        List<String> ids = ids(searchNoPipeline(NESTED_INDEX, body));
        // Union of the legs = {n_both_y, n_both_x, n_title_x}; both-legs docs outrank the title-only doc.
        assertEquals(3, ids.size());
        assertEquals("n_both_y", ids.get(0));
        assertTrue(ids.contains("n_both_x"));
        assertTrue(ids.contains("n_title_x"));
    }

    /**
     * CLAIM: an enclosing bool filter is PUSHED DOWN into each leg, so fusion runs over the filtered
     * candidate set (not a global fuse-then-filter). Decisive setup: rank_window_size=1 and the
     * globally strongest doc (n_both_y, higher term frequency) is category=y.
     * <ul>
     *   <li>With push-down (implemented): legs are filtered to category=x first, so the size-1 fused
     *       window is n_both_x -> result is [n_both_x].</li>
     *   <li>Without push-down (fuse-then-filter): the global size-1 window would be n_both_y, which the
     *       outer category=x filter then removes -> result would be EMPTY.</li>
     * </ul>
     * A non-empty [n_both_x] therefore proves the filter reached the legs.
     */
    @SneakyThrows
    public void testResolver_nestedInBool_pushesFilterIntoLegs() {
        initNestedIndexIfNeeded();
        String body = "{\"size\":10,\"query\":{\"bool\":{"
            + "\"must\":[{"
            + resolverFragment(1)
            + "}],"
            + "\"filter\":[{\"term\":{\"category\":\"x\"}}]}}}";
        List<String> ids = ids(searchNoPipeline(NESTED_INDEX, body));
        assertEquals(List.of("n_both_x"), ids);
    }

    @SneakyThrows
    private void initNestedIndexIfNeeded() {
        if (indexExists(NESTED_INDEX)) {
            return;
        }
        // Single shard for deterministic RRF ranks.
        String mapping = "{"
            + "\"settings\":{\"index\":{\"number_of_shards\":1,\"number_of_replicas\":0}},"
            + "\"mappings\":{\"properties\":{"
            + "\"title\":{\"type\":\"text\"},\"body\":{\"type\":\"text\"},\"category\":{\"type\":\"keyword\"}}}"
            + "}";
        createIndex(NESTED_INDEX, mapping);
        // Matches BOTH legs strongly (higher term frequency) but is category=y -> the global leader.
        ingestDocument(NESTED_INDEX, "{\"title\":\"apple apple\",\"body\":\"banana banana\",\"category\":\"y\"}", "n_both_y");
        // Matches BOTH legs (weaker) and is category=x.
        ingestDocument(NESTED_INDEX, "{\"title\":\"apple\",\"body\":\"banana\",\"category\":\"x\"}", "n_both_x");
        // Matches leg 1 (title) only, category=x.
        ingestDocument(NESTED_INDEX, "{\"title\":\"apple\",\"body\":\"grape\",\"category\":\"x\"}", "n_title_x");
    }

    @SneakyThrows
    private Map<String, Object> searchNoPipeline(final String index, final String body) {
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

    /**
     * CLAIM: a single search query can carry MULTIPLE resolver markers at DIFFERENT nesting levels,
     * each resolved independently with depth-dependent filter push-down. The hybrid query cannot be
     * placed in any of these positions.
     *
     * <pre>
     * bool {
     *   filter:  [ in_stock=true ]                         // (1) applies to BOTH resolvers
     *   must:    [ R1 ]                                     // R1 at level 2
     *   should:  [ bool {
     *                filter: [ category=shoes ]             // (2) applies to R2 only
     *                must:   [ R2 ]                         // R2 at level 3
     *              } ]
     * }
     * R1 legs = title:running, description:lightweight  -> push-down [in_stock=true]
     * R2 legs = title:trail,   description:grip         -> push-down [in_stock=true, category=shoes]
     * </pre>
     *
     * Docs: p1 running shoes, p2 trail running shoes (grip sole), p3 running jacket (category=apparel),
     * p4 trail grip boots (OUT OF STOCK), p5 casual sneakers.
     */
    @SneakyThrows
    public void testResolver_multipleMarkersAtDifferentLevels_withPerLevelPushDown() {
        initShopIndexIfNeeded();
        String body = "{"
            + "\"size\":10,"
            + "\"query\":{\"bool\":{"
            + "\"filter\":[{\"term\":{\"in_stock\":true}}],"
            + "\"must\":[{\"resolver\":{\"queries\":[{\"match\":{\"title\":\"running\"}},{\"match\":{\"description\":\"lightweight\"}}],"
            + "\"technique\":\"rrf\",\"rank_constant\":60,\"rank_window_size\":100}}],"
            + "\"should\":[{\"bool\":{"
            + "\"filter\":[{\"term\":{\"category\":\"shoes\"}}],"
            + "\"must\":[{\"resolver\":{\"queries\":[{\"match\":{\"title\":\"trail\"}},{\"match\":{\"description\":\"grip\"}}],"
            + "\"technique\":\"rrf\",\"rank_constant\":60,\"rank_window_size\":100}}]"
            + "}}]"
            + "}}}";
        Map<String, Object> response = searchNoPipeline(SHOP_INDEX, body);
        List<Map<String, Object>> hits = readHits(response);
        List<String> hitIds = hits.stream().map(hit -> (String) hit.get("_id")).toList();

        // R1's fused set (in-stock docs matching running/lightweight) = {p1, p2, p3}.
        assertEquals(3, totalHits(response));
        assertEquals(3, hitIds.size());
        assertTrue(hitIds.contains("p1"));
        assertTrue(hitIds.contains("p2"));
        assertTrue(hitIds.contains("p3"));
        assertFalse(hitIds.contains("p4")); // in_stock=true pushed into BOTH resolvers' legs -> excluded
        assertFalse(hitIds.contains("p5")); // matches neither R1 leg; bool.must requires an R1 match

        // p2 is in BOTH R1's and R2's fused sets, so the must (R1) and should->R2 scores add -> ranks first.
        assertEquals("p2", hitIds.get(0));

        // The nested R2 (a should clause, constrained to category=shoes) demonstrably boosted p2.
        Map<String, Double> scoreById = new HashMap<>();
        for (Map<String, Object> hit : hits) {
            scoreById.put((String) hit.get("_id"), ((Number) hit.get("_score")).doubleValue());
        }
        assertTrue(scoreById.get("p2") > scoreById.get("p1"));
        assertTrue(scoreById.get("p2") > scoreById.get("p3"));
    }

    @SneakyThrows
    private void initShopIndexIfNeeded() {
        if (indexExists(SHOP_INDEX)) {
            return;
        }
        // Single shard for deterministic RRF ranks.
        String mapping = "{"
            + "\"settings\":{\"index\":{\"number_of_shards\":1,\"number_of_replicas\":0}},"
            + "\"mappings\":{\"properties\":{"
            + "\"title\":{\"type\":\"text\"},\"description\":{\"type\":\"text\"},"
            + "\"brand\":{\"type\":\"keyword\"},\"category\":{\"type\":\"keyword\"},\"in_stock\":{\"type\":\"boolean\"}}}"
            + "}";
        createIndex(SHOP_INDEX, mapping);
        ingestDocument(
            SHOP_INDEX,
            "{\"title\":\"running shoes\",\"description\":\"lightweight breathable\",\"brand\":\"acme\",\"category\":\"shoes\",\"in_stock\":true}",
            "p1"
        );
        ingestDocument(
            SHOP_INDEX,
            "{\"title\":\"trail running shoes\",\"description\":\"lightweight grip sole\",\"brand\":\"acme\",\"category\":\"shoes\",\"in_stock\":true}",
            "p2"
        );
        ingestDocument(
            SHOP_INDEX,
            "{\"title\":\"running jacket\",\"description\":\"lightweight shell\",\"brand\":\"acme\",\"category\":\"apparel\",\"in_stock\":true}",
            "p3"
        );
        ingestDocument(
            SHOP_INDEX,
            "{\"title\":\"trail grip boots\",\"description\":\"rugged grip trail\",\"brand\":\"beta\",\"category\":\"shoes\",\"in_stock\":false}",
            "p4"
        );
        ingestDocument(
            SHOP_INDEX,
            "{\"title\":\"casual sneakers\",\"description\":\"everyday comfort\",\"brand\":\"beta\",\"category\":\"shoes\",\"in_stock\":true}",
            "p5"
        );
    }

    // ---------------------------------------------------------------------------------------------
    // Per-shard candidate collection (multi-shard). The resolver's min_max+arithmetic_mean fusion is
    // window-sensitive because the POC's default "coordinator" collection reduces each leg to the
    // GLOBAL top-rank_window_size before fusion, so min/max are computed over a narrower, compressed
    // pool than the hybrid query (which normalizes over each shard's local top-pagination_depth). The
    // "per_shard" collection fires each leg once PER SHARD (preference=_shards:i) and unions the
    // per-shard slices, reproducing hybrid's exact normalization pool.
    //
    // Decisive, placement-independent invariant: with candidate_depth == pagination_depth, the
    // per_shard resolver must fuse to the SAME ranking and (within float epsilon) the SAME scores as
    // an equivalent hybrid min_max+arithmetic_mean query, DOC-FOR-DOC — regardless of how the docs are
    // physically distributed across shards. A grouping / routing / union bug breaks this equality.
    // ---------------------------------------------------------------------------------------------

    /**
     * CLAIM: on a multi-shard index, per_shard resolver min_max+AM == hybrid min_max+AM doc-for-doc
     * (same doc set, per-id scores equal within epsilon), because both fuse over the union of each
     * shard's local top-depth. candidate_depth (10) == hybrid pagination_depth (10). Both size and window
     * are set >= the total matched docs (5) so the ENTIRE fused union is returned by both — no top-K
     * truncation, so a boundary tie cannot spuriously differ; any mismatch is a real fusion divergence.
     * This is the invariant that a grouping / routing / union regression would break.
     */
    @SneakyThrows
    public void testPerShard_matchesHybridDocForDoc() {
        initPerShardIndexIfNeeded();
        createMinMaxArithmeticMeanPipelineIfNeeded();

        // Hybrid oracle: match(title:apple) + match(body:banana), pagination_depth=10, min_max+AM, size=20 (all).
        String hybridBody = "{\"size\":20,\"query\":{\"hybrid\":{\"pagination_depth\":10,\"queries\":["
            + "{\"match\":{\"title\":\"apple\"}},{\"match\":{\"body\":\"banana\"}}]}}}";
        List<Map<String, Object>> hybridHits = readHits(searchWithPipeline(PERSHARD_INDEX, hybridBody, PERSHARD_PIPELINE));

        // Resolver per_shard: same legs, candidate_depth=10 (== pagination_depth), window/size >= all matches, no pipeline.
        String resolverBody = "{\"size\":20,\"query\":{\"resolver\":{\"queries\":["
            + "{\"match\":{\"title\":\"apple\"}},{\"match\":{\"body\":\"banana\"}}],"
            + "\"rank_window_size\":20,\"collection\":\"per_shard\",\"candidate_depth\":10,"
            + "\"normalization\":{\"technique\":\"min_max\"},\"combination\":{\"technique\":\"arithmetic_mean\"}}},"
            + "\"track_total_hits\":false}";
        List<Map<String, Object>> resolverHits = readHits(searchNoPipeline(PERSHARD_INDEX, resolverBody));

        assertFalse("hybrid returned no hits", hybridHits.isEmpty());
        assertEquals("per_shard resolver and hybrid must return the same number of hits", hybridHits.size(), resolverHits.size());

        // Doc-for-doc: identical id SET and identical per-id scores (within float epsilon). We compare by
        // id->score map (not raw position) so a genuine fusion difference surfaces as a set/score mismatch,
        // while a pure tie-break order difference between hybrid and resolver is not spuriously flagged.
        Map<String, Double> hybridScores = scoresById(hybridHits);
        Map<String, Double> resolverScores = scoresById(resolverHits);
        assertEquals("per_shard resolver must return the same doc set as hybrid", hybridScores.keySet(), resolverScores.keySet());
        for (Map.Entry<String, Double> e : hybridScores.entrySet()) {
            assertEquals("score for " + e.getKey() + " must match hybrid", e.getValue(), resolverScores.get(e.getKey()), 1e-4);
        }
    }

    private Map<String, Double> scoresById(final List<Map<String, Object>> hits) {
        Map<String, Double> byId = new HashMap<>();
        for (Map<String, Object> hit : hits) {
            byId.put((String) hit.get("_id"), ((Number) hit.get("_score")).doubleValue());
        }
        return byId;
    }

    /**
     * CLAIM: the per_shard collection genuinely widens the candidate pool - it is NOT a silent no-op /
     * fallback to coordinator. With a deliberately narrow coordinator window (rank_window_size = 2), each
     * leg's coordinator pool is the GLOBAL top-2, whereas per_shard (candidate_depth = 10) collects each
     * shard's full local pool. A leg's global top-2 is always a subset of its full per-shard union, so the
     * per_shard fused result set is a SUPERSET of the coordinator result set, and strictly larger here (5
     * matching docs; a window-2 coordinator fuses at most 4 distinct). Both facts are deterministic and
     * independent of BM25 tie-breaks and physical shard placement - a coordinator fallback (bug) would
     * instead make the two sets identical.
     */
    @SneakyThrows
    public void testPerShard_widensPoolVsCoordinator() {
        initPerShardIndexIfNeeded();

        // Coordinator: leg size = rank_window_size = 2 -> each leg's pool is the global top-2.
        String coordinatorBody = "{\"size\":20,\"query\":{\"resolver\":{\"queries\":["
            + "{\"match\":{\"title\":\"apple\"}},{\"match\":{\"body\":\"banana\"}}],"
            + "\"rank_window_size\":2,"
            + "\"normalization\":{\"technique\":\"min_max\"},\"combination\":{\"technique\":\"arithmetic_mean\"}}},"
            + "\"track_total_hits\":false}";
        Set<String> coordinatorIds = new HashSet<>(ids(searchNoPipeline(PERSHARD_INDEX, coordinatorBody)));

        // Per-shard: candidate_depth=10 -> each shard's full local pool -> the union is every matching doc.
        String perShardBody = "{\"size\":20,\"query\":{\"resolver\":{\"queries\":["
            + "{\"match\":{\"title\":\"apple\"}},{\"match\":{\"body\":\"banana\"}}],"
            + "\"rank_window_size\":20,\"collection\":\"per_shard\",\"candidate_depth\":10,"
            + "\"normalization\":{\"technique\":\"min_max\"},\"combination\":{\"technique\":\"arithmetic_mean\"}}},"
            + "\"track_total_hits\":false}";
        Set<String> perShardIds = new HashSet<>(ids(searchNoPipeline(PERSHARD_INDEX, perShardBody)));

        logger.info("PERSHARD_POOL coordinator={} per_shard={}", coordinatorIds, perShardIds);
        // Per-shard pool is a strict superset of the narrow coordinator pool -> the path ran and widened it.
        assertTrue(
            "per_shard result set must contain the coordinator result set: coord=" + coordinatorIds + " per_shard=" + perShardIds,
            perShardIds.containsAll(coordinatorIds)
        );
        assertTrue(
            "per_shard must fuse strictly more docs than a window-2 coordinator: coord=" + coordinatorIds + " per_shard=" + perShardIds,
            perShardIds.size() > coordinatorIds.size()
        );
        // candidate_depth (10) >= docs-per-shard, so per_shard recovers the entire 5-doc match set.
        assertEquals("per_shard must fuse the full match set", 5, perShardIds.size());
    }

    @SneakyThrows
    private Map<String, Object> searchWithPipeline(final String index, final String body, final String pipeline) {
        Response response = makeRequest(
            client(),
            "POST",
            "/" + index + "/_search?search_pipeline=" + pipeline,
            Map.of(),
            toHttpEntity(body),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    private void createMinMaxArithmeticMeanPipelineIfNeeded() {
        // min_max normalization + arithmetic_mean combination, unweighted — the hybrid equivalent of the resolver's
        // default min_max+AM. Reuses the base IT helper so the pipeline JSON matches production exactly.
        createSearchPipeline(PERSHARD_PIPELINE, "min_max", "arithmetic_mean", Map.of());
    }

    @SneakyThrows
    private void initPerShardIndexIfNeeded() {
        if (indexExists(PERSHARD_INDEX)) {
            return;
        }
        // Multiple shards so coordinator (global-top-K) and per_shard (per-shard union) pools genuinely differ.
        // The key doc is "wb" (weak in BOTH legs): it sits OUTSIDE each leg's global top-2 (its term frequency is
        // strictly below the strong docs) but INSIDE each shard's local top-candidate_depth. So a window-2
        // coordinator pool excludes wb entirely, while per-shard collection (and hybrid) include it — a
        // deterministic, shard-placement-independent divergence. Strong docs use high TF + padding so their BM25
        // clearly beats wb's regardless of length normalization.
        String mapping = "{"
            + "\"settings\":{\"index\":{\"number_of_shards\":3,\"number_of_replicas\":0}},"
            + "\"mappings\":{\"properties\":{\"title\":{\"type\":\"text\"},\"body\":{\"type\":\"text\"}}}"
            + "}";
        createIndex(PERSHARD_INDEX, mapping);
        // Leg A (title:apple) strong docs — global top of leg A.
        ingestDocument(PERSHARD_INDEX, "{\"title\":\"apple apple apple apple apple\",\"body\":\"grape grape grape\"}", "a1");
        ingestDocument(PERSHARD_INDEX, "{\"title\":\"apple apple apple apple\",\"body\":\"grape grape\"}", "a2");
        // Leg B (body:banana) strong docs — global top of leg B.
        ingestDocument(PERSHARD_INDEX, "{\"title\":\"cherry cherry\",\"body\":\"banana banana banana banana banana\"}", "b1");
        ingestDocument(PERSHARD_INDEX, "{\"title\":\"cherry\",\"body\":\"banana banana banana banana\"}", "b2");
        // wb: matches BOTH legs but weakly (TF 1 each) -> outside each leg's global top-2, inside each shard's pool.
        ingestDocument(PERSHARD_INDEX, "{\"title\":\"apple\",\"body\":\"banana\"}", "wb");
    }
}
