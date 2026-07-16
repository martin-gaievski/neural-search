/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import lombok.SneakyThrows;

import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getNestedHits;

/**
 * End-to-end multi-node test for condition-based result boost. It runs on a 3-shard index so documents are spread
 * across data nodes; the boost tier is computed on the data node holding the shard and must be transported to the
 * coordinator via the sentinel envelope (NOT a JVM-static registry). The centerpiece assertion promotes a document
 * with LOW text relevance, living on a (likely remote) shard, to the top of the ranking purely because it matches
 * the boost condition — which can only pass if the tier reached the coordinator from that shard.
 *
 * <p>Fully lexical (match + term arms), no ML model required. Run with {@code -PnumNodes=3} for a genuine
 * multi-node exercise; it is also correct (and passes) on a single node.
 */
public class HybridConditionalBoostIT extends BaseNeuralSearchIT {

    private static final String INDEX_NAME = "test-hybrid-conditional-boost-multi-shard-index";
    private static final String SEARCH_PIPELINE = "phase-results-conditional-boost-pipeline";
    private static final String TEXT_FIELD = "name";
    private static final String CATEGORY_FIELD = "category";
    private static final int SHARDS = 3;

    // Six docs. Doc 6 ("Avengers mission", Drama) matches the text query only WEAKLY (a single "mission" term in an
    // otherwise-unrelated title), so without boost it ranks at/near the bottom; it is the promotion target via the
    // Drama boost condition. Docs 1-2 are strong "Mission Impossible" (Action) matches. All docs match at least one
    // arm so they are retrieved (the boost re-ranks retrieved candidates, it does not inject unretrieved docs).
    private static final Map<String, String[]> DOCS = Map.of(
        "1",
        new String[] { "Mission Impossible 1", "Action" },
        "2",
        new String[] { "Mission Impossible 2", "Action" },
        "3",
        new String[] { "Mission to Mars", "Sci-fi" },
        "4",
        new String[] { "The Mission", "Drama" },
        "5",
        new String[] { "Impossible Dream", "Drama" },
        "6",
        new String[] { "Avengers mission", "Drama" }
    );

    @Override
    public boolean isUpdateClusterSettings() {
        return true;
    }

    @SneakyThrows
    public void testConditionalBoost_whenDocMatchesConditionOnRemoteShard_thenPromotedToTop() {
        try {
            initializeIndexIfNotExists();
            createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);

            // Baseline: no boost. "Avengers" (doc 6) matches neither "mission" nor "impossible", so it should NOT be
            // in the top results (or ranks at the very bottom).
            HybridQueryBuilder baseline = new HybridQueryBuilder();
            baseline.add(new MatchQueryBuilder(TEXT_FIELD, "mission"));
            baseline.add(QueryBuilders.termQuery(TEXT_FIELD, "impossible"));

            Map<String, Object> baselineResponse = search(INDEX_NAME, baseline, null, 10, Map.of("search_pipeline", SEARCH_PIPELINE), null);
            List<String> baselineOrder = idOrder(baselineResponse);
            assertFalse("baseline sanity: doc 6 should not lead without boost", "6".equals(firstOrNull(baselineOrder)));

            // Boosted: promote all Drama docs. Doc 6 ("Avengers", Drama) has low/zero text relevance but must be
            // promoted into the top tier. This only works if the Drama tier computed on doc 6's shard reaches the
            // coordinator through the envelope.
            HybridQueryBuilder boosted = new HybridQueryBuilder();
            boosted.add(new MatchQueryBuilder(TEXT_FIELD, "mission"));
            boosted.add(QueryBuilders.termQuery(TEXT_FIELD, "impossible"));
            boosted.addBoostCondition(QueryBuilders.termQuery(CATEGORY_FIELD, "Drama"));

            Map<String, Object> boostedResponse = search(INDEX_NAME, boosted, null, 10, Map.of("search_pipeline", SEARCH_PIPELINE), null);
            List<String> boostedOrder = idOrder(boostedResponse);

            // Every returned Drama doc must rank above every returned non-Drama doc.
            int lastDramaRank = -1;
            int firstNonDramaRank = Integer.MAX_VALUE;
            for (int rank = 0; rank < boostedOrder.size(); rank++) {
                String id = boostedOrder.get(rank);
                boolean isDrama = "Drama".equals(DOCS.get(id)[1]);
                if (isDrama) {
                    lastDramaRank = Math.max(lastDramaRank, rank);
                } else {
                    firstNonDramaRank = Math.min(firstNonDramaRank, rank);
                }
            }
            assertTrue("at least one Drama doc must be present", lastDramaRank >= 0);
            assertTrue(
                "all Drama (boosted) docs must rank above non-Drama (organic) docs; order=" + boostedOrder,
                lastDramaRank < firstNonDramaRank
            );
            // The strongest signal: doc 6, which does not match the text query at all, is now present and boosted.
            assertTrue("boosted low-relevance Drama doc 6 must appear in results; order=" + boostedOrder, boostedOrder.contains("6"));
        } finally {
            wipeOfTestResources(INDEX_NAME, null, null, SEARCH_PIPELINE);
        }
    }

    /**
     * Pagination test: a boost-matching document that naturally sorts BELOW page 1 must be promoted ONTO page 1
     * once the boost condition is applied. Runs multi-shard (core applies from/size trim AFTER the normalization
     * processor, ordering by the band-encoded score), which is where the "boost before pagination" property is
     * load-bearing. pagination_depth is set on every query so the weak target (doc 6) is retrieved into its
     * shard's collection window and gets a tier (retrieval-not-injection gate).
     */
    @SneakyThrows
    public void testConditionalBoost_whenTargetBelowPageOne_thenPromotedOntoPageOne() {
        try {
            initializeIndexIfNotExists();
            createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);

            HybridQueryBuilder baseline = new HybridQueryBuilder();
            baseline.add(new MatchQueryBuilder(TEXT_FIELD, "mission"));
            baseline.add(QueryBuilders.termQuery(TEXT_FIELD, "impossible"));
            baseline.paginationDepth(10);

            HybridQueryBuilder boosted = new HybridQueryBuilder();
            boosted.add(new MatchQueryBuilder(TEXT_FIELD, "mission"));
            boosted.add(QueryBuilders.termQuery(TEXT_FIELD, "impossible"));
            boosted.addBoostCondition(QueryBuilders.termQuery(CATEGORY_FIELD, "Drama"));
            boosted.paginationDepth(10);

            // Baseline size=1: the single top hit is a strong "Mission Impossible" doc, NOT the weak Drama doc 6.
            Map<String, Object> page1Baseline = search(
                INDEX_NAME,
                baseline,
                null,
                1,
                Map.of("search_pipeline", SEARCH_PIPELINE),
                null,
                null,
                null,
                false,
                null,
                0,
                null
            );
            assertFalse("baseline: weak target doc 6 must be below page 1", idOrder(page1Baseline).contains("6"));

            // Boosted size=1: the sole tier-0 (Drama) doc must dominate the cross-shard merge and lead page 1.
            Map<String, Object> page1Boosted = search(
                INDEX_NAME,
                boosted,
                null,
                1,
                Map.of("search_pipeline", SEARCH_PIPELINE),
                null,
                null,
                null,
                false,
                null,
                0,
                null
            );
            assertEquals("boost must promote below-page-1 Drama doc 6 to rank 0 of page 1", "6", idOrder(page1Boosted).get(0));

            // from>0 leg: with size=3, doc 6 is on page 1 (promoted) and must NOT reappear on page 2.
            Map<String, Object> page1Size3 = search(
                INDEX_NAME,
                boosted,
                null,
                3,
                Map.of("search_pipeline", SEARCH_PIPELINE),
                null,
                null,
                null,
                false,
                null,
                0,
                null
            );
            Map<String, Object> page2Size3 = search(
                INDEX_NAME,
                boosted,
                null,
                3,
                Map.of("search_pipeline", SEARCH_PIPELINE),
                null,
                null,
                null,
                false,
                null,
                3,
                null
            );
            assertTrue("promoted doc 6 must be on page 1 (size=3)", idOrder(page1Size3).contains("6"));
            assertFalse("promoted doc 6 must not reappear on page 2", idOrder(page2Size3).contains("6"));
        } finally {
            wipeOfTestResources(INDEX_NAME, null, null, SEARCH_PIPELINE);
        }
    }

    @SuppressWarnings("unchecked")
    private List<String> idOrder(Map<String, Object> searchResponseAsMap) {
        List<Map<String, Object>> hits = getNestedHits(searchResponseAsMap);
        return hits.stream().map(hit -> (String) hit.get("_id")).toList();
    }

    private String firstOrNull(List<String> list) {
        return list.isEmpty() ? null : list.get(0);
    }

    @SneakyThrows
    private void initializeIndexIfNotExists() {
        if (indexExists(INDEX_NAME)) {
            return;
        }
        createIndexWithConfiguration(
            INDEX_NAME,
            buildIndexConfiguration(List.of(), Map.of(), List.of(), List.of(CATEGORY_FIELD), List.of(), SHARDS),
            ""
        );
        for (Map.Entry<String, String[]> doc : DOCS.entrySet()) {
            indexTheDocument(
                INDEX_NAME,
                doc.getKey(),
                List.of(),
                List.of(),
                Collections.singletonList(TEXT_FIELD),
                Collections.singletonList(doc.getValue()[0]),
                List.of(),
                Map.of(),
                List.of(),
                List.of(),
                List.of(CATEGORY_FIELD),
                List.of(doc.getValue()[1]),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                null
            );
        }
    }
}
