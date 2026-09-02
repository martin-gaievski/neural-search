/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.explain;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit coverage for the shapes the fused {@code explain} path has to survive without a cluster: nothing collected, a hit
 * fusion never ranked, a score a post-fusion step moved, and a leg that returned no explanation of its own. What the tree
 * looks like for a real query is pinned end-to-end in {@code HybridQueryFusedModeExplainIT}.
 */
public class FusedExplanationMergerTests extends OpenSearchTestCase {

    private static final String INDEX = "test-index";
    private static final String COMBINATION = "arithmetic_mean combination of:";
    private static final String NORMALIZATION = "min_max normalization of:";
    private static final String FINAL_SCORE = "score of the fused hybrid query as round 2 returned it, computed from:";

    public void testGetMergedResponse_whenNothingCollected_thenResponseReturnedUntouched() {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        SearchResponse response = responseWithHits(hit("1", 0.5f));

        assertTrue("nothing was collected", merger.isEmpty());
        assertSame("an unexplained response must not be rebuilt", response, merger.getMergedResponse(response));
        assertNull("and no hit may gain an explanation", response.getHits().getHits()[0].getExplanation());
    }

    public void testGetMergedResponse_whenEmptyCollectionPublished_thenNothingIsAttached() {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(new FusedDocExplanations());
        SearchResponse response = responseWithHits(hit("1", 0.5f));

        assertTrue("an explained request whose legs ranked nothing publishes an empty collection", merger.isEmpty());
        assertNull(merger.getMergedResponse(response).getHits().getHits()[0].getExplanation());
    }

    public void testGetMergedResponse_whenDocumentWasRanked_thenTheFusedTreeReplacesRoundTwos() {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.6f, 0.4f, 0.8f));
        SearchHit ranked = hit("1", 0.6f);
        ranked.explanation(Explanation.match(0.6f, "ConstantScore(_id:[1])"));

        SearchResponse merged = merger.getMergedResponse(responseWithHits(ranked));
        Explanation explanation = merged.getHits().getHits()[0].getExplanation();

        assertEquals(COMBINATION, explanation.getDescription());
        assertEquals(0.6f, explanation.getValue().floatValue(), 0.0f);
        assertEquals("one node per leg", 2, explanation.getDetails().length);
        assertEquals(NORMALIZATION, explanation.getDetails()[0].getDescription());
        assertEquals(0.4f, explanation.getDetails()[0].getValue().floatValue(), 0.0f);
        assertEquals("the leg's own explanation is kept under it", 1, explanation.getDetails()[0].getDetails().length);
        assertEquals("leg 0 raw", explanation.getDetails()[0].getDetails()[0].getDescription());
    }

    public void testGetMergedResponse_whenDocumentWasNotRanked_thenItsOwnExplanationIsKept() {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.6f, 0.4f, 0.8f));
        SearchHit tailOnly = hit("2", 0.0f);
        tailOnly.explanation(Explanation.match(0.0f, "the Tail matched this document"));

        SearchResponse merged = merger.getMergedResponse(responseWithHits(tailOnly));

        assertEquals(
            "a document fusion never ranked has no fused breakdown to show",
            "the Tail matched this document",
            merged.getHits().getHits()[0].getExplanation().getDescription()
        );
    }

    public void testGetMergedResponse_whenScoreMovedAfterFusion_thenTheFusionIsNestedUnderTheFinalScore() {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.6f, 0.4f, 0.8f));

        SearchResponse merged = merger.getMergedResponse(responseWithHits(hit("1", 1.9f)));
        Explanation explanation = merged.getHits().getHits()[0].getExplanation();

        assertEquals("the top node must describe the score the hit has", FINAL_SCORE, explanation.getDescription());
        assertEquals(1.9f, explanation.getValue().floatValue(), 0.0f);
        assertEquals(1, explanation.getDetails().length);
        assertEquals("and the fusion keeps the number it actually produced", COMBINATION, explanation.getDetails()[0].getDescription());
        assertEquals(0.6f, explanation.getDetails()[0].getValue().floatValue(), 0.0f);
    }

    public void testGetMergedResponse_whenALegReturnedNoExplanation_thenItsNodeIsALeaf() {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        FusedDocExplanations collected = new FusedDocExplanations().combinationDescription(COMBINATION)
            .normalizationDescription(NORMALIZATION);
        collected.addDocument(
            FusedDocExplanations.documentKey(INDEX, "1"),
            0.4f,
            List.of(new FusedDocExplanations.LegContribution(0, 0.4f, null))
        );
        merger.consumer().accept(collected);

        Explanation explanation = merger.getMergedResponse(responseWithHits(hit("1", 0.4f))).getHits().getHits()[0].getExplanation();

        assertEquals("the normalized value is still reported", 0.4f, explanation.getDetails()[0].getValue().floatValue(), 0.0f);
        assertEquals("with nothing invented under it", 0, explanation.getDetails()[0].getDetails().length);
    }

    public void testGetMergedResponse_whenScoresAreNotTracked_thenTheFusedScoreIsReported() {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.6f, 0.4f, 0.8f));

        // A hit of a request that did not track scores carries NaN, so there is no final score to describe — the fused
        // score is the only number there is, and comparing against NaN must not nest it under one that does not exist.
        Explanation explanation = merger.getMergedResponse(responseWithHits(hit("1", Float.NaN))).getHits().getHits()[0].getExplanation();

        assertEquals(COMBINATION, explanation.getDescription());
        assertEquals(0.6f, explanation.getValue().floatValue(), 0.0f);
    }

    public void testGetMergedResponse_whenTheResponseCarriesNoHitsArray_thenItIsReturnedUntouched() {
        // A response section with no hits array at all — the shape a request that asked for nothing back leaves behind.
        // There is nothing to correlate against, and reaching for the array would fail rather than report anything.
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.6f, 0.4f, 0.8f));
        SearchHits noHits = new SearchHits(null, new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN);
        InternalSearchResponse internal = new InternalSearchResponse(noHits, InternalAggregations.EMPTY, null, null, false, null, 1);
        SearchResponse response = new SearchResponse(
            internal,
            null,
            1,
            1,
            0,
            1L,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );

        assertFalse("something was collected, so the guard is the array and not the collection", merger.isEmpty());
        assertSame(response, merger.getMergedResponse(response));
    }

    public void testGetMergedResponse_whenAHitCannotBeCorrelated_thenItIsSkippedAndTheRestAreStillAttached() {
        // Correlation is by _index + _id, so a hit missing either cannot be looked up. It is skipped rather than keyed on
        // what is left, which would collide with a real document of another index that happens to share the _id.
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.6f, 0.4f, 0.8f));
        SearchHit indexless = new SearchHit(0, "1", null, null);
        indexless.score(0.6f);
        SearchHit idless = new SearchHit(0, null, null, null);
        idless.shard(new SearchShardTarget("node", new ShardId(INDEX, INDEX + "-uuid", 0), null, OriginalIndices.NONE));
        idless.score(0.6f);

        SearchResponse merged = merger.getMergedResponse(responseWithHits(indexless, idless, hit("1", 0.6f)));

        assertNull("a hit with no _index is left exactly as it came back", merged.getHits().getHits()[0].getExplanation());
        assertNull("and so is one with no _id", merged.getHits().getHits()[1].getExplanation());
        assertEquals(
            "and skipping them does not stop the hits that can be correlated",
            COMBINATION,
            merged.getHits().getHits()[2].getExplanation().getDescription()
        );
    }

    public void testDocumentKey_thenDistinctDocumentsGetDistinctKeys() {
        // The key is never parsed back, so it only has to separate documents and be built the same way in the rewrite and
        // on the response. An _id may contain the separator; an index name may not (OpenSearch rejects '#' in one), so the
        // pair cannot be re-split ambiguously into a different (index, id) that is also a real document.
        assertEquals(FusedDocExplanations.documentKey(INDEX, "a#b"), FusedDocExplanations.documentKey(INDEX, "a#b"));
        assertNotEquals(FusedDocExplanations.documentKey(INDEX, "1"), FusedDocExplanations.documentKey(INDEX, "2"));
        assertNotEquals(FusedDocExplanations.documentKey(INDEX, "1"), FusedDocExplanations.documentKey("other-index", "1"));
    }

    /** One document, two legs, with a raw explanation under each. */
    private FusedDocExplanations collected(final String id, final float fusedScore, final float... normalizedScores) {
        FusedDocExplanations collected = new FusedDocExplanations().combinationDescription(COMBINATION)
            .normalizationDescription(NORMALIZATION);
        List<FusedDocExplanations.LegContribution> contributions = new ArrayList<>();
        for (int leg = 0; leg < normalizedScores.length; leg++) {
            contributions.add(
                new FusedDocExplanations.LegContribution(
                    leg,
                    normalizedScores[leg],
                    Explanation.match(normalizedScores[leg], "leg " + leg + " raw")
                )
            );
        }
        collected.addDocument(FusedDocExplanations.documentKey(INDEX, id), fusedScore, contributions);
        return collected;
    }

    /** A response hit as the fetch phase leaves it: the shard target is what gives it an {@code _index}. */
    private SearchHit hit(final String id, final float score) {
        SearchHit hit = new SearchHit(0, id, null, null);
        hit.shard(new SearchShardTarget("node", new ShardId(INDEX, INDEX + "-uuid", 0), null, OriginalIndices.NONE));
        hit.score(score);
        return hit;
    }

    private SearchResponse responseWithHits(final SearchHit... hits) {
        SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), Float.NaN);
        InternalSearchResponse internal = new InternalSearchResponse(searchHits, InternalAggregations.EMPTY, null, null, false, null, 1);
        return new SearchResponse(internal, null, 1, 1, 0, 1L, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }
}
