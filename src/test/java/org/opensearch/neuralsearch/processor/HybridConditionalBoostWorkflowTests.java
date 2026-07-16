/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.List;
import java.util.Map;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.neuralsearch.processor.combination.ScoreCombiner;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizer;
import org.opensearch.test.OpenSearchTestCase;

/**
 * POC: verifies the coordinator half of condition-based result boost — the {@code applyConditionalBoost} band
 * rewrite that promotes tiered documents by encoding a globally-dominant band into {@link ScoreDoc#score}.
 * The tier map is injected directly so the test needs no shard-&gt;coordinator transport and no normalization.
 */
public class HybridConditionalBoostWorkflowTests extends OpenSearchTestCase {

    private static final SearchShard SHARD = new SearchShard("promo-index", 0, "node-1");

    /**
     * Combined (post-fusion) scores, organic order:
     *   docA=0.95 (organic, HIGHEST relevance), docB=0.20 (tier 0), docC=0.50 (tier 1), docD=0.80 (organic),
     *   docE=0.30 (tier 0)
     * Expected final order after boost:
     *   tier 0 band: docE(0.30), docB(0.20)  [ordered by combined score within band]
     *   tier 1 band: docC(0.50)
     *   organic:     docA(0.95), docD(0.80)
     * The load-bearing assertion: the low-relevance tier-0 doc (docB, 0.20) outranks the high-relevance organic
     * doc (docA, 0.95).
     */
    public void testApplyConditionalBoost_whenLowRelevancePromoDoc_thenOutranksHighRelevanceOrganic() {
        NormalizationProcessorWorkflow workflow = new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner());

        int docA = 10, docB = 11, docC = 12, docD = 13, docE = 14;
        ScoreDoc[] combined = new ScoreDoc[] {
            new ScoreDoc(docA, 0.95f),
            new ScoreDoc(docD, 0.80f),
            new ScoreDoc(docC, 0.50f),
            new ScoreDoc(docE, 0.30f),
            new ScoreDoc(docB, 0.20f) };

        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(5, TotalHits.Relation.EQUAL_TO),
            List.of(new TopDocs(new TotalHits(5, TotalHits.Relation.EQUAL_TO), combined)),
            false,
            SHARD
        );
        compoundTopDocs.setScoreDocs(new java.util.ArrayList<>(List.of(combined)));

        // tier 0 = {docB, docE}, tier 1 = {docC}; 2 conditions
        Map<Integer, Integer> docIdToTier = Map.of(docB, 0, docE, 0, docC, 1);
        Map<SearchShard, HybridBoostTierRegistry.ShardTiers> tiers = Map.of(SHARD, new HybridBoostTierRegistry.ShardTiers(docIdToTier, 2));

        workflow.applyConditionalBoost(List.of(compoundTopDocs), tiers, 2);

        List<ScoreDoc> result = compoundTopDocs.getScoreDocs();
        List<Integer> order = result.stream().map(sd -> sd.doc).toList();

        assertEquals("expected tier0(docE,docB) > tier1(docC) > organic(docA,docD)", List.of(docE, docB, docC, docA, docD), order);

        // The whole point: a low-relevance promoted doc beats a high-relevance organic doc.
        assertTrue("tier-0 docB must outrank organic docA", result.indexOf(find(result, docB)) < result.indexOf(find(result, docA)));
        // Bands are strictly separated: every tier-0 score > tier-1 score > organic score.
        assertTrue(score(result, docE) > score(result, docC));
        assertTrue(score(result, docB) > score(result, docC));
        assertTrue(score(result, docC) > score(result, docA));
    }

    /**
     * An empty tier map leaves scores and order untouched (feature no-op).
     */
    public void testApplyConditionalBoost_whenNoTiers_thenNoChange() {
        NormalizationProcessorWorkflow workflow = new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner());
        ScoreDoc[] combined = new ScoreDoc[] { new ScoreDoc(1, 0.9f), new ScoreDoc(2, 0.5f) };
        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            List.of(new TopDocs(new TotalHits(2, TotalHits.Relation.EQUAL_TO), combined)),
            false,
            SHARD
        );
        compoundTopDocs.setScoreDocs(new java.util.ArrayList<>(List.of(combined)));

        workflow.applyConditionalBoost(List.of(compoundTopDocs), Map.of(), 0);

        List<Integer> order = compoundTopDocs.getScoreDocs().stream().map(sd -> sd.doc).toList();
        assertEquals(List.of(1, 2), order);
        assertEquals(0.9f, compoundTopDocs.getScoreDocs().get(0).score, 0.0001f);
    }

    private static ScoreDoc find(List<ScoreDoc> docs, int docId) {
        return docs.stream().filter(sd -> sd.doc == docId).findFirst().orElseThrow();
    }

    private static float score(List<ScoreDoc> docs, int docId) {
        return find(docs, docId).score;
    }
}
