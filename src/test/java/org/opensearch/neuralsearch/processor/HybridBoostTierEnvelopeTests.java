/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.Map;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.MAGIC_NUMBER_DELIMITER;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.MAGIC_NUMBER_START_STOP;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.MAGIC_NUMBER_TIER_DELIMITER;

/**
 * Verifies the multi-node transport for the conditional-boost tier: a sentinel envelope carrying a tier section
 * (as produced on the data node) is parsed back by {@link CompoundTopDocs} into a docId-&gt;tier map, WITHOUT the
 * tier rows leaking into the sub-query TopDocs that normalization/combination consume. This is the coordinator
 * side of the fix that replaces the JVM-static registry.
 */
public class HybridBoostTierEnvelopeTests extends OpenSearchTestCase {

    /**
     * Envelope shape emitted by getNewTopDocs for a 2-sub-query hybrid query with boost conditions:
     *   START_STOP
     *   DELIMITER, (d1,s), (d2,s)            <- sub-query 0
     *   DELIMITER, (d2,s), (d3,s)            <- sub-query 1
     *   TIER_DELIMITER, (d1,tier0), (d3,tier1)  <- boost tier section
     *   START_STOP
     * The re-parse must yield exactly 2 sub-query TopDocs (tier rows excluded) and a docId->tier map {d1:0, d3:1}.
     */
    public void testTierSection_whenParsedFromEnvelope_thenSplitOffAndSubQueriesClean() {
        int d1 = 5, d2 = 6, d3 = 7;
        ScoreDoc[] envelope = new ScoreDoc[] {
            new ScoreDoc(0, MAGIC_NUMBER_START_STOP),
            new ScoreDoc(0, MAGIC_NUMBER_DELIMITER),
            new ScoreDoc(d1, 0.9f),
            new ScoreDoc(d2, 0.6f),
            new ScoreDoc(0, MAGIC_NUMBER_DELIMITER),
            new ScoreDoc(d2, 0.8f),
            new ScoreDoc(d3, 0.4f),
            new ScoreDoc(0, MAGIC_NUMBER_TIER_DELIMITER),
            new ScoreDoc(d1, 0.0f),   // tier 0
            new ScoreDoc(d3, 1.0f),   // tier 1
            new ScoreDoc(0, MAGIC_NUMBER_START_STOP) };

        QuerySearchResult qsr = new QuerySearchResult();
        qsr.setSearchShardTarget(new SearchShardTarget("node-1", new ShardId(new Index("idx", "uuid"), 0), null, null));
        qsr.topDocs(
            new org.opensearch.common.lucene.search.TopDocsAndMaxScore(
                new TopDocs(new TotalHits(4, TotalHits.Relation.EQUAL_TO), envelope),
                0.9f
            ),
            new org.opensearch.search.DocValueFormat[0]
        );

        CompoundTopDocs compound = new CompoundTopDocs(qsr);

        // exactly 2 sub-queries reconstructed; tier rows must NOT appear as a third sub-query
        assertEquals(2, compound.getTopDocs().size());
        assertEquals(2, compound.getTopDocs().get(0).scoreDocs.length);
        assertEquals(2, compound.getTopDocs().get(1).scoreDocs.length);

        // tier map extracted correctly
        Map<Integer, Integer> tiers = compound.getDocIdToBoostTier();
        assertEquals(2, tiers.size());
        assertEquals(Integer.valueOf(0), tiers.get(d1));
        assertEquals(Integer.valueOf(1), tiers.get(d3));

        // ensure no tier row leaked into a sub-query series (no doc has a magic-number score)
        compound.getTopDocs().forEach(td -> {
            for (ScoreDoc sd : td.scoreDocs) {
                assertTrue(sd.score > 0.0f);
            }
        });
    }

    /**
     * When boost conditions are configured but a shard matched nothing, the data node still emits an empty tier
     * section (just the delimiter). The re-parse must produce an empty tier map and clean sub-queries.
     */
    public void testTierSection_whenEmpty_thenNoTiersAndSubQueriesClean() {
        int d1 = 5, d2 = 6;
        ScoreDoc[] envelope = new ScoreDoc[] {
            new ScoreDoc(0, MAGIC_NUMBER_START_STOP),
            new ScoreDoc(0, MAGIC_NUMBER_DELIMITER),
            new ScoreDoc(d1, 0.9f),
            new ScoreDoc(0, MAGIC_NUMBER_DELIMITER),
            new ScoreDoc(d2, 0.5f),
            new ScoreDoc(0, MAGIC_NUMBER_TIER_DELIMITER),  // empty tier section
            new ScoreDoc(0, MAGIC_NUMBER_START_STOP) };

        QuerySearchResult qsr = new QuerySearchResult();
        qsr.setSearchShardTarget(new SearchShardTarget("node-1", new ShardId(new Index("idx", "uuid"), 0), null, null));
        qsr.topDocs(
            new org.opensearch.common.lucene.search.TopDocsAndMaxScore(
                new TopDocs(new TotalHits(2, TotalHits.Relation.EQUAL_TO), envelope),
                0.9f
            ),
            new org.opensearch.search.DocValueFormat[0]
        );

        CompoundTopDocs compound = new CompoundTopDocs(qsr);

        assertEquals(2, compound.getTopDocs().size());
        assertTrue(compound.getDocIdToBoostTier().isEmpty());
    }
}
