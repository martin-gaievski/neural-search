/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.Bits;

import lombok.extern.log4j.Log4j2;

/**
 * BulkScorer that performs shard-level RRF (Reciprocal Rank Fusion) scoring.
 *
 * Two-pass approach for accurate segment-global ranking:
 * Pass 1: Iterate all sub-query scorers, collect (docId, rawScore) per sub-query
 * Pass 2: For each sub-query, sort all collected docs by score descending,
 *          assign global ranks 1..N, compute RRF: sum(1/(k + rank_i))
 * Pass 3: Feed collector with combined RRF scores in doc-id order
 *
 * This ensures RRF ranks are computed across ALL matching docs in the segment,
 * not fragmented into arbitrary windows, achieving parity with coordinator-level RRF.
 *
 * The RRF rank constant k is hardcoded to 60 for this POC.
 */
@Log4j2
public class FusionBulkScorer extends BulkScorer {

    private static final int RRF_K = 60;

    private final Scorer[] scorers;
    private final int numSubQueries;
    private final boolean needsScores;
    private final int maxDoc;
    private final long cost;
    private final int[] docIds; // last doc id per scorer

    // Scorable that returns the current RRF score for collector
    private final FusionScorable fusionScorable;

    public FusionBulkScorer(List<Scorer> scorers, boolean needsScores, int maxDoc) {
        this.numSubQueries = scorers.size();
        this.scorers = new Scorer[numSubQueries];
        long totalCost = 0;
        for (int i = 0; i < numSubQueries; i++) {
            Scorer scorer = scorers.get(i);
            if (scorer != null) {
                this.scorers[i] = scorer;
                totalCost += scorer.iterator().cost();
            }
        }
        this.cost = totalCost;
        this.needsScores = needsScores;
        this.maxDoc = maxDoc;
        this.docIds = new int[numSubQueries];
        Arrays.fill(docIds, DocIdSetIterator.NO_MORE_DOCS);
        this.fusionScorable = new FusionScorable();
    }

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
        collector.setScorer(fusionScorable);
        max = Math.min(max, maxDoc);

        // ========== PASS 1: Collect all (docId, rawScore) per sub-query ==========
        // For each sub-query, collect all matching docs with their raw scores
        @SuppressWarnings("unchecked")
        List<long[]>[] subQueryDocs = new List[numSubQueries]; // long = pack(docId, floatBits)
        for (int i = 0; i < numSubQueries; i++) {
            subQueryDocs[i] = new ArrayList<>();
        }

        for (int subIdx = 0; subIdx < numSubQueries; subIdx++) {
            if (scorers[subIdx] == null) continue;

            DocIdSetIterator it = scorers[subIdx].iterator();
            int doc = it.docID();
            if (doc < min) {
                doc = it.advance(min);
            }

            while (doc < max) {
                if (acceptDocs == null || acceptDocs.get(doc)) {
                    float score = needsScores ? scorers[subIdx].score() : 1.0f;
                    // Pack docId and score bits into a long for memory efficiency
                    long packed = ((long) doc << 32) | (Float.floatToRawIntBits(score) & 0xFFFFFFFFL);
                    subQueryDocs[subIdx].add(new long[] { packed });
                }
                doc = it.nextDoc();
            }
            docIds[subIdx] = doc;
        }

        // ========== PASS 2: Compute segment-global RRF scores ==========
        // For each sub-query, sort ALL docs by raw score descending, assign global ranks
        // TreeMap keeps docs in docId order for final output
        TreeMap<Integer, Float> rrfScores = new TreeMap<>();

        for (int subIdx = 0; subIdx < numSubQueries; subIdx++) {
            List<long[]> docs = subQueryDocs[subIdx];
            if (docs.isEmpty()) continue;

            // Sort by raw score descending (higher score = lower rank = better)
            docs.sort((a, b) -> {
                float sa = Float.intBitsToFloat((int) (a[0] & 0xFFFFFFFFL));
                float sb = Float.intBitsToFloat((int) (b[0] & 0xFFFFFFFFL));
                return Float.compare(sb, sa);
            });

            // Assign ranks 1..N globally and accumulate RRF contribution
            for (int rank = 0; rank < docs.size(); rank++) {
                int docId = (int) (docs.get(rank)[0] >>> 32);
                float rrfContribution = 1.0f / (RRF_K + rank + 1); // rank+1 for 1-based
                rrfScores.merge(docId, rrfContribution, Float::sum);
            }
        }

        // ========== PASS 3: Feed collector in docId order ==========
        for (Map.Entry<Integer, Float> entry : rrfScores.entrySet()) {
            fusionScorable.currentScore = entry.getValue();
            collector.collect(entry.getKey());
        }

        // Return next candidate (smallest unexhausted docId, or NO_MORE_DOCS)
        return nextCandidate();
    }

    private int nextCandidate() {
        int min = DocIdSetIterator.NO_MORE_DOCS;
        for (int docId : docIds) {
            if (docId < min) min = docId;
        }
        return min;
    }

    @Override
    public long cost() {
        return cost;
    }

    /**
     * Simple Scorable that returns the current RRF score for the document being collected.
     */
    private static class FusionScorable extends Scorable {
        float currentScore;

        @Override
        public float score() throws IOException {
            return currentScore;
        }
    }
}
