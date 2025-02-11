/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.Data;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class HybridBulkScorer extends BulkScorer {
    // private final DocIdSetIterator iterator;
    private final HybridQueryWeight.HybridScorerSupplier scorers;
    private static final int BULK_SIZE = 128; // Size of the block to process in bulk
    final long cost;

    public HybridBulkScorer(HybridQueryWeight.HybridScorerSupplier scorers) {
        // this.iterator = iterator;
        this.scorers = scorers;
        long cost = 0;
        for (ScorerSupplier scorer : scorers.getScorerSuppliers()) {
            if (Objects.isNull(scorer)) {
                continue;
            }
            cost += scorer.cost();
        }
        this.cost = cost;
    }

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
        int doc = min;
        List<Scorer> scorers = new ArrayList<>();
        for (ScorerSupplier scorerSupplier : this.scorers.getScorerSuppliers()) {
            if (Objects.isNull(scorerSupplier)) {
                scorers.add(null);
            } else {
                scorers.add(scorerSupplier.get(Integer.MAX_VALUE));
            }
        }
        for (int i = 0; i < scorers.size(); i++) {
            Scorer scorer = scorers.get(i);
            if (scorer == null) {
                continue;
            }

            SubQueryScorer subQueryScorer = new SubQueryScorer(scorer, i, scorers.size());
            collector.setScorer(subQueryScorer);
            /*TwoPhaseIterator twoPhase = scorer.twoPhaseIterator();
            DocIdSetIterator docIdSetIterator = scorer.iterator();
            DocIdSetIterator scorerIterator = twoPhase == null ? docIdSetIterator : twoPhase.approximation();
            DocIdSetIterator competitiveIterator = collector.competitiveIterator();
            // if (competitiveIterator == null && scorerIterator.docID() == -1) {
            if (twoPhase == null) {
                for (int doc1 = scorerIterator.nextDoc(); doc1 != DocIdSetIterator.NO_MORE_DOCS; doc1 = scorerIterator.nextDoc()) {
                    if (acceptDocs == null || acceptDocs.get(doc1)) {
                        collector.collect(doc1);
                    }
                }
            } else {
                // The scorer has an approximation, so run the approximation first, then check acceptDocs,
                // then confirm
                for (int doc1 = scorerIterator.nextDoc(); doc1 != DocIdSetIterator.NO_MORE_DOCS; doc1 = scorerIterator.nextDoc()) {
                    if ((acceptDocs == null || acceptDocs.get(doc1)) && twoPhase.matches()) {
                        collector.collect(doc1);
                    }
                }
            }*/
            // }
            DocIdSetIterator docIdSetIterator = scorer.iterator();
            docIdSetIterator.advance(min);
            for (doc = docIdSetIterator.docID(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docIdSetIterator.nextDoc()) {
                if (acceptDocs == null || acceptDocs.get(doc)) {
                    collector.collect(doc);
                }
                // collector.collect(doc);
            }

            /*for (doc = docIdSetIterator.docID(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docIdSetIterator.nextDoc()) {
                if (acceptDocs == null || acceptDocs.get(doc)) {
                    collector.collect(doc);
                }
                // collector.collect(doc);
            }*/
        }
        return DocIdSetIterator.NO_MORE_DOCS;
    }

    @Override
    public long cost() {
        return cost;
    }

    @Data
    public static class SubQueryScorer extends Scorable {
        final private Scorer scorer;
        final private int index;
        final private int numOfSubQueries;

        @Override
        public float score() throws IOException {
            return scorer.score();
        }

        @Override
        public int docID() {
            return scorer.docID();
        }
    }
}
