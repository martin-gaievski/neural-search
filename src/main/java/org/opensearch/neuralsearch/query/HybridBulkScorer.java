/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.Data;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CheckedIntConsumer;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class HybridBulkScorer extends BulkScorer {
    // private final DocIdSetIterator iterator;
    private final HybridQueryScorer scorers;// Size of the block to process in bulk
    final long cost;
    private final int maxDoc;
    List<ScorerSupplier> scorerSuppliers;
    // List<BulkScorer> bulkScorers;
    HybridCombinedSubQueryScorer hybridCombinedSubQueryScorer = new HybridCombinedSubQueryScorer();

    Map<Integer, float[]> scoresByDoc = new HashMap<>();

    static final class HeadPriorityQueue extends PriorityQueue<DisiWrapper> {

        public HeadPriorityQueue(int maxSize) {
            super(maxSize);
        }

        @Override
        protected boolean lessThan(DisiWrapper a, DisiWrapper b) {
            return a.doc < b.doc;
        }
    }

    static final class TailPriorityQueue extends PriorityQueue<DisiWrapper> {

        public TailPriorityQueue(int maxSize) {
            super(maxSize);
        }

        @Override
        protected boolean lessThan(DisiWrapper a, DisiWrapper b) {
            return a.cost < b.cost;
        }

        public DisiWrapper get(int i) {
            Objects.checkIndex(i, size());
            return (DisiWrapper) getHeapArray()[1 + i];
        }
    }

    public HybridBulkScorer(HybridQueryScorer scorers, int maxDoc, List<ScorerSupplier> scorerSuppliers, List<BulkScorer> bulkScorers) {
        // this.iterator = iterator;
        this.scorers = scorers;
        long cost = 0;
        for (Scorer scorer : scorers.getSubScorers()) {
            if (Objects.isNull(scorer)) {
                continue;
            }
            cost += scorer.iterator().cost();
        }
        this.cost = cost;
        // this.cost = cost(bulkScorers);
        this.maxDoc = maxDoc;
        this.scorerSuppliers = scorerSuppliers;
    }

    private static long cost(Collection<BulkScorer> scorers) {
        final PriorityQueue<BulkScorer> pq = new PriorityQueue<BulkScorer>(scorers.size()) {
            @Override
            protected boolean lessThan(BulkScorer a, BulkScorer b) {
                return a.cost() > b.cost();
            }
        };
        for (BulkScorer scorer : scorers) {
            pq.insertWithOverflow(scorer);
        }
        long cost = 0;
        for (BulkScorer scorer = pq.pop(); scorer != null; scorer = pq.pop()) {
            cost += scorer.cost();
        }
        return cost;
    }

    /*public HybridBulkScorer(HybridQueryScorer scorers, int maxDoc) {
        this(scorers, maxDoc, null, null);
    }*/

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
        // Map<Integer, float[]> scoresByDoc = new HashMap<>();
        scoresByDoc.clear();

        /*for (int i = 0 ; i < leads.size(); i++) {
            DisiWrapper wrapper = leads.get(i);
            DocIdSetIterator it = wrapper.iterator;
            Scorable scorer = wrapper.scorable;
            if (scorer == null) {
                continue;
            }

            int doc = wrapper.doc;
            if (doc < min) {
                doc = it.advance(min);
            }
            for (; doc < max; doc = it.nextDoc()) {
                if (acceptDocs == null || acceptDocs.get(doc)) {
                    float score = scorer.score();
                }
            }
        }*/
        // scoreAndDoc.doc = -1;
        // collector.setScorer(scoreAndDoc);
        // if (collector instanceof HybridTopScoreDocCollector.HybridTopScoreLeafCollector) {
        /*if (true) {
            if (Objects.nonNull(scorerSuppliers)) {
                for (ScorerSupplier scorerSupplier : scorerSuppliers) {
                    if (Objects.nonNull(scorerSupplier)) {
                        BulkScorer bulkScorer = scorerSupplier.bulkScorer();
                        bulkScorer.score(collector, acceptDocs, min, max);
                    }
                }
            }
            return DocIdSetIterator.NO_MORE_DOCS;
        }*/
        /*for (int i = 0; i < bulkScorers.size(); i++) {
            BulkScorer bulkScorer = bulkScorers.get(i);
            if (bulkScorer == null) {
                continue;
            }
            SubQueryScorer subQueryScorer = new SubQueryScorer( i, bulkScorers.size());
            collector.setScorer(subQueryScorer);
            collector.collect();
        }*/

        List<Scorer> scorers = new ArrayList<>(this.scorers.getSubScorers());
        for (int i = 0; i < scorers.size(); i++) {
            Scorer scorer = scorers.get(i);
            if (scorer == null) {
                continue;
            }
            // SubQueryScorer subQueryScorer = new SubQueryScorer(scorer, i, scorers.size());

            // collector.setScorer(subQueryScorer);
            /*TwoPhaseIterator twoPhase = scorer.twoPhaseIterator();
            // TwoPhaseIterator twoPhase = this.scorers.twoPhaseIterator();
            DocIdSetIterator scorerIterator = twoPhase == null ? scorer.iterator() : twoPhase.approximation();
            DocIdSetIterator competitiveIterator = collector.competitiveIterator();
            // scoreAll
            if (competitiveIterator == null && scorerIterator.docID() == -1 && min == 0 && max == DocIdSetIterator.NO_MORE_DOCS) {
                if (twoPhase == null) {
                    for (int doc = scorerIterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = scorerIterator.nextDoc()) {
                        if (acceptDocs == null || acceptDocs.get(doc)) {
                            // collector.collect(doc);
                            collectScoreOnly(scorer, doc, scorers, i);
                        }
                    }
                } else {
                    // The scorer has an approximation, so run the approximation first, then check acceptDocs,
                    // then confirm
                    for (int doc = scorerIterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = scorerIterator.nextDoc()) {
                        if ((acceptDocs == null || acceptDocs.get(doc)) && twoPhase.matches()) {
                            // collector.collect(doc);
                            collectScoreOnly(scorer, doc, scorers, i);
                        }
                    }
                }
                // return DocIdSetIterator.NO_MORE_DOCS;
            } else {
                // scoreRange
                if (competitiveIterator != null) {
                    if (competitiveIterator.docID() > min) {
                        min = competitiveIterator.docID();
                        // The competitive iterator may not match any docs in the range.
                        min = Math.min(min, max);
                    }
                }

                int doc = scorerIterator.docID();
                if (doc < min) {
                    if (doc == min - 1) {
                        doc = scorerIterator.nextDoc();
                    } else {
                        doc = scorerIterator.advance(min);
                    }
                }

                if (twoPhase == null && competitiveIterator == null) {
                    while (doc < max) {
                        if (acceptDocs == null || acceptDocs.get(doc)) {
                            // collector.collect(doc);
                            collectScoreOnly(scorer, doc, scorers, i);
                        }
                        doc = scorerIterator.nextDoc();
                    }
                } else {
                    while (doc < max) {
                        if (competitiveIterator != null) {
                            assert competitiveIterator.docID() <= doc;
                            if (competitiveIterator.docID() < doc) {
                                competitiveIterator.advance(doc);
                            }
                            if (competitiveIterator.docID() != doc) {
                                doc = scorerIterator.advance(competitiveIterator.docID());
                                continue;
                            }
                        }

                        if ((acceptDocs == null || acceptDocs.get(doc)) && (twoPhase == null || twoPhase.matches())) {
                            // collector.collect(doc);
                            collectScoreOnly(scorer, doc, scorers, i);
                        }
                        doc = scorerIterator.nextDoc();
                    }
                }
            }*/
            DocIdSetIterator it = scorer.iterator();
            int doc = -1;
            if (doc < min) {
                doc = it.advance(min);
            }
            for (; doc < max; doc = it.nextDoc()) {
                if (acceptDocs == null || acceptDocs.get(doc)) {
                    scoresByDoc.computeIfAbsent(doc, k -> new float[scorers.size()])[i] = scorer.score();
                }
            }
        }

        hybridCombinedSubQueryScorer.setScoresByDoc(scoresByDoc);
        hybridCombinedSubQueryScorer.setNumOfSubQueries(scorers.size());
        collector.setScorer(hybridCombinedSubQueryScorer);
        DocIdStreamView docIdStreamView = new DocIdStreamView();
        collector.collect(docIdStreamView);

        return DocIdSetIterator.NO_MORE_DOCS;
    }

    @Override
    public long cost() {
        return cost;
    }

    class DocIdStreamView extends DocIdStream {

        @Override
        public void forEach(CheckedIntConsumer<IOException> consumer) throws IOException {
            for (int doc : scoresByDoc.keySet()) {
                float[] scores = scoresByDoc.get(doc);
                float combinedScore = 0.0f;
                for (float score : scores) {
                    if (score > 0) {
                        combinedScore += score;
                    }
                }
                hybridCombinedSubQueryScorer.setScore(combinedScore);
                consumer.accept(doc);
            }
        }

        @Override
        public int count() throws IOException {
            return scoresByDoc.size();
        }
    }

    @Data
    public static class HybridCombinedSubQueryScorer extends Scorable {
        Map<Integer, float[]> scoresByDoc;
        int numOfSubQueries;
        float score;

        @Override
        public float score() throws IOException {
            return score;
        }
    }
}
