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
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class HybridBulkScorer extends BulkScorer {
    // private final DocIdSetIterator iterator;
    private final HybridQueryScorer scorers;// Size of the block to process in bulk
    final long cost;
    private final int maxDoc;

    public HybridBulkScorer(HybridQueryScorer scorers, int maxDoc) {
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
        this.maxDoc = maxDoc;
    }

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
        // int doc = min;
        List<Scorer> scorers = new ArrayList<>(this.scorers.getSubScorers());
        for (int i = 0; i < scorers.size(); i++) {
            Scorer scorer = scorers.get(i);
            if (scorer == null) {
                continue;
            }
            SubQueryScorer subQueryScorer = new SubQueryScorer(scorer, i, scorers.size());
            collector.setScorer(subQueryScorer);
            TwoPhaseIterator twoPhase = scorer.twoPhaseIterator();
            DocIdSetIterator scorerIterator = twoPhase == null ? scorer.iterator() : twoPhase.approximation();
            DocIdSetIterator competitiveIterator = collector.competitiveIterator();
            // scoreAll
            if (competitiveIterator == null && scorerIterator.docID() == -1 && min == 0 && max == DocIdSetIterator.NO_MORE_DOCS) {
                if (twoPhase == null) {
                    for (int doc = scorerIterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = scorerIterator.nextDoc()) {
                        if (acceptDocs == null || acceptDocs.get(doc)) {
                            collector.collect(doc);
                        }
                    }
                } else {
                    // The scorer has an approximation, so run the approximation first, then check acceptDocs,
                    // then confirm
                    for (int doc = scorerIterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = scorerIterator.nextDoc()) {
                        if ((acceptDocs == null || acceptDocs.get(doc)) && twoPhase.matches()) {
                            collector.collect(doc);
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
                    // Optimize simple iterators with collectors that can't skip
                    while (doc < max) {
                        if (acceptDocs == null || acceptDocs.get(doc)) {
                            collector.collect(doc);
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
                            collector.collect(doc);
                        }
                        doc = scorerIterator.nextDoc();
                    }
                }
            }
            //
            /*if (competitiveIterator != null) {
                if (competitiveIterator.docID() > min) {
                    min = competitiveIterator.docID();
                    // The competitive iterator may not match any docs in the range.
                    min = Math.min(min, max);
                }
            }

            scorerIterator.advance(min);
            int doc = scorerIterator.docID();
            if (doc < min) {
                if (doc == min - 1) {
                    doc = scorerIterator.nextDoc();
                } else {
                    doc = scorerIterator.advance(min);
                }
            }
            if (twoPhase == null) {
                // Optimize simple iterators with collectors that can't skip
                while (doc < max) {
                    if (acceptDocs == null || acceptDocs.get(doc)) {
                        collector.collect(doc);
                    }
                    doc = scorerIterator.nextDoc();
                }
            } else {
                while (doc < max) {
                    /*if (competitiveIterator != null) {
                        assert competitiveIterator.docID() <= doc;
                        if (competitiveIterator.docID() < doc) {
                            competitiveIterator.advance(doc);
                        }
                        if (competitiveIterator.docID() != doc) {
                            doc = iterator.advance(competitiveIterator.docID());
                            continue;
                        }
                    }*/
            /*if ((acceptDocs == null || acceptDocs.get(doc)) && (twoPhase == null || twoPhase.matches())) {
                collector.collect(doc);
            }
            doc = scorerIterator.nextDoc();
            }
            }
            */

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
    }
}
