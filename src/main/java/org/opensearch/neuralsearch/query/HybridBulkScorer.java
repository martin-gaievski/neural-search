/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.Data;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CheckedIntConsumer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class HybridBulkScorer extends BulkScorer {
    final long cost;
    final Scorer[] disiWrappers;
    HybridCombinedSubQueryScorer hybridCombinedSubQueryScorer;

    static final int SHIFT = 10;
    static final int SIZE = 1 << SHIFT;
    static final int MASK = SIZE - 1;

    private boolean needsScores;
    FixedBitSet matching;
    float[][] windowScores;
    DocIdStreamView docIdStreamView = new DocIdStreamView();
    private final int[] docIds;

    HybridBulkScorer(List<Scorer> scorers, boolean needsScores) {
        int numberOfSubQueries = scorers.size();
        long cost = 0;
        int i = 0;
        this.disiWrappers = new Scorer[numberOfSubQueries];
        for (Scorer scorer : scorers) {
            if (Objects.isNull(scorer)) {
                i++;
                continue;
            }
            cost += scorer.iterator().cost();
            disiWrappers[i++] = scorer;
        }
        this.cost = cost;
        hybridCombinedSubQueryScorer = new HybridCombinedSubQueryScorer(numberOfSubQueries);
        this.needsScores = needsScores;
        matching = new FixedBitSet(SIZE);
        windowScores = new float[numberOfSubQueries][SIZE];
        docIds = new int[numberOfSubQueries];
    }

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
        collector.setScorer(hybridCombinedSubQueryScorer);

        advance(min, disiWrappers);
        int nextDoc = -1;
        while (allDocIdsUsed(docIds, max) == false) {
            int topDoc = -1;
            for (int docsId : docIds) {
                if (docsId < max) {
                    topDoc = docsId;
                    break;
                }
            }

            final int windowBase = topDoc & ~MASK; // take the next match (at random) and find the window where it belongs
            final int windowMin = Math.max(min, windowBase);
            final int windowMax = Math.min(max, windowBase + SIZE);

            for (int i = 0; i < disiWrappers.length; i++) {
                if (disiWrappers[i] == null || docIds[i] >= max) {
                    continue;
                }
                DocIdSetIterator it = disiWrappers[i].iterator();
                int doc = docIds[i];
                if (doc < windowMin) {
                    doc = it.advance(windowMin);
                }
                for (; doc < windowMax; doc = it.nextDoc()) {
                    if (acceptDocs == null || acceptDocs.get(doc)) {
                        // Atomic operation to ensure thread-safe array creation and update
                        int d = doc & MASK;
                        matching.set(d);
                        if (needsScores) {
                            float score = disiWrappers[i].score();
                            if (score >= 0) {
                                windowScores[i][d] = score;
                            }
                        }
                    }
                }
                docIds[i] = doc;
            }

            docIdStreamView.base = windowBase;
            collector.collect(docIdStreamView);

            matching.clear();

            for (float[] windowScore : windowScores) {
                Arrays.fill(windowScore, 0);
            }
        }
        for (int doc : docIds) {
            if (doc != DocIdSetIterator.NO_MORE_DOCS) {
                nextDoc = Math.max(nextDoc, doc);
            }
        }
        return nextDoc == -1 ? DocIdSetIterator.NO_MORE_DOCS : nextDoc;
    }

    private int[] advance(int min, Scorer[] scorers) throws IOException {
        for (int i = 0; i < scorers.length; i++) {
            if (scorers[i] == null) {
                docIds[i] = DocIdSetIterator.NO_MORE_DOCS;
                continue;
            }
            DocIdSetIterator it = scorers[i].iterator();
            int doc = it.docID();
            if (doc < min) {
                doc = it.advance(min);
            }
            docIds[i] = doc;
        }
        return docIds;
    }

    private boolean allDocIdsUsed(int[] docsIds, int max) {
        for (int docId : docsIds) {
            if (docId < max) {
                return false;
            }
        }
        return true;
    }

    @Override
    public long cost() {
        return cost;
    }

    class DocIdStreamView extends DocIdStream {
        int base;

        @Override
        public void forEach(CheckedIntConsumer<IOException> consumer) throws IOException {
            FixedBitSet matchingBitSet = matching;
            long[] bitArray = matchingBitSet.getBits();
            for (int idx = 0; idx < bitArray.length; idx++) {
                long bits = bitArray[idx];
                while (bits != 0L) {
                    int ntz = Long.numberOfTrailingZeros(bits);
                    final int indexInWindow = (idx << 6) | ntz;
                    for (int i = 0; i < windowScores.length; i++) {
                        if (Objects.isNull(windowScores[i])) {
                            continue;
                        }
                        float score = windowScores[i][indexInWindow];
                        hybridCombinedSubQueryScorer.getScoresByDoc()[i] = score;
                    }
                    consumer.accept(base | indexInWindow);
                    hybridCombinedSubQueryScorer.resetScores();
                    bits ^= 1L << ntz;
                }
            }
        }

        @Override
        public int count() throws IOException {
            return super.count();
        }
    }

    @Data
    public static class HybridCombinedSubQueryScorer extends Scorable {
        float[] scoresByDoc;
        int numOfSubQueries;
        float score;
        // float[] minScores;

        HybridCombinedSubQueryScorer(int numOfSubQueries) {
            this.numOfSubQueries = numOfSubQueries;
            // this.minScores = new float[numOfSubQueries];
            scoresByDoc = new float[numOfSubQueries];
        }

        @Override
        public float score() throws IOException {
            float score = 0.0f;
            for (float scoreByDoc : scoresByDoc) {
                score += scoreByDoc;
            }
            return score;
        }

        public void resetScores() {
            scoresByDoc = new float[numOfSubQueries];
        }
    }
}
