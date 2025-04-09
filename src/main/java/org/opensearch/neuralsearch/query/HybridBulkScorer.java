/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.Getter;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
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
    @Getter
    private final HybridSubQueryScorer hybridSubQueryScorer;

    static final int SHIFT = 10;
    static final int SIZE = 1 << SHIFT;
    static final int MASK = SIZE - 1;

    private final boolean needsScores;
    @Getter
    private final FixedBitSet matching;
    @Getter
    private final float[][] windowScores;
    DocIdStreamView docIdStreamView = new DocIdStreamView(this);

    private final int maxDoc;

    HybridBulkScorer(List<Scorer> scorers, boolean needsScores, int maxDoc) {
        long cost = 0;
        int i = 0;
        this.disiWrappers = new Scorer[scorers.size()];
        for (Scorer scorer : scorers) {
            if (Objects.isNull(scorer)) {
                i++;
                continue;
            }
            cost += scorer.iterator().cost();
            disiWrappers[i++] = scorer;
        }
        this.cost = cost;
        hybridSubQueryScorer = new HybridSubQueryScorer(scorers.size());
        this.needsScores = needsScores;
        matching = new FixedBitSet(SIZE);
        windowScores = new float[disiWrappers.length][SIZE];
        this.maxDoc = maxDoc;
    }

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
        collector.setScorer(hybridSubQueryScorer);

        max = Math.min(max, maxDoc);

        int[] docsIds = advance(min, disiWrappers);
        int nextDoc = -1;
        while (allDocIdsUsed(docsIds, max) == false) {
            int topDoc = -1;
            for (int docsId : docsIds) {
                if (docsId < max) {
                    topDoc = docsId;
                    break;
                }
            }

            final int windowBase = topDoc & ~MASK; // take the next match (at random) and find the window where it belongs
            final int windowMin = Math.max(min, windowBase);
            final int windowMax = Math.min(max, windowBase + SIZE);

            for (int i = 0; i < disiWrappers.length; i++) {
                if (disiWrappers[i] == null || docsIds[i] >= max) {
                    continue;
                }
                DocIdSetIterator it = disiWrappers[i].iterator();
                int doc = docsIds[i];
                if (doc < windowMin) {
                    doc = it.advance(windowMin);
                }
                for (; doc < windowMax; doc = it.nextDoc()) {
                    if (acceptDocs == null || acceptDocs.get(doc)) {
                        int d = doc & MASK;
                        if (needsScores) {
                            float score = disiWrappers[i].score();
                            if (score > hybridSubQueryScorer.getMinScores()[i]) {
                                matching.set(d);
                                windowScores[i][d] = score;
                            }
                        } else {
                            matching.set(d);
                        }
                    }
                }
                docsIds[i] = doc;
            }

            docIdStreamView.setBase(windowBase);
            collector.collect(docIdStreamView);

            matching.clear();

            for (float[] windowScore : windowScores) {
                Arrays.fill(windowScore, 0.0f);
            }

        }
        for (int doc : docsIds) {
            if (doc != DocIdSetIterator.NO_MORE_DOCS) {
                nextDoc = Math.max(nextDoc, doc);
            }
        }
        return nextDoc == -1 ? DocIdSetIterator.NO_MORE_DOCS : nextDoc;
    }

    private int[] advance(int min, Scorer[] scorers) throws IOException {
        int[] docIds = new int[scorers.length];
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

}
