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
import org.opensearch.neuralsearch.executors.HybridQueryExecutor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * Bulk scorer for hybrid query
 */
public class HybridBulkScorer extends BulkScorer {
    private static final int SHIFT = 12;
    private static final int WINDOW_SIZE = 1 << SHIFT;
    private static final int MASK = WINDOW_SIZE - 1;

    private final long cost;
    private final Scorer[] scorers;
    @Getter
    private final HybridSubQueryScorer hybridSubQueryScorer;
    private final boolean needsScores;
    @Getter
    private final FixedBitSet matching;
    @Getter
    private final float[][] windowScores;
    private final HybridQueryDocIdStream hybridQueryDocIdStream;
    private final int maxDoc;

    /**
     * Constructor for HybridBulkScorer
     * @param scorers list of scorers for each sub query
     * @param needsScores whether the scorer needs scores
     * @param maxDoc maximum document id
     */
    public HybridBulkScorer(List<Scorer> scorers, boolean needsScores, int maxDoc) {
        long cost = 0;
        this.scorers = new Scorer[scorers.size()];
        for (int subQueryIndex = 0; subQueryIndex < scorers.size(); subQueryIndex++) {
            Scorer scorer = scorers.get(subQueryIndex);
            if (Objects.isNull(scorer)) {
                continue;
            }
            cost += scorer.iterator().cost();
            this.scorers[subQueryIndex] = scorer;
        }
        this.cost = cost;
        this.hybridSubQueryScorer = new HybridSubQueryScorer(scorers.size());
        this.needsScores = needsScores;
        this.matching = new FixedBitSet(WINDOW_SIZE);
        this.windowScores = new float[this.scorers.length][WINDOW_SIZE];
        this.maxDoc = maxDoc;
        this.hybridQueryDocIdStream = new HybridQueryDocIdStream(this);
    }

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
        collector.setScorer(hybridSubQueryScorer);
        // making sure we are not going over the global limit defined by maxDoc
        max = Math.min(max, maxDoc);
        // advance all scorers to the segment's minimum doc id
        int[] docsIds = advance(min, scorers);
        while (allDocIdsUsed(docsIds, max) == false) {
            scoreWindow(collector, acceptDocs, min, max, docsIds);
        }
        return getNextDocIdCandidate(docsIds);
    }

    private void scoreWindow(LeafCollector collector, Bits acceptDocs, int min, int max, int[] docIds) throws IOException {
        // pick the lowest out of all not yet used doc ids
        int topDoc = -1;
        for (int docId : docIds) {
            if (docId < max) {
                topDoc = docId;
                break;
            }
        }

        final int windowBase = topDoc & ~MASK; // take the next match (at random) and find the window where it belongs
        final int windowMin = Math.max(min, windowBase);
        final int windowMax = Math.min(max, windowBase + WINDOW_SIZE);
        // collect doc ids and scores for this window using leaf collector
        scoreWindowIntoBitSetWithSubqueryScorers(collector, acceptDocs, max, docIds, windowMin, windowMax, windowBase);
    }

    /**
     * Collect scores for the window using segment level leaf collector
     * @param collector leaf collector for the segment
     * @param acceptDocs bitset with live docs
     * @param max max doc id
     * @param docIds last used doc ids per scorer
     * @param windowMin min doc id of this collector window
     * @param windowMax max doc id of this collector window
     * @param windowBase offset for this collector window
     * @throws IOException
     */
    private void scoreWindowIntoBitSetWithSubqueryScorers(
        LeafCollector collector,
        Bits acceptDocs,
        int max,
        int[] docIds,
        int windowMin,
        int windowMax,
        int windowBase
    ) throws IOException {
        List<Callable<Void>> tasks = new ArrayList<>();

        for (int subQueryIndex = 0; subQueryIndex < scorers.length; subQueryIndex++) {
            if (Objects.isNull(scorers[subQueryIndex]) || docIds[subQueryIndex] >= max) {
                continue;
            }
            final int currentIndex = subQueryIndex;
            tasks.add(() -> {
                try {
                    DocIdSetIterator it = scorers[currentIndex].iterator();
                    int doc = docIds[currentIndex];
                    if (doc < windowMin) {
                        doc = it.advance(windowMin);
                    }
                    while (doc < windowMax) {
                        if (Objects.isNull(acceptDocs) || acceptDocs.get(doc)) {
                            int d = doc & MASK;
                            if (needsScores) {
                                float score = scorers[currentIndex].score();
                                // collect score only in case it's gt competitive score
                                if (score > hybridSubQueryScorer.getMinScores()[currentIndex]) {
                                    synchronized (matching) {
                                        matching.set(d);
                                    }
                                    windowScores[currentIndex][d] = score;
                                }
                            } else {
                                synchronized (matching) {
                                    matching.set(d);
                                }
                            }
                        }
                        doc = it.nextDoc();
                    }
                    docIds[currentIndex] = doc;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return null;
            });
        }
        HybridQueryExecutor.getExecutor().invokeAll(tasks);

        hybridQueryDocIdStream.setBase(windowBase);
        collector.collect(hybridQueryDocIdStream);

        resetWindowState();
    }

    /**
     * Advance all scorers to the next document that is >= min
     */
    private int[] advance(int min, Scorer[] scorers) throws IOException {
        int[] docIds = new int[scorers.length];
        for (int subQueryIndex = 0; subQueryIndex < scorers.length; subQueryIndex++) {
            if (Objects.isNull(scorers[subQueryIndex])) {
                docIds[subQueryIndex] = DocIdSetIterator.NO_MORE_DOCS;
                continue;
            }
            DocIdSetIterator it = scorers[subQueryIndex].iterator();
            int doc = it.docID();
            if (doc < min) {
                doc = it.advance(min);
            }
            docIds[subQueryIndex] = doc;
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

    private int getNextDocIdCandidate(final int[] docsIds) {
        int nextDoc = -1;
        for (int doc : docsIds) {
            if (doc != DocIdSetIterator.NO_MORE_DOCS) {
                nextDoc = Math.max(nextDoc, doc);
            }
        }
        return nextDoc == -1 ? DocIdSetIterator.NO_MORE_DOCS : nextDoc;
    }

    /**
     * Reset the internal state for the next window of documents
     */
    private void resetWindowState() {
        matching.clear();

        for (float[] windowScore : windowScores) {
            Arrays.fill(windowScore, 0.0f);
        }
    }

    @Override
    public long cost() {
        return cost;
    }
}
