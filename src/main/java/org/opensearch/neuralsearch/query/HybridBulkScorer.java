/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CheckedIntConsumer;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

public class HybridBulkScorer extends BulkScorer {

    static final int SHIFT = 11;
    static final int SIZE = 1 << SHIFT;
    static final int MASK = SIZE - 1;
    static final int SET_SIZE = 1 << (SHIFT - 6);
    static final int SET_MASK = SET_SIZE - 1;

    static class Bucket {
        double score;
        int freq;
    }

    private class BulkScorerAndDoc {
        final BulkScorer scorer;
        final long cost;
        int next;

        BulkScorerAndDoc(BulkScorer scorer) {
            this.scorer = scorer;
            this.cost = scorer.cost();
            this.next = -1;
        }

        void advance(int min) throws IOException {
            score(orCollector, null, min, min);
        }

        void score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
            next = scorer.score(collector, acceptDocs, min, max);
        }
    }

    // See WANDScorer for an explanation
    private static long cost(Collection<BulkScorer> scorers, int minShouldMatch) {
        final PriorityQueue<BulkScorer> pq = new PriorityQueue<BulkScorer>(scorers.size() - minShouldMatch + 1) {
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

    static final class HeadPriorityQueue extends PriorityQueue<HybridBulkScorer.BulkScorerAndDoc> {

        public HeadPriorityQueue(int maxSize) {
            super(maxSize);
        }

        @Override
        protected boolean lessThan(HybridBulkScorer.BulkScorerAndDoc a, HybridBulkScorer.BulkScorerAndDoc b) {
            return a.next < b.next;
        }
    }

    static final class TailPriorityQueue extends PriorityQueue<HybridBulkScorer.BulkScorerAndDoc> {

        public TailPriorityQueue(int maxSize) {
            super(maxSize);
        }

        @Override
        protected boolean lessThan(HybridBulkScorer.BulkScorerAndDoc a, HybridBulkScorer.BulkScorerAndDoc b) {
            return a.cost < b.cost;
        }

        public HybridBulkScorer.BulkScorerAndDoc get(int i) {
            Objects.checkIndex(i, size());
            return (HybridBulkScorer.BulkScorerAndDoc) getHeapArray()[1 + i];
        }
    }

    // One bucket per doc ID in the window, non-null if scores are needed or if frequencies need to be
    // counted
    final HybridBulkScorer.Bucket[] buckets;
    // This is basically an inlined FixedBitSet... seems to help with bound checks
    final long[] matching = new long[SET_SIZE];

    final HybridBulkScorer.BulkScorerAndDoc[] leads;
    final HybridBulkScorer.HeadPriorityQueue head;
    final HybridBulkScorer.TailPriorityQueue tail;
    final ScoreAndDoc scoreAndDoc = new ScoreAndDoc();
    final int minShouldMatch;
    final long cost;
    final boolean needsScores;

    final class OrCollector implements LeafCollector {
        Scorable scorer;

        @Override
        public void setScorer(Scorable scorer) {
            this.scorer = scorer;
        }

        @Override
        public void collect(int doc) throws IOException {
            final int i = doc & MASK;
            final int idx = i >>> 6;
            matching[idx] |= 1L << i;
            if (buckets != null) {
                final HybridBulkScorer.Bucket bucket = buckets[i];
                bucket.freq++;
                if (needsScores) {
                    bucket.score += scorer.score();
                }
            }
        }
    }

    final HybridBulkScorer.OrCollector orCollector = new HybridBulkScorer.OrCollector();

    final class DocIdStreamView extends DocIdStream {

        int base;

        @Override
        public void forEach(CheckedIntConsumer<IOException> consumer) throws IOException {
            long[] matching = HybridBulkScorer.this.matching;
            HybridBulkScorer.Bucket[] buckets = HybridBulkScorer.this.buckets;
            int base = this.base;
            for (int idx = 0; idx < matching.length; idx++) {
                long bits = matching[idx];
                while (bits != 0L) {
                    int ntz = Long.numberOfTrailingZeros(bits);
                    if (buckets != null) {
                        final int indexInWindow = (idx << 6) | ntz;
                        final HybridBulkScorer.Bucket bucket = buckets[indexInWindow];
                        if (bucket.freq >= minShouldMatch) {
                            final int doc = base | indexInWindow;
                            scoreAndDoc.doc = doc;
                            scoreAndDoc.score = (float) bucket.score;
                            consumer.accept(doc);
                        }
                        bucket.freq = 0;
                        bucket.score = 0;
                    } else {
                        final int doc = base | (idx << 6) | ntz;
                        scoreAndDoc.doc = doc;
                        consumer.accept(doc);
                    }
                    bits ^= 1L << ntz;
                }
            }
        }

        @Override
        public int count() throws IOException {
            if (minShouldMatch > 1) {
                // We can't just count bits in that case
                return super.count();
            }
            int count = 0;
            for (long l : matching) {
                count += Long.bitCount(l);
            }
            return count;
        }
    }

    private final HybridBulkScorer.DocIdStreamView docIdStreamView = new HybridBulkScorer.DocIdStreamView();

    HybridBulkScorer(HybridQueryWeight weight, Collection<BulkScorer> scorers, int minShouldMatch, boolean needsScores) {
        if (minShouldMatch < 1 || minShouldMatch > scorers.size()) {
            throw new IllegalArgumentException("minShouldMatch should be within 1..num_scorers. Got " + minShouldMatch);
        }
        if (scorers.size() <= 1) {
            throw new IllegalArgumentException("This scorer can only be used with two scorers or more, got " + scorers.size());
        }
        if (needsScores || minShouldMatch > 1) {
            buckets = new HybridBulkScorer.Bucket[SIZE];
            for (int i = 0; i < buckets.length; i++) {
                buckets[i] = new HybridBulkScorer.Bucket();
            }
        } else {
            buckets = null;
        }
        this.leads = new HybridBulkScorer.BulkScorerAndDoc[scorers.size()];
        this.head = new HybridBulkScorer.HeadPriorityQueue(scorers.size() - minShouldMatch + 1);
        this.tail = new HybridBulkScorer.TailPriorityQueue(minShouldMatch - 1);
        this.minShouldMatch = minShouldMatch;
        this.needsScores = needsScores;
        for (BulkScorer scorer : scorers) {
            final HybridBulkScorer.BulkScorerAndDoc evicted = tail.insertWithOverflow(new HybridBulkScorer.BulkScorerAndDoc(scorer));
            if (evicted != null) {
                head.add(evicted);
            }
        }
        this.cost = cost(scorers, minShouldMatch);
    }

    @Override
    public long cost() {
        return cost;
    }

    private void scoreWindowIntoBitSetAndReplay(
        LeafCollector collector,
        Bits acceptDocs,
        int base,
        int min,
        int max,
        HybridBulkScorer.BulkScorerAndDoc[] scorers,
        int numScorers
    ) throws IOException {
        for (int i = 0; i < numScorers; ++i) {
            final HybridBulkScorer.BulkScorerAndDoc scorer = scorers[i];
            assert scorer.next < max;
            scorer.score(orCollector, acceptDocs, min, max);
        }

        docIdStreamView.base = base;
        collector.collect(docIdStreamView);

        Arrays.fill(matching, 0L);
    }

    private HybridBulkScorer.BulkScorerAndDoc advance(int min) throws IOException {
        assert tail.size() == minShouldMatch - 1;
        final HybridBulkScorer.HeadPriorityQueue head = this.head;
        final HybridBulkScorer.TailPriorityQueue tail = this.tail;
        HybridBulkScorer.BulkScorerAndDoc headTop = head.top();
        HybridBulkScorer.BulkScorerAndDoc tailTop = tail.top();
        while (headTop.next < min) {
            if (tailTop == null || headTop.cost <= tailTop.cost) {
                headTop.advance(min);
                headTop = head.updateTop();
            } else {
                // swap the top of head and tail
                final HybridBulkScorer.BulkScorerAndDoc previousHeadTop = headTop;
                tailTop.advance(min);
                headTop = head.updateTop(tailTop);
                tailTop = tail.updateTop(previousHeadTop);
            }
        }
        return headTop;
    }

    private void scoreWindowMultipleScorers(
        LeafCollector collector,
        Bits acceptDocs,
        int windowBase,
        int windowMin,
        int windowMax,
        int maxFreq
    ) throws IOException {
        while (maxFreq < minShouldMatch && maxFreq + tail.size() >= minShouldMatch) {
            // a match is still possible
            final HybridBulkScorer.BulkScorerAndDoc candidate = tail.pop();
            candidate.advance(windowMin);
            if (candidate.next < windowMax) {
                leads[maxFreq++] = candidate;
            } else {
                head.add(candidate);
            }
        }

        if (maxFreq >= minShouldMatch) {
            // There might be matches in other scorers from the tail too
            for (int i = 0; i < tail.size(); ++i) {
                leads[maxFreq++] = tail.get(i);
            }
            tail.clear();

            scoreWindowIntoBitSetAndReplay(collector, acceptDocs, windowBase, windowMin, windowMax, leads, maxFreq);
        }

        // Push back scorers into head and tail
        for (int i = 0; i < maxFreq; ++i) {
            final HybridBulkScorer.BulkScorerAndDoc evicted = head.insertWithOverflow(leads[i]);
            if (evicted != null) {
                tail.add(evicted);
            }
        }
    }

    private void scoreWindowSingleScorer(
        HybridBulkScorer.BulkScorerAndDoc bulkScorer,
        LeafCollector collector,
        Bits acceptDocs,
        int windowMin,
        int windowMax,
        int max
    ) throws IOException {
        assert tail.size() == 0;
        final int nextWindowBase = head.top().next & ~MASK;
        final int end = Math.max(windowMax, Math.min(max, nextWindowBase));

        bulkScorer.score(collector, acceptDocs, windowMin, end);

        // reset the scorer that should be used for the general case
        collector.setScorer(scoreAndDoc);
    }

    private HybridBulkScorer.BulkScorerAndDoc scoreWindow(
        HybridBulkScorer.BulkScorerAndDoc top,
        LeafCollector collector,
        Bits acceptDocs,
        int min,
        int max
    ) throws IOException {
        final int windowBase = top.next & ~MASK; // find the window that the next match belongs to
        final int windowMin = Math.max(min, windowBase);
        final int windowMax = Math.min(max, windowBase + SIZE);

        // Fill 'leads' with all scorers from 'head' that are in the right window
        leads[0] = head.pop();
        int maxFreq = 1;
        while (head.size() > 0 && head.top().next < windowMax) {
            leads[maxFreq++] = head.pop();
        }

        if (minShouldMatch == 1 && maxFreq == 1) {
            // special case: only one scorer can match in the current window,
            // we can collect directly
            final HybridBulkScorer.BulkScorerAndDoc bulkScorer = leads[0];
            scoreWindowSingleScorer(bulkScorer, collector, acceptDocs, windowMin, windowMax, max);
            return head.add(bulkScorer);
        } else {
            // general case, collect through a bit set first and then replay
            scoreWindowMultipleScorers(collector, acceptDocs, windowBase, windowMin, windowMax, maxFreq);
            return head.top();
        }
    }

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
        scoreAndDoc.doc = -1;
        collector.setScorer(scoreAndDoc);

        HybridBulkScorer.BulkScorerAndDoc top = advance(min);
        while (top.next < max) {
            top = scoreWindow(top, collector, acceptDocs, min, max);
        }

        return top.next;
    }

    class ScoreAndDoc extends Scorable {
        float score;
        int doc = -1;

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public float score() {
            return score;
        }
    }
}
