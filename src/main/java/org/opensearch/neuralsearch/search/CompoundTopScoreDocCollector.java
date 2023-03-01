/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.search;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.LongAccumulator;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.HitQueue;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;

public class CompoundTopScoreDocCollector extends TopDocsCollector<ScoreDoc> {
    int docBase;
    float minCompetitiveScore;
    final HitsThresholdChecker hitsThresholdChecker;
    final MaxScoreAccumulator minScoreAcc;
    ScoreDoc pqTop;

    public CompoundTopScoreDocCollector(int numHits, HitsThresholdChecker hitsThresholdChecker, MaxScoreAccumulator minScoreAcc) {
        super(new HitQueue(numHits, true));
        this.hitsThresholdChecker = hitsThresholdChecker;
        this.minScoreAcc = minScoreAcc;
        pqTop = pq.top();
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        // reset the minimum competitive score
        docBase = context.docBase;
        minCompetitiveScore = 0f;

        return new TopScoreDocCollector.ScorerLeafCollector() {
            @Override
            public void setScorer(Scorable scorer) throws IOException {
                super.setScorer(scorer);
                if (minScoreAcc == null) {
                    updateMinCompetitiveScore(scorer);
                } else {
                    updateGlobalMinCompetitiveScore(scorer);
                }
            }

            @Override
            public void collect(int doc) throws IOException {
                float score = scorer.score();

                // This collector relies on the fact that scorers produce positive values:
                assert score >= 0; // NOTE: false for NaN

                totalHits++;
                hitsThresholdChecker.incrementHitCount();

                if (minScoreAcc != null && (totalHits & minScoreAcc.modInterval) == 0) {
                    updateGlobalMinCompetitiveScore(scorer);
                }

                if (score <= pqTop.score) {
                    if (totalHitsRelation == TotalHits.Relation.EQUAL_TO) {
                        // we just reached totalHitsThreshold, we can start setting the min
                        // competitive score now
                        updateMinCompetitiveScore(scorer);
                    }
                    // Since docs are returned in-order (i.e., increasing doc Id), a document
                    // with equal score to pqTop.score cannot compete since HitQueue favors
                    // documents with lower doc Ids. Therefore reject those docs too.
                    return;
                }
                pqTop.doc = doc + docBase;
                pqTop.score = score;
                pqTop = pq.updateTop();
                updateMinCompetitiveScore(scorer);
            }
        };
    }

    @Override
    public ScoreMode scoreMode() {
        return hitsThresholdChecker.scoreMode();
    }

    @Override
    public void setWeight(Weight weight) {
        super.setWeight(weight);
    }

    protected void updateMinCompetitiveScore(Scorable scorer) throws IOException {
        if (hitsThresholdChecker.isThresholdReached() && pqTop != null && pqTop.score != Float.NEGATIVE_INFINITY) { // -Infinity is the
                                                                                                                    // score of sentinels
            // since we tie-break on doc id and collect in doc id order, we can require
            // the next float
            float localMinScore = Math.nextUp(pqTop.score);
            if (localMinScore > minCompetitiveScore) {
                scorer.setMinCompetitiveScore(localMinScore);
                totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
                minCompetitiveScore = localMinScore;
                if (minScoreAcc != null) {
                    // we don't use the next float but we register the document
                    // id so that other leaves can require it if they are after
                    // the current maximum
                    minScoreAcc.accumulate(docBase, pqTop.score);
                }
            }
        }
    }

    protected void updateGlobalMinCompetitiveScore(Scorable scorer) throws IOException {
        assert minScoreAcc != null;
        MaxScoreAccumulator.DocAndScore maxMinScore = minScoreAcc.get();
        if (maxMinScore != null) {
            // since we tie-break on doc id and collect in doc id order we can require
            // the next float if the global minimum score is set on a document id that is
            // smaller than the ids in the current leaf
            float score = docBase >= maxMinScore.docBase ? Math.nextUp(maxMinScore.score) : maxMinScore.score;
            if (score > minCompetitiveScore) {
                assert hitsThresholdChecker.isThresholdReached();
                scorer.setMinCompetitiveScore(score);
                minCompetitiveScore = score;
                totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
            }
        }
    }
}

final class MaxScoreAccumulator {
    // we use 2^10-1 to check the remainder with a bitwise operation
    static final int DEFAULT_INTERVAL = 0x3ff;

    // scores are always positive
    final LongAccumulator acc = new LongAccumulator(MaxScoreAccumulator::maxEncode, Long.MIN_VALUE);

    // non-final and visible for tests
    long modInterval;

    MaxScoreAccumulator() {
        this.modInterval = DEFAULT_INTERVAL;
    }

    private static long maxEncode(long v1, long v2) {
        float score1 = Float.intBitsToFloat((int) (v1 >> 32));
        float score2 = Float.intBitsToFloat((int) (v2 >> 32));
        int cmp = Float.compare(score1, score2);
        if (cmp == 0) {
            // tie-break on the minimum doc base
            return (int) v1 < (int) v2 ? v1 : v2;
        } else if (cmp > 0) {
            return v1;
        }
        return v2;
    }

    void accumulate(int docBase, float score) {
        assert docBase >= 0 && score >= 0;
        long encode = (((long) Float.floatToIntBits(score)) << 32) | docBase;
        acc.accumulate(encode);
    }

    MaxScoreAccumulator.DocAndScore get() {
        long value = acc.get();
        if (value == Long.MIN_VALUE) {
            return null;
        }
        float score = Float.intBitsToFloat((int) (value >> 32));
        int docBase = (int) value;
        return new MaxScoreAccumulator.DocAndScore(docBase, score);
    }

    static class DocAndScore implements Comparable<MaxScoreAccumulator.DocAndScore> {
        final int docBase;
        final float score;

        DocAndScore(int docBase, float score) {
            this.docBase = docBase;
            this.score = score;
        }

        public int compareTo(MaxScoreAccumulator.DocAndScore o) {
            int cmp = Float.compare(score, o.score);
            if (cmp == 0) {
                // tie-break on the minimum doc base
                // For a given minimum competitive score, we want to know the first segment
                // where this score occurred, hence the reverse order here.
                // On segments with a lower docBase, any document whose score is greater
                // than or equal to this score would be competitive, while on segments with a
                // higher docBase, documents need to have a strictly greater score to be
                // competitive since we tie break on doc ID.
                return Integer.compare(o.docBase, docBase);
            }
            return cmp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MaxScoreAccumulator.DocAndScore result = (MaxScoreAccumulator.DocAndScore) o;
            return docBase == result.docBase && Float.compare(result.score, score) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(docBase, score);
        }

        @Override
        public String toString() {
            return "DocAndScore{" + "docBase=" + docBase + ", score=" + score + '}';
        }
    }
}
