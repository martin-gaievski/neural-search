/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.search.DisiPriorityQueue;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DisjunctionDISIApproximation;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

public class CompoundQueryScorer extends Scorer {

    private final List<Scorer> subScorers;

    private final DisiPriorityQueue subScorersPQ;

    private final DocIdSetIterator approximation;

    CompoundQueryScorer(Weight weight, List<Scorer> subScorers, ScoreMode scoreMode) throws IOException {
        super(weight);
        this.subScorers = subScorers;
        this.subScorersPQ = new DisiPriorityQueue(subScorers.size());
        for (Scorer scorer : subScorers) {
            final DisiWrapper w = new DisiWrapper(scorer);
            this.subScorersPQ.add(w);
        }
        this.approximation = new DisjunctionDISIApproximation(this.subScorersPQ);
    }

    public float score() throws IOException {
        DisiWrapper topList = subScorersPQ.topList();
        float scoreMax = 0;
        double otherScoreSum = 0;
        for (DisiWrapper w = topList; w != null; w = w.next) {
            float subScore = w.scorer.score();
            if (subScore >= scoreMax) {
                otherScoreSum += scoreMax;
                scoreMax = subScore;
            } else {
                otherScoreSum += subScore;
            }
        }
        return (float) (scoreMax + otherScoreSum);
    }

    DisiWrapper getSubMatches() throws IOException {
        return subScorersPQ.topList();
    }

    @Override
    public DocIdSetIterator iterator() {
        return approximation;
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
        float scoreMax = 0;
        double otherScoreSum = 0;
        for (Scorer scorer : subScorers) {
            if (scorer.docID() <= upTo) {
                float subScore = scorer.getMaxScore(upTo);
                if (subScore >= scoreMax) {
                    otherScoreSum += scoreMax;
                    scoreMax = subScore;
                } else {
                    otherScoreSum += subScore;
                }
            }
        }

        return scoreMax;
    }

    @Override
    public int docID() {
        return subScorersPQ.top().doc;
    }
}
