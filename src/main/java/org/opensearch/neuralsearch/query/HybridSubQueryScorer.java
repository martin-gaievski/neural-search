/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.Data;
import org.apache.lucene.search.Scorable;

import java.io.IOException;
import java.util.Arrays;

@Data
public class HybridSubQueryScorer extends Scorable {
    private final float[] subQueryScores;
    private final int numOfSubQueries;
    private final float[] minScores;

    HybridSubQueryScorer(int numOfSubQueries) {
        this.numOfSubQueries = numOfSubQueries;
        this.minScores = new float[numOfSubQueries];
        this.subQueryScores = new float[numOfSubQueries];
    }

    @Override
    public float score() throws IOException {
        float totalScore = 0.0f;
        for (float score : subQueryScores) {
            totalScore += score;
        }
        return totalScore;
    }

    public void resetScores() {
        Arrays.fill(subQueryScores, 0.0f);
    }
}
