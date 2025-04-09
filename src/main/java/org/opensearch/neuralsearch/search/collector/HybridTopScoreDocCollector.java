/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import lombok.Getter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.HitQueue;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.PriorityQueue;

import lombok.extern.log4j.Log4j2;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;

/**
 * Collects the TopDocs after executing hybrid query. Uses HybridQueryTopDocs as DTO to handle each sub query results
 */
@Log4j2
public class HybridTopScoreDocCollector implements HybridSearchCollector {
    private static final TopDocs EMPTY_TOPDOCS = new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
    private int docBase;
    private final HitsThresholdChecker hitsThresholdChecker;
    private TotalHits.Relation totalHitsRelation = TotalHits.Relation.EQUAL_TO;

    @Getter
    private int totalHits;
    private int[] collectedHitsPerSubQuery;
    private final int numOfHits;
    private PriorityQueue<ScoreDoc>[] compoundScores;
    @Getter
    private float maxScore = 0.0f;

    public HybridTopScoreDocCollector(int numHits, HitsThresholdChecker hitsThresholdChecker) {
        numOfHits = numHits;
        this.hitsThresholdChecker = hitsThresholdChecker;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) {
        docBase = context.docBase;
        return new HybridTopScoreLeafCollector();
    }

    @Override
    public ScoreMode scoreMode() {
        return hitsThresholdChecker.scoreMode();
    }

    /**
     * Get resulting collection of TopDocs for hybrid query after we ran search for each of its sub query
     * @return
     */
    public List<TopDocs> topDocs() {
        if (compoundScores == null) {
            return new ArrayList<>();
        }
        final List<TopDocs> topDocs = new ArrayList<>();
        for (int i = 0; i < compoundScores.length; i++) {
            topDocs.add(
                topDocsPerQuery(
                    0,
                    Math.min(collectedHitsPerSubQuery[i], compoundScores[i].size()),
                    compoundScores[i],
                    collectedHitsPerSubQuery[i]
                )
            );
        }
        return topDocs;
    }

    private TopDocs topDocsPerQuery(int start, int howMany, PriorityQueue<ScoreDoc> pq, int totalHits) {
        if (howMany < 0) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Number of hits requested must be greater than 0 but value was %d", howMany)
            );
        }

        if (start < 0) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Expected value of starting position is between 0 and %d, got %d", howMany, start)
            );
        }

        if (start >= howMany || howMany == 0) {
            return EMPTY_TOPDOCS;
        }

        int size = howMany - start;
        ScoreDoc[] results = new ScoreDoc[size];

        // Get the requested results from pq.
        populateResults(results, size, pq);

        return new TopDocs(new TotalHits(totalHits, totalHitsRelation), results);
    }

    protected void populateResults(ScoreDoc[] results, int howMany, PriorityQueue<ScoreDoc> pq) {
        for (int i = howMany - 1; i >= 0 && pq.size() > 0; i--) {
            // adding to array if index is within [0..array_length - 1]
            if (i < results.length) {
                results[i] = pq.pop();
            }
        }
    }

    public class HybridTopScoreLeafCollector extends HybridLeafCollector {
        float[] minScoreThresholds;

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            super.setScorer(scorer);
            if (Objects.isNull(minScoreThresholds)) {
                minScoreThresholds = new float[getCompoundQueryScorer().getNumOfSubQueries()];
                Arrays.fill(minScoreThresholds, Float.MIN_VALUE);
            }
        }

        @Override
        public void collect(int doc) throws IOException {
            if (Objects.isNull(getCompoundQueryScorer())) {
                return;
            }
            ensureSubQueryScoreQueues();
            // Increment total hit count which represents unique doc found on the shard
            totalHits++;
            float[] scores = getCompoundQueryScorer().getSubQueryScores();
            int docWithBase = doc + docBase;
            for (int subQueryIndex = 0; subQueryIndex < scores.length; subQueryIndex++) {
                float score = scores[subQueryIndex];
                // if score is 0.0 there is no hits for that sub-query
                if (score <= 0) {
                    continue;
                }

                if (score < minScoreThresholds[subQueryIndex]) {
                    continue;
                }

                if (hitsThresholdChecker.isThresholdReached() && totalHitsRelation == TotalHits.Relation.EQUAL_TO) {
                    totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
                }
                collectedHitsPerSubQuery[subQueryIndex]++;
                PriorityQueue<ScoreDoc> pq = compoundScores[subQueryIndex];
                ScoreDoc currentDoc = new ScoreDoc(docWithBase, score);
                maxScore = Math.max(currentDoc.score, maxScore);
                // this way we're inserting into heap and do nothing else unless we reach the capacity
                // after that we pull out the lowest score element on each insert
                ScoreDoc evictedScoreDoc = pq.insertWithOverflow(currentDoc);
                if (Objects.nonNull(evictedScoreDoc)) {
                    float newThresholdScore = evictedScoreDoc.score;
                    minScoreThresholds[subQueryIndex] = newThresholdScore;
                    compoundQueryScorer.getMinScores()[subQueryIndex] = newThresholdScore;
                }
            }
        }

        private void ensureSubQueryScoreQueues() {
            if (Objects.isNull(compoundScores)) {
                compoundScores = new PriorityQueue[compoundQueryScorer.getNumOfSubQueries()];
                for (int i = 0; i < compoundQueryScorer.getNumOfSubQueries(); i++) {
                    compoundScores[i] = new HitQueue(numOfHits, false);
                }
                collectedHitsPerSubQuery = new int[compoundQueryScorer.getNumOfSubQueries()];
            }
        }
    };
}
