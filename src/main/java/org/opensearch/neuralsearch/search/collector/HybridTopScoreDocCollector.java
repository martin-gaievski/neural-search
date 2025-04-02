/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

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
import org.opensearch.neuralsearch.query.HybridBulkScorer;
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
    private Set<Integer> docIds = new HashSet<>();
    private int[] collectedHitsPerSubQuery;
    private final int numOfHits;
    private PriorityQueue<ScoreDoc>[] compoundScores;
    @Getter
    private float maxScore = 0.0f;

    public HybridTopScoreDocCollector(int numHits, HitsThresholdChecker hitsThresholdChecker) {
        numOfHits = numHits;
        this.hitsThresholdChecker = hitsThresholdChecker;
    }

    /*public int getTotalHits() {
        return docIds.size();
    }*/

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

    public class HybridTopScoreLeafCollector implements LeafCollector {
        HybridBulkScorer.HybridCombinedSubQueryScorer compoundQueryScorer;
        // float[] minScoreThresholds;

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            if (scorer instanceof HybridBulkScorer.HybridCombinedSubQueryScorer) {
                compoundQueryScorer = (HybridBulkScorer.HybridCombinedSubQueryScorer) scorer;
            } else {
                compoundQueryScorer = getHybridQueryScorer(scorer);
                if (Objects.isNull(compoundQueryScorer)) {
                    log.error(
                        String.format(Locale.ROOT, "cannot find scorer of type HybridQueryScorer in a hierarchy of scorer %s", scorer)
                    );
                }
            }
            /*if (Objects.isNull(minScoreThresholds)) {
                minScoreThresholds = new float[compoundQueryScorer.getNumOfSubQueries()];
                Arrays.fill(minScoreThresholds, Float.MIN_VALUE);
            }*/
        }

        private HybridBulkScorer.HybridCombinedSubQueryScorer getHybridQueryScorer(final Scorable scorer) throws IOException {
            if (scorer == null) {
                return null;
            }
            if (scorer instanceof HybridBulkScorer.HybridCombinedSubQueryScorer) {
                return (HybridBulkScorer.HybridCombinedSubQueryScorer) scorer;
            }
            for (Scorable.ChildScorable childScorable : scorer.getChildren()) {
                HybridBulkScorer.HybridCombinedSubQueryScorer hybridQueryScorer = getHybridQueryScorer(childScorable.child());
                if (Objects.nonNull(hybridQueryScorer)) {
                    log.debug(
                        String.format(
                            Locale.ROOT,
                            "found hybrid query scorer, it's child of scorer %s",
                            childScorable.child().getClass().getSimpleName()
                        )
                    );
                    return hybridQueryScorer;
                }
            }
            return null;
        }

        @Override
        public void collect(int doc) throws IOException {
            if (Objects.isNull(compoundQueryScorer)) {
                return;
            }
            // iterate over results for each query
            if (compoundScores == null) {
                // compoundScores = new PriorityQueue[subScoresByQuery.length];
                compoundScores = new PriorityQueue[compoundQueryScorer.getNumOfSubQueries()];
                for (int i = 0; i < compoundQueryScorer.getNumOfSubQueries(); i++) {
                    compoundScores[i] = new HitQueue(numOfHits, false);
                }
                collectedHitsPerSubQuery = new int[compoundQueryScorer.getNumOfSubQueries()];
            }
            // Increment total hit count which represents unique doc found on the shard
            totalHits++;
            // docIds.add(doc);

            float[] scores = compoundQueryScorer.getScoresByDoc();
            int docWithBase = doc + docBase;
            for (int i = 0; i < scores.length; i++) {
                float score = scores[i];
                // if score is 0.0 there is no hits for that sub-query
                if (score <= 0) {
                    continue;
                }

                /*if (score < minScoreThresholds[i]) {
                    continue;
                }*/

                if (hitsThresholdChecker.isThresholdReached() && totalHitsRelation == TotalHits.Relation.EQUAL_TO) {
                    totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
                }
                collectedHitsPerSubQuery[i]++;
                PriorityQueue<ScoreDoc> pq = compoundScores[i];
                ScoreDoc currentDoc = new ScoreDoc(docWithBase, score);
                maxScore = Math.max(currentDoc.score, maxScore);
                // this way we're inserting into heap and do nothing else unless we reach the capacity
                // after that we pull out the lowest score element on each insert
                ScoreDoc popedScoreDoc = pq.insertWithOverflow(currentDoc);
                if (Objects.nonNull(popedScoreDoc)) {
                    float newThresholdScore = popedScoreDoc.score;
                    // minScoreThresholds[i] = newThresholdScore;
                    compoundQueryScorer.getMinScores()[i] = newThresholdScore;
                }
            }
        }
    };
}
