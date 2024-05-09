/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.HitQueue;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.PriorityQueue;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.opensearch.neuralsearch.query.HybridQueryScorer;

/**
 * Collects the TopDocs after executing hybrid query. Uses HybridQueryTopDocs as DTO to handle each sub query results
 */
@Log4j2
public class HybridTopScoreDocCollector implements Collector {
    private static final TopDocs EMPTY_TOPDOCS = new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
    private int docBase;
    private final HitsThresholdChecker hitsThresholdChecker;
    private TotalHits.Relation totalHitsRelation = TotalHits.Relation.EQUAL_TO;
    private int[] totalHits;
    private final int numOfHits;
    @Getter
    private PriorityQueue<ScoreDoc>[] compoundScores;

    public HybridTopScoreDocCollector(int numHits, HitsThresholdChecker hitsThresholdChecker) {
        numOfHits = numHits;
        this.hitsThresholdChecker = hitsThresholdChecker;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) {
        docBase = context.docBase;

        return new LeafCollector() {
            HybridQueryScorer compoundQueryScorer;

            @Override
            public void setScorer(Scorable scorer) throws IOException {
                if (scorer instanceof HybridQueryScorer) {
                    log.debug("passed scorer is of type HybridQueryScorer, saving it for collecting documents and scores");
                    compoundQueryScorer = (HybridQueryScorer) scorer;
                } else {
                    compoundQueryScorer = getHybridQueryScorer(scorer);
                    if (Objects.isNull(compoundQueryScorer)) {
                        log.error(
                            String.format(Locale.ROOT, "cannot find scorer of type HybridQueryScorer in a hierarchy of scorer %s", scorer)
                        );
                    }
                }
            }

            private HybridQueryScorer getHybridQueryScorer(final Scorable scorer) throws IOException {
                if (scorer == null) {
                    return null;
                }
                if (scorer instanceof HybridQueryScorer) {
                    return (HybridQueryScorer) scorer;
                }
                for (Scorable.ChildScorable childScorable : scorer.getChildren()) {
                    HybridQueryScorer hybridQueryScorer = getHybridQueryScorer(childScorable.child);
                    if (Objects.nonNull(hybridQueryScorer)) {
                        log.debug(
                            String.format(
                                Locale.ROOT,
                                "found hybrid query scorer, it's child of scorer %s",
                                childScorable.child.getClass().getSimpleName()
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
                    throw new IllegalArgumentException("scorers are null for all sub-queries in hybrid query");
                }

                float[] subScoresByQuery = compoundQueryScorer.hybridScores();
                // iterate over results for each query
                if (compoundScores == null) {
                    compoundScores = new PriorityQueue[subScoresByQuery.length];
                    for (int i = 0; i < subScoresByQuery.length; i++) {
                        compoundScores[i] = new HitQueue(numOfHits, false);
                    }
                    totalHits = new int[subScoresByQuery.length];
                }
                for (int i = 0; i < subScoresByQuery.length; i++) {
                    float score = subScoresByQuery[i];
                    // if score is 0.0 there is no hits for that sub-query
                    if (score == 0) {
                        continue;
                    }
                    totalHits[i]++;
                    PriorityQueue<ScoreDoc> pq = compoundScores[i];
                    ScoreDoc topDoc = new ScoreDoc(doc + docBase, score);
                    // this way we're inserting into heap and do nothing else unless we reach the capacity
                    // after that we pull out the lowest score element on each insert
                    pq.insertWithOverflow(topDoc);
                }
            }
        };
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
        final List<TopDocs> topDocs = IntStream.range(0, compoundScores.length)
            .mapToObj(i -> topDocsPerQuery(0, Math.min(totalHits[i], compoundScores[i].size()), compoundScores[i], totalHits[i]))
            .collect(Collectors.toList());
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
        // pq's pop() returns the 'least' element in the queue, therefore need
        // to discard the first ones, until we reach the requested range.
        for (int i = pq.size() - start - size; i > 0; i--) {
            pq.pop();
        }

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
}
