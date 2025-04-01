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
import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class HybridBulkScorer extends BulkScorer {
    final long cost;
    final Scorer[] disiWrappers;
    HybridCombinedSubQueryScorer hybridCombinedSubQueryScorer = new HybridCombinedSubQueryScorer();

    Map<Integer, float[]> scoresByDoc = new HashMap<>();

    HybridBulkScorer(List<Scorer> scorers) {
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
    }

    private static long cost(Collection<BulkScorer> scorers) {
        final PriorityQueue<BulkScorer> pq = new PriorityQueue<BulkScorer>(scorers.size()) {
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

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
        scoresByDoc.clear();

        hybridCombinedSubQueryScorer.setScoresByDoc(scoresByDoc);
        hybridCombinedSubQueryScorer.setNumOfSubQueries(disiWrappers.length);
        collector.setScorer(hybridCombinedSubQueryScorer);

        // List<Scorer> scorers = new ArrayList<>(this.scorers.getSubScorers());
        int nextDoc = -1;
        for (int i = 0; i < disiWrappers.length; i++) {
            if (disiWrappers[i] == null) {
                continue;
            }
            DocIdSetIterator it = disiWrappers[i].iterator();
            int doc = it.docID();
            if (doc < min) {
                doc = it.advance(min);
            }
            for (; doc < max; doc = it.nextDoc()) {
                if (acceptDocs == null || acceptDocs.get(doc)) {
                    // scoresByDoc.computeIfAbsent(doc, k -> new float[disiWrappers.length])[i] = scorer.score();
                    // Atomic operation to ensure thread-safe array creation and update
                    getScore(i, doc, disiWrappers[i], disiWrappers.length);
                }
            }
            if (doc != DocIdSetIterator.NO_MORE_DOCS) {
                nextDoc = Math.max(nextDoc, doc);
            }
        }

        DocIdStreamView docIdStreamView = new DocIdStreamView();
        collector.collect(docIdStreamView);

        return nextDoc == -1 ? DocIdSetIterator.NO_MORE_DOCS : nextDoc;
    }

    private void getScore(int i, int doc, Scorer scorer, int numOfQueries) throws IOException {
        // scoresByDoc.computeIfAbsent(doc, k -> new float[numOfQueries])[i] = scorer.score();
        float score = scorer.score();
        if (scoresByDoc.containsKey(doc) == false) {
            float[] scores = new float[numOfQueries];
            scores[i] = score;
            scoresByDoc.put(doc, scores);
        } else {
            scoresByDoc.get(doc)[i] = score;
        }
    }

    @Override
    public long cost() {
        return cost;
    }

    class DocIdStreamView extends DocIdStream {

        @Override
        public void forEach(CheckedIntConsumer<IOException> consumer) throws IOException {
            for (int doc : scoresByDoc.keySet()) {
                float[] scores = scoresByDoc.get(doc);
                float combinedScore = 0.0f;
                for (float score : scores) {
                    if (score > 0) {
                        combinedScore += score;
                    }
                }
                hybridCombinedSubQueryScorer.setScore(combinedScore);
                consumer.accept(doc);
            }
        }

        @Override
        public int count() throws IOException {
            return scoresByDoc.size();
        }
    }

    @Data
    public static class HybridCombinedSubQueryScorer extends Scorable {
        Map<Integer, float[]> scoresByDoc;
        int numOfSubQueries;
        float score;

        @Override
        public float score() throws IOException {
            return score;
        }
    }
}
