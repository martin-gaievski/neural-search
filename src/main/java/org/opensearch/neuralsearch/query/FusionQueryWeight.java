/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesUtils;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

/**
 * Weight implementation for FusionQuery. Creates per-sub-query weights and produces
 * FusionBulkScorer for shard-level RRF scoring.
 *
 * Two scoring paths exist:
 * - BulkScorer path (normal search): FusionBulkScorer with windowed RRF — called via bulkScorer()
 * - Scorer path (profiler, explain): PrecomputedRRFScorer — called via get()
 *   The profiler wraps Weight in ProfileWeight which calls get() instead of bulkScorer().
 *   To ensure correct RRF scores in both paths, get() pre-computes all RRF scores for the segment.
 */
public final class FusionQueryWeight extends Weight {

    private static final int RRF_K = 60;

    private final List<Weight> weights;
    private final ScoreMode scoreMode;

    public FusionQueryWeight(FusionQuery fusionQuery, IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        super(fusionQuery);
        this.scoreMode = scoreMode;
        this.weights = fusionQuery.getSubQueries().stream().map(q -> {
            try {
                return searcher.createWeight(searcher.rewrite(q), scoreMode, boost);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
    }

    @Override
    public Matches matches(LeafReaderContext context, int doc) throws IOException {
        List<Matches> mis = weights.stream().map(weight -> {
            try {
                return weight.matches(context, doc);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());
        return MatchesUtils.fromSubMatches(mis);
    }

    /**
     * Returns a ScorerSupplier that provides:
     * - bulkScorer() → FusionBulkScorer for the main scoring path (windowed RRF)
     * - get() → PrecomputedRRFScorer for the profiler/explain path (full-segment RRF)
     */
    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        // Collect scorers for each sub-query
        List<Scorer> scorers = new ArrayList<>();
        boolean hasAnyScorable = false;
        for (Weight weight : weights) {
            Scorer scorer = weight.scorer(context);
            scorers.add(scorer); // may be null for non-matching sub-queries
            if (scorer != null) {
                hasAnyScorable = true;
            }
        }
        if (!hasAnyScorable) {
            return null;
        }

        final List<Scorer> finalScorers = scorers;
        final int maxDoc = context.reader().maxDoc();
        final boolean needsScores = scoreMode.needsScores();
        // Keep reference to weights and context for creating fresh scorers in get()
        final List<Weight> subWeights = this.weights;
        final LeafReaderContext leafContext = context;

        return new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) throws IOException {
                // Pre-compute RRF scores for the entire segment using fresh scorers.
                // This path is used by the profiler (ProfileWeight wraps our ScorerSupplier
                // and calls get() instead of bulkScorer()).
                return createPrecomputedRRFScorer(subWeights, leafContext);
            }

            @Override
            public BulkScorer bulkScorer() throws IOException {
                return new FusionBulkScorer(finalScorers, needsScores, maxDoc);
            }

            @Override
            public long cost() {
                long cost = 0;
                for (Scorer s : finalScorers) {
                    if (s != null) {
                        cost += s.iterator().cost();
                    }
                }
                return cost;
            }
        };
    }

    /**
     * Creates a Scorer that pre-computes RRF scores for the entire segment.
     * Uses fresh scorers from sub-weights (not the ones consumed by BulkScorer).
     */
    private Scorer createPrecomputedRRFScorer(List<Weight> subWeights, LeafReaderContext context) throws IOException {
        // Step 1: Collect all matching docs with scores per sub-query using fresh scorers
        List<List<int[]>> perSubQueryDocs = new ArrayList<>();
        for (Weight w : subWeights) {
            List<int[]> docs = new ArrayList<>();
            Scorer s = w.scorer(context);
            if (s != null) {
                DocIdSetIterator it = s.iterator();
                int doc;
                while ((doc = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    docs.add(new int[] { doc, Float.floatToRawIntBits(s.score()) });
                }
            }
            perSubQueryDocs.add(docs);
        }

        // Step 2: Sort each sub-query by score desc → assign ranks → compute RRF
        Map<Integer, Float> rrfScoreMap = new HashMap<>();
        for (List<int[]> docs : perSubQueryDocs) {
            if (docs.isEmpty()) continue;
            docs.sort((a, b) -> Float.compare(Float.intBitsToFloat(b[1]), Float.intBitsToFloat(a[1])));
            for (int rank = 0; rank < docs.size(); rank++) {
                int docId = docs.get(rank)[0];
                rrfScoreMap.merge(docId, 1.0f / (RRF_K + rank + 1), Float::sum);
            }
        }

        if (rrfScoreMap.isEmpty()) {
            return null;
        }

        // Step 3: Sort by docId for ordered iteration
        List<Map.Entry<Integer, Float>> sortedDocs = new ArrayList<>(rrfScoreMap.entrySet());
        sortedDocs.sort(Comparator.comparingInt(Map.Entry::getKey));

        // Step 4: Build arrays for the scorer
        final int[] docIds = new int[sortedDocs.size()];
        final float[] scores = new float[sortedDocs.size()];
        for (int i = 0; i < sortedDocs.size(); i++) {
            docIds[i] = sortedDocs.get(i).getKey();
            scores[i] = sortedDocs.get(i).getValue();
        }

        return new PrecomputedRRFScorer(docIds, scores);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return weights.stream().allMatch(w -> w.isCacheable(ctx));
    }

    /**
     * Explains the RRF scoring for a specific document.
     */
    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        List<Explanation> subsOnMatch = new ArrayList<>();
        List<Explanation> subsOnNoMatch = new ArrayList<>();
        boolean match = false;

        for (int i = 0; i < weights.size(); i++) {
            Explanation e = weights.get(i).explain(context, doc);
            if (e.isMatch()) {
                match = true;
                subsOnMatch.add(
                    Explanation.match(
                        e.getValue(),
                        String.format(Locale.ROOT, "sub-query %d matched, RRF contribution = 1/(60 + rank)", i),
                        e
                    )
                );
            } else {
                subsOnNoMatch.add(e);
                subsOnMatch.add(Explanation.noMatch(String.format(Locale.ROOT, "sub-query %d did not match", i), e));
            }
        }

        if (match) {
            return Explanation.match(
                0.0f, // actual RRF score is computed in BulkScorer/PrecomputedScorer
                "fusion query with RRF combination (k=60), actual score computed during collection",
                subsOnMatch
            );
        } else {
            return Explanation.noMatch("no sub-query matched", subsOnNoMatch);
        }
    }

    /**
     * A Scorer that returns pre-computed RRF scores. Used by the profiler path
     * where ProfileWeight calls ScorerSupplier.get() instead of bulkScorer().
     * All RRF scores are computed upfront for the entire segment.
     */
    private static class PrecomputedRRFScorer extends Scorer {
        private final int[] docIds;
        private final float[] scores;
        private int index = -1;

        PrecomputedRRFScorer(int[] docIds, float[] scores) {
            this.docIds = docIds;
            this.scores = scores;
        }

        @Override
        public DocIdSetIterator iterator() {
            return new DocIdSetIterator() {
                @Override
                public int docID() {
                    return index >= 0 && index < docIds.length ? docIds[index] : index == -1 ? -1 : NO_MORE_DOCS;
                }

                @Override
                public int nextDoc() {
                    index++;
                    return index < docIds.length ? docIds[index] : NO_MORE_DOCS;
                }

                @Override
                public int advance(int target) {
                    // Binary search for target
                    index++;
                    while (index < docIds.length && docIds[index] < target) {
                        index++;
                    }
                    return index < docIds.length ? docIds[index] : NO_MORE_DOCS;
                }

                @Override
                public long cost() {
                    return docIds.length;
                }
            };
        }

        @Override
        public float getMaxScore(int upTo) {
            // Maximum possible RRF score: 2 sub-queries, both at rank 1
            return 2.0f / (RRF_K + 1);
        }

        @Override
        public float score() {
            return index >= 0 && index < scores.length ? scores[index] : 0.0f;
        }

        @Override
        public int docID() {
            return index >= 0 && index < docIds.length ? docIds[index] : index == -1 ? -1 : DocIdSetIterator.NO_MORE_DOCS;
        }
    }
}
