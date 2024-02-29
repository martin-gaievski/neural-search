/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesUtils;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

/**
 * Calculates query weights and build query scorers for hybrid query.
 */
public final class HybridQueryWeight extends Weight {

    // The Weights for our subqueries, in 1-1 correspondence
    private final List<Weight> weights;

    private final ScoreMode scoreMode;

    static final int BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD = 16;

    /**
     * Construct the Weight for this Query searched by searcher. Recursively construct subquery weights.
     */
    public HybridQueryWeight(HybridQuery hybridQuery, IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        super(hybridQuery);
        weights = hybridQuery.getSubQueries().stream().map(q -> {
            try {
                return searcher.createWeight(q, scoreMode, boost);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
        this.scoreMode = scoreMode;
    }

    /**
     * Returns Matches for a specific document, or null if the document does not match the parent query
     *
     * @param context the reader's context to create the {@link Matches} for
     * @param doc     the document's id relative to the given context's reader
     * @return
     * @throws IOException
     */
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

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        // critical section
        // return super.scorerSupplier(context);
        List<ScorerSupplier> scorerSuppliers = new ArrayList<>();
        for (Weight w : weights) {
            ScorerSupplier ss = w.scorerSupplier(context);
            scorerSuppliers.add(ss);
        }

        if (scorerSuppliers.isEmpty()) {
            return null;
        } else {
            final Weight thisWeight = this;
            return new ScorerSupplier() {
                private long cost = -1;

                @Override
                public Scorer get(long leadCost) throws IOException {
                    List<Scorer> tScorers = new ArrayList<>();
                    for (int i = 0; i < scorerSuppliers.size(); i++) {
                        ScorerSupplier ss = scorerSuppliers.get(i);
                        if (Objects.nonNull(ss)) {
                            tScorers.add(ss.get(leadCost));
                        } else {
                            tScorers.add(null);
                        }
                    }
                    return new HybridQueryScorer(thisWeight, tScorers, scoreMode);
                }

                @Override
                public long cost() {
                    if (cost == -1) {
                        long cost = 0;
                        for (ScorerSupplier ss : scorerSuppliers) {
                            if (Objects.nonNull(ss)) {
                                cost += ss.cost();
                            }
                        }
                        this.cost = cost;
                    }
                    return cost;
                }

                @Override
                public void setTopLevelScoringClause() throws IOException {
                    for (ScorerSupplier ss : scorerSuppliers) {
                        // sub scorers need to be able to skip too as calls to setMinCompetitiveScore get
                        // propagated
                        if (Objects.nonNull(ss)) {
                            ss.setTopLevelScoringClause();
                        }
                    }
                }
            };
        }
    }

    /**
     * Create the scorer used to score our associated Query
     *
     * @param context the {@link LeafReaderContext} for which to return the
     *                {@link Scorer}.
     * @return scorer of hybrid query that contains scorers of each sub-query, null if there are no matches in any sub-query
     * @throws IOException
     */
    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
        /*List<Scorer> scorers = weights.stream().map(w -> {
            try {
                return w.scorer(context);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
        // if there are no matches in any of the scorers (sub-queries) we need to return
        // scorer as null to avoid problems with disi result iterators
        if (scorers.stream().allMatch(Objects::isNull)) {
            return null;
        }
        return new HybridQueryScorer(this, scorers);*/
        // critical section
        ScorerSupplier supplier = scorerSupplier(context);
        if (supplier == null) {
            return null;
        }
        supplier.setTopLevelScoringClause();
        return supplier.get(Long.MAX_VALUE);
    }

    /**
     * Check if weight object can be cached
     *
     * @param ctx
     * @return true if the object can be cached against a given leaf
     */
    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        if (weights.size() > BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD) {
            // Disallow caching large queries to not encourage users
            // to build large queries
            return false;
        }
        return weights.stream().allMatch(w -> w.isCacheable(ctx));
    }

    /**
     * Explain is not supported for hybrid query
     *
     * @param context the readers context to create the {@link Explanation} for.
     * @param doc     the document's id relative to the given context's reader
     * @return
     * @throws IOException
     */
    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        throw new UnsupportedOperationException("Explain is not supported");
    }
}
