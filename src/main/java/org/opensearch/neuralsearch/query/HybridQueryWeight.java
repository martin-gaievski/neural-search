/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import lombok.RequiredArgsConstructor;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesUtils;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.PriorityQueue;
import org.opensearch.neuralsearch.executors.HybridQueryExecutor;
import org.opensearch.neuralsearch.executors.HybridQueryExecutorCollector;
import org.opensearch.neuralsearch.executors.HybridQueryScoreSupplierCollectorManager;

import static org.opensearch.neuralsearch.query.HybridQueryBuilder.MAX_NUMBER_OF_SUB_QUERIES;

/**
 * Calculates query weights and build query scorers for hybrid query.
 */
public final class HybridQueryWeight extends Weight {

    @Override
    public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
        final BulkScorer bulkScorer = hybridScorer(context);
        if (bulkScorer != null) {
            // bulk scoring is applicable, use it
            return bulkScorer;
        } else {
            // use a Scorer-based impl (BS2)
            return super.bulkScorer(context);
        }
    }

    BulkScorer hybridScorer(LeafReaderContext context) throws IOException {
        List<BulkScorer> optional = new ArrayList<>();
        for (Weight w : weights) {
            BulkScorer subScorer = w.bulkScorer(context);

            if (subScorer != null) {
                optional.add(subScorer);
            }
        }
        if (optional.isEmpty()) {
            return null;
        }
        return new HybridBulkScorer(this, optional, 1, true);
    }

    // The Weights for our subqueries, in 1-1 correspondence
    private final List<Weight> weights;

    private final ScoreMode scoreMode;

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
        List<Matches> mis = new ArrayList<>();
        int countMatches = 0;
        for (Weight w : weights) {
            Matches m = w.matches(context, doc);
            if (m != null) {
                countMatches++;
                mis.add(m);
            }
        }
        if (countMatches == MAX_NUMBER_OF_SUB_QUERIES) {
            return MatchesUtils.fromSubMatches(mis);
        }
        return null;
    }

    /**
     * Returns {@link HybridScorerSupplier} which contains list of {@link ScorerSupplier} from its
     * sub queries. Here, add score supplier from individual sub query is parallelized and finally
     * {@link HybridScorerSupplier} is created with list of {@link ScorerSupplier}
     */
    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        HybridQueryScoreSupplierCollectorManager manager = new HybridQueryScoreSupplierCollectorManager(context);
        List<Callable<Void>> scoreSupplierTasks = new ArrayList<>();
        List<HybridQueryExecutorCollector<LeafReaderContext, ScorerSupplier>> collectors = new ArrayList<>();
        for (Weight weight : weights) {
            HybridQueryExecutorCollector<LeafReaderContext, ScorerSupplier> collector = manager.newCollector();
            collectors.add(collector);
            scoreSupplierTasks.add(() -> addScoreSupplier(weight, collector));
        }
        HybridQueryExecutor.getExecutor().invokeAll(scoreSupplierTasks);
        final List<ScorerSupplier> scorerSuppliers = manager.mergeScoreSuppliers(collectors);
        if (scorerSuppliers.isEmpty()) {
            return null;
        }
        return new HybridScorerSupplier(scorerSuppliers, this, scoreMode);
    }

    private Void addScoreSupplier(Weight weight, HybridQueryExecutorCollector<LeafReaderContext, ScorerSupplier> collector) {
        collector.collect(leafReaderContext -> {
            try {
                return weight.scorerSupplier(leafReaderContext);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return null;
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
        if (weights.size() > MAX_NUMBER_OF_SUB_QUERIES) {
            // this situation should never happen, but in case it do such query will not be cached
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

    @RequiredArgsConstructor
    static class HybridScorerSupplier extends ScorerSupplier {
        private long cost = -1;
        private final List<ScorerSupplier> scorerSuppliers;
        private final Weight weight;
        private final ScoreMode scoreMode;

        @Override
        public Scorer get(long leadCost) throws IOException {
            List<Scorer> tScorers = new ArrayList<>();
            for (ScorerSupplier ss : scorerSuppliers) {
                if (Objects.nonNull(ss)) {
                    tScorers.add(ss.get(leadCost));
                } else {
                    tScorers.add(null);
                }
            }
            return new HybridQueryScorer(weight, tScorers, scoreMode);
            // return new DisjunctionSumScorer(weight, tScorers, scoreMode);
        }

        @Override
        public long cost() {
            if (cost == -1) {
                long cost = 0;

                /*for (ScorerSupplier ss : scorerSuppliers) {
                    if (Objects.nonNull(ss)) {
                        cost += ss.cost();
                    }
                }*/
                cost = computeCost(
                    scorerSuppliers.stream().map(ScorerSupplier::cost).mapToLong(Number::longValue),
                    scorerSuppliers.size(),
                    1
                );
                this.cost = cost;
            }
            return cost;
        }

        static long computeCost(LongStream costs, int numScorers, int minShouldMatch) {
            final PriorityQueue<Long> pq = new PriorityQueue<>(numScorers - minShouldMatch + 1) {
                @Override
                protected boolean lessThan(Long a, Long b) {
                    return a > b;
                }
            };
            costs.forEach(pq::insertWithOverflow);
            return StreamSupport.stream(pq.spliterator(), false).mapToLong(Number::longValue).sum();
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
