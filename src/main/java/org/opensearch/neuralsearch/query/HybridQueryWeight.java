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

import lombok.AccessLevel;
import lombok.Getter;
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
import org.opensearch.neuralsearch.executors.HybridQueryExecutor;
import org.opensearch.neuralsearch.executors.HybridQueryExecutorCollector;
import org.opensearch.neuralsearch.executors.HybridQueryScoreSupplierCollectorManager;

import static org.opensearch.neuralsearch.query.HybridQueryBuilder.MAX_NUMBER_OF_SUB_QUERIES;

/**
 * Calculates query weights and build query scorers for hybrid query.
 */
public final class HybridQueryWeight extends Weight {

    // The Weights for our subqueries, in 1-1 correspondence
    @Getter(AccessLevel.PACKAGE)
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
        return new HybridScorerSupplier(scorerSuppliers, this, scoreMode, context);
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
     * Returns a shard level {@link Explanation} that describes how the weight and scoring are calculated.
     * @param context the readers context to create the {@link Explanation} for.
     * @param doc the document's id relative to the given context's reader
     * @return shard level {@link Explanation}, each sub-query explanation is a single nested element
     * @throws IOException
     */
    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        boolean match = false;
        double max = 0;
        List<Explanation> subsOnNoMatch = new ArrayList<>();
        List<Explanation> subsOnMatch = new ArrayList<>();
        for (Weight wt : weights) {
            Explanation e = wt.explain(context, doc);
            if (e.isMatch()) {
                match = true;
                double score = e.getValue().doubleValue();
                max = Math.max(max, score);
                subsOnMatch.add(e);
            } else {
                if (match == false) {
                    subsOnNoMatch.add(e);
                }
                subsOnMatch.add(e);
            }
        }
        if (match) {
            final String desc = "combined score of:";
            return Explanation.match(max, desc, subsOnMatch);
        } else {
            return Explanation.noMatch("no matching clause", subsOnNoMatch);
        }
    }

    @RequiredArgsConstructor
    static class HybridScorerSupplier extends ScorerSupplier {

        @Override
        public BulkScorer bulkScorer() throws IOException {
            /*if (hybridQueryWeight.parentQuery.s == null) {
                return new HybridTopScoreDocCollector(numHits, hitsThresholdChecker);
            } else {
                // Sorting is applied
                if (after == null) {
                    return new SimpleFieldCollector(numHits, hitsThresholdChecker, sortAndFormats.sort);
                } else {
                    // search_after is applied
                    validateSearchAfterFieldAndSortFormats();
                    return new PagingFieldCollector(numHits, hitsThresholdChecker, sortAndFormats.sort, after);
                }
            }*/
            /*Supplier<BulkScorer> supplier = () -> {
                try {
                    return super.bulkScorer();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            };*/
            List<Scorer> scorers = new ArrayList<>();
            for (Weight weight : weight.getWeights()) {
                Scorer scorer = weight.scorer(context);
                scorers.add(scorer);
            }
            // return new HybridBulkScorer(scorer, Integer.MAX_VALUE);
            return new HybridBulkScorer(scorers, scoreMode.needsScores());
            // return new HybridBulkScorer(scorer, Integer.MAX_VALUE);
        }

        private long cost = -1;
        @Getter
        private final List<ScorerSupplier> scorerSuppliers;
        private final HybridQueryWeight weight;
        private final ScoreMode scoreMode;
        private final LeafReaderContext context;

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
