/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createDelimiterElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createStartStopElementForHybridSearchResults;
import static org.opensearch.search.query.TopDocsCollectorContext.createTopDocsCollectorContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import lombok.AllArgsConstructor;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.search.NestedHelper;
import org.opensearch.neuralsearch.query.HybridQuery;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;
import org.opensearch.neuralsearch.search.HybridTopScoreDocCollector;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QueryCollectorContext;
import org.opensearch.search.query.QueryPhase;
import org.opensearch.search.query.QueryPhaseExecutionException;
import org.opensearch.search.query.QueryPhaseSearcherWrapper;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.ReduceableSearchResult;
import org.opensearch.search.query.TopDocsCollectorContext;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.sort.SortAndFormats;

import com.google.common.annotations.VisibleForTesting;

import lombok.extern.log4j.Log4j2;

/**
 * Custom search implementation to be used at {@link QueryPhase} for Hybrid Query search. For queries other than Hybrid the
 * upstream standard implementation of searcher is called.
 */
@Log4j2
public class HybridQueryPhaseSearcher extends QueryPhaseSearcherWrapper {

    public HybridQueryPhaseSearcher() {
        super();
    }

    public boolean searchWith(
        final SearchContext searchContext,
        final ContextIndexSearcher searcher,
        final Query query,
        final LinkedList<QueryCollectorContext> collectors,
        final boolean hasFilterCollector,
        final boolean hasTimeout
    ) throws IOException {
        if (!isHybridQuery(query, searchContext)) {
            validateQuery(searchContext, query);
        }
        return super.searchWith(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
    }

    private boolean isHybridQuery(final Query query, final SearchContext searchContext) {
        if (query instanceof HybridQuery) {
            return true;
        } else if (isWrappedHybridQuery(query) && hasNestedFieldOrNestedDocs(query, searchContext)) {
            /* Checking if this is a hybrid query that is wrapped into a Bool query by core Opensearch code
            https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/search/DefaultSearchContext.java#L367-L370.
            main reason for that is performance optimization, at time of writing we are ok with loosing on performance if that's unblocks
            hybrid query for indexes with nested field types.
            in such case we consider query a valid hybrid query. Later in the code we will extract it and execute as a main query for
            this search request.
            below is sample structure of such query:

            Boolean {
               should: {
                   hybrid: {
                       sub_query1 {}
                       sub_query2 {}
                   }
               }
               filter: {
                   exists: {
                       field: "_primary_term"
                   }
               }
            }
            TODO Need to add logic for passing hybrid sub-queries through the same logic in core to ensure there is no latency regression */
            // we have already checked if query in instance of Boolean in higher level else if condition
            return ((BooleanQuery) query).clauses()
                .stream()
                .filter(clause -> clause.getQuery() instanceof HybridQuery == false)
                .allMatch(clause -> {
                    return clause.getOccur() == BooleanClause.Occur.FILTER
                        && clause.getQuery() instanceof FieldExistsQuery
                        && SeqNoFieldMapper.PRIMARY_TERM_NAME.equals(((FieldExistsQuery) clause.getQuery()).getField());
                });
        }
        return false;
    }

    private boolean hasNestedFieldOrNestedDocs(final Query query, final SearchContext searchContext) {
        return searchContext.mapperService().hasNested() && new NestedHelper(searchContext.mapperService()).mightMatchNestedDocs(query);
    }

    private boolean isWrappedHybridQuery(final Query query) {
        return query instanceof BooleanQuery
            && ((BooleanQuery) query).clauses().stream().anyMatch(clauseQuery -> clauseQuery.getQuery() instanceof HybridQuery);
    }

    /**
     * Validate the query from neural-search plugin point of view. Current main goal for validation is to block cases
     * when hybrid query is wrapped into other compound queries.
     * For example, if we have Bool query like below we need to throw an error
     * bool: {
     *   should: [
     *      match: {},
     *      hybrid: {
     *        sub_query1 {}
     *        sub_query2 {}
     *      }
     *   ]
     * }
     * TODO add similar validation for other compound type queries like dis_max, constant_score etc.
     *
     * @param query query to validate
     */
    private void validateQuery(final SearchContext searchContext, final Query query) {
        if (query instanceof BooleanQuery) {
            List<BooleanClause> booleanClauses = ((BooleanQuery) query).clauses();
            for (BooleanClause booleanClause : booleanClauses) {
                validateNestedBooleanQuery(booleanClause.getQuery(), getMaxDepthLimit(searchContext));
            }
        }
    }

    private void validateNestedBooleanQuery(final Query query, final int level) {
        if (query instanceof HybridQuery) {
            throw new IllegalArgumentException("hybrid query must be a top level query and cannot be wrapped into other queries");
        }
        if (level <= 0) {
            // ideally we should throw an error here but this code is on the main search workflow path and that might block
            // execution of some queries. Instead, we're silently exit and allow such query to execute and potentially produce incorrect
            // results in case hybrid query is wrapped into such bool query
            log.error("reached max nested query limit, cannot process bool query with that many nested clauses");
            return;
        }
        if (query instanceof BooleanQuery) {
            for (BooleanClause booleanClause : ((BooleanQuery) query).clauses()) {
                validateNestedBooleanQuery(booleanClause.getQuery(), level - 1);
            }
        }
    }

    private int getMaxDepthLimit(final SearchContext searchContext) {
        Settings indexSettings = searchContext.getQueryShardContext().getIndexSettings().getSettings();
        return MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.get(indexSettings).intValue();
    }

    @Override
    public AggregationProcessor aggregationProcessor(SearchContext searchContext) {
        AggregationProcessor coreAggProcessor = super.aggregationProcessor(searchContext);
        return new HybridAggregationProcessor(coreAggProcessor);
    }

    @AllArgsConstructor
    public class HybridAggregationProcessor implements AggregationProcessor {

        private final AggregationProcessor delegateAggsProcessor;

        @Override
        public void preProcess(SearchContext context) {
            delegateAggsProcessor.preProcess(context);

            if (isHybridQuery(context.query(), context)) {
                // adding collector manager for hybrid query
                CollectorManager collectorManager;
                try {
                    collectorManager = HybridCollectorManager.createHybridCollectorManager(context);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> collectorManagersByManagerClass = context
                    .queryCollectorManagers();
                collectorManagersByManagerClass.put(HybridCollectorManager.class, collectorManager);
            }
        }

        @Override
        public void postProcess(SearchContext context) {
            if (isHybridQuery(context.query(), context)) {
                if (!context.shouldUseConcurrentSearch()) {
                    reduceCollectorResults(context);
                }
                updateQueryResult(context.queryResult(), context);
            }

            delegateAggsProcessor.postProcess(context);
        }

        private void reduceCollectorResults(SearchContext context) {
            CollectorManager<?, ReduceableSearchResult> collectorManager = context.queryCollectorManagers()
                .get(HybridCollectorManager.class);
            try {
                final Collection collectors = List.of(collectorManager.newCollector());
                collectorManager.reduce(collectors).reduce(context.queryResult());
            } catch (IOException e) {
                throw new QueryPhaseExecutionException(context.shardTarget(), "failed to execute hybrid query aggregation processor", e);
            }
        }

        private void updateQueryResult(final QuerySearchResult queryResult, final SearchContext searchContext) {
            boolean isSingleShard = searchContext.numberOfShards() == 1;
            if (isSingleShard) {
                searchContext.size(queryResult.queryResult().topDocs().topDocs.scoreDocs.length);
            }
        }
    }
}
