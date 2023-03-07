/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.search.query;

import static org.opensearch.search.query.TopDocsCollectorContext.createTopDocsCollectorContext;

import java.io.IOException;
import java.util.LinkedList;
import java.util.function.Supplier;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.util.CachedSupplier;
import org.opensearch.neuralsearch.query.CompoundQuery;
import org.opensearch.neuralsearch.search.CompoundTopScoreDocCollector;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;
import org.opensearch.neuralsearch.search.MultiQueryTopDocs;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QueryCollectorContext;
import org.opensearch.search.query.QueryPhase;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.TopDocsCollectorContext;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.sort.SortAndFormats;

public class CompoundQueryPhaseSearcher extends QueryPhase.DefaultQueryPhaseSearcher {

    private CompoundTopScoreDocCollector collector;
    private Supplier<TotalHits> totalHitsSupplier;
    private Supplier<TopDocs[]> topDocsSupplier;
    private Supplier<Float> maxScoreSupplier;
    protected SortAndFormats sortAndFormats;

    public boolean searchWith(
        SearchContext searchContext,
        ContextIndexSearcher searcher,
        Query query,
        LinkedList<QueryCollectorContext> collectors,
        boolean hasFilterCollector,
        boolean hasTimeout
    ) throws IOException {
        if (query instanceof CompoundQuery) {
            return searchWithCollector(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
        }
        return super.searchWithCollector(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
    }

    protected boolean searchWithCollector(
        SearchContext searchContext,
        ContextIndexSearcher searcher,
        Query query,
        LinkedList<QueryCollectorContext> collectors,
        boolean hasFilterCollector,
        boolean hasTimeout
    ) throws IOException {

        final TopDocsCollectorContext topDocsFactory = createTopDocsCollectorContext(searchContext, hasFilterCollector);
        collectors.addFirst(topDocsFactory);

        IndexReader reader = searchContext.searcher().getIndexReader();
        int totalNumDocs = Math.max(1, reader.numDocs());
        if (searchContext.size() == 0) {
            throw new UnsupportedOperationException("Need to create no hits collector");
        }
        int numDocs = Math.min(searchContext.from() + searchContext.size(), totalNumDocs);
        final boolean rescore = !searchContext.rescore().isEmpty();
        if (rescore) {
            assert searchContext.sort() == null;
            for (RescoreContext rescoreContext : searchContext.rescore()) {
                numDocs = Math.max(numDocs, rescoreContext.getWindowSize());
            }
        }

        QuerySearchResult queryResult = searchContext.queryResult();

        // TODO Wrap collector into context
        // Problems:
        // - incorrect score for multiple shards
        collector = new CompoundTopScoreDocCollector(
            numDocs,
            HitsThresholdChecker.create(Math.max(numDocs, searchContext.trackTotalHitsUpTo())),
            null
        );
        topDocsSupplier = new CachedSupplier<>(collector::topDocs);
        totalHitsSupplier = () -> {
            TopDocs[] topDocs = topDocsSupplier.get();
            // for now we do max from each sub-query result
            long numHits = 0;
            for (TopDocs td : topDocs) {
                numHits = Math.max(numHits, td.totalHits.value);
            }
            /*for(Map.Entry<Query, TopDocs> entry: topDocs.entrySet()) {
                numHits = Math.max(numHits, entry.getValue().totalHits.value);
            }*/
            return new TotalHits(numHits, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
            // return topDocs.get(topDocs.keySet().iterator().next()).totalHits;
        };// topDocsSupplier.get().totalHits;
        maxScoreSupplier = () -> {
            TopDocs[] topDocs = topDocsSupplier.get();
            // TODO review later, need to came up with valid max score
            // if (topDocs.scoreDocs.length == 0) {
            if (topDocs.length == 0) {
                return Float.NaN;
            } else {
                // return topDocs.scoreDocs[0].score;
                TopDocs docs = topDocs[0];
                return docs.scoreDocs.length == 0 ? Float.NaN : docs.scoreDocs[0].score;
            }
        };
        sortAndFormats = searchContext.sort();

        searcher.search(query, collector);

        if (searchContext.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER && queryResult.terminatedEarly() == null) {
            queryResult.terminatedEarly(false);
        }

        postProcess(queryResult, collector);

        return rescore;
    }

    void postProcess(QuerySearchResult queryResult, CompoundTopScoreDocCollector collector) {
        final TopDocsAndMaxScore topDocs = newTopDocs();
        queryResult.topDocs(topDocs, sortAndFormats == null ? null : sortAndFormats.formats);
    }

    TopDocsAndMaxScore newTopDocs() {
        TopDocs[] in = topDocsSupplier.get();
        float maxScore = maxScoreSupplier.get();

        /*if (in instanceof TopFieldDocs) {
            TopFieldDocs fieldDocs = (TopFieldDocs) in;
            newTopDocs = new TopFieldDocs(totalHitsSupplier.get(), fieldDocs.scoreDocs, fieldDocs.fields);
        } else {
            newTopDocs = new TopDocs(totalHitsSupplier.get(), in.scoreDocs);
        }*/
        final TopDocs newTopDocs = new MultiQueryTopDocs(totalHitsSupplier.get(), in);
        return new TopDocsAndMaxScore(newTopDocs, maxScore);
    }
}
