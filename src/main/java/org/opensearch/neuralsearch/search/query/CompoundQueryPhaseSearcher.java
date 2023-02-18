/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.search.query;

import java.io.IOException;
import java.util.LinkedList;
import java.util.function.Supplier;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.util.CachedSupplier;
import org.opensearch.neuralsearch.search.CompoundTopScoreDocCollector;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QueryCollectorContext;
import org.opensearch.search.query.QueryPhase;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.sort.SortAndFormats;

public class CompoundQueryPhaseSearcher extends QueryPhase.DefaultQueryPhaseSearcher {

    private CompoundTopScoreDocCollector collector;
    private Supplier<TotalHits> totalHitsSupplier;
    private Supplier<TopDocs> topDocsSupplier;
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
        return searchWithCollector(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
    }

    protected boolean searchWithCollector(
        SearchContext searchContext,
        ContextIndexSearcher searcher,
        Query query,
        LinkedList<QueryCollectorContext> collectors,
        boolean hasFilterCollector,
        boolean hasTimeout
    ) throws IOException {
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
        // - incorrect score for multiple sub-queries for docs that match both
        // - for multiple shards failing with exception, need to implement reduce in collector
        collector = new CompoundTopScoreDocCollector(
            numDocs,
            HitsThresholdChecker.create(Math.max(numDocs, searchContext.trackTotalHitsUpTo())),
            null
        );
        topDocsSupplier = new CachedSupplier<>(collector::topDocs);
        totalHitsSupplier = () -> topDocsSupplier.get().totalHits;
        maxScoreSupplier = () -> {
            TopDocs topDocs = topDocsSupplier.get();
            if (topDocs.scoreDocs.length == 0) {
                return Float.NaN;
            } else {
                return topDocs.scoreDocs[0].score;
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
        TopDocs in = topDocsSupplier.get();
        float maxScore = maxScoreSupplier.get();
        final TopDocs newTopDocs;
        if (in instanceof TopFieldDocs) {
            TopFieldDocs fieldDocs = (TopFieldDocs) in;
            newTopDocs = new TopFieldDocs(totalHitsSupplier.get(), fieldDocs.scoreDocs, fieldDocs.fields);
        } else {
            newTopDocs = new TopDocs(totalHitsSupplier.get(), in.scoreDocs);
        }
        return new TopDocsAndMaxScore(newTopDocs, maxScore);
    }
}
