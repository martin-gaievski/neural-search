/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.rrf;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.opensearch.action.search.QueryPhaseResultConsumer;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchPhaseName;
import org.opensearch.action.search.SearchPhaseResults;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;
import org.opensearch.search.query.QuerySearchResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.isHybridQueryStartStopElement;

@AllArgsConstructor
@Log4j2
public class RRFProcessor implements SearchPhaseResultsProcessor {

    public static final String TYPE = "rrf-processor";
    @Getter
    private final String tag;
    @Getter
    private final String description;

    @Override
    public <Result extends SearchPhaseResult> void process(SearchPhaseResults<Result> searchPhaseResult, SearchPhaseContext searchPhaseContext) {
        if (shouldSkipProcessor(searchPhaseResult)) {
            log.debug("Query results are not compatible with normalization processor");
            return;
        }
        List<QuerySearchResult> querySearchResults = getQueryPhaseSearchResults(searchPhaseResult);
        //unwrapped top docs per shard
        List<CompoundTopDocs> queryTopDocs = getQueryTopDocs(querySearchResults);
        int numOfSubQueries = queryTopDocs.get(0).getTopDocs().size();
        //put together list of results for each sub query. results are sorted by score
        List<List<ScoreDoc>> scoreDocsPerSubquery = new ArrayList<>();
        for ( int i = 0; i < numOfSubQueries; i++) {
            scoreDocsPerSubquery.add(new ArrayList<>());
        }
        //put all score docs in list
        for (CompoundTopDocs compoundTopDocs : queryTopDocs) {
            List<TopDocs> topDocsPerSubQuery = compoundTopDocs.getTopDocs();
            for (int subQueryIndex = 0; subQueryIndex < topDocsPerSubQuery.size(); subQueryIndex++) {
                TopDocs topDocs = topDocsPerSubQuery.get(subQueryIndex);
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    scoreDocsPerSubquery.get(subQueryIndex).add(scoreDoc);
                }
            }
        }
        // sort lists with score docs
        for (List<ScoreDoc> scoreDocs : scoreDocsPerSubquery) {
            scoreDocs.sort((x, y) -> Float.compare(y.score, x.score));
        }
        Map<Integer, Double> docRanks = new HashMap<>();
        for (List<ScoreDoc> scoreDocs : scoreDocsPerSubquery) {
            for (int subQueryRank = 1; subQueryRank <= scoreDocs.size(); subQueryRank++) {
                ScoreDoc scoreDoc = scoreDocs.get(subQueryRank - 1);
                double rank = docRanks.getOrDefault(scoreDoc.doc, 0.0);
                rank += (double) 1 / subQueryRank;
                docRanks.put(scoreDoc.doc, rank);
            }
        }

        List<Map.Entry<Integer, Double>> sortedResults = new ArrayList<>(docRanks.entrySet());
        sortedResults.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));

        for (int index = 0; index < querySearchResults.size(); index++) {
            QuerySearchResult querySearchResult = querySearchResults.get(index);
            Map.Entry<Integer, Double> sortedScoreDocs = sortedResults.get(index);
            //float maxScore = updatedTopDocs.getTotalHits().value > 0 ? updatedTopDocs.getScoreDocs().get(0).score : 0.0f;

            // create final version of top docs with all updated values
            TopDocs topDocs = new TopDocs(updatedTopDocs.getTotalHits(), updatedTopDocs.getScoreDocs().toArray(new ScoreDoc[0]));

            TopDocsAndMaxScore updatedTopDocsAndMaxScore = new TopDocsAndMaxScore(topDocs, maxScore);
            querySearchResult.topDocs(updatedTopDocsAndMaxScore, null);
        }
    }

    private <Result extends SearchPhaseResult> boolean shouldSkipProcessor(SearchPhaseResults<Result> searchPhaseResult) {
        if (Objects.isNull(searchPhaseResult) || !(searchPhaseResult instanceof QueryPhaseResultConsumer)) {
            return true;
        }

        QueryPhaseResultConsumer queryPhaseResultConsumer = (QueryPhaseResultConsumer) searchPhaseResult;
        return queryPhaseResultConsumer.getAtomicArray().asList().stream().filter(Objects::nonNull).noneMatch(this::isHybridQuery);
    }

    /**
     * Return true if results are from hybrid query.
     * @param searchPhaseResult
     * @return true if results are from hybrid query
     */
    private boolean isHybridQuery(final SearchPhaseResult searchPhaseResult) {
        // check for delimiter at the end of the score docs.
        return Objects.nonNull(searchPhaseResult.queryResult())
                && Objects.nonNull(searchPhaseResult.queryResult().topDocs())
                && Objects.nonNull(searchPhaseResult.queryResult().topDocs().topDocs.scoreDocs)
                && searchPhaseResult.queryResult().topDocs().topDocs.scoreDocs.length > 0
                && isHybridQueryStartStopElement(searchPhaseResult.queryResult().topDocs().topDocs.scoreDocs[0]);
    }

    private <Result extends SearchPhaseResult> List<QuerySearchResult> getQueryPhaseSearchResults(
            final SearchPhaseResults<Result> results
    ) {
        return results.getAtomicArray()
                .asList()
                .stream()
                .map(result -> result == null ? null : result.queryResult())
                .collect(Collectors.toList());
    }

    private List<CompoundTopDocs> getQueryTopDocs(final List<QuerySearchResult> querySearchResults) {
        List<CompoundTopDocs> queryTopDocs = querySearchResults.stream()
                .filter(searchResult -> Objects.nonNull(searchResult.topDocs()))
                .map(querySearchResult -> querySearchResult.topDocs().topDocs)
                .map(CompoundTopDocs::new)
                .collect(Collectors.toList());
        if (queryTopDocs.size() != querySearchResults.size()) {
            throw new IllegalStateException(
                    String.format(
                            Locale.ROOT,
                            "query results were not formatted correctly by the hybrid query; sizes of querySearchResults [%d] and queryTopDocs [%d] must match",
                            querySearchResults.size(),
                            queryTopDocs.size()
                    )
            );
        }
        return queryTopDocs;
    }

    @Override
    public SearchPhaseName getBeforePhase() {
        return SearchPhaseName.QUERY;
    }

    @Override
    public SearchPhaseName getAfterPhase() {
        return SearchPhaseName.FETCH;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public boolean isIgnoreFailure() {
        return false;
    }
}
