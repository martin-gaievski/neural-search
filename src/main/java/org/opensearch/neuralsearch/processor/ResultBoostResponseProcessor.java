/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.core.action.ActionListener;
import org.opensearch.neuralsearch.processor.resultboost.DocumentBoost;
import org.opensearch.neuralsearch.processor.resultboost.ResultBoostConfig;
import org.opensearch.neuralsearch.query.ext.ResultBoostSearchExtBuilder;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.search.profile.SearchProfileShardResults;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;

/**
 * A SearchResponseProcessor that applies boost factors to specific documents
 * based on their document IDs (_id field). This processor runs AFTER the fetch
 * phase when document IDs are available, making it work in multi-node clusters.
 *
 * <p>The processor can receive boost configuration from:
 * <ul>
 *     <li>Pipeline configuration (static boosts)</li>
 *     <li>Query ext parameter (dynamic per-query boosts)</li>
 * </ul>
 *
 * <p>Usage in search pipeline:
 * <pre>
 * PUT _search/pipeline/my_pipeline
 * {
 *   "response_processors": [
 *     {
 *       "result_boost": {
 *         "boosts": [
 *           { "document_id": "doc1", "factor": 2.0, "type": "multiplicative" },
 *           { "document_id": "doc2", "factor": 1.5, "type": "multiplicative" }
 *         ]
 *       }
 *     }
 *   ]
 * }
 * </pre>
 *
 * <p>Or via query ext:
 * <pre>
 * GET my_index/_search
 * {
 *   "query": { "hybrid": { ... } },
 *   "ext": {
 *     "result_boost": {
 *       "boosts": [
 *         { "document_id": "doc1", "factor": 2.0 }
 *       ]
 *     }
 *   }
 * }
 * </pre>
 */
@Log4j2
public class ResultBoostResponseProcessor implements SearchResponseProcessor {

    public static final String TYPE = "result_boost";

    @Getter
    private final String tag;
    @Getter
    private final String description;
    @Getter
    private final boolean ignoreFailure;

    /**
     * Boost configuration from pipeline definition (static).
     */
    private final ResultBoostConfig pipelineBoostConfig;

    /**
     * Constructor for ResultBoostResponseProcessor.
     *
     * @param tag the processor tag
     * @param description the processor description
     * @param ignoreFailure whether to ignore failures
     * @param pipelineBoostConfig boost configuration from pipeline (can be null)
     */
    public ResultBoostResponseProcessor(String tag, String description, boolean ignoreFailure, ResultBoostConfig pipelineBoostConfig) {
        this.tag = tag;
        this.description = description;
        this.ignoreFailure = ignoreFailure;
        this.pipelineBoostConfig = pipelineBoostConfig;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) throws Exception {
        return applyBoosts(request, response);
    }

    @Override
    public void processResponseAsync(
        SearchRequest request,
        SearchResponse response,
        PipelineProcessingContext ctx,
        ActionListener<SearchResponse> responseListener
    ) {
        try {
            SearchResponse boostedResponse = applyBoosts(request, response);
            responseListener.onResponse(boostedResponse);
        } catch (Exception e) {
            log.error("Error applying result boosts", e);
            if (ignoreFailure) {
                responseListener.onResponse(response);
            } else {
                responseListener.onFailure(e);
            }
        }
    }

    /**
     * Apply boost factors to search results based on document IDs.
     *
     * @param request the search request
     * @param response the search response to modify
     * @return a new SearchResponse with boosted scores
     */
    private SearchResponse applyBoosts(SearchRequest request, SearchResponse response) {
        // Get boost configuration from query ext or pipeline config
        ResultBoostConfig effectiveConfig = getEffectiveBoostConfig(request);

        if (effectiveConfig == null || !effectiveConfig.hasBoosts()) {
            log.debug("No boost configuration found, returning original response");
            return response;
        }

        SearchHits originalHits = response.getHits();
        if (originalHits == null || originalHits.getHits() == null || originalHits.getHits().length == 0) {
            log.debug("No search hits to boost");
            return response;
        }

        Map<String, DocumentBoost> boostMap = effectiveConfig.toBoostMap();
        SearchHit[] hits = originalHits.getHits();
        SearchHit[] boostedHits = new SearchHit[hits.length];

        float newMaxScore = 0.0f;
        int boostsApplied = 0;

        // Apply boosts to matching documents
        for (int i = 0; i < hits.length; i++) {
            SearchHit hit = hits[i];
            boostedHits[i] = hit;

            if (hit == null || hit.getId() == null) {
                continue;
            }

            String docId = hit.getId();
            DocumentBoost boost = boostMap.get(docId);

            if (boost != null) {
                float originalScore = hit.getScore();
                float boostedScore = applyBoost(originalScore, boost);
                hit.score(boostedScore);
                boostsApplied++;

                log.debug(
                    "Boosted document '{}': {} -> {} (factor={}, type={})",
                    docId,
                    originalScore,
                    boostedScore,
                    boost.getFactor(),
                    boost.getType()
                );
            }

            if (!Float.isNaN(hit.getScore()) && hit.getScore() > newMaxScore) {
                newMaxScore = hit.getScore();
            }
        }

        if (boostsApplied == 0) {
            log.debug("No documents matched boost configuration");
            return response;
        }

        log.info("Applied {} boosts out of {} configured, new max score: {}", boostsApplied, boostMap.size(), newMaxScore);

        // Sort by boosted score (descending)
        Arrays.sort(boostedHits, Comparator.comparingDouble(SearchHit::getScore).reversed());

        // Create new SearchHits with updated scores and order
        SearchHits newSearchHits = new SearchHits(
            boostedHits,
            originalHits.getTotalHits() != null
                ? originalHits.getTotalHits()
                : new TotalHits(boostedHits.length, TotalHits.Relation.EQUAL_TO),
            newMaxScore
        );

        // Create new SearchResponse with boosted hits
        SearchResponseSections sections = new SearchResponseSections(
            newSearchHits,
            (InternalAggregations) response.getAggregations(),
            response.getSuggest(),
            response.isTimedOut(),
            response.isTerminatedEarly(),
            response.getProfileResults() != null ? new SearchProfileShardResults(response.getProfileResults()) : null,
            response.getNumReducePhases()
        );

        return new SearchResponse(
            sections,
            response.getScrollId(),
            response.getTotalShards(),
            response.getSuccessfulShards(),
            response.getSkippedShards(),
            response.getTook().millis(),
            response.getShardFailures(),
            response.getClusters()
        );
    }

    /**
     * Apply boost to a score based on boost configuration.
     *
     * @param score the original score
     * @param boost the boost configuration
     * @return the boosted score
     */
    private float applyBoost(float score, DocumentBoost boost) {
        if (Float.isNaN(score)) {
            return score;
        }

        switch (boost.getType()) {
            case MULTIPLICATIVE:
                return score * boost.getFactor();
            case ADDITIVE:
                return score + boost.getFactor();
            default:
                return score * boost.getFactor();
        }
    }

    /**
     * Get the effective boost configuration, preferring query ext over pipeline config.
     *
     * @param request the search request
     * @return the effective boost configuration
     */
    private ResultBoostConfig getEffectiveBoostConfig(SearchRequest request) {
        // First, try to get boosts from query ext (dynamic, per-query)
        if (request != null && request.source() != null && request.source().ext() != null) {
            List<SearchExtBuilder> extBuilders = request.source().ext();
            for (SearchExtBuilder extBuilder : extBuilders) {
                if (extBuilder instanceof ResultBoostSearchExtBuilder) {
                    ResultBoostSearchExtBuilder boostExt = (ResultBoostSearchExtBuilder) extBuilder;
                    ResultBoostConfig config = boostExt.getResultBoostConfig();
                    if (config != null && config.hasBoosts()) {
                        log.debug("Using boost configuration from query ext");
                        return config;
                    }
                }
            }
        }

        // Fall back to pipeline configuration (static)
        if (pipelineBoostConfig != null && pipelineBoostConfig.hasBoosts()) {
            log.debug("Using boost configuration from pipeline");
            return pipelineBoostConfig;
        }

        return null;
    }

    /**
     * Factory for creating ResultBoostResponseProcessor instances.
     */
    public static class Factory implements Processor.Factory<SearchResponseProcessor> {

        @Override
        public SearchResponseProcessor create(
            Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) throws Exception {
            ResultBoostConfig boostConfig = null;

            // Parse boost configuration from pipeline config
            if (config.containsKey("boosts")) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> boostsConfig = (List<Map<String, Object>>) config.remove("boosts");
                List<DocumentBoost> boosts = boostsConfig.stream().map(boostMap -> {
                    String documentId = (String) boostMap.get("document_id");
                    Number factorNum = (Number) boostMap.get("factor");
                    float factor = factorNum != null ? factorNum.floatValue() : 1.0f;
                    String typeStr = (String) boostMap.get("type");
                    DocumentBoost.BoostType type = typeStr != null
                        ? DocumentBoost.BoostType.valueOf(typeStr.toUpperCase())
                        : DocumentBoost.BoostType.MULTIPLICATIVE;
                    return DocumentBoost.builder().documentId(documentId).factor(factor).type(type).build();
                }).collect(Collectors.toList());

                boostConfig = new ResultBoostConfig(boosts);
            }

            return new ResultBoostResponseProcessor(tag, description, ignoreFailure, boostConfig);
        }
    }
}
