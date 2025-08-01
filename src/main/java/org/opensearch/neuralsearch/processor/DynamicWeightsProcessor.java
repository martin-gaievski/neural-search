/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.isHybridQueryStartStopElement;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.opensearch.action.search.QueryPhaseResultConsumer;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchPhaseName;
import org.opensearch.action.search.SearchPhaseResults;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.neuralsearch.transport.DynamicWeightsMetadata;

import lombok.extern.log4j.Log4j2;

/**
 * Processor for dynamic weight prediction based on query features.
 * This processor extracts features from the query, predicts weights using stored model coefficients,
 * and passes them to the next processor (typically normalization processor).
 */
@Log4j2
public class DynamicWeightsProcessor extends AbstractScoreHybridizationProcessor {
    public static final String TYPE = "dynamic-weights-processor";
    public static final String DYNAMIC_WEIGHTS_KEY = "dynamic_hybrid_weights";

    private final String tag;
    private final String description;
    private final ClusterService clusterService;

    public DynamicWeightsProcessor(String tag, String description, ClusterService clusterService) {
        this.tag = tag;
        this.description = description;
        this.clusterService = clusterService;
    }

    @Override
    <Result extends SearchPhaseResult> void hybridizeScores(
        SearchPhaseResults<Result> searchPhaseResult,
        SearchPhaseContext searchPhaseContext,
        Optional<PipelineProcessingContext> requestContextOptional
    ) {
        if (shouldSkipProcessor(searchPhaseResult)) {
            log.debug("Query results are not compatible with dynamic weights processor");
            return;
        }

        try {
            // Extract query string from search request
            String queryString = extractQueryString(searchPhaseContext);
            if (queryString == null || queryString.trim().isEmpty()) {
                log.debug("No query string found, skipping dynamic weights");
                return;
            }

            // Extract features from query
            Map<String, Double> features = extractFeatures(queryString);

            // Predict weights using stored model coefficients
            List<Float> predictedWeights = predictWeights(features);

            if (predictedWeights != null && !predictedWeights.isEmpty()) {
                // Store weights in pipeline context for normalization processor
                if (requestContextOptional.isPresent()) {
                    PipelineProcessingContext context = requestContextOptional.get();
                    context.setAttribute(DYNAMIC_WEIGHTS_KEY, predictedWeights);
                    log.debug("Predicted dynamic weights: {}", predictedWeights);
                }
            }

        } catch (Exception e) {
            log.error("Error in dynamic weights processor", e);
            // Don't fail the search, just continue without dynamic weights
        }
    }

    private String extractQueryString(SearchPhaseContext searchPhaseContext) {
        try {
            // Extract query string from search request
            if (searchPhaseContext.getRequest() != null
                && searchPhaseContext.getRequest().source() != null
                && searchPhaseContext.getRequest().source().query() != null) {

                // Get the query as a string representation
                String queryStr = searchPhaseContext.getRequest().source().query().toString();
                log.debug("Full query string: {}", queryStr);

                // Try multiple patterns to extract query text

                // Pattern 1: Look for match query pattern like "match":{..."query":"<text>"...}
                if (queryStr.contains("match") && queryStr.contains("query")) {
                    // Find the query value after "query":"
                    int queryIndex = queryStr.indexOf("\"query\"");
                    if (queryIndex > 0) {
                        int colonIndex = queryStr.indexOf(":", queryIndex);
                        if (colonIndex > 0) {
                            int startQuote = queryStr.indexOf("\"", colonIndex);
                            if (startQuote > 0) {
                                int endQuote = queryStr.indexOf("\"", startQuote + 1);
                                if (endQuote > startQuote) {
                                    String extracted = queryStr.substring(startQuote + 1, endQuote);
                                    log.debug("Extracted query text: {}", extracted);
                                    return extracted;
                                }
                            }
                        }
                    }
                }

                // Pattern 2: Look for any text between quotes after "content":"
                if (queryStr.contains("content")) {
                    int contentIndex = queryStr.lastIndexOf("\"content\"");
                    if (contentIndex > 0) {
                        int colonIndex = queryStr.indexOf(":", contentIndex);
                        if (colonIndex > 0) {
                            int startQuote = queryStr.indexOf("\"", colonIndex);
                            if (startQuote > 0) {
                                int endQuote = queryStr.indexOf("\"", startQuote + 1);
                                if (endQuote > startQuote) {
                                    String extracted = queryStr.substring(startQuote + 1, endQuote);
                                    log.debug("Extracted content text: {}", extracted);
                                    return extracted;
                                }
                            }
                        }
                    }
                }

                // If we couldn't extract, log the query structure
                log.debug("Could not extract query text from: {}", queryStr);
            }
        } catch (Exception e) {
            log.debug("Failed to extract query string: {}", e.getMessage(), e);
        }
        return null;
    }

    private Map<String, Double> extractFeatures(String queryString) {
        // Extract all 9 features that the model expects
        String lowerQuery = queryString.toLowerCase();
        String[] tokens = queryString.split("\\s+");

        // Feature 1: query_length
        double queryLength = (double) queryString.length();

        // Feature 2: token_count
        double tokenCount = (double) tokens.length;

        // Feature 3: has_special_chars
        double hasSpecialChars = queryString.matches(".*[^a-zA-Z0-9\\s].*") ? 1.0 : 0.0;

        // Feature 4: has_size_terms
        double hasSizeTerms = 0.0;
        String[] sizeTerms = { "small", "medium", "large", "xl", "xxl", "xs", "size", "big", "tiny", "huge" };
        for (String term : sizeTerms) {
            if (lowerQuery.contains(term)) {
                hasSizeTerms = 1.0;
                break;
            }
        }

        // Feature 5: contains_brand_keywords
        double containsBrandKeywords = 0.0;
        // Common brand patterns: capitalized words, all caps, known brands
        for (String token : tokens) {
            if (token.length() > 2 && (token.equals(token.toUpperCase()) || // All caps (NIKE, SONY)
                Character.isUpperCase(token.charAt(0)))) { // Capitalized (Apple, Samsung)
                containsBrandKeywords = 1.0;
                break;
            }
        }

        // Feature 6: has_currency
        double hasCurrency = 0.0;
        if (queryString.matches(".*[\\$€£¥₹].*") || lowerQuery.matches(".*(dollar|euro|pound|yen|rupee|usd|eur|gbp).*")) {
            hasCurrency = 1.0;
        }

        // Feature 7: has_numbers
        double hasNumbers = queryString.matches(".*\\d.*") ? 1.0 : 0.0;

        // Feature 8: is_product_search
        double isProductSearch = 0.0;
        String[] productTerms = {
            "buy",
            "price",
            "cost",
            "cheap",
            "expensive",
            "sale",
            "discount",
            "deal",
            "review",
            "best",
            "top",
            "vs",
            "versus" };
        for (String term : productTerms) {
            if (lowerQuery.contains(term)) {
                isProductSearch = 1.0;
                break;
            }
        }
        // Also check for model numbers, product codes
        if (hasNumbers == 1.0 && containsBrandKeywords == 1.0) {
            isProductSearch = 1.0;
        }

        // Feature 9: has_sku_pattern
        double hasSkuPattern = 0.0;
        // Common SKU patterns: ABC-123, SKU12345, PROD_456, etc.
        if (queryString.matches(".*[A-Z]{2,}[-_]?\\d{2,}.*")
            || queryString.matches(".*\\d{2,}[-_]?[A-Z]{2,}.*")
            || lowerQuery.matches(".*(sku|asin|isbn|ean|upc|model).*\\d+.*")) {
            hasSkuPattern = 1.0;
        }

        // Return all 9 features in a HashMap
        Map<String, Double> features = new HashMap<>();
        features.put("query_length", queryLength);
        features.put("token_count", tokenCount);
        features.put("has_special_chars", hasSpecialChars);
        features.put("has_size_terms", hasSizeTerms);
        features.put("contains_brand_keywords", containsBrandKeywords);
        features.put("has_currency", hasCurrency);
        features.put("has_numbers", hasNumbers);
        features.put("is_product_search", isProductSearch);
        features.put("has_sku_pattern", hasSkuPattern);

        log.debug("Extracted features for '{}': {}", queryString, features);

        return features;
    }

    private List<Float> predictWeights(Map<String, Double> features) {
        // Load coefficients from cluster metadata
        DynamicWeightsMetadata metadata = DynamicWeightsMetadata.fromClusterState(clusterService.state());

        if (metadata == null) {
            log.debug("No dynamic weights metadata found in cluster state, using default weights");
            return List.of(0.5f, 0.5f); // Default equal weights
        }

        double intercept = metadata.getIntercept();
        Map<String, Double> coefficients = metadata.getCoefficients();

        log.debug("Using model type: {} with intercept: {} and {} coefficients", metadata.getModelType(), intercept, coefficients.size());

        // Calculate prediction: intercept + sum(coefficient * feature_value)
        double prediction = intercept;
        for (Map.Entry<String, Double> entry : coefficients.entrySet()) {
            String featureName = entry.getKey();
            Double coefficient = entry.getValue();
            Double featureValue = features.get(featureName);

            if (featureValue != null) {
                prediction += coefficient * featureValue;
            }
        }

        // Ensure prediction is between 0 and 1
        prediction = Math.max(0.0, Math.min(1.0, prediction));

        // For hybrid search with 2 sub-queries (lexical and neural)
        float neuralWeight = (float) prediction;
        float lexicalWeight = 1.0f - neuralWeight;

        return List.of(lexicalWeight, neuralWeight);
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
    public String getTag() {
        return tag;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public boolean isIgnoreFailure() {
        return true; // Don't fail search if dynamic weights fail
    }

    private <Result extends SearchPhaseResult> boolean shouldSkipProcessor(SearchPhaseResults<Result> searchPhaseResult) {
        if (Objects.isNull(searchPhaseResult) || !(searchPhaseResult instanceof QueryPhaseResultConsumer)) {
            return true;
        }

        QueryPhaseResultConsumer queryPhaseResultConsumer = (QueryPhaseResultConsumer) searchPhaseResult;
        return queryPhaseResultConsumer.getAtomicArray().asList().stream().filter(Objects::nonNull).noneMatch(this::isHybridQuery);
    }

    private boolean isHybridQuery(final SearchPhaseResult searchPhaseResult) {
        return Objects.nonNull(searchPhaseResult.queryResult())
            && Objects.nonNull(searchPhaseResult.queryResult().topDocs())
            && Objects.nonNull(searchPhaseResult.queryResult().topDocs().topDocs.scoreDocs)
            && searchPhaseResult.queryResult().topDocs().topDocs.scoreDocs.length > 0
            && isHybridQueryStartStopElement(searchPhaseResult.queryResult().topDocs().topDocs.scoreDocs[0]);
    }
}
