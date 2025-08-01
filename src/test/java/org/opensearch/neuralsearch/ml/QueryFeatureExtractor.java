/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

/**
 * Extracts features from query strings for ML model input
 */
public class QueryFeatureExtractor {
    private static final Pattern NUMERIC_PATTERN = Pattern.compile(".*\\d.*");
    private static final Pattern SPECIAL_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9\\s]");

    /**
     * Extract simple features from query string for ML model input
     */
    public Map<String, Double> extractFeatures(String queryText) {
        Map<String, Double> features = new HashMap<>();

        features.put("query_length", (double) queryText.length());
        features.put("token_count", (double) tokenCount(queryText));
        features.put("has_numbers", NUMERIC_PATTERN.matcher(queryText).matches() ? 1.0 : 0.0);
        features.put("has_special_chars", SPECIAL_CHAR_PATTERN.matcher(queryText).find() ? 1.0 : 0.0);

        return features;
    }

    private int tokenCount(String text) {
        StringTokenizer tokenizer = new StringTokenizer(text);
        return tokenizer.countTokens();
    }
}
