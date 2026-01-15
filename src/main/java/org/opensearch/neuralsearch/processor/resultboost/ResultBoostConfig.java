/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.resultboost;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration container for post-normalization result boosting.
 * Parses the "ext" -> "result_boost" section from search request.
 *
 * Example usage in search request:
 * <pre>
 * {
 *   "query": { ... },
 *   "ext": {
 *     "result_boost": {
 *       "boosts": [
 *         { "document_id": "PROMO-12345", "factor": 3.0 },
 *         { "document_id": "FEATURED-789", "factor": 2.5, "type": "additive" }
 *       ]
 *     }
 *   }
 * }
 * </pre>
 */
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Log4j2
public class ResultBoostConfig {

    public static final String EXT_KEY = "result_boost";
    public static final String BOOSTS_KEY = "boosts";
    public static final String DOCUMENT_ID_KEY = "document_id";
    public static final String FACTOR_KEY = "factor";
    public static final String TYPE_KEY = "type";

    /**
     * List of document boosts to apply.
     */
    @Builder.Default
    private List<DocumentBoost> boosts = Collections.emptyList();

    /**
     * Whether this config is enabled/has any boosts.
     */
    public boolean hasBoosts() {
        return boosts != null && !boosts.isEmpty();
    }

    /**
     * Creates a map of document ID to boost configuration for efficient lookup.
     * @return Map of document ID to DocumentBoost
     */
    public Map<String, DocumentBoost> toBoostMap() {
        if (!hasBoosts()) {
            return Collections.emptyMap();
        }
        Map<String, DocumentBoost> map = new HashMap<>(boosts.size());
        for (DocumentBoost boost : boosts) {
            map.put(boost.getDocumentId(), boost);
        }
        return map;
    }

    /**
     * Validates the configuration.
     * @throws IllegalArgumentException if the configuration is invalid
     */
    public void validate() {
        if (boosts == null) {
            return;
        }
        for (DocumentBoost boost : boosts) {
            boost.validate();
        }
    }

    /**
     * Parse result_boost configuration from ext section of search request.
     * @param extContent the ext section content as a Map
     * @return ResultBoostConfig or null if not present
     */
    @SuppressWarnings("unchecked")
    public static ResultBoostConfig fromExtContent(Map<String, Object> extContent) {
        if (extContent == null || !extContent.containsKey(EXT_KEY)) {
            return null;
        }

        Object resultBoostObj = extContent.get(EXT_KEY);
        if (!(resultBoostObj instanceof Map)) {
            log.warn("result_boost in ext is not a map, ignoring");
            return null;
        }

        Map<String, Object> resultBoostMap = (Map<String, Object>) resultBoostObj;

        if (!resultBoostMap.containsKey(BOOSTS_KEY)) {
            log.warn("result_boost does not contain boosts array, ignoring");
            return null;
        }

        Object boostsObj = resultBoostMap.get(BOOSTS_KEY);
        if (!(boostsObj instanceof List)) {
            log.warn("boosts in result_boost is not a list, ignoring");
            return null;
        }

        List<Object> boostsList = (List<Object>) boostsObj;
        List<DocumentBoost> documentBoosts = new ArrayList<>(boostsList.size());

        for (Object boostObj : boostsList) {
            if (!(boostObj instanceof Map)) {
                log.warn("boost entry is not a map, skipping");
                continue;
            }

            Map<String, Object> boostMap = (Map<String, Object>) boostObj;

            String documentId = getStringValue(boostMap, DOCUMENT_ID_KEY);
            if (documentId == null) {
                log.warn("boost entry missing document_id, skipping");
                continue;
            }

            float factor = getFloatValue(boostMap, FACTOR_KEY, 1.0f);
            DocumentBoost.BoostType type = parseBoostType(boostMap);

            DocumentBoost boost = DocumentBoost.builder().documentId(documentId).factor(factor).type(type).build();

            documentBoosts.add(boost);
        }

        if (documentBoosts.isEmpty()) {
            return null;
        }

        ResultBoostConfig config = ResultBoostConfig.builder().boosts(documentBoosts).build();

        config.validate();
        log.debug("Parsed result_boost config with {} boosts", documentBoosts.size());

        return config;
    }

    private static String getStringValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) {
            return null;
        }
        return value.toString();
    }

    private static float getFloatValue(Map<String, Object> map, String key, float defaultValue) {
        Object value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        try {
            return Float.parseFloat(value.toString());
        } catch (NumberFormatException e) {
            log.warn("Could not parse {} as float: {}", key, value);
            return defaultValue;
        }
    }

    private static DocumentBoost.BoostType parseBoostType(Map<String, Object> boostMap) {
        String typeStr = getStringValue(boostMap, TYPE_KEY);
        if (typeStr == null) {
            return DocumentBoost.BoostType.MULTIPLICATIVE;
        }
        try {
            return DocumentBoost.BoostType.valueOf(typeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            log.warn("Unknown boost type: {}, using MULTIPLICATIVE", typeStr);
            return DocumentBoost.BoostType.MULTIPLICATIVE;
        }
    }
}
