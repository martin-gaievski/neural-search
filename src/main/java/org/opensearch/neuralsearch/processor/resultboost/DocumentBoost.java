/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.resultboost;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Represents a boost configuration for a specific document.
 * This is used to promote specific documents in hybrid search results
 * after score normalization and combination.
 */
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
public class DocumentBoost {

    /**
     * The document ID (_id field value) to apply the boost to.
     * This is the actual OpenSearch document ID, not the Lucene internal doc ID.
     */
    private String documentId;

    /**
     * The boost factor to apply to the document's combined score.
     * For multiplicative boost: final_score = combined_score * factor
     * Must be positive. Values > 1.0 promote the document, values < 1.0 demote it.
     */
    private float factor;

    /**
     * The type of boost to apply. Default is MULTIPLICATIVE.
     * - MULTIPLICATIVE: final_score = combined_score * factor
     * - ADDITIVE: final_score = combined_score + factor
     */
    @Builder.Default
    private BoostType type = BoostType.MULTIPLICATIVE;

    /**
     * Enumeration of supported boost types.
     */
    public enum BoostType {
        /**
         * Multiplicative boost: final_score = combined_score * factor
         * Good for proportional boosting that respects relevance.
         */
        MULTIPLICATIVE,

        /**
         * Additive boost: final_score = combined_score + factor
         * Good for fixed boosting regardless of relevance.
         */
        ADDITIVE
    }

    /**
     * Validates the boost configuration.
     * @throws IllegalArgumentException if the configuration is invalid
     */
    public void validate() {
        if (documentId == null || documentId.isBlank()) {
            throw new IllegalArgumentException("document_id is required for result boost");
        }
        if (factor <= 0) {
            throw new IllegalArgumentException("factor must be positive, got: " + factor);
        }
    }
}
