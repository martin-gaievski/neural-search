/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.List;
import java.util.Map;

/**
 * Abstracts explanation of score combination or normalization technique.
 */
public interface ExplainableTechnique {

    default String describe() {
        return "generic score processing technique";
    }

    default Map<DocIdAtQueryPhase, String> explain(final List<CompoundTopDocs> queryTopDocs) {
        return Map.of();
    }
}
