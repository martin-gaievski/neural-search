/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import org.opensearch.search.SearchShardTarget;

public record DocIdAtQueryPhase(Integer docId, SearchShardTarget searchShardTarget) {
}
