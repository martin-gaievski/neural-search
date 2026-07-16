/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Weight;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.SortAndFormats;

@AllArgsConstructor
@Builder
@Getter
public class HybridCollectorFactoryDTO {
    private final SortAndFormats sortAndFormats;
    private final SearchContext searchContext;
    private final HitsThresholdChecker hitsThresholdChecker;
    private final int numHits;
    private final FieldDoc after;
    // Compiled Lucene weights for the ordered result-boost conditions (may be null/empty). Only consumed by the
    // non-sort, non-collapse HybridTopScoreDocCollector path in this POC.
    private final List<Weight> boostConditionWeights;
}
