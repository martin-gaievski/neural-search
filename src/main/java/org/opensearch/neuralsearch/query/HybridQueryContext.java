/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.apache.lucene.search.Query;

import java.util.List;

/**
 * Class that holds the low level information of hybrid query in the form of context
 */
@Builder
@Getter
@AllArgsConstructor
public class HybridQueryContext {
    private Integer paginationDepth;
    /**
     * Compiled Lucene queries for the ordered result-boost conditions (may be null/empty). Each condition
     * promotes matching documents into a tier band at the coordinator, without restricting the result set.
     */
    private List<Query> boostConditionQueries;

    /**
     * Backwards-compatible single-argument constructor so existing call sites that only set pagination depth
     * keep compiling after {@link #boostConditionQueries} was added.
     */
    public HybridQueryContext(Integer paginationDepth) {
        this.paginationDepth = paginationDepth;
    }
}
