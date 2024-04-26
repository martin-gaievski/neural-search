/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.Getter;
import lombok.Setter;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.Scorer;

/**
 * Wrapper for DisiWrapper, saves state of sub-queries for performance reasons
 */
@Getter
@Setter
public class HybridDisiWrapper extends DisiWrapper {

    //index of disi wrapper sub-query object when its part of the hybrid query
    int subQueryIndex;

    public HybridDisiWrapper(Scorer scorer) {
        super(scorer);
    }
}
