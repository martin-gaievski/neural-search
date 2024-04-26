/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.Getter;
import lombok.Setter;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.Scorer;

@Getter
@Setter
public class MyDisiWrapper extends DisiWrapper {

    int subQueryIndex;

    public MyDisiWrapper(Scorer scorer) {
        super(scorer);
    }
}
