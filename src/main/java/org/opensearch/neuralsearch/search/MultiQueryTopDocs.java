/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.search;

import java.util.Map;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

public class MultiQueryTopDocs extends TopDocs {

    Map<Query, TopDocs> docs;

    public MultiQueryTopDocs(TotalHits totalHits, ScoreDoc[] scoreDocs) {
        super(totalHits, scoreDocs);
    }

    public MultiQueryTopDocs(TotalHits totalHits, Map<Query, TopDocs> docs) {
        super(totalHits, docs.get(docs.keySet().iterator().next()).scoreDocs);
        this.docs = docs;
    }
}
