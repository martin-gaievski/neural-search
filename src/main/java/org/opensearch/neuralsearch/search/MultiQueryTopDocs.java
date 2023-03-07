/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.search;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

public class MultiQueryTopDocs extends TopDocs {

    TopDocs[] docs;

    public MultiQueryTopDocs(TotalHits totalHits, ScoreDoc[] scoreDocs) {
        super(totalHits, scoreDocs);
    }

    public MultiQueryTopDocs(TotalHits totalHits, TopDocs[] docs) {
        super(totalHits, docs[0].scoreDocs);
        this.docs = docs;
    }
}
