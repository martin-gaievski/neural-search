/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.Bits;

import java.io.IOException;

public class HybridBulkScorer extends BulkScorer {
    private final DocIdSetIterator iterator;
    private final Scorer scorer;
    private static final int BULK_SIZE = 128; // Size of the block to process in bulk

    public HybridBulkScorer(DocIdSetIterator iterator, Scorer scorer) {
        this.iterator = iterator;
        this.scorer = scorer;
    }

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
        int doc;
        collector.setScorer(scorer);
        iterator.advance(min);
        while ((doc = iterator.docID()) < max) {
            int nextBlockEnd = Math.min(max, doc + BULK_SIZE); // Process in blocks of BULK_SIZE docs
            while (doc < nextBlockEnd) {
                if (acceptDocs == null || acceptDocs.get(doc)) {
                    collector.collect(doc);
                }
                doc = iterator.nextDoc();
            }
        }
        return doc;
    }

    @Override
    public long cost() {
        return iterator.cost();
    }
}
