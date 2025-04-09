/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.lucene.search.CheckedIntConsumer;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.Objects;

@RequiredArgsConstructor
public class DocIdStreamView extends DocIdStream {
    private final HybridBulkScorer hybridBulkScorer;
    @Setter
    private int base;

    @Override
    public void forEach(CheckedIntConsumer<IOException> consumer) throws IOException {
        FixedBitSet matchingBitSet = hybridBulkScorer.getMatching();
        long[] bitArray = matchingBitSet.getBits();
        for (int idx = 0; idx < bitArray.length; idx++) {
            long bits = bitArray[idx];
            while (bits != 0L) {
                int ntz = Long.numberOfTrailingZeros(bits);
                final int indexInWindow = (idx << 6) | ntz;
                float[][] windowScores = hybridBulkScorer.getWindowScores();
                for (int i = 0; i < windowScores.length; i++) {
                    if (Objects.isNull(windowScores[i])) {
                        continue;
                    }
                    float scoreOfDocIdForSubQuery = windowScores[i][indexInWindow];
                    hybridBulkScorer.getHybridSubQueryScorer().getSubQueryScores()[i] = scoreOfDocIdForSubQuery;
                }
                consumer.accept(base | indexInWindow);
                hybridBulkScorer.getHybridSubQueryScorer().resetScores();
                bits ^= 1L << ntz;
            }
        }
    }

    @Override
    public int count() throws IOException {
        return super.count();
    }
}
