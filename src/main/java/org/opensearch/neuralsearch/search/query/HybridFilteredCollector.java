/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.search.FilteredCollector;
import org.opensearch.search.profile.query.ProfileWeight;

import java.io.IOException;

public class HybridFilteredCollector extends FilteredCollector {
    private final Collector collector;
    private final Weight filter;

    public HybridFilteredCollector(Collector collector, Weight filter) {
        super(collector, filter);
        this.collector = collector;
        this.filter = filter;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        if (filter instanceof ProfileWeight) {
            ((ProfileWeight) filter).associateCollectorToLeaves(context, collector);
        }
        final ScorerSupplier filterScorerSupplier = filter.scorerSupplier(context);
        final LeafCollector in = collector.getLeafCollector(context);

        return new FilterLeafCollector(in) {

            @Override
            public void collect(int doc) throws IOException {
                final Bits bits = Lucene.asSequentialAccessBits(context.reader().maxDoc(), filterScorerSupplier);
                if (bits.get(doc)) {
                    in.collect(doc);
                }
            }
        };
    }
}
