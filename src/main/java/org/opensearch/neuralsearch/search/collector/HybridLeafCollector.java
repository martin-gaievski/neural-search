/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.opensearch.neuralsearch.query.HybridSubQueryScorer;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

@Log4j2
public abstract class HybridLeafCollector implements LeafCollector {
    @Getter(AccessLevel.PACKAGE)
    HybridSubQueryScorer compoundQueryScorer;

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        if (scorer instanceof HybridSubQueryScorer) {
            compoundQueryScorer = (HybridSubQueryScorer) scorer;
        } else {
            compoundQueryScorer = getHybridQueryScorer(scorer);
            if (Objects.isNull(compoundQueryScorer)) {
                log.error(String.format(Locale.ROOT, "cannot find scorer of type HybridQueryScorer in a hierarchy of scorer %s", scorer));
            }
        }
    }

    private HybridSubQueryScorer getHybridQueryScorer(final Scorable scorer) throws IOException {
        if (scorer == null) {
            return null;
        }
        if (scorer instanceof HybridSubQueryScorer) {
            return (HybridSubQueryScorer) scorer;
        }
        for (Scorable.ChildScorable childScorable : scorer.getChildren()) {
            HybridSubQueryScorer hybridQueryScorer = getHybridQueryScorer(childScorable.child());
            if (Objects.nonNull(hybridQueryScorer)) {
                log.debug(
                    String.format(
                        Locale.ROOT,
                        "found hybrid query scorer, it's child of scorer %s",
                        childScorable.child().getClass().getSimpleName()
                    )
                );
                return hybridQueryScorer;
            }
        }
        return null;
    }
}
