/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.fetch;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.FetchSubPhaseProcessor;
import org.opensearch.search.query.ExplainResult;

import java.io.IOException;
import java.util.Objects;

public class ExplainNormalizationPhase implements FetchSubPhase {

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext context) {
        if (context.explain() == false) {
            return null;
        }
        // SearchContext searchContext = context.query();
        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {

            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                final int topLevelDocId = hitContext.hit().docId();
                ExplainResult coordinatorLevelExplain = context.getQueryResult().getExplainResult();
                Explanation searchQueryExplanation = hitContext.hit().getExplanation();
                if (Objects.nonNull(searchQueryExplanation)) {
                    Explanation explanation = coordinatorLevelExplain.getExplanation();
                    Explanation combinedExplanation = Explanation.match(
                        1.0,
                        "combined explannation of search and coordinator levels",
                        explanation,
                        searchQueryExplanation
                    );
                    hitContext.hit().explanation(combinedExplanation);
                }
                // we use the top level doc id, since we work with the top level searcher
            }
        };
    }
}
