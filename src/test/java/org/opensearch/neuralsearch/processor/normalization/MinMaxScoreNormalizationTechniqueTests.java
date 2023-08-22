/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.normalization;

import java.util.List;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;

/**
 * Abstracts normalization of scores based on min-max method
 */
public class MinMaxScoreNormalizationTechniqueTests extends OpenSearchQueryTestCase {
    private static final float DELTA_FOR_ASSERTION = 0.0001f;

    public void testNormalization_whenResultFromOneShardOneSubQuery_thenSuccessful() {
        MinMaxScoreNormalizationTechnique normalizationTechnique = new MinMaxScoreNormalizationTechnique();
        List<CompoundTopDocs> compoundTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(2, 0.5f), new ScoreDoc(4, 0.2f) }
                    )
                )
            )
        );
        normalizationTechnique.normalize(compoundTopDocs);

        CompoundTopDocs expectedCompoundDocs = new CompoundTopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            List.of(
                new TopDocs(
                    new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] { new ScoreDoc(2, 1.0f), new ScoreDoc(4, 0.001f) }
                )
            )
        );
        assertNotNull(compoundTopDocs);
        assertEquals(1, compoundTopDocs.size());
        assertNotNull(compoundTopDocs.get(0).getCompoundTopDocs());
        assertCompoundTopDocs(
            new TopDocs(expectedCompoundDocs.getTotalHits(), expectedCompoundDocs.getScoreDocs()),
            compoundTopDocs.get(0).getCompoundTopDocs().get(0)
        );
    }

    public void testNormalization_whenResultFromOneShardMultipleSubQueries_thenSuccessful() {
        MinMaxScoreNormalizationTechnique normalizationTechnique = new MinMaxScoreNormalizationTechnique();
        List<CompoundTopDocs> compoundTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(2, 0.5f), new ScoreDoc(4, 0.2f) }
                    ),
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                    new TopDocs(
                        new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(3, 0.9f), new ScoreDoc(4, 0.7f), new ScoreDoc(2, 0.1f) }
                    )
                )
            )
        );
        normalizationTechnique.normalize(compoundTopDocs);

        CompoundTopDocs expectedCompoundDocs = new CompoundTopDocs(
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            List.of(
                new TopDocs(
                    new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] { new ScoreDoc(2, 1.0f), new ScoreDoc(4, 0.001f) }
                ),
                new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                new TopDocs(
                    new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] { new ScoreDoc(3, 1.0f), new ScoreDoc(4, 0.75f), new ScoreDoc(2, 0.001f) }
                )
            )
        );
        assertNotNull(compoundTopDocs);
        assertEquals(1, compoundTopDocs.size());
        assertNotNull(compoundTopDocs.get(0).getCompoundTopDocs());
        for (int i = 0; i < expectedCompoundDocs.getCompoundTopDocs().size(); i++) {
            assertCompoundTopDocs(expectedCompoundDocs.getCompoundTopDocs().get(i), compoundTopDocs.get(0).getCompoundTopDocs().get(i));
        }
    }

    public void testNormalization_whenResultFromMultipleShardsMultipleSubQueries_thenSuccessful() {
        MinMaxScoreNormalizationTechnique normalizationTechnique = new MinMaxScoreNormalizationTechnique();
        List<CompoundTopDocs> compoundTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(2, 0.5f), new ScoreDoc(4, 0.2f) }
                    ),
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                    new TopDocs(
                        new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(3, 0.9f), new ScoreDoc(4, 0.7f), new ScoreDoc(2, 0.1f) }
                    )
                )
            ),
            new CompoundTopDocs(
                new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(7, 2.9f), new ScoreDoc(9, 0.7f) }
                    )
                )
            )
        );
        normalizationTechnique.normalize(compoundTopDocs);

        CompoundTopDocs expectedCompoundDocsShard1 = new CompoundTopDocs(
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            List.of(
                new TopDocs(
                    new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] { new ScoreDoc(2, 1.0f), new ScoreDoc(4, 0.001f) }
                ),
                new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                new TopDocs(
                    new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] { new ScoreDoc(3, 1.0f), new ScoreDoc(4, 0.75f), new ScoreDoc(2, 0.001f) }
                )
            )
        );

        CompoundTopDocs expectedCompoundDocsShard2 = new CompoundTopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            List.of(
                new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                new TopDocs(
                    new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] { new ScoreDoc(7, 1.0f), new ScoreDoc(9, 0.001f) }
                )
            )
        );

        assertNotNull(compoundTopDocs);
        assertEquals(2, compoundTopDocs.size());
        assertNotNull(compoundTopDocs.get(0).getCompoundTopDocs());
        for (int i = 0; i < expectedCompoundDocsShard1.getCompoundTopDocs().size(); i++) {
            assertCompoundTopDocs(
                expectedCompoundDocsShard1.getCompoundTopDocs().get(i),
                compoundTopDocs.get(0).getCompoundTopDocs().get(i)
            );
        }
        assertNotNull(compoundTopDocs.get(1).getCompoundTopDocs());
        for (int i = 0; i < expectedCompoundDocsShard2.getCompoundTopDocs().size(); i++) {
            assertCompoundTopDocs(
                expectedCompoundDocsShard2.getCompoundTopDocs().get(i),
                compoundTopDocs.get(1).getCompoundTopDocs().get(i)
            );
        }
    }

    private void assertCompoundTopDocs(TopDocs expected, TopDocs actual) {
        assertEquals(expected.totalHits.value, actual.totalHits.value);
        assertEquals(expected.totalHits.relation, actual.totalHits.relation);
        assertEquals(expected.scoreDocs.length, actual.scoreDocs.length);
        for (int i = 0; i < expected.scoreDocs.length; i++) {
            assertEquals(expected.scoreDocs[i].score, actual.scoreDocs[i].score, DELTA_FOR_ASSERTION);
            assertEquals(expected.scoreDocs[i].doc, actual.scoreDocs[i].doc);
            assertEquals(expected.scoreDocs[i].shardIndex, actual.scoreDocs[i].shardIndex);
        }
    }
}
