/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.resultboost;

import org.junit.Test;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for the ResultBooster class.
 * These tests validate the core boost logic for the POC.
 */
public class ResultBoosterTests extends OpenSearchTestCase {

    @Test
    public void testMultiplicativeBoost() {
        ResultBooster booster = new ResultBooster();

        DocumentBoost boost = DocumentBoost.builder().documentId("doc1").factor(3.0f).type(DocumentBoost.BoostType.MULTIPLICATIVE).build();

        float originalScore = 0.8f;
        float boostedScore = booster.applyBoost(originalScore, boost);

        // 0.8 * 3.0 = 2.4
        assertEquals(2.4f, boostedScore, 0.001f);
    }

    @Test
    public void testAdditiveBoost() {
        ResultBooster booster = new ResultBooster();

        DocumentBoost boost = DocumentBoost.builder().documentId("doc1").factor(1.5f).type(DocumentBoost.BoostType.ADDITIVE).build();

        float originalScore = 0.8f;
        float boostedScore = booster.applyBoost(originalScore, boost);

        // 0.8 + 1.5 = 2.3
        assertEquals(2.3f, boostedScore, 0.001f);
    }

    @Test
    public void testNullBoostReturnsOriginalScore() {
        ResultBooster booster = new ResultBooster();

        float originalScore = 0.8f;
        float result = booster.applyBoost(originalScore, null);

        assertEquals(originalScore, result, 0.001f);
    }

    @Test
    public void testBoostedScoreExceedsNormalizedCeiling() {
        ResultBooster booster = new ResultBooster();

        // Document with high combined score (from min-max normalization, capped at 1.0)
        DocumentBoost boost = DocumentBoost.builder()
            .documentId("promo-item")
            .factor(3.0f)
            .type(DocumentBoost.BoostType.MULTIPLICATIVE)
            .build();

        // Score of 0.95 (near ceiling of normalized scores)
        float originalScore = 0.95f;
        float boostedScore = booster.applyBoost(originalScore, boost);

        // 0.95 * 3.0 = 2.85 (exceeds 1.0 ceiling)
        assertEquals(2.85f, boostedScore, 0.001f);
        assertTrue("Boosted score should exceed normalized ceiling of 1.0", boostedScore > 1.0f);
    }
}
