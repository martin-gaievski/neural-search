/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.resultboost;

import org.junit.Test;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for ResultBoostConfig parsing.
 */
public class ResultBoostConfigTests extends OpenSearchTestCase {

    @Test
    public void testParseFromExtContent() {
        // Simulate ext content from search request
        Map<String, Object> extContent = new HashMap<>();
        Map<String, Object> resultBoost = new HashMap<>();

        List<Map<String, Object>> boosts = Arrays.asList(createBoostMap("PROMO-12345", 3.0), createBoostMap("FEATURED-789", 2.5));

        resultBoost.put("boosts", boosts);
        extContent.put("result_boost", resultBoost);

        ResultBoostConfig config = ResultBoostConfig.fromExtContent(extContent);

        assertNotNull(config);
        assertTrue(config.hasBoosts());
        assertEquals(2, config.getBoosts().size());

        // Verify first boost
        DocumentBoost first = config.getBoosts().get(0);
        assertEquals("PROMO-12345", first.getDocumentId());
        assertEquals(3.0f, first.getFactor(), 0.001f);
        assertEquals(DocumentBoost.BoostType.MULTIPLICATIVE, first.getType());

        // Verify second boost
        DocumentBoost second = config.getBoosts().get(1);
        assertEquals("FEATURED-789", second.getDocumentId());
        assertEquals(2.5f, second.getFactor(), 0.001f);
    }

    @Test
    public void testParseWithAdditiveType() {
        Map<String, Object> extContent = new HashMap<>();
        Map<String, Object> resultBoost = new HashMap<>();

        Map<String, Object> boostMap = new HashMap<>();
        boostMap.put("document_id", "doc1");
        boostMap.put("factor", 1.5);
        boostMap.put("type", "additive");

        resultBoost.put("boosts", Arrays.asList(boostMap));
        extContent.put("result_boost", resultBoost);

        ResultBoostConfig config = ResultBoostConfig.fromExtContent(extContent);

        assertNotNull(config);
        assertEquals(1, config.getBoosts().size());
        assertEquals(DocumentBoost.BoostType.ADDITIVE, config.getBoosts().get(0).getType());
    }

    @Test
    public void testNullExtContentReturnsNull() {
        ResultBoostConfig config = ResultBoostConfig.fromExtContent(null);
        assertNull(config);
    }

    @Test
    public void testEmptyExtContentReturnsNull() {
        ResultBoostConfig config = ResultBoostConfig.fromExtContent(new HashMap<>());
        assertNull(config);
    }

    @Test
    public void testMissingResultBoostKeyReturnsNull() {
        Map<String, Object> extContent = new HashMap<>();
        extContent.put("other_key", "value");

        ResultBoostConfig config = ResultBoostConfig.fromExtContent(extContent);
        assertNull(config);
    }

    @Test
    public void testToBoostMap() {
        ResultBoostConfig config = ResultBoostConfig.builder()
            .boosts(
                Arrays.asList(
                    DocumentBoost.builder().documentId("doc1").factor(2.0f).build(),
                    DocumentBoost.builder().documentId("doc2").factor(3.0f).build()
                )
            )
            .build();

        Map<String, DocumentBoost> boostMap = config.toBoostMap();

        assertEquals(2, boostMap.size());
        assertNotNull(boostMap.get("doc1"));
        assertNotNull(boostMap.get("doc2"));
        assertEquals(2.0f, boostMap.get("doc1").getFactor(), 0.001f);
        assertEquals(3.0f, boostMap.get("doc2").getFactor(), 0.001f);
    }

    private Map<String, Object> createBoostMap(String documentId, double factor) {
        Map<String, Object> boost = new HashMap<>();
        boost.put("document_id", documentId);
        boost.put("factor", factor);
        return boost;
    }
}
