/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.resolver;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.search.SearchModule;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class RankDocsQueryBuilderTests extends OpenSearchTestCase {

    private NamedWriteableRegistry namedWriteableRegistry() {
        return new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables());
    }

    private RankDocsQueryBuilder sample() {
        return new RankDocsQueryBuilder(
            new String[] { "d1", "d2" },
            new float[] { 0.032f, 0.016f },
            List.of(new MatchQueryBuilder("title", "apple"), new MatchQueryBuilder("body", "banana"))
        );
    }

    public void testWriteableName() {
        assertEquals("rank_docs", sample().getWriteableName());
    }

    public void testSerializationRoundTrip() throws Exception {
        RankDocsQueryBuilder original = sample();
        RankDocsQueryBuilder deserialized = copyWriteable(original, namedWriteableRegistry(), RankDocsQueryBuilder::new);
        assertEquals(original, deserialized);
        assertEquals(original.hashCode(), deserialized.hashCode());
    }

    public void testNotParseableFromXContent() {
        // Internal query created by the resolver processor; must not be parseable from a request.
        expectThrows(UnsupportedOperationException.class, () -> RankDocsQueryBuilder.fromXContent(null));
    }

    public void testRawSubQueryScoresSurviveSerialization() throws Exception {
        // POC standard path: raw per-leg scores are carried INSIDE this (serialized-to-data-nodes) query.
        java.util.Map<String, float[]> raw = new java.util.HashMap<>();
        raw.put("d1", new float[] { 12.44f, 0.83f });
        raw.put("d2", new float[] { Float.NaN, 1.20f }); // leg-0 did not match d2
        RankDocsQueryBuilder original = new RankDocsQueryBuilder(
            new String[] { "d1", "d2" },
            new float[] { 0.032f, 0.016f },
            List.of(new MatchQueryBuilder("title", "apple"), new MatchQueryBuilder("body", "banana")),
            raw
        );
        RankDocsQueryBuilder deserialized = copyWriteable(original, namedWriteableRegistry(), RankDocsQueryBuilder::new);
        assertEquals(original, deserialized);
        java.util.Map<String, float[]> got = deserialized.rawSubQueryScoresById();
        assertNotNull("raw scores map must survive the wire", got);
        assertArrayEquals(new float[] { 12.44f, 0.83f }, got.get("d1"), 0.0f);
        assertArrayEquals(new float[] { Float.NaN, 1.20f }, got.get("d2"), 0.0f); // NaN preserved on the wire
    }

    public void testNullRawScoresSerializesAsAbsent() throws Exception {
        // Opt-out (null payload) must round-trip as null — the common no-sub-query-scores case pays nothing.
        RankDocsQueryBuilder deserialized = copyWriteable(sample(), namedWriteableRegistry(), RankDocsQueryBuilder::new);
        assertNull(deserialized.rawSubQueryScoresById());
    }
}
