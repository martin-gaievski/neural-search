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
}
