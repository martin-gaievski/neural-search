/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.resolver;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.search.SearchModule;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class ResolverQueryBuilderTests extends OpenSearchTestCase {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents());
    }

    private NamedWriteableRegistry namedWriteableRegistry() {
        return new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables());
    }

    private ResolverQueryBuilder sampleBuilder() {
        return new ResolverQueryBuilder(
            List.of(new MatchQueryBuilder("title", "neural search"), new MatchQueryBuilder("body", "vector fusion")),
            ResolverQueryBuilder.TECHNIQUE_RRF,
            40,
            50
        );
    }

    public void testAccessorsAndWriteableName() {
        ResolverQueryBuilder builder = sampleBuilder();
        assertEquals("resolver", builder.getWriteableName());
        assertEquals(2, builder.queries().size());
        assertEquals(ResolverQueryBuilder.TECHNIQUE_RRF, builder.technique());
        assertEquals(40, builder.rankConstant());
        assertEquals(50, builder.rankWindowSize());
    }

    public void testDoToQueryThrowsBecauseResolverIsCoordinatorOnly() {
        ResolverQueryBuilder builder = sampleBuilder();
        // A resolver must be resolved by the request processor on the coordinator; reaching a shard is an error.
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> builder.doToQuery(null));
        assertTrue(e.getMessage().contains("resolver"));
        assertTrue(e.getMessage().contains(ResolverQueryBuilder.NAME));
    }

    public void testEqualsAndHashCode() {
        ResolverQueryBuilder a = sampleBuilder();
        ResolverQueryBuilder b = sampleBuilder();
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        ResolverQueryBuilder differentConstant = new ResolverQueryBuilder(
            List.of(new MatchQueryBuilder("title", "neural search"), new MatchQueryBuilder("body", "vector fusion")),
            ResolverQueryBuilder.TECHNIQUE_RRF,
            60,
            50
        );
        assertNotEquals(a, differentConstant);
    }

    public void testSerializationRoundTrip() throws Exception {
        ResolverQueryBuilder original = sampleBuilder();
        ResolverQueryBuilder deserialized = copyWriteable(original, namedWriteableRegistry(), ResolverQueryBuilder::new);
        assertEquals(original, deserialized);
        assertEquals(original.queries(), deserialized.queries());
        assertEquals(original.rankConstant(), deserialized.rankConstant());
        assertEquals(original.rankWindowSize(), deserialized.rankWindowSize());
    }

    public void testFromXContentParsesAllFields() throws Exception {
        String json = "{"
            + "\"queries\":[{\"match\":{\"title\":\"neural search\"}},{\"match\":{\"body\":\"vector fusion\"}}],"
            + "\"technique\":\"rrf\",\"rank_constant\":40,\"rank_window_size\":50}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken(); // position at START_OBJECT, as the query framework does
        ResolverQueryBuilder builder = ResolverQueryBuilder.fromXContent(parser);

        assertEquals(2, builder.queries().size());
        assertTrue(builder.queries().get(0) instanceof MatchQueryBuilder);
        assertEquals("rrf", builder.technique());
        assertEquals(40, builder.rankConstant());
        assertEquals(50, builder.rankWindowSize());
    }

    public void testFromXContentDefaults() throws Exception {
        String json = "{\"queries\":[{\"match\":{\"title\":\"a\"}},{\"match\":{\"body\":\"b\"}}]}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken();
        ResolverQueryBuilder builder = ResolverQueryBuilder.fromXContent(parser);
        assertEquals(ResolverQueryBuilder.TECHNIQUE_RRF, builder.technique());
        assertEquals(ResolverQueryBuilder.DEFAULT_RANK_CONSTANT, builder.rankConstant());
        assertEquals(ResolverQueryBuilder.DEFAULT_RANK_WINDOW_SIZE, builder.rankWindowSize());
    }

    public void testFromXContentRejectsTooFewSubQueries() throws Exception {
        String json = "{\"queries\":[{\"match\":{\"title\":\"a\"}}]}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ResolverQueryBuilder.fromXContent(parser));
        assertTrue(e.getMessage().contains("at least"));
    }

    public void testFromXContentRejectsUnsupportedTechnique() throws Exception {
        String json = "{\"queries\":[{\"match\":{\"title\":\"a\"}},{\"match\":{\"body\":\"b\"}}],\"technique\":\"linear\"}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ResolverQueryBuilder.fromXContent(parser));
        assertTrue(e.getMessage().contains("rrf"));
    }

    public void testDefaultsAreCoordinatorCollection() {
        ResolverQueryBuilder builder = sampleBuilder();
        assertEquals(ResolverQueryBuilder.COLLECTION_COORDINATOR, builder.collection());
        assertFalse(builder.isPerShardCollection());
        // candidate_depth falls back to rank_window_size when unset
        assertEquals(builder.rankWindowSize(), builder.candidateDepth());
    }

    public void testPerShardCollectionParsingAndGating() throws Exception {
        // per_shard + arithmetic_mean => per-shard collection active, candidate_depth honoured
        String json = "{\"queries\":[{\"match\":{\"title\":\"a\"}},{\"match\":{\"body\":\"b\"}}],"
            + "\"rank_window_size\":100,\"collection\":\"per_shard\",\"candidate_depth\":50,"
            + "\"normalization\":{\"technique\":\"min_max\"},\"combination\":{\"technique\":\"arithmetic_mean\"}}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken();
        ResolverQueryBuilder builder = ResolverQueryBuilder.fromXContent(parser);
        assertEquals(ResolverQueryBuilder.COLLECTION_PER_SHARD, builder.collection());
        assertEquals(50, builder.candidateDepth());
        assertTrue(builder.isPerShardCollection());
    }

    public void testPerShardCollectionRejectedForRrf() throws Exception {
        // per_shard is only meaningful for score-based arithmetic_mean; requesting it with RRF is rejected
        // (rather than silently ignored) so the knob never appears to have an effect it doesn't.
        String json = "{\"queries\":[{\"match\":{\"title\":\"a\"}},{\"match\":{\"body\":\"b\"}}],"
            + "\"technique\":\"rrf\",\"collection\":\"per_shard\"}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ResolverQueryBuilder.fromXContent(parser));
        assertTrue(e.getMessage().contains("per_shard"));
        assertTrue(e.getMessage().contains(ResolverQueryBuilder.TECHNIQUE_ARITHMETIC_MEAN));
    }

    public void testCandidateDepthRejectedWithoutPerShard() throws Exception {
        // candidate_depth only applies to per_shard collection; with the default coordinator collection it is rejected.
        String json = "{\"queries\":[{\"match\":{\"title\":\"a\"}},{\"match\":{\"body\":\"b\"}}],\"candidate_depth\":50}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ResolverQueryBuilder.fromXContent(parser));
        assertTrue(e.getMessage().contains("candidate_depth"));
    }

    public void testFromXContentRejectsUnknownCollection() throws Exception {
        String json = "{\"queries\":[{\"match\":{\"title\":\"a\"}},{\"match\":{\"body\":\"b\"}}],\"collection\":\"segment\"}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ResolverQueryBuilder.fromXContent(parser));
        assertTrue(e.getMessage().contains("collection"));
    }

    public void testFromXContentRejectsNonPositiveCandidateDepth() throws Exception {
        String json = "{\"queries\":[{\"match\":{\"title\":\"a\"}},{\"match\":{\"body\":\"b\"}}],\"candidate_depth\":0}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ResolverQueryBuilder.fromXContent(parser));
        assertTrue(e.getMessage().contains("candidate_depth"));
    }

    public void testSerializationRoundTripWithPerShardFields() throws Exception {
        ResolverQueryBuilder original = new ResolverQueryBuilder(
            List.of(new MatchQueryBuilder("title", "a"), new MatchQueryBuilder("body", "b")),
            ResolverQueryBuilder.TECHNIQUE_ARITHMETIC_MEAN,
            ResolverQueryBuilder.NORMALIZATION_MIN_MAX,
            ResolverQueryBuilder.DEFAULT_RANK_CONSTANT,
            100,
            new float[0],
            ResolverQueryBuilder.COLLECTION_PER_SHARD,
            50
        );
        ResolverQueryBuilder deserialized = copyWriteable(original, namedWriteableRegistry(), ResolverQueryBuilder::new);
        assertEquals(original, deserialized);
        assertEquals(ResolverQueryBuilder.COLLECTION_PER_SHARD, deserialized.collection());
        assertEquals(50, deserialized.candidateDepth());
        assertTrue(deserialized.isPerShardCollection());
    }
}
