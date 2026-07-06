/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.resolver;

import org.opensearch.action.IndicesRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryCoordinatorContext;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.search.SearchModule;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResolverQueryBuilderTests extends OpenSearchTestCase {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents());
    }

    private NamedWriteableRegistry namedWriteableRegistry() {
        return new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables());
    }

    /** fast path eligibility for the totals gate: track_total_hits:false is eligible; the DEFAULT (accurate totals
     *  beyond the window) is NOT — the fast path fires legs at size=rank_window_size so it cannot faithfully
     *  reconstruct an index-wide total larger than the window; that request must take the standard (Tail) path.
     *  (See ResolverOrchestrator.fastPathEligible: the widening to the default shape is a documented follow-up that
     *  needs a leg-side accurate count independent of the retrieved window size.) */
    public void testFastPathEligible_totalsGate() {
        ResolverQueryBuilder r = new ResolverQueryBuilder(
            List.of(new MatchQueryBuilder("title", "a"), new MatchQueryBuilder("body", "b")),
            ResolverQueryBuilder.TECHNIQUE_ARITHMETIC_MEAN,
            ResolverQueryBuilder.NORMALIZATION_MIN_MAX,
            ResolverQueryBuilder.DEFAULT_RANK_CONSTANT,
            100,
            new float[0]
        );
        assertTrue(
            "track_total_hits:false + plain top-K is fast-path eligible",
            ResolverOrchestrator.fastPathEligible(new SearchSourceBuilder().size(10).trackTotalHits(false), r)
        );
        assertFalse(
            "default (accurate totals beyond the window) is NOT fast-path eligible — must take the Tail path",
            ResolverOrchestrator.fastPathEligible(new SearchSourceBuilder().size(10), r)
        );
        assertTrue(
            "a finite track_total_hits cap within the window is fast-path eligible",
            ResolverOrchestrator.fastPathEligible(new SearchSourceBuilder().size(10).trackTotalHitsUpTo(10), r)
        );
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

    /** On a shard / base rewrite context (no coordinator context) doRewrite is a no-op: it returns `this` and
     *  registers no async action, so the marker stays intact for the coordinator round (or trips doToQuery). */
    public void testDoRewriteReturnsThisOnShardOrBaseContext() throws Exception {
        ResolverQueryBuilder builder = sampleBuilder();
        QueryRewriteContext ctx = mock(QueryRewriteContext.class);
        when(ctx.convertToCoordinatorContext()).thenReturn(null);
        assertSame(builder, builder.doRewrite(ctx));
        org.mockito.Mockito.verify(ctx, org.mockito.Mockito.never()).registerAsyncAction(org.mockito.Mockito.any());
    }

    /** At the coordinator rewrite, if the request is not a SearchRequest (e.g. _explain/_validate pass other
     *  IndicesRequest types), the guarded cast fails and doRewrite returns `this` without registering an async action. */
    public void testDoRewriteReturnsThisWhenRequestNotSearchRequest() throws Exception {
        ResolverQueryBuilder builder = sampleBuilder();
        QueryRewriteContext ctx = mock(QueryRewriteContext.class);
        QueryCoordinatorContext coordinatorContext = mock(QueryCoordinatorContext.class);
        when(ctx.convertToCoordinatorContext()).thenReturn(coordinatorContext);
        when(coordinatorContext.getSearchRequest()).thenReturn(mock(IndicesRequest.class)); // not a SearchRequest
        assertSame(builder, builder.doRewrite(ctx));
        org.mockito.Mockito.verify(ctx, org.mockito.Mockito.never()).registerAsyncAction(org.mockito.Mockito.any());
    }

    /** At the coordinator rewrite over a real SearchRequest, doRewrite registers EXACTLY ONE async action (the leg
     *  MultiSearch) and returns a DISTINCT builder (a new object drives the next rewrite round after the async drains).
     *  While that builder's fused result is still pending, a further doRewrite on it makes no progress (returns `this`),
     *  so the whole-tree rewrite stays inside MAX_REWRITE_ROUNDS. */
    public void testDoRewriteRegistersExactlyOneAsyncActionAndSelfErasesWhenPending() throws Exception {
        ResolverQueryBuilder builder = sampleBuilder();
        QueryRewriteContext ctx = mock(QueryRewriteContext.class);
        QueryCoordinatorContext coordinatorContext = mock(QueryCoordinatorContext.class);
        when(ctx.convertToCoordinatorContext()).thenReturn(coordinatorContext);
        SearchRequest searchRequest = new SearchRequest("idx").source(new SearchSourceBuilder().query(builder));
        when(coordinatorContext.getSearchRequest()).thenReturn(searchRequest);

        AtomicInteger registered = new AtomicInteger();
        org.mockito.Mockito.doAnswer(inv -> {
            registered.incrementAndGet();
            return null;
        }).when(ctx).registerAsyncAction(org.mockito.Mockito.any());

        QueryBuilder rewritten = builder.doRewrite(ctx);
        assertEquals("exactly one async action (the leg MultiSearch) is registered", 1, registered.get());
        assertNotSame("must return a distinct builder to drive another rewrite round", builder, rewritten);
        assertTrue(rewritten instanceof ResolverQueryBuilder);

        // Supplier still pending (the mocked async action never populated it) -> the next round makes no progress.
        ResolverQueryBuilder pending = (ResolverQueryBuilder) rewritten;
        QueryRewriteContext ctx2 = mock(QueryRewriteContext.class);
        assertSame(pending, pending.doRewrite(ctx2));
        // A still-pending supplier must not touch the coordinator context (no re-fire) — it returns before the gate.
        org.mockito.Mockito.verify(ctx2, org.mockito.Mockito.never()).convertToCoordinatorContext();
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

    public void testWeightedRrfParsesAndSerializes() throws Exception {
        // POC v2: RRF now accepts per-leg weights (weighted RRF, mirrors ES 9.2).
        String json = "{\"queries\":[{\"match\":{\"title\":\"a\"}},{\"match\":{\"body\":\"b\"}}],"
            + "\"combination\":{\"technique\":\"rrf\",\"parameters\":{\"rank_constant\":60,\"weights\":[2.0,0.5]}}}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken();
        ResolverQueryBuilder builder = ResolverQueryBuilder.fromXContent(parser);
        assertEquals(ResolverQueryBuilder.TECHNIQUE_RRF, builder.technique());
        assertEquals(2, builder.weights().length);
        assertEquals(2.0f, builder.weights()[0], 1e-6);
        assertEquals(0.5f, builder.weights()[1], 1e-6);
        ResolverQueryBuilder deserialized = copyWriteable(builder, namedWriteableRegistry(), ResolverQueryBuilder::new);
        assertEquals(builder, deserialized);
        assertArrayEquals(builder.weights(), deserialized.weights(), 1e-6f);
    }

    public void testWeightsRejectNegative() throws Exception {
        // A negative weight yields a negative fused score -> crashes core checkNegativeBoost on the standard path
        // and silently mis-ranks on the fast path. Must be rejected at parse time.
        String json = "{\"queries\":[{\"match\":{\"title\":\"a\"}},{\"match\":{\"body\":\"b\"}}],"
            + "\"normalization\":{\"technique\":\"min_max\"},\"combination\":{\"technique\":\"arithmetic_mean\",\"parameters\":{\"weights\":[-1.0,2.0]}}}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ResolverQueryBuilder.fromXContent(parser));
        assertTrue(e.getMessage().contains("weights"));
        assertTrue(e.getMessage().contains(">= 0"));
    }

    public void testWeightsRejectAllZero() throws Exception {
        // All-zero weights make the arithmetic-mean denominator 0 -> every score forced to 0 (relevance wipeout).
        String json = "{\"queries\":[{\"match\":{\"title\":\"a\"}},{\"match\":{\"body\":\"b\"}}],"
            + "\"normalization\":{\"technique\":\"min_max\"},\"combination\":{\"technique\":\"arithmetic_mean\",\"parameters\":{\"weights\":[0.0,0.0]}}}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ResolverQueryBuilder.fromXContent(parser));
        assertTrue(e.getMessage().contains("all zero"));
    }

    public void testWeightsAcceptValidNonNegative() throws Exception {
        // Arbitrary non-negative weights (not constrained to sum=1, matching ES) are accepted.
        String json = "{\"queries\":[{\"match\":{\"title\":\"a\"}},{\"match\":{\"body\":\"b\"}}],"
            + "\"normalization\":{\"technique\":\"min_max\"},\"combination\":{\"technique\":\"arithmetic_mean\",\"parameters\":{\"weights\":[5.0,1.5]}}}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken();
        ResolverQueryBuilder builder = ResolverQueryBuilder.fromXContent(parser);
        assertArrayEquals(new float[] { 5.0f, 1.5f }, builder.weights(), 1e-6f);
    }

    public void testZScoreNormalizationParses() throws Exception {
        // POC v2 adaptive-fusion #1: z_score (DBSF-style) normalization + arithmetic_mean.
        String json = "{\"queries\":[{\"match\":{\"title\":\"a\"}},{\"match\":{\"body\":\"b\"}}],"
            + "\"normalization\":{\"technique\":\"z_score\"},\"combination\":{\"technique\":\"arithmetic_mean\"}}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken();
        ResolverQueryBuilder builder = ResolverQueryBuilder.fromXContent(parser);
        assertEquals(ResolverQueryBuilder.NORMALIZATION_Z_SCORE, builder.normalization());
        assertEquals(ResolverQueryBuilder.TECHNIQUE_ARITHMETIC_MEAN, builder.technique());
    }

    public void testZScoreNormalizationRejectedWithRrf() throws Exception {
        // z_score normalizes by score distribution, so it is incoherent with rank-based RRF (which ignores scores).
        String json = "{\"queries\":[{\"match\":{\"title\":\"a\"}},{\"match\":{\"body\":\"b\"}}],"
            + "\"technique\":\"rrf\",\"normalization\":{\"technique\":\"z_score\"}}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ResolverQueryBuilder.fromXContent(parser));
        assertTrue(e.getMessage().contains("z_score"));
        assertTrue(e.getMessage().contains(ResolverQueryBuilder.TECHNIQUE_ARITHMETIC_MEAN));
    }

    public void testZScoreSerializationRoundTrip() throws Exception {
        ResolverQueryBuilder original = new ResolverQueryBuilder(
            List.of(new MatchQueryBuilder("title", "a"), new MatchQueryBuilder("body", "b")),
            ResolverQueryBuilder.TECHNIQUE_ARITHMETIC_MEAN,
            ResolverQueryBuilder.NORMALIZATION_Z_SCORE,
            ResolverQueryBuilder.DEFAULT_RANK_CONSTANT,
            100,
            new float[0]
        );
        ResolverQueryBuilder deserialized = copyWriteable(original, namedWriteableRegistry(), ResolverQueryBuilder::new);
        assertEquals(original, deserialized);
        assertEquals(ResolverQueryBuilder.NORMALIZATION_Z_SCORE, deserialized.normalization());
    }

    public void testL2NormalizationParsesAndSerializes() throws Exception {
        // POC v2: l2 normalization + arithmetic_mean (parity with OS hybrid processor / ES l2_norm).
        String json = "{\"queries\":[{\"match\":{\"title\":\"a\"}},{\"match\":{\"body\":\"b\"}}],"
            + "\"normalization\":{\"technique\":\"l2\"},\"combination\":{\"technique\":\"arithmetic_mean\"}}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken();
        ResolverQueryBuilder builder = ResolverQueryBuilder.fromXContent(parser);
        assertEquals(ResolverQueryBuilder.NORMALIZATION_L2, builder.normalization());
        assertEquals(ResolverQueryBuilder.TECHNIQUE_ARITHMETIC_MEAN, builder.technique());
        ResolverQueryBuilder deserialized = copyWriteable(builder, namedWriteableRegistry(), ResolverQueryBuilder::new);
        assertEquals(builder, deserialized);
        assertEquals(ResolverQueryBuilder.NORMALIZATION_L2, deserialized.normalization());
    }

    public void testL2NormalizationRejectedWithRrf() throws Exception {
        // l2 normalizes by score magnitude, incoherent with rank-based RRF (which ignores scores).
        String json = "{\"queries\":[{\"match\":{\"title\":\"a\"}},{\"match\":{\"body\":\"b\"}}],"
            + "\"technique\":\"rrf\",\"normalization\":{\"technique\":\"l2\"}}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ResolverQueryBuilder.fromXContent(parser));
        assertTrue(e.getMessage().contains("l2"));
        assertTrue(e.getMessage().contains(ResolverQueryBuilder.TECHNIQUE_ARITHMETIC_MEAN));
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
