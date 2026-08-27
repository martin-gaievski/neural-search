/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import static org.opensearch.neuralsearch.common.MinClusterVersionUtil.MINIMAL_SUPPORTED_VERSION_FUSED_MODE_IN_HYBRID_QUERY;

import java.util.List;
import java.util.Set;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.FilterStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.SearchModule;
import org.opensearch.test.OpenSearchTestCase;

public class HybridFusionQueryBuilderTests extends OpenSearchTestCase {

    private NamedWriteableRegistry namedWriteableRegistry() {
        return new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables());
    }

    public void testWriteableName() {
        HybridFusionQueryBuilder query = new HybridFusionQueryBuilder(
            new String[] { "d1" },
            new String[] { "idx" },
            new float[] { 1.0f },
            List.of()
        );
        assertEquals("hybrid_fusion", query.getWriteableName());
    }

    public void testIndicesAreRequired() {
        // A fused document is addressed by _index and _id together; there is no unqualified form of this query, so a
        // caller that lost the per-doc index fails here rather than silently addressing every same-_id document.
        expectThrows(
            NullPointerException.class,
            () -> new HybridFusionQueryBuilder(new String[] { "d1" }, null, new float[] { 1.0f }, List.of())
        );
    }

    public void testNotParseableFromXContent() {
        // Internal query built by the coordinator self-erase; never parsed from a request.
        expectThrows(UnsupportedOperationException.class, () -> HybridFusionQueryBuilder.fromXContent(null));
    }

    public void testSerializationRoundTrip() throws Exception {
        HybridFusionQueryBuilder original = new HybridFusionQueryBuilder(
            new String[] { "d1", "d2" },
            new String[] { "idx", "idx" },
            new float[] { 0.9f, 0.4f },
            List.of(new MatchQueryBuilder("title", "apple"), new MatchQueryBuilder("body", "banana"))
        );
        HybridFusionQueryBuilder deserialized = copyWriteable(original, namedWriteableRegistry(), HybridFusionQueryBuilder::new);
        assertEquals(original, deserialized);
        assertEquals(original.hashCode(), deserialized.hashCode());
    }

    public void testSelfErasedShape_whenSourceQueriesPresent_thenTopPlusTail() {
        HybridFusionQueryBuilder query = new HybridFusionQueryBuilder(
            new String[] { "d1", "d2" },
            new String[] { "idx", "idx" },
            new float[] { 0.9f, 0.4f },
            List.of(new MatchQueryBuilder("title", "apple"), new MatchQueryBuilder("body", "banana"))
        );
        BoolQueryBuilder self = query.buildSelfErasedQuery();
        // Top: one scoring SHOULD (constant_score) per ranked id; Tail: exactly one non-scoring FILTER clause.
        assertEquals(2, self.should().size());
        assertEquals(1, self.filter().size());
        assertTrue(self.should().get(0) instanceof ConstantScoreQueryBuilder);
        // The Tail is a bool{ should: [ real legs ] } — the leg union as one filter clause.
        assertTrue(self.filter().get(0) instanceof BoolQueryBuilder);
        assertEquals(2, ((BoolQueryBuilder) self.filter().get(0)).should().size());
    }

    public void testSelfErasedShape_whenNoSourceQueries_thenTopOnly() {
        HybridFusionQueryBuilder query = new HybridFusionQueryBuilder(
            new String[] { "d1", "d2" },
            new String[] { "idx", "idx" },
            new float[] { 0.9f, 0.4f },
            List.of()
        );
        BoolQueryBuilder self = query.buildSelfErasedQuery();
        assertEquals(2, self.should().size());
        assertEquals("Top-only fused query carries no Tail filter", 0, self.filter().size());
    }

    public void testSelfErasedShape_whenEmptyWindow_thenEmptyBool() {
        // An empty fused window produces an empty bool (no should, no filter) → compiles to match-no-docs.
        HybridFusionQueryBuilder query = new HybridFusionQueryBuilder(new String[0], new String[0], new float[0], List.of());
        BoolQueryBuilder self = query.buildSelfErasedQuery();
        assertEquals(0, self.should().size());
        assertEquals(0, self.filter().size());
    }

    public void testDoXContent_isInformationalOnly() throws Exception {
        HybridFusionQueryBuilder query = new HybridFusionQueryBuilder(
            new String[] { "d1", "d2", "d3" },
            new String[] { "idx", "idx", "idx" },
            new float[] { 0.9f, 0.5f, 0.1f },
            List.of()
        );
        org.opensearch.core.xcontent.XContentBuilder builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder();
        query.toXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
        String json = builder.toString();
        assertTrue(json.contains("hybrid_fusion"));
        assertTrue("informational representation reports the fused doc count", json.contains("fused_docs_count"));
        assertTrue(json.contains("3"));
    }

    public void testDoRewrite_whenNoSourceQueryChanges_thenReturnsSame() throws Exception {
        // Tail source queries that don't rewrite (already terminal term queries) → doRewrite returns the same instance.
        HybridFusionQueryBuilder query = new HybridFusionQueryBuilder(
            new String[] { "d1" },
            new String[] { "idx" },
            new float[] { 0.7f },
            List.of(new org.opensearch.index.query.TermQueryBuilder("text", "keyword"))
        );
        org.opensearch.index.query.QueryRewriteContext ctx = mock(org.opensearch.index.query.QueryRewriteContext.class);
        assertSame(query, query.rewrite(ctx));
    }

    public void testDoRewrite_whenSourceQueryRewrites_thenReturnsRewrittenCopy() throws Exception {
        // A source query that always rewrites to a new instance forces the changed==true branch: doRewrite returns a
        // NEW HybridFusionQueryBuilder preserving ids/scores/boost/queryName.
        org.opensearch.index.query.QueryBuilder alwaysRewrites = new org.opensearch.index.query.MatchAllQueryBuilder() {
            @Override
            protected org.opensearch.index.query.QueryBuilder doRewrite(org.opensearch.index.query.QueryRewriteContext c) {
                return new org.opensearch.index.query.MatchAllQueryBuilder(); // different instance each rewrite
            }
        };
        HybridFusionQueryBuilder query = new HybridFusionQueryBuilder(
            new String[] { "d1", "d2" },
            new String[] { "idx", "idx" },
            new float[] { 0.7f, 0.3f },
            List.of(alwaysRewrites)
        );
        query.boost(1.0f);
        org.opensearch.index.query.QueryRewriteContext ctx = mock(org.opensearch.index.query.QueryRewriteContext.class);

        org.opensearch.index.query.QueryBuilder rewritten = query.rewrite(ctx);
        assertTrue(rewritten instanceof HybridFusionQueryBuilder);
        assertNotSame("changed source → new copy", query, rewritten);
        assertEquals(2, ((HybridFusionQueryBuilder) rewritten).buildSelfErasedQuery().should().size());
    }

    // ---- name-only legs: carried so matched_queries survives a Top-only query, never executed ----

    /** A leg whose conversion needs no mappings, so registration can be asserted against a mocked shard context. */
    private QueryBuilder contextFreeLeg(String queryName) {
        QueryBuilder leg = new MatchAllQueryBuilder();
        return queryName == null ? leg : leg.queryName(queryName);
    }

    public void testSerializationRoundTrip_whenNamedOnlyQueriesPresent_thenPreserved() throws Exception {
        HybridFusionQueryBuilder original = new HybridFusionQueryBuilder(
            new String[] { "d1" },
            new String[] { "idx" },
            new float[] { 0.9f },
            List.of(),
            List.of(),
            List.of(new MatchQueryBuilder("title", "apple").queryName("lexical_leg"))
        );
        HybridFusionQueryBuilder deserialized = copyWriteable(original, namedWriteableRegistry(), HybridFusionQueryBuilder::new);
        assertEquals(original, deserialized);
        assertEquals(original.hashCode(), deserialized.hashCode());
        assertEquals(1, deserialized.namedOnlyQueries().size());
        assertEquals("lexical_leg", deserialized.namedOnlyQueries().get(0).queryName());
    }

    public void testSerializationRoundTrip_whenStreamPinnedToMinimumSupportedVersion_thenAllThreeListsSurvive() throws Exception {
        // This query's wire form carries three query lists and reads them unconditionally, with no TransportVersion gate:
        // it is built only for a cluster whose every node supports fused mode (HybridQueryBuilder#requireClusterSupportsFusedMode)
        // and has never shipped in a released version, so there is no older reader to stay compatible with. That makes the
        // minimum supported version the ONLY version the format has to hold at — pin it here, so a future field added
        // without a gate fails against the oldest peer the gate admits rather than only against Version.CURRENT.
        HybridFusionQueryBuilder original = new HybridFusionQueryBuilder(
            new String[] { "d1", "d2" },
            new String[] { "idx", "idx" },
            new float[] { 0.9f, 0.4f },
            List.of(new MatchQueryBuilder("title", "apple").queryName("tail_leg")),
            List.of(new MatchQueryBuilder("body", "banana")),
            List.of(new MatchQueryBuilder("title", "cherry").queryName("named_only_leg"))
        );

        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(MINIMAL_SUPPORTED_VERSION_FUSED_MODE_IN_HYBRID_QUERY);
        original.writeTo(out);

        FilterStreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry());
        in.setVersion(MINIMAL_SUPPORTED_VERSION_FUSED_MODE_IN_HYBRID_QUERY);
        HybridFusionQueryBuilder deserialized = new HybridFusionQueryBuilder(in);

        // equals() covers all three lists, so this alone pins the format; the assertions below name what would break.
        assertEquals(original, deserialized);
        assertEquals("the stream must be fully consumed — a trailing list would leave bytes behind", 0, in.available());
        assertEquals("named_only_leg", deserialized.namedOnlyQueries().get(0).queryName());
        assertEquals("the Tail must survive as the compiled query's single filter", 1, deserialized.buildSelfErasedQuery().filter().size());
    }

    public void testEquals_whenOnlyNamedOnlyQueriesDiffer_thenNotEqual() {
        // The name-only list is part of this query's identity: it changes what the shard registers, hence the response.
        HybridFusionQueryBuilder withNames = new HybridFusionQueryBuilder(
            new String[] { "d1" },
            new String[] { "idx" },
            new float[] { 0.9f },
            List.of(),
            List.of(),
            List.of(new MatchQueryBuilder("title", "apple").queryName("lexical_leg"))
        );
        HybridFusionQueryBuilder withoutNames = new HybridFusionQueryBuilder(
            new String[] { "d1" },
            new String[] { "idx" },
            new float[] { 0.9f },
            List.of()
        );
        assertNotEquals(withNames, withoutNames);
    }

    public void testSelfErasedShape_whenOnlyNamedOnlyQueriesPresent_thenStillTopOnly() {
        // The whole point of the name-only list: it is registered, never executed. A non-empty list must leave the
        // compiled query untouched — no Tail filter appears, so a Top-only request stays Top-only.
        HybridFusionQueryBuilder query = new HybridFusionQueryBuilder(
            new String[] { "d1", "d2" },
            new String[] { "idx", "idx" },
            new float[] { 0.9f, 0.4f },
            List.of(),
            List.of(),
            List.of(new MatchQueryBuilder("title", "apple").queryName("lexical_leg"))
        );
        BoolQueryBuilder self = query.buildSelfErasedQuery();
        assertEquals(2, self.should().size());
        assertEquals("name-only legs must never become an executed clause", 0, self.filter().size());
    }

    public void testDoRewrite_whenNamedOnlyQueryRewrites_thenCarriedIntoTheCopy() throws Exception {
        // A name is registered against the query the builder compiles to, so an un-rewritten name-only leg either compiles
        // to something else or refuses to compile — it has to travel through doRewrite like the other two lists.
        QueryBuilder alwaysRewrites = new MatchAllQueryBuilder() {
            @Override
            protected QueryBuilder doRewrite(QueryRewriteContext c) {
                return new MatchAllQueryBuilder().queryName("lexical_leg");
            }
        }.queryName("lexical_leg");
        HybridFusionQueryBuilder query = new HybridFusionQueryBuilder(
            new String[] { "d1" },
            new String[] { "idx" },
            new float[] { 0.7f },
            List.of(),
            List.of(),
            List.of(alwaysRewrites)
        );
        QueryRewriteContext ctx = mock(QueryRewriteContext.class);

        QueryBuilder rewritten = query.rewrite(ctx);
        assertNotSame("a changed name-only leg produces a new copy", query, rewritten);
        List<QueryBuilder> carried = ((HybridFusionQueryBuilder) rewritten).namedOnlyQueries();
        assertEquals(1, carried.size());
        assertEquals("lexical_leg", carried.get(0).queryName());
    }

    /**
     * The fix itself: converting the query registers each name-only leg's {@code _name}, which is the only thing
     * {@code matched_queries} is built from in the fetch phase. Ids are empty so the self-erased query is an empty
     * {@code bool} — this asserts the registration, not the Top's compilation.
     */
    public void testDoToQuery_whenNamedOnlyLegsCarried_thenTheirNamesAreRegistered() throws Exception {
        HybridFusionQueryBuilder query = new HybridFusionQueryBuilder(
            new String[0],
            new String[0],
            new float[0],
            List.of(),
            List.of(),
            List.of(contextFreeLeg("lexical_leg"), contextFreeLeg("vector_leg"))
        );
        QueryShardContext context = mock(QueryShardContext.class);

        query.doToQuery(context);

        verify(context).addNamedQuery(eq("lexical_leg"), any());
        verify(context).addNamedQuery(eq("vector_leg"), any());
    }

    public void testDoToQuery_whenNamedOnlyLegHasNoName_thenNothingIsRegistered() throws Exception {
        // Registration follows the user's _name and invents none: an unnamed leg carried here reports nothing.
        HybridFusionQueryBuilder query = new HybridFusionQueryBuilder(
            new String[0],
            new String[0],
            new float[0],
            List.of(),
            List.of(),
            List.of(contextFreeLeg(null))
        );
        QueryShardContext context = mock(QueryShardContext.class);

        query.doToQuery(context);

        verify(context, never()).addNamedQuery(anyString(), any());
    }

    /**
     * The self-erase does not cost the {@code hybrid} clause its own {@code _name}: {@code queryName} is copied onto this
     * builder and {@code AbstractQueryBuilder#toQuery} — which is {@code final} — registers it. Pinned because the opposite
     * was assumed while diagnosing the leg-name loss.
     */
    public void testToQuery_whenTheFusedQueryItselfIsNamed_thenItsOwnNameIsRegistered() throws Exception {
        HybridFusionQueryBuilder query = new HybridFusionQueryBuilder(new String[0], new String[0], new float[0], List.of());
        query.queryName("my_hybrid");
        QueryShardContext context = mock(QueryShardContext.class);

        query.toQuery(context);

        verify(context).addNamedQuery(eq("my_hybrid"), any());
    }

    public void testExtractInnerHitBuilders_recursesIntoSourceQueries() {
        // A nested source query declaring inner_hits must be surfaced through extractInnerHitBuilders so the self-erased
        // query still fetches leg-level inner_hits.
        org.opensearch.index.query.NestedQueryBuilder nested = new org.opensearch.index.query.NestedQueryBuilder(
            "user",
            new org.opensearch.index.query.MatchQueryBuilder("user.name", "alice"),
            org.apache.lucene.search.join.ScoreMode.None
        ).innerHit(new org.opensearch.index.query.InnerHitBuilder());
        HybridFusionQueryBuilder query = new HybridFusionQueryBuilder(
            new String[] { "d1" },
            new String[] { "idx" },
            new float[] { 0.9f },
            List.of(nested)
        );

        java.util.Map<String, org.opensearch.index.query.InnerHitContextBuilder> innerHits = new java.util.HashMap<>();
        query.extractInnerHitBuilders(innerHits);
        assertFalse("leg inner_hits must be surfaced", innerHits.isEmpty());
    }

    // ---- the fused window as a filter, for confining a rescore ----

    private void assertAddressedTo(QueryBuilder clause, String index, String... ids) {
        assertTrue("expected an _index-qualified bool, got " + clause, clause instanceof BoolQueryBuilder);
        BoolQueryBuilder qualified = (BoolQueryBuilder) clause;
        assertEquals("qualified by _id AND _index", 2, qualified.filter().size());
        assertEquals(Set.of(ids), ((IdsQueryBuilder) qualified.filter().get(0)).ids());
        TermQueryBuilder indexTerm = (TermQueryBuilder) qualified.filter().get(1);
        assertEquals("_index", indexTerm.fieldName());
        assertEquals(index, indexTerm.value());
    }

    public void testFusedWindowFilter_singleIndex_isOneQualifiedClause() {
        HybridFusionQueryBuilder query = new HybridFusionQueryBuilder(
            new String[] { "d1", "d2" },
            new String[] { "idx", "idx" },
            new float[] { 0.9f, 0.4f },
            List.of()
        );
        // The window filter goes through the same addressing primitive as the Top, so one index means one clause — no
        // pointless bool wrapper around a single group.
        assertAddressedTo(query.fusedWindowFilter(), "idx", "d1", "d2");
    }

    public void testFusedWindowFilter_multipleIndices_isQualifiedPerIndexGroup() {
        // Same-_id documents in two indices: the reason the window filter cannot be an _id-only ids query. An unqualified
        // filter would readmit idx-b/d1 — a document outside the window — into whatever the filter is scoping.
        HybridFusionQueryBuilder query = new HybridFusionQueryBuilder(
            new String[] { "d1", "d2", "d1" },
            new String[] { "idx-a", "idx-a", "idx-b" },
            new float[] { 0.9f, 0.5f, 0.4f },
            List.of()
        );

        QueryBuilder window = query.fusedWindowFilter();

        assertTrue(window instanceof BoolQueryBuilder);
        BoolQueryBuilder perIndex = (BoolQueryBuilder) window;
        assertEquals("one OR-ed clause per index in the window", 2, perIndex.should().size());
        assertAddressedTo(perIndex.should().get(0), "idx-a", "d1", "d2");
        assertAddressedTo(perIndex.should().get(1), "idx-b", "d1");
    }

    public void testFusedWindowFilter_emptyWindow_isMatchNone() {
        // An empty window has to compile to match_none, not to the empty bool that addressing an empty set would produce —
        // an empty bool filter matches everything, which as a rescore scope would be the whole defect back again.
        HybridFusionQueryBuilder query = new HybridFusionQueryBuilder(new String[0], new String[0], new float[0], List.of());
        assertTrue(query.fusedWindowFilter() instanceof MatchNoneQueryBuilder);
    }

    // ---- fused scores must be usable as clause boosts ----

    public void testScoresMustBeUsableAsBoosts() {
        // Each score becomes a constant_score boost. Negative dies in AbstractQueryBuilder#boost per shard; NaN and
        // +Infinity slip past that guard (Float.compare(NaN, 0f) > 0) and die inside Lucene's BoostQuery instead. -0.0f is
        // the interesting one: `< 0.0f` is false for it, but core's checkNegativeBoost uses Float.compare and rejects it.
        for (float rejected : new float[] { -1.0f, -0.0f, Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY }) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                "expected [" + rejected + "] to be refused",
                () -> new HybridFusionQueryBuilder(new String[] { "d1" }, new String[] { "idx" }, new float[] { rejected }, List.of())
            );
            assertTrue(e.getMessage(), e.getMessage().contains("fused scores must all be finite and non-negative"));
        }
    }

    public void testScoresOfZeroAreAcceptedByTheBuilder() {
        // The builder enforces only the boost contract. Keeping a ranked document strictly above the non-scoring Tail is a
        // stronger, fusion-specific guarantee, and it lives where the ranking is decided — not here.
        HybridFusionQueryBuilder query = new HybridFusionQueryBuilder(
            new String[] { "d1" },
            new String[] { "idx" },
            new float[] { 0.0f },
            List.of()
        );
        assertEquals(1, query.buildSelfErasedQuery().should().size());
    }

    public void testScoresAreRevalidatedOffTheWire() throws Exception {
        // The wire constructor is why this is a real check rather than an assert: a bad score arriving from a peer would
        // otherwise be discovered per shard, at query-build time, as what reads like an engine bug. Core re-validates its
        // own boost on deserialization for exactly this reason.
        assertNotNull("baseline: this byte layout deserializes when the score is valid", readBackWithScore(0.5f));

        for (float rejected : new float[] { -1.0f, Float.NaN, Float.POSITIVE_INFINITY }) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                "expected [" + rejected + "] to be refused off the wire",
                () -> readBackWithScore(rejected)
            );
            assertTrue(e.getMessage(), e.getMessage().contains("fused scores must all be finite and non-negative"));
        }
    }

    /** Hand-writes this query's wire form with one chosen fused score — the only way to present a value the ctor refuses. */
    private HybridFusionQueryBuilder readBackWithScore(float score) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeFloat(AbstractQueryBuilder.DEFAULT_BOOST); // AbstractQueryBuilder#writeTo: boost, then queryName
            out.writeOptionalString(null);
            out.writeStringArray(new String[] { "d1" });
            out.writeStringArray(new String[] { "idx" });
            out.writeFloatArray(new float[] { score });
            // tailQueries, innerHitsQueries, namedOnlyQueries — one per list in doWriteTo, in that order
            out.writeNamedWriteableList(List.of());
            out.writeNamedWriteableList(List.of());
            out.writeNamedWriteableList(List.of());
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry())) {
                return new HybridFusionQueryBuilder(in);
            }
        }
    }
}
