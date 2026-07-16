/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.List;
import java.util.Map;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralKNNQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.neuralsearch.processor.HybridDictionaryRewriteProcessor.MatchMode;
import org.opensearch.search.builder.SearchSourceBuilder;

import static org.opensearch.neuralsearch.util.NeuralSearchClusterTestUtils.setUpClusterService;

public class HybridDictionaryRewriteProcessorTests extends OpenSearchQueryTestCase {

    private static final String TEXT_FIELD = "body";
    private static final String VECTOR_FIELD = "body_embedding";
    private static final String MODEL_ID = "model-1";
    private static final int K = 10;
    private static final float[] VECTOR = new float[] { 1.0f, 2.0f, 3.0f };

    @Override
    public void setUp() throws Exception {
        super.setUp();
        setUpClusterService();
    }

    private HybridDictionaryRewriteProcessor processor(final MatchMode mode, final String... terms) {
        return new HybridDictionaryRewriteProcessor("tag", "desc", false, java.util.Set.of(terms), mode);
    }

    private MatchQueryBuilder match(final String text) {
        return new MatchQueryBuilder(TEXT_FIELD, text);
    }

    private NeuralQueryBuilder neural(final String text) {
        return NeuralQueryBuilder.builder().fieldName(VECTOR_FIELD).queryText(text).modelId(MODEL_ID).k(K).build();
    }

    private NeuralKNNQueryBuilder neuralKnn() {
        return NeuralKNNQueryBuilder.builder().fieldName(VECTOR_FIELD).vector(VECTOR).k(K).build();
    }

    private SearchRequest requestWith(final QueryBuilder query) {
        return new SearchRequest().source(new SearchSourceBuilder().query(query));
    }

    private SearchRequest hybridRequest(final QueryBuilder... legs) {
        final HybridQueryBuilder hybrid = new HybridQueryBuilder();
        for (final QueryBuilder leg : legs) {
            hybrid.add(leg);
        }
        return requestWith(hybrid);
    }

    // ---- match => rewrite to lexical -----------------------------------------------------------------------------

    public void testDictionaryHitRewritesToLexicalLegOnly() {
        HybridDictionaryRewriteProcessor proc = processor(MatchMode.ANY_TOKEN, "medicare");
        SearchRequest req = hybridRequest(match("medicare enrollment"), neural("medicare enrollment"));

        SearchRequest out = proc.processRequest(req);

        QueryBuilder rewritten = out.source().query();
        assertTrue("expected the bare lexical leg, got " + rewritten.getClass().getSimpleName(), rewritten instanceof MatchQueryBuilder);
        assertEquals("medicare enrollment", ((MatchQueryBuilder) rewritten).value());
    }

    public void testDictionaryMissLeavesHybridUnchanged() {
        HybridDictionaryRewriteProcessor proc = processor(MatchMode.ANY_TOKEN, "vaccine");
        SearchRequest req = hybridRequest(match("medicare enrollment"), neural("medicare enrollment"));

        SearchRequest out = proc.processRequest(req);

        assertTrue(out.source().query() instanceof HybridQueryBuilder);
    }

    public void testMatchOnSemanticLegQueryTextAlsoTriggers() {
        // the query text is read from all legs, incl. the neural leg's query_text
        HybridDictionaryRewriteProcessor proc = processor(MatchMode.ANY_TOKEN, "medicare");
        SearchRequest req = hybridRequest(match("something else"), neural("medicare plan"));

        SearchRequest out = proc.processRequest(req);

        // matched -> semantic dropped, lexical match kept bare
        assertTrue(out.source().query() instanceof MatchQueryBuilder);
    }

    public void testMultipleLexicalLegsCollapseToBoolShould() {
        HybridDictionaryRewriteProcessor proc = processor(MatchMode.ANY_TOKEN, "medicare");
        SearchRequest req = hybridRequest(match("medicare"), new TermQueryBuilder(TEXT_FIELD, "plan"), neural("medicare"));

        SearchRequest out = proc.processRequest(req);

        QueryBuilder rewritten = out.source().query();
        assertTrue(rewritten instanceof BoolQueryBuilder);
        BoolQueryBuilder bool = (BoolQueryBuilder) rewritten;
        assertEquals(2, bool.should().size());
        assertTrue(bool.must().isEmpty());
        assertTrue(bool.should().get(0) instanceof MatchQueryBuilder);
        assertTrue(bool.should().get(1) instanceof TermQueryBuilder);
    }

    // ---- fail-safe no-ops -----------------------------------------------------------------------------------------

    public void testMixedLegDeclinesRewrite() {
        // one leg is bool{must:[match, knn]} => MIXED => decline entirely, leave hybrid as-is
        BoolQueryBuilder mixed = new BoolQueryBuilder().must(match("medicare")).must(neuralKnn());
        HybridDictionaryRewriteProcessor proc = processor(MatchMode.ANY_TOKEN, "medicare");
        SearchRequest req = hybridRequest(mixed, neural("medicare"));

        SearchRequest out = proc.processRequest(req);

        assertTrue("MIXED leg must cause a fail-safe no-op", out.source().query() instanceof HybridQueryBuilder);
    }

    public void testNoLexicalLegDeclinesRewrite() {
        HybridDictionaryRewriteProcessor proc = processor(MatchMode.ANY_TOKEN, "medicare");
        // both legs semantic; query text still matches, but there is no lexical leg to keep
        SearchRequest req = hybridRequest(neural("medicare"), neuralKnn());

        SearchRequest out = proc.processRequest(req);

        assertTrue(out.source().query() instanceof HybridQueryBuilder);
    }

    public void testNonHybridTopQueryIsUntouched() {
        HybridDictionaryRewriteProcessor proc = processor(MatchMode.ANY_TOKEN, "medicare");
        SearchRequest req = requestWith(match("medicare"));

        SearchRequest out = proc.processRequest(req);

        assertTrue(out.source().query() instanceof MatchQueryBuilder);
    }

    public void testNullQueryBodyIsUntouched() {
        HybridDictionaryRewriteProcessor proc = processor(MatchMode.ANY_TOKEN, "medicare");
        SearchRequest req = new SearchRequest().source(new SearchSourceBuilder());

        SearchRequest out = proc.processRequest(req);

        assertNull(out.source().query());
    }

    // ---- phrase mode ----------------------------------------------------------------------------------------------

    public void testPhraseModeMatchesFullText() {
        HybridDictionaryRewriteProcessor proc = processor(MatchMode.PHRASE, "prior authorization");
        SearchRequest hit = hybridRequest(match("Prior Authorization"), neural("prior authorization"));
        SearchRequest miss = hybridRequest(match("authorization"), neural("authorization"));

        assertTrue(proc.processRequest(hit).source().query() instanceof MatchQueryBuilder);
        assertTrue(proc.processRequest(miss).source().query() instanceof HybridQueryBuilder);
    }

    public void testMatchingIsCaseInsensitive() {
        HybridDictionaryRewriteProcessor proc = processor(MatchMode.ANY_TOKEN, "medicare");
        SearchRequest req = hybridRequest(match("MEDICARE plan"), neural("x"));

        assertTrue(proc.processRequest(req).source().query() instanceof MatchQueryBuilder);
    }

    // ---- factory --------------------------------------------------------------------------------------------------

    public void testFactoryBuildsProcessor() throws Exception {
        HybridDictionaryRewriteProcessor.Factory factory = new HybridDictionaryRewriteProcessor.Factory();
        Map<String, Object> config = new java.util.HashMap<>();
        config.put("dictionary", List.of("Medicare", "vaccine"));
        config.put("match_mode", "any_token");

        HybridDictionaryRewriteProcessor proc = factory.create(Map.of(), "t", "d", false, config, null);

        assertEquals(HybridDictionaryRewriteProcessor.TYPE, proc.getType());
        assertEquals(MatchMode.ANY_TOKEN, proc.getMatchMode());
        // dictionary normalized to lower case
        assertTrue(proc.getDictionary().contains("medicare"));
    }

    public void testFactoryRejectsEmptyDictionary() {
        HybridDictionaryRewriteProcessor.Factory factory = new HybridDictionaryRewriteProcessor.Factory();
        Map<String, Object> config = new java.util.HashMap<>();
        config.put("dictionary", List.of());

        expectThrows(IllegalArgumentException.class, () -> factory.create(Map.of(), "t", "d", false, config, null));
    }

    public void testFactoryRejectsMissingDictionary() {
        HybridDictionaryRewriteProcessor.Factory factory = new HybridDictionaryRewriteProcessor.Factory();
        expectThrows(IllegalArgumentException.class, () -> factory.create(Map.of(), "t", "d", false, new java.util.HashMap<>(), null));
    }

    public void testFactoryRejectsBadMatchMode() {
        HybridDictionaryRewriteProcessor.Factory factory = new HybridDictionaryRewriteProcessor.Factory();
        Map<String, Object> config = new java.util.HashMap<>();
        config.put("dictionary", List.of("medicare"));
        config.put("match_mode", "bogus");

        expectThrows(IllegalArgumentException.class, () -> factory.create(Map.of(), "t", "d", false, config, null));
    }
}
