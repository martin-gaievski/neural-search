/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.BoostingQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.ScriptScoreQueryBuilder;
import org.opensearch.script.Script;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.knn.index.query.KNNQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralKNNQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.neuralsearch.util.HybridLegClassifier.Verdict;

import static org.opensearch.neuralsearch.util.NeuralSearchClusterTestUtils.setUpClusterService;

/**
 * Unit tests for {@link HybridLegClassifier}. Covers the flat leaves, every container type, the retrieval-vs-scoring
 * distinction (script_score/function_score), the non-scoring-clause "poison" rule, deep nesting, the depth guard and
 * the fail-safe defaults.
 */
public class HybridLegClassifierTests extends OpenSearchQueryTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // NeuralQueryBuilder.builder().build() resolves the cluster min-version during validation; wire a mock cluster
        // service so the neural-leg construction below does not NPE on NeuralSearchClusterUtil.
        setUpClusterService();
    }

    private static final String TEXT_FIELD = "body";
    private static final String VECTOR_FIELD = "body_embedding";
    private static final String QUERY_TEXT = "medicare enrollment";
    private static final String MODEL_ID = "model-1";
    private static final float[] VECTOR = new float[] { 1.0f, 2.0f, 3.0f };
    private static final int K = 10;

    // ---- leaf construction helpers -------------------------------------------------------------------------------

    private MatchQueryBuilder match() {
        return new MatchQueryBuilder(TEXT_FIELD, QUERY_TEXT);
    }

    private TermQueryBuilder term() {
        return new TermQueryBuilder(TEXT_FIELD, "medicare");
    }

    private NeuralQueryBuilder neural() {
        return NeuralQueryBuilder.builder().fieldName(VECTOR_FIELD).queryText(QUERY_TEXT).modelId(MODEL_ID).k(K).build();
    }

    private NeuralSparseQueryBuilder neuralSparse() {
        return new NeuralSparseQueryBuilder().fieldName(VECTOR_FIELD).queryText(QUERY_TEXT).modelId(MODEL_ID);
    }

    private NeuralKNNQueryBuilder neuralKnn() {
        return NeuralKNNQueryBuilder.builder().fieldName(VECTOR_FIELD).vector(VECTOR).k(K).build();
    }

    private KNNQueryBuilder rawKnn() {
        return new KNNQueryBuilder(VECTOR_FIELD, VECTOR, K);
    }

    // ---- flat leaves ---------------------------------------------------------------------------------------------

    public void testNullLegIsUnknown() {
        assertEquals(Verdict.UNKNOWN, HybridLegClassifier.classify(null));
    }

    public void testPlainMatchIsLexical() {
        assertEquals(Verdict.LEXICAL, HybridLegClassifier.classify(match()));
    }

    public void testPlainTermIsLexical() {
        assertEquals(Verdict.LEXICAL, HybridLegClassifier.classify(term()));
    }

    public void testRangeIsLexical() {
        assertEquals(Verdict.LEXICAL, HybridLegClassifier.classify(new RangeQueryBuilder(TEXT_FIELD)));
    }

    public void testMatchAllIsLexical() {
        assertEquals(Verdict.LEXICAL, HybridLegClassifier.classify(new MatchAllQueryBuilder()));
    }

    public void testExpandedLexicalTypesAreLexical() {
        // spot-check some of the broader lexical allow-list added for coverage
        assertEquals(
            Verdict.LEXICAL,
            HybridLegClassifier.classify(new org.opensearch.index.query.WildcardQueryBuilder(TEXT_FIELD, "med*"))
        );
        assertEquals(Verdict.LEXICAL, HybridLegClassifier.classify(new org.opensearch.index.query.PrefixQueryBuilder(TEXT_FIELD, "med")));
        assertEquals(
            Verdict.LEXICAL,
            HybridLegClassifier.classify(new org.opensearch.index.query.SpanTermQueryBuilder(TEXT_FIELD, "medicare"))
        );
        assertEquals(Verdict.LEXICAL, HybridLegClassifier.classify(new org.opensearch.index.query.MatchNoneQueryBuilder()));
        assertEquals(Verdict.LEXICAL, HybridLegClassifier.classify(new org.opensearch.index.query.ExistsQueryBuilder(TEXT_FIELD)));
    }

    public void testNeuralIsSemantic() {
        assertEquals(Verdict.SEMANTIC, HybridLegClassifier.classify(neural()));
    }

    public void testNeuralSparseIsSemantic() {
        assertEquals(Verdict.SEMANTIC, HybridLegClassifier.classify(neuralSparse()));
    }

    public void testNeuralKnnIsSemantic() {
        // neural_knn does NOT implement ModelInferenceQueryBuilder — the instanceof NeuralKNNQueryBuilder branch catches it
        assertEquals(Verdict.SEMANTIC, HybridLegClassifier.classify(neuralKnn()));
    }

    public void testRawKnnIsSemantic() {
        assertEquals(Verdict.SEMANTIC, HybridLegClassifier.classify(rawKnn()));
    }

    // ---- script_score / function_score: retrieval driver, not the score ------------------------------------------

    public void testScriptScoreOverMatchIsLexical() {
        // headline case: the match set is BM25; the script only re-scores
        ScriptScoreQueryBuilder ss = new ScriptScoreQueryBuilder(match(), new Script("_score"));
        assertEquals(Verdict.LEXICAL, HybridLegClassifier.classify(ss));
    }

    public void testScriptScoreOverKnnIsSemantic() {
        ScriptScoreQueryBuilder ss = new ScriptScoreQueryBuilder(neuralKnn(), new Script("_score"));
        assertEquals(Verdict.SEMANTIC, HybridLegClassifier.classify(ss));
    }

    public void testFunctionScoreOverNeuralIsSemantic() {
        // even with a scoring function replacing the score, retrieval is vector-driven
        FunctionScoreQueryBuilder fs = new FunctionScoreQueryBuilder(neural());
        assertEquals(Verdict.SEMANTIC, HybridLegClassifier.classify(fs));
    }

    public void testFunctionScoreOverMatchIsLexical() {
        FunctionScoreQueryBuilder fs = new FunctionScoreQueryBuilder(match());
        assertEquals(Verdict.LEXICAL, HybridLegClassifier.classify(fs));
    }

    // ---- constant_score / boosting / nested ----------------------------------------------------------------------

    public void testConstantScoreOverMatchIsLexical() {
        assertEquals(Verdict.LEXICAL, HybridLegClassifier.classify(new ConstantScoreQueryBuilder(match())));
    }

    public void testConstantScoreOverNeuralIsSemantic() {
        assertEquals(Verdict.SEMANTIC, HybridLegClassifier.classify(new ConstantScoreQueryBuilder(neural())));
    }

    public void testBoostingPositiveMatchNegativeNeuralIsLexical() {
        // positive drives retrieval; the semantic negative is a demotion only and must be ignored
        BoostingQueryBuilder b = new BoostingQueryBuilder(match(), neural());
        b.negativeBoost(0.5f);
        assertEquals(Verdict.LEXICAL, HybridLegClassifier.classify(b));
    }

    public void testBoostingPositiveNeuralNegativeMatchIsSemantic() {
        BoostingQueryBuilder b = new BoostingQueryBuilder(neural(), match());
        b.negativeBoost(0.5f);
        assertEquals(Verdict.SEMANTIC, HybridLegClassifier.classify(b));
    }

    public void testNestedOverKnnIsSemantic() {
        NestedQueryBuilder n = new NestedQueryBuilder("chunks", neuralKnn(), ScoreMode.Max);
        assertEquals(Verdict.SEMANTIC, HybridLegClassifier.classify(n));
    }

    public void testNestedOverMatchIsLexical() {
        NestedQueryBuilder n = new NestedQueryBuilder("chunks", match(), ScoreMode.Max);
        assertEquals(Verdict.LEXICAL, HybridLegClassifier.classify(n));
    }

    // ---- dis_max -------------------------------------------------------------------------------------------------

    public void testDisMaxAllLexicalIsLexical() {
        DisMaxQueryBuilder d = new DisMaxQueryBuilder().add(match()).add(term());
        assertEquals(Verdict.LEXICAL, HybridLegClassifier.classify(d));
    }

    public void testDisMaxMixedIsMixed() {
        DisMaxQueryBuilder d = new DisMaxQueryBuilder().add(match()).add(neural());
        assertEquals(Verdict.MIXED, HybridLegClassifier.classify(d));
    }

    // ---- bool: primary group -------------------------------------------------------------------------------------

    public void testBoolMustMatchAndKnnIsMixed() {
        // both are co-primary retrieval clauses; no lexical-only subset preserves the doc set
        BoolQueryBuilder b = new BoolQueryBuilder().must(match()).must(neuralKnn());
        assertEquals(Verdict.MIXED, HybridLegClassifier.classify(b));
    }

    public void testBoolShouldMatchAndNeuralIsMixed() {
        BoolQueryBuilder b = new BoolQueryBuilder().should(match()).should(neural());
        assertEquals(Verdict.MIXED, HybridLegClassifier.classify(b));
    }

    public void testBoolMustMatchShouldNeuralIsLexical() {
        // must is the primary retrieval group; a semantic should only boosts and does not define the match set
        BoolQueryBuilder b = new BoolQueryBuilder().must(match()).should(neural());
        assertEquals(Verdict.LEXICAL, HybridLegClassifier.classify(b));
    }

    public void testBoolAllLexicalMustIsLexical() {
        BoolQueryBuilder b = new BoolQueryBuilder().must(match()).must(term());
        assertEquals(Verdict.LEXICAL, HybridLegClassifier.classify(b));
    }

    // ---- bool: non-scoring clause poison rule --------------------------------------------------------------------

    public void testBoolMatchWithStructuralFilterIsLexical() {
        // the common HybridQueryBuilder.filter() push-down shape — the structural filter is ignored
        BoolQueryBuilder b = new BoolQueryBuilder().must(match()).filter(term());
        assertEquals(Verdict.LEXICAL, HybridLegClassifier.classify(b));
    }

    public void testBoolMatchWithSemanticFilterIsMixed() {
        // a semantic query in a filter still constrains which docs are retrieved -> poison to MIXED
        BoolQueryBuilder b = new BoolQueryBuilder().must(match()).filter(neuralKnn());
        assertEquals(Verdict.MIXED, HybridLegClassifier.classify(b));
    }

    public void testBoolNeuralWithStructuralFilterIsSemantic() {
        BoolQueryBuilder b = new BoolQueryBuilder().must(neural()).filter(term());
        assertEquals(Verdict.SEMANTIC, HybridLegClassifier.classify(b));
    }

    public void testBoolMatchWithSemanticMustNotIsMixed() {
        BoolQueryBuilder b = new BoolQueryBuilder().must(match()).mustNot(neuralKnn());
        assertEquals(Verdict.MIXED, HybridLegClassifier.classify(b));
    }

    public void testBoolFilterOnlyIsUnknown() {
        // no scoring/retrieval primary clause to base a call on
        BoolQueryBuilder b = new BoolQueryBuilder().filter(term());
        assertEquals(Verdict.UNKNOWN, HybridLegClassifier.classify(b));
    }

    // ---- deep nesting --------------------------------------------------------------------------------------------

    public void testDeepAllLexicalIsLexical() {
        BoolQueryBuilder inner = new BoolQueryBuilder().should(match()).should(new MatchPhraseQueryBuilder(TEXT_FIELD, QUERY_TEXT));
        BoolQueryBuilder outer = new BoolQueryBuilder().must(inner);
        assertEquals(Verdict.LEXICAL, HybridLegClassifier.classify(outer));
    }

    public void testScriptScoreOverBoolMixedIsMixed() {
        BoolQueryBuilder mixed = new BoolQueryBuilder().must(match()).must(neuralKnn());
        ScriptScoreQueryBuilder ss = new ScriptScoreQueryBuilder(mixed, new Script("_score"));
        assertEquals(Verdict.MIXED, HybridLegClassifier.classify(ss));
    }

    public void testDepthGuardReturnsUnknown() {
        // nest bool > MAX_DEPTH deep; the guard should bail to UNKNOWN rather than recurse unboundedly
        QueryBuilder q = match();
        for (int i = 0; i <= HybridLegClassifier.MAX_DEPTH + 2; i++) {
            q = new BoolQueryBuilder().must(q);
        }
        assertEquals(Verdict.UNKNOWN, HybridLegClassifier.classify(q));
    }

    // ---- unknown / fail-safe -------------------------------------------------------------------------------------

    public void testUnrecognizedLeafIsUnknownNotLexical() {
        // a geo/other query we don't classify must be UNKNOWN, never silently LEXICAL
        QueryBuilder geo = new org.opensearch.index.query.GeoDistanceQueryBuilder(TEXT_FIELD).point(1.0, 2.0).distance("1km");
        assertEquals(Verdict.UNKNOWN, HybridLegClassifier.classify(geo));
    }

    public void testUnknownPropagatesThroughContainer() {
        // an unknown child inside a bool must makes the whole leg UNKNOWN (absorbing)
        QueryBuilder geo = new org.opensearch.index.query.GeoDistanceQueryBuilder(TEXT_FIELD).point(1.0, 2.0).distance("1km");
        BoolQueryBuilder b = new BoolQueryBuilder().must(match()).must(geo);
        assertEquals(Verdict.UNKNOWN, HybridLegClassifier.classify(b));
    }

    // ---- verdict lattice -----------------------------------------------------------------------------------------

    public void testVerdictJoinLattice() {
        assertEquals(Verdict.LEXICAL, Verdict.LEXICAL.join(Verdict.LEXICAL));
        assertEquals(Verdict.SEMANTIC, Verdict.SEMANTIC.join(Verdict.SEMANTIC));
        assertEquals(Verdict.MIXED, Verdict.LEXICAL.join(Verdict.SEMANTIC));
        assertEquals(Verdict.MIXED, Verdict.SEMANTIC.join(Verdict.LEXICAL));
        assertEquals(Verdict.MIXED, Verdict.MIXED.join(Verdict.LEXICAL));
        assertEquals(Verdict.UNKNOWN, Verdict.UNKNOWN.join(Verdict.LEXICAL));
        assertEquals(Verdict.UNKNOWN, Verdict.MIXED.join(Verdict.UNKNOWN));
    }

    // ---- CI drift guard ------------------------------------------------------------------------------------------

    /**
     * Guards against a future neural-search query type being added to the plugin's query registry without being taught
     * to the classifier. Every registered query type must be either (a) a recognized SEMANTIC leaf, or (b) an
     * explicitly acknowledged non-leg query ({@code hybrid} — never nested in itself; {@code agentic} — top-level only).
     * If this test fails, a new query type was registered: decide whether it is semantic (add it to
     * {@code HybridLegClassifier.SEMANTIC_QUERY_NAMES} / the instanceof checks) or lexical/other, and update this
     * acknowledgement set — do NOT let it silently fall to UNKNOWN.
     */
    public void testEveryRegisteredNeuralSearchQueryTypeIsCategorized() {
        // hybrid is never a leg of itself; agentic is a top-level-only marker query, not a retrieval leg.
        final java.util.Set<String> nonLegQueryNames = java.util.Set.of("hybrid", "agentic");

        final java.util.List<String> registeredNames = new org.opensearch.neuralsearch.plugin.NeuralSearch().getQueries()
            .stream()
            .map(spec -> spec.getName().getPreferredName())
            .collect(java.util.stream.Collectors.toList());
        assertFalse("plugin should register query types", registeredNames.isEmpty());

        for (final String name : registeredNames) {
            if (nonLegQueryNames.contains(name)) {
                continue;
            }
            // Any registered query type that CAN be a hybrid leg must be recognized as semantic by the name set.
            // (neural, neural_sparse, neural_knn are all semantic; this is the drift tripwire.)
            assertTrue(
                "registered neural-search query type ["
                    + name
                    + "] is not categorized by HybridLegClassifier; classify it as semantic or acknowledge it as a non-leg",
                HybridLegClassifier.SEMANTIC_QUERY_NAMES.contains(name)
            );
        }
    }
}
