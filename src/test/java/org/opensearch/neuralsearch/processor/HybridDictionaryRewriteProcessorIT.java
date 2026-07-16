/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import static org.opensearch.neuralsearch.util.TestUtils.TEST_DIMENSION;
import static org.opensearch.neuralsearch.util.TestUtils.TEST_SPACE_TYPE;
import static org.opensearch.neuralsearch.util.TestUtils.createRandomVector;
import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_USER_AGENT;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.message.BasicHeader;
import org.junit.Before;
import org.opensearch.client.Response;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.knn.index.query.KNNQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Floats;

import lombok.SneakyThrows;

/**
 * Integration test for the {@code hybrid_dictionary_rewrite} search request processor.
 *
 * <p>Models a realistic "policy / program" search scenario: documents carry a text field (BM25) and a vector field
 * (semantic). A hybrid query fuses a lexical {@code match} leg with a semantic {@code knn} leg. A search pipeline pairs
 * the {@code hybrid_dictionary_rewrite} request processor (which fires when the query contains a dictionary term such as
 * a program name) with the standard {@code normalization-processor} that fuses hybrid scores when the rewrite does not
 * fire.
 *
 * <ul>
 *   <li>Dictionary HIT: the query mentions a program name → rewritten to the lexical leg only → results are exactly the
 *       BM25 matches (the semantic-only doc that the knn leg would have surfaced is absent).</li>
 *   <li>Dictionary MISS: an ordinary query → the hybrid runs and fuses both legs, so the semantic-only doc is present.</li>
 * </ul>
 */
public class HybridDictionaryRewriteProcessorIT extends BaseNeuralSearchIT {

    private static final String INDEX_NAME = "test-hybrid-dictionary-rewrite-index";
    private static final String PIPELINE_NAME = "hybrid-dictionary-rewrite-pipeline";
    private static final String TEXT_FIELD = "body";
    private static final String VECTOR_FIELD = "body_vector";

    // Program-name doc (matches the lexical term), a plain relevant doc, and a semantic-only doc (no lexical match).
    private static final String DOC_PROGRAM = "Medicare Advantage enrollment guide";
    private static final String DOC_LEXICAL = "Medicare eligibility overview";
    private static final String DOC_SEMANTIC_ONLY = "general wellness handbook";

    private final float[] vectorProgram = createRandomVector(TEST_DIMENSION);
    private final float[] vectorLexical = createRandomVector(TEST_DIMENSION);
    private final float[] vectorSemanticOnly = createRandomVector(TEST_DIMENSION);

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @SneakyThrows
    public void testDictionaryHit_rewritesToLexicalOnly_semanticOnlyDocAbsent() {
        initializeIndexIfNotExist();
        createRewritePipeline(List.of("medicare", "medicaid"), "any_token");

        // The knn leg targets the semantic-only doc's vector, so a plain hybrid would surface DOC_SEMANTIC_ONLY.
        // But the query text contains "medicare" (a dictionary term), so the processor rewrites to the match leg only.
        HybridQueryBuilder hybrid = new HybridQueryBuilder();
        hybrid.add(new MatchQueryBuilder(TEXT_FIELD, "medicare"));
        hybrid.add(new KNNQueryBuilder(VECTOR_FIELD, vectorSemanticOnly, 3));

        Map<String, Object> response = search(INDEX_NAME, hybrid, null, 10, Map.of("search_pipeline", PIPELINE_NAME), null);

        List<String> ids = idsOf(response);
        // pure-BM25 result: only the two docs whose body matches "medicare"
        assertEquals(2, ids.size());
        assertTrue(ids.contains("program"));
        assertTrue(ids.contains("lexical"));
        // the semantic-only doc that the knn leg would have fused in must NOT be present after the lexical rewrite
        assertFalse("semantic-only doc must be absent after lexical rewrite", ids.contains("semantic"));
    }

    @SneakyThrows
    public void testDictionaryMiss_keepsHybridFusion_semanticOnlyDocPresent() {
        initializeIndexIfNotExist();
        createRewritePipeline(List.of("medicare", "medicaid"), "any_token");

        // Query text has no dictionary term -> rewrite does NOT fire -> hybrid fuses both legs.
        HybridQueryBuilder hybrid = new HybridQueryBuilder();
        hybrid.add(new MatchQueryBuilder(TEXT_FIELD, "eligibility"));                 // matches DOC_LEXICAL
        hybrid.add(new KNNQueryBuilder(VECTOR_FIELD, vectorSemanticOnly, 3));         // surfaces DOC_SEMANTIC_ONLY

        Map<String, Object> response = search(INDEX_NAME, hybrid, null, 10, Map.of("search_pipeline", PIPELINE_NAME), null);

        List<String> ids = idsOf(response);
        // fused hybrid: the semantic-only doc is present because the knn leg contributes it
        assertTrue("semantic-only doc must be present when hybrid fusion runs", ids.contains("semantic"));
    }

    @SuppressWarnings("unchecked")
    private List<String> idsOf(final Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        List<Map<String, Object>> hits = (List<Map<String, Object>>) hitsMap.get("hits");
        return hits.stream().map(h -> (String) h.get("_id")).toList();
    }

    @SneakyThrows
    private void createRewritePipeline(final List<String> dictionary, final String matchMode) {
        String terms = dictionary.stream().map(t -> "\"" + t + "\"").reduce((a, b) -> a + ", " + b).orElse("");
        String body = "{"
            + "\"description\": \"conditional lexical rewrite + hybrid normalization\","
            + "\"request_processors\": [ {"
            + "  \"hybrid_dictionary_rewrite\": {"
            + "    \"dictionary\": ["
            + terms
            + "],"
            + "    \"match_mode\": \""
            + matchMode
            + "\""
            + "  } } ],"
            + "\"phase_results_processors\": [ {"
            + "  \"normalization-processor\": {"
            + "    \"normalization\": { \"technique\": \"min_max\" },"
            + "    \"combination\": { \"technique\": \"arithmetic_mean\" }"
            + "  } } ]"
            + "}";
        Response response = makeRequest(
            client(),
            "PUT",
            "/_search/pipeline/" + PIPELINE_NAME,
            null,
            toHttpEntity(body),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    @SneakyThrows
    private void initializeIndexIfNotExist() {
        if (indexExists(INDEX_NAME)) {
            return;
        }
        prepareKnnIndex(INDEX_NAME, Collections.singletonList(new KNNFieldConfig(VECTOR_FIELD, TEST_DIMENSION, TEST_SPACE_TYPE)));
        addKnnDoc(
            INDEX_NAME,
            "program",
            Collections.singletonList(VECTOR_FIELD),
            Collections.singletonList(Floats.asList(vectorProgram).toArray()),
            Collections.singletonList(TEXT_FIELD),
            Collections.singletonList(DOC_PROGRAM)
        );
        addKnnDoc(
            INDEX_NAME,
            "lexical",
            Collections.singletonList(VECTOR_FIELD),
            Collections.singletonList(Floats.asList(vectorLexical).toArray()),
            Collections.singletonList(TEXT_FIELD),
            Collections.singletonList(DOC_LEXICAL)
        );
        addKnnDoc(
            INDEX_NAME,
            "semantic",
            Collections.singletonList(VECTOR_FIELD),
            Collections.singletonList(Floats.asList(vectorSemanticOnly).toArray()),
            Collections.singletonList(TEXT_FIELD),
            Collections.singletonList(DOC_SEMANTIC_ONLY)
        );
        assertEquals(3, getDocCount(INDEX_NAME));
    }
}
