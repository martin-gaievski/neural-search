/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.query;

import static org.opensearch.neuralsearch.TestUtils.createRandomVector;
import static org.opensearch.neuralsearch.TestUtils.objectToFloat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import lombok.SneakyThrows;

import org.junit.Before;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.neuralsearch.common.BaseNeuralSearchIT;

import com.google.common.primitives.Floats;

public class HybridQueryIT extends BaseNeuralSearchIT {
    private static final String TEST_BASIC_INDEX_NAME = "test-neural-basic-index";
    private static final String TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME = "test-neural-vector-doc-field-index";
    private static final String TEST_MULTI_DOC_INDEX_NAME = "test-neural-multi-doc-index";
    private static final String TEST_QUERY_TEXT = "greetings";
    private static final String TEST_QUERY_TEXT2 = "salute";
    private static final String TEST_QUERY_TEXT3 = "hello";
    private static final String TEST_QUERY_TEXT4 = "place";
    private static final String TEST_QUERY_TEXT5 = "welcome";
    private static final String TEST_DOC_TEXT1 = "Hello world";
    private static final String TEST_DOC_TEXT2 = "Hi to this place";
    private static final String TEST_DOC_TEXT3 = "We would like to welcome everyone";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_1 = "test-knn-vector-1";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_2 = "test-knn-vector-2";
    private static final String TEST_TEXT_FIELD_NAME_1 = "test-text-field-1";
    private static final String TEST_TEXT_FIELD_NAME_2 = "test-text-field-2";
    private static final String TEST_TEXT_FIELD_NAME_3 = "test-text-field-3";

    private static final int TEST_DIMENSION = 768;
    private static final SpaceType TEST_SPACE_TYPE = SpaceType.L2;
    private static final AtomicReference<String> modelId = new AtomicReference<>();
    private static final float EXPECTED_SCORE_BM25 = 0.287682082504034f;
    private final float[] testVector1 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector2 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector3 = createRandomVector(TEST_DIMENSION);

    @Before
    public void setUp() throws Exception {
        super.setUp();
        modelId.compareAndSet(null, prepareModel());
    }

    /**
     * Tests basic query, example of query structure:
     * {
     *     "query": {
     *         "hybrid": {
     *              "queries": [
     *                  {
     *                      "neural": {
     *                          "text_knn": {
     *                              "query_text": "Hello world",
     *                              "model_id": "dcsdcasd",
     *                              "k": 1
     *                          }
     *                      }
     *                  }
     *              ]
     *          }
     *      }
     * }
     */
    @SneakyThrows
    public void testBasicQuery_whenOneSubQuery_thenSuccessful() {
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_NAME);

        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(TEST_KNN_VECTOR_FIELD_NAME_1, "", modelId.get(), 5, null, null);

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(neuralQueryBuilder);

        Map<String, Object> searchResponseAsMap1 = search(TEST_MULTI_DOC_INDEX_NAME, hybridQueryBuilder, 10);

        assertEquals(3, getHitCount(searchResponseAsMap1));

        List<Map<String, Object>> hits1NestedList = getNestedHits(searchResponseAsMap1);
        List<String> ids1 = new ArrayList<>();
        List<Double> scores1 = new ArrayList<>();
        for (Map<String, Object> oneHit : hits1NestedList) {
            ids1.add((String) oneHit.get("_id"));
            scores1.add((Double) oneHit.get("_score"));
        }

        // verify that scores are in desc order
        assertTrue(IntStream.range(0, scores1.size() - 1).noneMatch(idx -> scores1.get(idx) < scores1.get(idx + 1)));
        // verify that all ids are unique
        assertEquals(Set.copyOf(ids1).size(), ids1.size());
    }

    @SneakyThrows
    public void testScoreCorrectness_whenHybridWithNeuralQuery_thenScoresAreCorrect() {
        initializeIndexIfNotExist(TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME);

        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_1,
            TEST_QUERY_TEXT,
            modelId.get(),
            1,
            null,
            null
        );
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);

        HybridQueryBuilder hybridQueryBuilderNeuralThenTerm = new HybridQueryBuilder();
        hybridQueryBuilderNeuralThenTerm.add(neuralQueryBuilder);
        hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder);

        Map<String, Object> searchResponseAsMap = search(TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME, hybridQueryBuilderNeuralThenTerm, 1);

        assertEquals(1, getHitCount(searchResponseAsMap));

        List<Map<String, Object>> hitsNestedList = getNestedHits(searchResponseAsMap);
        List<Double> scores = new ArrayList<>();
        for (Map<String, Object> oneHit : hitsNestedList) {
            scores.add((Double) oneHit.get("_score"));
        }
        // expected score is from knn query because it has more hits and is the first in the list of sub-queries
        float expectedScore = computeExpectedScore(modelId.get(), testVector1, TEST_SPACE_TYPE, TEST_QUERY_TEXT);
        assertEquals(expectedScore, objectToFloat(scores.get(0)), 0.001f);
    }

    /**
     * Tests complex query with multiple nested sub-queries:
     * {
     *     "query": {
     *         "hybrid": {
     *              "queries": [
     *                  {
     *                      "bool": {
     *                          "should": [
     *                              {
     *                                  "term": {
     *                                      "text": "word1"
     *                                  }
     *                             },
     *                             {
     *                                  "term": {
     *                                      "text": "word2"
     *                                   }
     *                              }
     *                         ]
     *                      }
     *                  },
     *                  {
     *                      "term": {
     *                          "text": "word3"
     *                      }
     *                  }
     *              ]
     *          }
     *      }
     * }
     */
    @SneakyThrows
    public void testComplexQuery_whenMultipleSubqueries_thenSuccessful() {
        initializeIndexIfNotExist(TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME);

        TermQueryBuilder termQueryBuilder1 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
        TermQueryBuilder termQueryBuilder2 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_2, TEST_QUERY_TEXT4);
        TermQueryBuilder termQueryBuilder3 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_3, TEST_QUERY_TEXT5);
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.should(termQueryBuilder2).should(termQueryBuilder3);

        HybridQueryBuilder hybridQueryBuilderNeuralThenTerm = new HybridQueryBuilder();
        hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder1);
        hybridQueryBuilderNeuralThenTerm.add(boolQueryBuilder);

        Map<String, Object> searchResponseAsMap1 = search(TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME, hybridQueryBuilderNeuralThenTerm, 10);

        assertEquals(3, getHitCount(searchResponseAsMap1));

        List<Map<String, Object>> hits1NestedList = getNestedHits(searchResponseAsMap1);
        List<String> ids1 = new ArrayList<>();
        List<Double> scores1 = new ArrayList<>();
        for (Map<String, Object> oneHit : hits1NestedList) {
            ids1.add((String) oneHit.get("_id"));
            scores1.add((Double) oneHit.get("_score"));
        }

        // verify that scores are in desc order
        assertTrue(IntStream.range(0, scores1.size() - 1).noneMatch(idx -> scores1.get(idx) < scores1.get(idx + 1)));
        // verify that all ids are unique
        assertEquals(Set.copyOf(ids1).size(), ids1.size());
    }

    /**
     * Using queries similar to below to test sub-queries order:
     * {
     *     "query": {
     *         "hybrid": {
     *              "queries": [
     *                  {
     *                      "neural": {
     *                          "text_knn": {
     *                              "query_text": "Hello world",
     *                              "model_id": "dcsdcasd",
     *                              "k": 1
     *                          }
     *                      }
     *                  },
     *                  {
     *                      "term": {
     *                          "text": "word"
     *                      }
     *                  }
     *              ]
     *          }
     *      }
     * }
     */
    @SneakyThrows
    public void testSubQuery_whenSubqueriesInDifferentOrder_thenResultIsSame() {
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_NAME);

        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(TEST_KNN_VECTOR_FIELD_NAME_1, "", modelId.get(), 5, null, null);
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT);

        HybridQueryBuilder hybridQueryBuilderNeuralThenTerm = new HybridQueryBuilder();
        hybridQueryBuilderNeuralThenTerm.add(neuralQueryBuilder);
        hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder);

        Map<String, Object> searchResponseAsMap1 = search(TEST_MULTI_DOC_INDEX_NAME, hybridQueryBuilderNeuralThenTerm, 10);

        assertEquals(3, getHitCount(searchResponseAsMap1));

        List<Map<String, Object>> hits1NestedList = getNestedHits(searchResponseAsMap1);
        List<String> ids1 = new ArrayList<>();
        List<Double> scores1 = new ArrayList<>();
        for (Map<String, Object> oneHit : hits1NestedList) {
            ids1.add((String) oneHit.get("_id"));
            scores1.add((Double) oneHit.get("_score"));
        }

        // verify that scores are in desc order
        assertTrue(IntStream.range(0, scores1.size() - 1).noneMatch(idx -> scores1.get(idx) < scores1.get(idx + 1)));
        // verify that all ids are unique
        assertEquals(Set.copyOf(ids1).size(), ids1.size());

        // check similar query when sub-queries are in reverse order, results must be same as in previous test case
        HybridQueryBuilder hybridQueryBuilderTermThenNeural = new HybridQueryBuilder();
        hybridQueryBuilderTermThenNeural.add(termQueryBuilder);
        hybridQueryBuilderTermThenNeural.add(neuralQueryBuilder);

        Map<String, Object> searchResponseAsMap2 = search(TEST_MULTI_DOC_INDEX_NAME, hybridQueryBuilderNeuralThenTerm, 10);

        assertEquals(3, getHitCount(searchResponseAsMap2));

        List<String> ids2 = new ArrayList<>();
        List<Double> scores2 = new ArrayList<>();
        for (Map<String, Object> oneHit : hits1NestedList) {
            ids2.add((String) oneHit.get("_id"));
            scores2.add((Double) oneHit.get("_score"));
        }
        // doc ids must match same from the previous query, order of sub-queries doesn't change the result
        assertEquals(ids1, ids2);
        assertEquals(scores1, scores2);
    }

    @SneakyThrows
    public void test_whenOnlyTermSubQueryWithoutMatch_thenEmptyResult() {
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_NAME);

        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT);
        TermQueryBuilder termQuery2Builder = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT2);
        HybridQueryBuilder hybridQueryBuilderOnlyTerm = new HybridQueryBuilder();
        hybridQueryBuilderOnlyTerm.add(termQueryBuilder);
        hybridQueryBuilderOnlyTerm.add(termQuery2Builder);

        Map<String, Object> searchResponseAsMap = search(TEST_MULTI_DOC_INDEX_NAME, hybridQueryBuilderOnlyTerm, 10);

        assertEquals(0, getHitCount(searchResponseAsMap));
    }

    @SneakyThrows
    public void testBoostQuery_whenHybridWithBoost_thenScoreMultipliedCorrectly() {
        initializeIndexIfNotExist(TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME);

        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_1,
            TEST_QUERY_TEXT,
            modelId.get(),
            1,
            null,
            null
        );
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);

        final float boost = 2.0f;

        HybridQueryBuilder hybridQueryBuilderNeuralThenTerm = new HybridQueryBuilder();
        hybridQueryBuilderNeuralThenTerm.add(neuralQueryBuilder);
        hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder);
        hybridQueryBuilderNeuralThenTerm.boost(boost);

        Map<String, Object> searchResponseAsMap = search(TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME, hybridQueryBuilderNeuralThenTerm, 1);

        assertEquals(1, getHitCount(searchResponseAsMap));

        List<Map<String, Object>> hits1NestedList = getNestedHits(searchResponseAsMap);
        List<String> ids1 = new ArrayList<>();
        List<Double> scores1 = new ArrayList<>();
        for (Map<String, Object> oneHit : hits1NestedList) {
            ids1.add((String) oneHit.get("_id"));
            scores1.add((Double) oneHit.get("_score"));
        }

        float expectedScore = boost * (computeExpectedScore(modelId.get(), testVector1, TEST_SPACE_TYPE, TEST_QUERY_TEXT)
            + EXPECTED_SCORE_BM25);
        assertEquals(expectedScore, objectToFloat(scores1.get(0)), 0.001f);
    }

    private void initializeIndexIfNotExist(String indexName) throws IOException {
        if (TEST_BASIC_INDEX_NAME.equals(indexName) && !indexExists(TEST_BASIC_INDEX_NAME)) {
            prepareKnnIndex(
                TEST_BASIC_INDEX_NAME,
                Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE))
            );
            addKnnDoc(
                TEST_BASIC_INDEX_NAME,
                "1",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector1).toArray())
            );
            assertEquals(1, getDocCount(TEST_BASIC_INDEX_NAME));
        }
        if (TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME.equals(indexName) && !indexExists(TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME)) {
            prepareKnnIndex(
                TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME,
                List.of(
                    new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE),
                    new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_2, TEST_DIMENSION, TEST_SPACE_TYPE)
                )
            );
            addKnnDoc(
                TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME,
                "1",
                List.of(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_KNN_VECTOR_FIELD_NAME_2),
                List.of(Floats.asList(testVector1).toArray(), Floats.asList(testVector1).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT1)
            );
            addKnnDoc(
                TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME,
                "2",
                List.of(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_KNN_VECTOR_FIELD_NAME_2),
                List.of(Floats.asList(testVector2).toArray(), Floats.asList(testVector2).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_2),
                Collections.singletonList(TEST_DOC_TEXT2)
            );
            addKnnDoc(
                TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME,
                "3",
                List.of(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_KNN_VECTOR_FIELD_NAME_2),
                List.of(Floats.asList(testVector3).toArray(), Floats.asList(testVector3).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_3),
                Collections.singletonList(TEST_DOC_TEXT3)
            );
            assertEquals(3, getDocCount(TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME));
        }

        if (TEST_MULTI_DOC_INDEX_NAME.equals(indexName) && !indexExists(TEST_MULTI_DOC_INDEX_NAME)) {
            prepareKnnIndex(
                TEST_MULTI_DOC_INDEX_NAME,
                Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE))
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_NAME,
                "1",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector1).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT1)
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_NAME,
                "2",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector2).toArray())
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_NAME,
                "3",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector3).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT2)
            );
            assertEquals(3, getDocCount(TEST_MULTI_DOC_INDEX_NAME));
        }
    }

    private static List<Map<String, Object>> getNestedHits(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        return (List<Map<String, Object>>) hitsMap.get("hits");
    }
}
