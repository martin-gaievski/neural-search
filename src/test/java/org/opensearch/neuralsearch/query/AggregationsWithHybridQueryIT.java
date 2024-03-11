/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Before;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static org.opensearch.neuralsearch.TestUtils.DELTA_FOR_SCORE_ASSERTION;
import static org.opensearch.neuralsearch.TestUtils.RELATION_EQUAL_TO;
import static org.opensearch.neuralsearch.TestUtils.TEST_DIMENSION;
import static org.opensearch.neuralsearch.TestUtils.createRandomVector;

public class AggregationsWithHybridQueryIT extends BaseNeuralSearchIT {
    private static final String TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD = "test-neural-aggs-multi-doc-index-single-shard";
    private static final String TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS = "test-neural-aggs-multi-doc-index-multiple-shards";
    private static final String TEST_QUERY_TEXT = "greetings";
    private static final String TEST_QUERY_TEXT2 = "salute";
    private static final String TEST_QUERY_TEXT3 = "hello";
    private static final String TEST_QUERY_TEXT4 = "place";
    private static final String TEST_QUERY_TEXT5 = "welcome";
    private static final String TEST_DOC_TEXT1 = "Hello world";
    private static final String TEST_DOC_TEXT2 = "Hi to this place";
    private static final String TEST_DOC_TEXT3 = "We would like to welcome everyone";
    private static final String TEST_DOC_TEXT4 = "Hello, I'm glad to you see you pal";
    private static final String TEST_DOC_TEXT5 = "People keep telling me orange but I still prefer pink";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_1 = "test-knn-vector-1";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_2 = "test-knn-vector-2";
    private static final String TEST_TEXT_FIELD_NAME_1 = "test-text-field-1";
    private static final String TEST_NESTED_TYPE_FIELD_NAME_1 = "user";
    private static final String NESTED_FIELD_1 = "firstname";
    private static final String NESTED_FIELD_2 = "lastname";
    private static final String NESTED_FIELD_1_VALUE = "john";
    private static final String NESTED_FIELD_2_VALUE = "black";
    private static final String INTEGER_FIELD_1 = "doc_index";
    private static final int INTEGER_FIELD_1_VALUE = 1234;
    private static final int INTEGER_FIELD_2_VALUE = 2345;
    private static final int INTEGER_FIELD_3_VALUE = 3456;
    private static final String KEYWORD_FIELD_1 = "doc_keyword";
    private static final String KEYWORD_FIELD_1_VALUE = "workable";
    private static final String KEYWORD_FIELD_2_VALUE = "angry";
    private static final String KEYWORD_FIELD_3_VALUE = "likeable";
    private static final String KEYWORD_FIELD_4_VALUE = "entire";
    private final float[] testVector1 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector2 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector3 = createRandomVector(TEST_DIMENSION);
    private static final String SEARCH_PIPELINE = "phase-results-hybrid-pipeline";
    private static final String MAX_AGGREGATION_NAME = "max_aggs";
    private static final String AVG_AGGREGATION_NAME = "avg_field";
    private static final String GENERIC_AGGREGATION_NAME = "my_aggregation";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
        prepareModel();
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
    }

    @After
    @SneakyThrows
    public void tearDown() {
        super.tearDown();
        deleteSearchPipeline(SEARCH_PIPELINE);
    }

    @Override
    public boolean isUpdateClusterSettings() {
        return false;
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    /**
     * Tests complex query with multiple nested sub-queries:
     * {
     *     "query": {
     *         "hybrid": {
     *              "queries": [
     *                  {
     *                      "term": {
     *                          "text": "word1"
     *                       }
     *                  },
     *                  {
     *                      "term": {
     *                          "text": "word3"
     *                      }
     *                  }
     *              ]
     *          }
     *      },
     *     "aggs": {
     *         "max_index": {
     *             "max": {
     *                 "field": "doc_index"
     *             }
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testSingleShardAndMetric_whenMaxAggs_thenSuccessful() {
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);

        TermQueryBuilder termQueryBuilder1 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
        TermQueryBuilder termQueryBuilder2 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT5);

        HybridQueryBuilder hybridQueryBuilderNeuralThenTerm = new HybridQueryBuilder();
        hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder1);
        hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder2);

        AggregationBuilder aggsBuilder = AggregationBuilders.max(MAX_AGGREGATION_NAME).field(INTEGER_FIELD_1);
        Map<String, Object> searchResponseAsMap1 = search(
            TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD,
            hybridQueryBuilderNeuralThenTerm,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            aggsBuilder
        );

        assertHitResultsFromQuery(1, searchResponseAsMap1);

        Map<String, Object> aggregations = getAggregations(searchResponseAsMap1);
        assertNotNull(aggregations);
        assertTrue(aggregations.containsKey(MAX_AGGREGATION_NAME));
        double maxAggsValue = getAggregationValue(aggregations, MAX_AGGREGATION_NAME);
        assertTrue(maxAggsValue >= 0);
    }

    @SneakyThrows
    public void testMultipleShardsAndMetricAggs_whenAvgAggs_thenSuccessful() {
        AggregationBuilder aggsBuilder = AggregationBuilders.avg(AVG_AGGREGATION_NAME).field(INTEGER_FIELD_1);
        Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(aggsBuilder);

        Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
        assertNotNull(aggregations);
        assertTrue(aggregations.containsKey(AVG_AGGREGATION_NAME));
        double maxAggsValue = getAggregationValue(aggregations, AVG_AGGREGATION_NAME);
        assertEquals(maxAggsValue, 2345.0, DELTA_FOR_SCORE_ASSERTION);
    }

    @SneakyThrows
    public void testMetricAggs_whenCardinalityAggs_thenSuccessful() {
        AggregationBuilder aggsBuilder = AggregationBuilders.cardinality(GENERIC_AGGREGATION_NAME).field(INTEGER_FIELD_1);
        Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(aggsBuilder);

        Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
        assertNotNull(aggregations);
        assertTrue(aggregations.containsKey(GENERIC_AGGREGATION_NAME));
        int aggsValue = getAggregationValue(aggregations, GENERIC_AGGREGATION_NAME);
        assertEquals(aggsValue, 3);
    }

    @SneakyThrows
    public void testMetricAggs_whenExtendedStatsAggs_thenSuccessful() {
        AggregationBuilder aggsBuilder = AggregationBuilders.extendedStats(GENERIC_AGGREGATION_NAME).field(INTEGER_FIELD_1);
        Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(aggsBuilder);

        Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
        assertNotNull(aggregations);
        assertTrue(aggregations.containsKey(GENERIC_AGGREGATION_NAME));
        Map<String, Object> extendedStatsValues = getAggregationValues(aggregations, GENERIC_AGGREGATION_NAME);
        assertNotNull(extendedStatsValues);

        assertEquals((double) extendedStatsValues.get("max"), 3456.0, DELTA_FOR_SCORE_ASSERTION);
        assertEquals((int) extendedStatsValues.get("count"), 3);
        assertEquals((double) extendedStatsValues.get("sum"), 7035.0, DELTA_FOR_SCORE_ASSERTION);
        assertEquals((double) extendedStatsValues.get("avg"), 2345.0, DELTA_FOR_SCORE_ASSERTION);
        assertEquals((double) extendedStatsValues.get("variance"), 822880.666, DELTA_FOR_SCORE_ASSERTION);
        assertEquals((double) extendedStatsValues.get("std_deviation"), 907.127, DELTA_FOR_SCORE_ASSERTION);
    }

    @SneakyThrows
    public void testMetricAggs_whenTopHitsAggs_thenSuccessful() {
        AggregationBuilder aggsBuilder = AggregationBuilders.topHits(GENERIC_AGGREGATION_NAME).size(4);
        Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(aggsBuilder);

        Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
        assertNotNull(aggregations);
        assertTrue(aggregations.containsKey(GENERIC_AGGREGATION_NAME));
        Map<String, Object> aggsValues = getAggregationValues(aggregations, GENERIC_AGGREGATION_NAME);
        assertNotNull(aggsValues);
        assertHitResultsFromQuery(3, aggsValues);
    }

    @SneakyThrows
    public void testMetricAggs_whenScriptedMetrics_thenSuccessful() {
        // compute sum of all int fields that are not blank
        AggregationBuilder aggsBuilder = AggregationBuilders.scriptedMetric(GENERIC_AGGREGATION_NAME)
            .initScript(new Script("state.price = []"))
            .mapScript(new Script("state.price.add(doc[\"" + INTEGER_FIELD_1 + "\"].size() == 0 ? 0 : doc." + INTEGER_FIELD_1 + ".value)"))
            .combineScript(new Script("state.price.stream().mapToInt(Integer::intValue).sum()"))
            .reduceScript(new Script("states.stream().mapToInt(Integer::intValue).sum()"));
        Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(aggsBuilder);

        Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
        assertNotNull(aggregations);
        assertTrue(aggregations.containsKey(GENERIC_AGGREGATION_NAME));
        int aggsValue = getAggregationValue(aggregations, GENERIC_AGGREGATION_NAME);
        assertEquals(7035, aggsValue);
    }

    @SneakyThrows
    public void testMetricAggs_whenPercentileRank_thenSuccessful() {
        AggregationBuilder aggsBuilder = AggregationBuilders.percentileRanks(GENERIC_AGGREGATION_NAME, new double[] { 2000, 3000 })
            .field(INTEGER_FIELD_1);
        Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(aggsBuilder);

        Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
        assertNotNull(aggregations);
        assertTrue(aggregations.containsKey(GENERIC_AGGREGATION_NAME));
        Map<String, Map<String, Double>> aggsValues = getAggregationValues(aggregations, GENERIC_AGGREGATION_NAME);
        assertNotNull(aggsValues);
        Map<String, Double> values = aggsValues.get("values");
        assertNotNull(values);
        assertEquals(33.333, values.get("2000.0"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals(66.666, values.get("3000.0"), DELTA_FOR_SCORE_ASSERTION);
    }

    @SneakyThrows
    public void testBucketAndNestedAggs_whenAvgNestedIntoFilter_thenSuccessful() {
        AggregationBuilder aggsBuilder = AggregationBuilders.filter(
            GENERIC_AGGREGATION_NAME,
            QueryBuilders.rangeQuery(INTEGER_FIELD_1).lte(3000)
        ).subAggregation(AggregationBuilders.avg(AVG_AGGREGATION_NAME).field(INTEGER_FIELD_1));
        Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(aggsBuilder);

        Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
        assertNotNull(aggregations);
        assertTrue(aggregations.containsKey(GENERIC_AGGREGATION_NAME));
        double avgValue = getAggregationValue(getAggregationValues(aggregations, GENERIC_AGGREGATION_NAME), AVG_AGGREGATION_NAME);
        assertEquals(1789.5, avgValue, DELTA_FOR_SCORE_ASSERTION);
    }

    @SneakyThrows
    public void testBucketAndNestedAggs_whenAdjacencyMatrix_thenSuccessful() {
        AggregationBuilder aggsBuilder = AggregationBuilders.adjacencyMatrix(
            GENERIC_AGGREGATION_NAME,
            Map.of(
                "grpA",
                QueryBuilders.matchQuery(KEYWORD_FIELD_1, KEYWORD_FIELD_1_VALUE),
                "grpB",
                QueryBuilders.matchQuery(KEYWORD_FIELD_1, KEYWORD_FIELD_2_VALUE),
                "grpC",
                QueryBuilders.matchQuery(KEYWORD_FIELD_1, KEYWORD_FIELD_3_VALUE)
            )
        );
        Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(aggsBuilder);

        Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
        assertNotNull(aggregations);
        List<Map<String, Object>> buckets = ((Map<String, List>) getAggregationValues(aggregations, GENERIC_AGGREGATION_NAME)).get(
            "buckets"
        );
        assertNotNull(buckets);
        assertEquals(2, buckets.size());
        Map<String, Object> grpA = buckets.get(0);
        assertEquals(1, grpA.get("doc_count"));
        assertEquals("grpA", grpA.get("key"));
        Map<String, Object> grpC = buckets.get(1);
        assertEquals(1, grpC.get("doc_count"));
        assertEquals("grpC", grpC.get("key"));
    }

    private Map<String, Object> executeQueryAndGetAggsResults(final AggregationBuilder aggsBuilder) {
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);

        TermQueryBuilder termQueryBuilder1 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
        TermQueryBuilder termQueryBuilder2 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT5);

        HybridQueryBuilder hybridQueryBuilderNeuralThenTerm = new HybridQueryBuilder();
        hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder1);
        hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder2);

        Map<String, Object> searchResponseAsMap = search(
            TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
            hybridQueryBuilderNeuralThenTerm,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            aggsBuilder
        );

        assertHitResultsFromQuery(2, searchResponseAsMap);
        return searchResponseAsMap;
    }

    private void assertHitResultsFromQuery(int expected, Map<String, Object> searchResponseAsMap1) {
        assertEquals(expected, getHitCount(searchResponseAsMap1));

        List<Map<String, Object>> hits1NestedList = getNestedHits(searchResponseAsMap1);
        List<String> ids = new ArrayList<>();
        List<Double> scores = new ArrayList<>();
        for (Map<String, Object> oneHit : hits1NestedList) {
            ids.add((String) oneHit.get("_id"));
            scores.add((Double) oneHit.get("_score"));
        }

        // verify that scores are in desc order
        assertTrue(IntStream.range(0, scores.size() - 1).noneMatch(idx -> scores.get(idx) < scores.get(idx + 1)));
        // verify that all ids are unique
        assertEquals(Set.copyOf(ids).size(), ids.size());

        Map<String, Object> total = getTotalHits(searchResponseAsMap1);
        assertNotNull(total.get("value"));
        assertEquals(expected, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));
    }

    @SneakyThrows
    private void initializeIndexIfNotExist(String indexName) {

        if (TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD.equals(indexName)
            && !indexExists(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD)) {
            createIndexWithConfiguration(
                indexName,
                buildIndexConfiguration(List.of(), List.of(), List.of(INTEGER_FIELD_1), List.of(KEYWORD_FIELD_1), 1),
                ""
            );

            addKnnDoc(
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD,
                "1",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT1),
                List.of(),
                List.of(),
                List.of(INTEGER_FIELD_1),
                List.of(INTEGER_FIELD_1_VALUE),
                List.of(),
                List.of()
            );

            addKnnDoc(
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD,
                "2",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT3),
                List.of(),
                List.of(),
                List.of(INTEGER_FIELD_1),
                List.of(INTEGER_FIELD_2_VALUE),
                List.of(),
                List.of()
            );
        }

        if (TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS.equals(indexName)
            && !indexExists(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS)) {
            createIndexWithConfiguration(
                indexName,
                buildIndexConfiguration(List.of(), List.of(), List.of(INTEGER_FIELD_1), List.of(), 3),
                ""
            );

            addKnnDoc(
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
                "1",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT1),
                List.of(),
                List.of(),
                List.of(INTEGER_FIELD_1),
                List.of(INTEGER_FIELD_1_VALUE),
                List.of(KEYWORD_FIELD_1),
                List.of(KEYWORD_FIELD_1_VALUE)
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
                "2",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT3),
                List.of(),
                List.of(),
                List.of(INTEGER_FIELD_1),
                List.of(INTEGER_FIELD_2_VALUE),
                List.of(),
                List.of()
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
                "3",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT2),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(KEYWORD_FIELD_1),
                List.of(KEYWORD_FIELD_2_VALUE)
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
                "4",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT4),
                List.of(),
                List.of(),
                List.of(INTEGER_FIELD_1),
                List.of(INTEGER_FIELD_3_VALUE),
                List.of(KEYWORD_FIELD_1),
                List.of(KEYWORD_FIELD_3_VALUE)
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
                "5",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT5),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(KEYWORD_FIELD_1),
                List.of(KEYWORD_FIELD_4_VALUE)
            );
        }
    }

    private List<Map<String, Object>> getNestedHits(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        return (List<Map<String, Object>>) hitsMap.get("hits");
    }

    private Map<String, Object> getTotalHits(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        return (Map<String, Object>) hitsMap.get("total");
    }

    private Optional<Float> getMaxScore(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        return hitsMap.get("max_score") == null ? Optional.empty() : Optional.of(((Double) hitsMap.get("max_score")).floatValue());
    }

    private Map<String, Object> getAggregations(final Map<String, Object> searchResponseAsMap) {
        Map<String, Object> aggsMap = (Map<String, Object>) searchResponseAsMap.get("aggregations");
        return aggsMap;
    }

    private <T> T getAggregationValue(final Map<String, Object> aggsMap, final String aggName) {
        Map<String, Object> aggValues = (Map<String, Object>) aggsMap.get(aggName);
        return (T) aggValues.get("value");
    }

    private <T> T getAggregationValues(final Map<String, Object> aggsMap, final String aggName) {
        return (T) aggsMap.get(aggName);
    }
}
