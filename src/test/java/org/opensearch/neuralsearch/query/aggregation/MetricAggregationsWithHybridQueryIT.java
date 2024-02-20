/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query.aggregation;

import lombok.SneakyThrows;
import org.junit.Before;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.opensearch.neuralsearch.TestUtils.DELTA_FOR_SCORE_ASSERTION;
import static org.opensearch.neuralsearch.TestUtils.RELATION_EQUAL_TO;
import static org.opensearch.neuralsearch.util.AggregationsUtils.getAggregationValue;
import static org.opensearch.neuralsearch.util.AggregationsUtils.getAggregationValues;
import static org.opensearch.neuralsearch.util.AggregationsUtils.getAggregations;
import static org.opensearch.neuralsearch.util.AggregationsUtils.getNestedHits;
import static org.opensearch.neuralsearch.util.AggregationsUtils.getTotalHits;

public class MetricAggregationsWithHybridQueryIT extends BaseNeuralSearchIT {
    private static final String TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD = "test-neural-aggs-multi-doc-index-single-shard";
    private static final String TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS = "test-neural-aggs-multi-doc-index-multiple-shards";
    private static final String TEST_QUERY_TEXT3 = "hello";
    private static final String TEST_QUERY_TEXT4 = "cost";
    private static final String TEST_QUERY_TEXT5 = "welcome";
    private static final String TEST_DOC_TEXT1 = "Hello world";
    private static final String TEST_DOC_TEXT2 = "Hi to this place";
    private static final String TEST_DOC_TEXT3 = "We would like to welcome everyone";
    private static final String TEST_DOC_TEXT4 = "Hello, I'm glad to you see you pal";
    private static final String TEST_DOC_TEXT5 = "People keep telling me orange but I still prefer pink";
    private static final String TEST_DOC_TEXT6 = "She traveled because it cost the same as therapy and was a lot more enjoyable";
    private static final String TEST_TEXT_FIELD_NAME_1 = "test-text-field-1";
    private static final String INTEGER_FIELD_1 = "doc_index";
    private static final int INTEGER_FIELD_1_VALUE = 1234;
    private static final int INTEGER_FIELD_2_VALUE = 2345;
    private static final int INTEGER_FIELD_3_VALUE = 3456;
    private static final int INTEGER_FIELD_4_VALUE = 4567;
    private static final String KEYWORD_FIELD_1 = "doc_keyword";
    private static final String KEYWORD_FIELD_1_VALUE = "workable";
    private static final String KEYWORD_FIELD_2_VALUE = "angry";
    private static final String KEYWORD_FIELD_3_VALUE = "likeable";
    private static final String KEYWORD_FIELD_4_VALUE = "entire";
    private static final String DATE_FIELD_1 = "doc_date";
    private static final String DATE_FIELD_1_VALUE = "01/03/1995";
    private static final String DATE_FIELD_2_VALUE = "05/02/2015";
    private static final String DATE_FIELD_3_VALUE = "07/23/2007";
    private static final String DATE_FIELD_4_VALUE = "08/21/2012";
    private static final String INTEGER_FIELD_PRICE = "doc_price";
    private static final int INTEGER_FIELD_PRICE_1_VALUE = 130;
    private static final int INTEGER_FIELD_PRICE_2_VALUE = 100;
    private static final int INTEGER_FIELD_PRICE_3_VALUE = 200;
    private static final int INTEGER_FIELD_PRICE_4_VALUE = 25;
    private static final int INTEGER_FIELD_PRICE_5_VALUE = 30;
    private static final int INTEGER_FIELD_PRICE_6_VALUE = 350;
    private static final String BUCKET_AGG_DOC_COUNT_FIELD = "doc_count";
    private static final String KEY = "key";
    private static final String BUCKET_AGG_KEY_AS_STRING = "key_as_string";
    private static final String SEARCH_PIPELINE = "phase-results-hybrid-pipeline";
    private static final String MAX_AGGREGATION_NAME = "max_aggs";
    private static final String SUM_AGGREGATION_NAME = "sum_aggs";
    private static final String AVG_AGGREGATION_NAME = "avg_field";
    private static final String GENERIC_AGGREGATION_NAME = "my_aggregation";
    private static final String BUCKETS_AGGREGATION_NAME_1 = "date_buckets_1";
    private static final String BUCKETS_AGGREGATION_NAME_2 = "date_buckets_2";
    private static final String BUCKETS_AGGREGATION_NAME_3 = "date_buckets_3";
    private static final String BUCKETS_AGGREGATION_NAME_4 = "date_buckets_4";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
    }

    @Override
    public boolean isUpdateClusterSettings() {
        return false;
    }

    @Override
    protected void updateClusterSettings() {
        updateClusterSettings("plugins.ml_commons.only_run_on_ml_node", false);
        // default threshold for native circuit breaker is 90, it may be not enough on test runner machine
        updateClusterSettings("plugins.ml_commons.native_memory_threshold", 100);
        updateClusterSettings("plugins.ml_commons.allow_registering_model_via_url", true);
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
    public void testSingleShardAndMetricAgg_whenMaxAggs_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", false);
        testMaxAggsOnSingleShardCluster();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenMaxAggs_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", true);
        testMaxAggsOnSingleShardCluster();
    }

    @SneakyThrows
    public void testMetricAgg_whenAvgAggs_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", false);
        testAvgAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenAvgAggs_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", true);
        testAvgAggs();
    }

    @SneakyThrows
    public void testMetricAggs_whenCardinalityAggs_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", false);
        testCardinalityAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenCardinalityAggs_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", true);
        testCardinalityAggs();
    }

    @SneakyThrows
    public void testMetricAggs_whenExtendedStatsAggs_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", false);
        testExtendedStatsAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenExtendedStatsAggs_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", true);
        testExtendedStatsAggs();
    }

    @SneakyThrows
    public void testMetricAggs_whenTopHitsAggs_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", false);
        testTopHitsAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenTopHitsAggs_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", true);
        testTopHitsAggs();
    }

    @SneakyThrows
    public void testMetricAggs_whenPercentileRank_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", false);
        testPercentileRankAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenPercentileRank_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", true);
        testPercentileRankAggs();
    }

    @SneakyThrows
    public void testMetricAggs_whenPercentile_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", false);
        testPercentileAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenPercentile_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", true);
        testPercentileAggs();
    }

    @SneakyThrows
    public void testMetricAggs_whenScriptedMetrics_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", false);
        testScriptedMetricsAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenScriptedMetrics_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", true);
        testScriptedMetricsAggs();
    }

    @SneakyThrows
    public void testMetricAggs_whenSumAgg_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", false);
        testSumAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenSumAgg_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", true);
        testSumAggs();
    }

    @SneakyThrows
    public void testMetricAggs_whenValueCount_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", false);
        testValueCountAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenValueCount_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", true);
        testValueCountAggs();
    }

    @SneakyThrows
    public void testAggWithPostFilter_whenSumAggAndRangeFilter_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", false);
        testSumAggsAndRangePostFilter();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenSumAggAndRangeFilter_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", true);
        testSumAggsAndRangePostFilter();
    }

    private void testMaxAggsOnSingleShardCluster() throws Exception {
        try {
            prepareResourcesForSingleShardIndex(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD, SEARCH_PIPELINE);

            TermQueryBuilder termQueryBuilder1 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
            TermQueryBuilder termQueryBuilder2 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT5);

            HybridQueryBuilder hybridQueryBuilderNeuralThenTerm = new HybridQueryBuilder();
            hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder1);
            hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder2);

            AggregationBuilder aggsBuilder = AggregationBuilders.max(MAX_AGGREGATION_NAME).field(INTEGER_FIELD_1);
            Map<String, Object> searchResponseAsMap = search(
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD,
                hybridQueryBuilderNeuralThenTerm,
                null,
                10,
                Map.of("search_pipeline", SEARCH_PIPELINE),
                List.of(aggsBuilder)
            );

            assertHitResultsFromQuery(2, searchResponseAsMap);

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);
            assertTrue(aggregations.containsKey(MAX_AGGREGATION_NAME));
            double maxAggsValue = getAggregationValue(aggregations, MAX_AGGREGATION_NAME);
            assertTrue(maxAggsValue >= 0);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD, null, null, SEARCH_PIPELINE);
        }
    }

    private void testAvgAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

            AggregationBuilder aggsBuilder = AggregationBuilders.avg(AVG_AGGREGATION_NAME).field(INTEGER_FIELD_1);
            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);
            assertTrue(aggregations.containsKey(AVG_AGGREGATION_NAME));
            double maxAggsValue = getAggregationValue(aggregations, AVG_AGGREGATION_NAME);
            assertEquals(maxAggsValue, 2345.0, DELTA_FOR_SCORE_ASSERTION);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testCardinalityAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);
            AggregationBuilder aggsBuilder = AggregationBuilders.cardinality(GENERIC_AGGREGATION_NAME).field(INTEGER_FIELD_1);
            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);
            assertTrue(aggregations.containsKey(GENERIC_AGGREGATION_NAME));
            int aggsValue = getAggregationValue(aggregations, GENERIC_AGGREGATION_NAME);
            assertEquals(aggsValue, 3);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testExtendedStatsAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);
            AggregationBuilder aggsBuilder = AggregationBuilders.extendedStats(GENERIC_AGGREGATION_NAME).field(INTEGER_FIELD_1);
            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

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
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testTopHitsAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);
            AggregationBuilder aggsBuilder = AggregationBuilders.topHits(GENERIC_AGGREGATION_NAME).size(4);
            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);
            assertTrue(aggregations.containsKey(GENERIC_AGGREGATION_NAME));
            Map<String, Object> aggsValues = getAggregationValues(aggregations, GENERIC_AGGREGATION_NAME);
            assertNotNull(aggsValues);
            assertHitResultsFromQuery(3, aggsValues);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testScriptedMetricsAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);
            // compute sum of all int fields that are not blank
            AggregationBuilder aggsBuilder = AggregationBuilders.scriptedMetric(GENERIC_AGGREGATION_NAME)
                .initScript(new Script("state.price = []"))
                .mapScript(
                    new Script("state.price.add(doc[\"" + INTEGER_FIELD_1 + "\"].size() == 0 ? 0 : doc." + INTEGER_FIELD_1 + ".value)")
                )
                .combineScript(new Script("state.price.stream().mapToInt(Integer::intValue).sum()"))
                .reduceScript(new Script("states.stream().mapToInt(Integer::intValue).sum()"));
            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);
            assertTrue(aggregations.containsKey(GENERIC_AGGREGATION_NAME));
            int aggsValue = getAggregationValue(aggregations, GENERIC_AGGREGATION_NAME);
            assertEquals(7035, aggsValue);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testPercentileAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);
            AggregationBuilder aggsBuilder = AggregationBuilders.percentiles(GENERIC_AGGREGATION_NAME).field(INTEGER_FIELD_1);
            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            assertHitResultsFromQuery(3, searchResponseAsMap);

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);
            assertTrue(aggregations.containsKey(GENERIC_AGGREGATION_NAME));
            Map<String, Map<String, Double>> aggsValues = getAggregationValues(aggregations, GENERIC_AGGREGATION_NAME);
            assertNotNull(aggsValues);

            Map<String, Double> values = aggsValues.get("values");
            assertNotNull(values);
            assertEquals(7, values.size());
            assertEquals(1234.0, values.get("1.0"), DELTA_FOR_SCORE_ASSERTION);
            assertEquals(1234.0, values.get("5.0"), DELTA_FOR_SCORE_ASSERTION);
            assertEquals(1234.0, values.get("25.0"), DELTA_FOR_SCORE_ASSERTION);
            assertEquals(2345.0, values.get("50.0"), DELTA_FOR_SCORE_ASSERTION);
            assertEquals(3456.0, values.get("75.0"), DELTA_FOR_SCORE_ASSERTION);
            assertEquals(3456.0, values.get("95.0"), DELTA_FOR_SCORE_ASSERTION);
            assertEquals(3456.0, values.get("99.0"), DELTA_FOR_SCORE_ASSERTION);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testPercentileRankAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);
            AggregationBuilder aggsBuilder = AggregationBuilders.percentileRanks(GENERIC_AGGREGATION_NAME, new double[] { 2000, 3000 })
                .field(INTEGER_FIELD_1);
            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            assertHitResultsFromQuery(3, searchResponseAsMap);

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);
            assertTrue(aggregations.containsKey(GENERIC_AGGREGATION_NAME));
            Map<String, Map<String, Double>> aggsValues = getAggregationValues(aggregations, GENERIC_AGGREGATION_NAME);
            assertNotNull(aggsValues);
            Map<String, Double> values = aggsValues.get("values");
            assertNotNull(values);
            assertEquals(33.333, values.get("2000.0"), DELTA_FOR_SCORE_ASSERTION);
            assertEquals(66.666, values.get("3000.0"), DELTA_FOR_SCORE_ASSERTION);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testSumAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

            AggregationBuilder aggsBuilder = AggregationBuilders.sum(SUM_AGGREGATION_NAME).field(INTEGER_FIELD_1);
            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);
            assertTrue(aggregations.containsKey(SUM_AGGREGATION_NAME));
            double maxAggsValue = getAggregationValue(aggregations, SUM_AGGREGATION_NAME);
            assertEquals(7035.0, maxAggsValue, DELTA_FOR_SCORE_ASSERTION);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testValueCountAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);
            AggregationBuilder aggsBuilder = AggregationBuilders.count(GENERIC_AGGREGATION_NAME).field(INTEGER_FIELD_1);
            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            assertHitResultsFromQuery(3, searchResponseAsMap);

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);

            assertTrue(aggregations.containsKey(GENERIC_AGGREGATION_NAME));
            assertEquals(3, (int) getAggregationValue(aggregations, GENERIC_AGGREGATION_NAME));
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testSumAggsAndRangePostFilter() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

            AggregationBuilder aggsBuilder = AggregationBuilders.sum(SUM_AGGREGATION_NAME).field(INTEGER_FIELD_1);

            TermQueryBuilder termQueryBuilder1 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
            TermQueryBuilder termQueryBuilder2 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT4);
            TermQueryBuilder termQueryBuilder3 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT5);

            HybridQueryBuilder hybridQueryBuilderNeuralThenTerm = new HybridQueryBuilder();
            hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder1);
            hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder2);
            hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder3);

            QueryBuilder rangeFilterQuery = QueryBuilders.rangeQuery(INTEGER_FIELD_1).gte(3000).lte(5000);

            Map<String, Object> searchResponseAsMap = search(
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
                hybridQueryBuilderNeuralThenTerm,
                null,
                10,
                Map.of("search_pipeline", SEARCH_PIPELINE),
                List.of(aggsBuilder),
                rangeFilterQuery
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);
            assertTrue(aggregations.containsKey(SUM_AGGREGATION_NAME));
            double maxAggsValue = getAggregationValue(aggregations, SUM_AGGREGATION_NAME);
            assertEquals(11602.0, maxAggsValue, DELTA_FOR_SCORE_ASSERTION);

            assertHitResultsFromQuery(2, searchResponseAsMap);

            // assert post-filter
            List<Map<String, Object>> hitsNestedList = getNestedHits(searchResponseAsMap);

            List<Integer> docIndexes = new ArrayList<>();
            for (Map<String, Object> oneHit : hitsNestedList) {
                assertNotNull(oneHit.get("_source"));
                Map<String, Object> source = (Map<String, Object>) oneHit.get("_source");
                int docIndex = (int) source.get(INTEGER_FIELD_1);
                docIndexes.add(docIndex);
            }
            assertEquals(0, docIndexes.stream().filter(docIndex -> docIndex < 3000 || docIndex > 5000).count());
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private Map<String, Object> executeQueryAndGetAggsResults(final Object aggsBuilder, String indexName) {
        return executeQueryAndGetAggsResults(List.of(aggsBuilder), indexName);
    }

    private Map<String, Object> executeQueryAndGetAggsResults(
        final List<Object> aggsBuilders,
        QueryBuilder queryBuilder,
        String indexName,
        int expectedHits
    ) {
        initializeIndexIfNotExist(indexName);

        Map<String, Object> searchResponseAsMap = search(
            indexName,
            queryBuilder,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            aggsBuilders
        );

        assertHitResultsFromQuery(expectedHits, searchResponseAsMap);
        return searchResponseAsMap;
    }

    private Map<String, Object> executeQueryAndGetAggsResults(
        final List<Object> aggsBuilders,
        QueryBuilder queryBuilder,
        String indexName
    ) {
        return executeQueryAndGetAggsResults(aggsBuilders, queryBuilder, indexName, 3);
    }

    private Map<String, Object> executeQueryAndGetAggsResults(final List<Object> aggsBuilders, String indexName) {

        TermQueryBuilder termQueryBuilder1 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
        TermQueryBuilder termQueryBuilder2 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT5);

        HybridQueryBuilder hybridQueryBuilderNeuralThenTerm = new HybridQueryBuilder();
        hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder1);
        hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder2);

        return executeQueryAndGetAggsResults(aggsBuilders, hybridQueryBuilderNeuralThenTerm, indexName);
    }

    private void assertHitResultsFromQuery(int expected, Map<String, Object> searchResponseAsMap) {
        assertEquals(expected, getHitCount(searchResponseAsMap));

        List<Map<String, Object>> hits1NestedList = getNestedHits(searchResponseAsMap);
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

        Map<String, Object> total = getTotalHits(searchResponseAsMap);
        assertNotNull(total.get("value"));
        assertEquals(expected, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));
    }

    @SneakyThrows
    private void initializeIndexIfNotExist(String indexName) {
        if (!indexExists(indexName)) {
            createIndexWithConfiguration(
                indexName,
                buildIndexConfiguration(List.of(), List.of(), List.of(INTEGER_FIELD_1), List.of(KEYWORD_FIELD_1), List.of(DATE_FIELD_1), 3),
                ""
            );

            addKnnDoc(
                indexName,
                "1",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT1),
                List.of(),
                List.of(),
                List.of(INTEGER_FIELD_1, INTEGER_FIELD_PRICE),
                List.of(INTEGER_FIELD_1_VALUE, INTEGER_FIELD_PRICE_1_VALUE),
                List.of(KEYWORD_FIELD_1),
                List.of(KEYWORD_FIELD_1_VALUE),
                List.of(DATE_FIELD_1),
                List.of(DATE_FIELD_1_VALUE)
            );
            addKnnDoc(
                indexName,
                "2",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT3),
                List.of(),
                List.of(),
                List.of(INTEGER_FIELD_1, INTEGER_FIELD_PRICE),
                List.of(INTEGER_FIELD_2_VALUE, INTEGER_FIELD_PRICE_2_VALUE),
                List.of(),
                List.of(),
                List.of(DATE_FIELD_1),
                List.of(DATE_FIELD_2_VALUE)
            );
            addKnnDoc(
                indexName,
                "3",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT2),
                List.of(),
                List.of(),
                List.of(INTEGER_FIELD_PRICE),
                List.of(INTEGER_FIELD_PRICE_3_VALUE),
                List.of(KEYWORD_FIELD_1),
                List.of(KEYWORD_FIELD_2_VALUE),
                List.of(DATE_FIELD_1),
                List.of(DATE_FIELD_3_VALUE)
            );
            addKnnDoc(
                indexName,
                "4",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT4),
                List.of(),
                List.of(),
                List.of(INTEGER_FIELD_1, INTEGER_FIELD_PRICE),
                List.of(INTEGER_FIELD_3_VALUE, INTEGER_FIELD_PRICE_4_VALUE),
                List.of(KEYWORD_FIELD_1),
                List.of(KEYWORD_FIELD_3_VALUE),
                List.of(DATE_FIELD_1),
                List.of(DATE_FIELD_2_VALUE)
            );
            addKnnDoc(
                indexName,
                "5",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT5),
                List.of(),
                List.of(),
                List.of(INTEGER_FIELD_1, INTEGER_FIELD_PRICE),
                List.of(INTEGER_FIELD_3_VALUE, INTEGER_FIELD_PRICE_5_VALUE),
                List.of(KEYWORD_FIELD_1),
                List.of(KEYWORD_FIELD_4_VALUE),
                List.of(DATE_FIELD_1),
                List.of(DATE_FIELD_4_VALUE)
            );
            addKnnDoc(
                indexName,
                "6",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT6),
                List.of(),
                List.of(),
                List.of(INTEGER_FIELD_1, INTEGER_FIELD_PRICE),
                List.of(INTEGER_FIELD_4_VALUE, INTEGER_FIELD_PRICE_6_VALUE),
                List.of(KEYWORD_FIELD_1),
                List.of(KEYWORD_FIELD_4_VALUE),
                List.of(DATE_FIELD_1),
                List.of(DATE_FIELD_4_VALUE)
            );
        }
    }

    @SneakyThrows
    private void initializeIndexWithOneShardIfNotExists(String indexName) {
        if (!indexExists(indexName)) {
            createIndexWithConfiguration(
                indexName,
                buildIndexConfiguration(List.of(), List.of(), List.of(INTEGER_FIELD_1), List.of(KEYWORD_FIELD_1), List.of(), 1),
                ""
            );

            addKnnDoc(
                indexName,
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
                List.of(),
                List.of(),
                List.of()
            );

            addKnnDoc(
                indexName,
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
                List.of(),
                List.of(),
                List.of()
            );
        }
    }

    void prepareResources(String indexName, String pipelineName) {
        initializeIndexIfNotExist(indexName);
        createSearchPipelineWithResultsPostProcessor(pipelineName);
    }

    void prepareResourcesForSingleShardIndex(String indexName, String pipelineName) {
        initializeIndexWithOneShardIfNotExists(indexName);
        createSearchPipelineWithResultsPostProcessor(pipelineName);
    }
}
