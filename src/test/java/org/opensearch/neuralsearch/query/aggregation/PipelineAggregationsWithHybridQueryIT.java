/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query.aggregation;

import lombok.SneakyThrows;
import org.junit.Before;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.PipelineAggregatorBuilders;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.pipeline.AvgBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.BucketMetricsPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.MaxBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.MinBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.StatsBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.SumBucketPipelineAggregationBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.opensearch.neuralsearch.TestUtils.DELTA_FOR_SCORE_ASSERTION;
import static org.opensearch.neuralsearch.TestUtils.RELATION_EQUAL_TO;
import static org.opensearch.neuralsearch.util.AggregationsUtils.getAggregationBuckets;
import static org.opensearch.neuralsearch.util.AggregationsUtils.getAggregationValue;
import static org.opensearch.neuralsearch.util.AggregationsUtils.getAggregationValues;
import static org.opensearch.neuralsearch.util.AggregationsUtils.getAggregations;
import static org.opensearch.neuralsearch.util.AggregationsUtils.getNestedHits;
import static org.opensearch.neuralsearch.util.AggregationsUtils.getTotalHits;

public class PipelineAggregationsWithHybridQueryIT extends BaseNeuralSearchIT {
    private static final String TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS =
        "test-neural-aggs-pipeline-multi-doc-index-multiple-shards";
    private static final String TEST_QUERY_TEXT3 = "hello";
    private static final String TEST_QUERY_TEXT5 = "welcome";
    private static final String TEST_DOC_TEXT1 = "Hello world";
    private static final String TEST_DOC_TEXT2 = "Hi to this place";
    private static final String TEST_DOC_TEXT3 = "We would like to welcome everyone";
    private static final String TEST_DOC_TEXT4 = "Hello, I'm glad to you see you pal";
    private static final String TEST_DOC_TEXT5 = "People keep telling me orange but I still prefer pink";
    private static final String TEST_DOC_TEXT6 = "She traveled because it cost the same as therapy and was a lot more enjoyable";
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
    private static final String SUM_AGGREGATION_NAME_2 = "sum_aggs_2";
    private static final String AVG_AGGREGATION_NAME = "avg_field";
    private static final String GENERIC_AGGREGATION_NAME = "my_aggregation";
    private static final String DATE_AGGREGATION_NAME = "date_aggregation";
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

    @SneakyThrows
    public void testQueryVariationsWithConcurrentSearch_whenAnyQueryAndAggCombination_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", true);
        testVariousQueries();
    }

    @SneakyThrows
    public void testQueryVariationsWithoutConcurrentSearch_whenAnyQueryAndAggCombination_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", false);
        testVariousQueries();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenDateBucketedSumsPipelinedToBucketStatsAggs_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", true);
        testDateBucketedSumsPipelinedToBucketStatsAggs();
    }

    @SneakyThrows
    public void testPipelineSiblingAggs_whenDateBucketedSumsPipelinedToBucketStatsAggs_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", false);
        testDateBucketedSumsPipelinedToBucketStatsAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenDateBucketedSumsPipelinedToBucketScriptedAggs_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", true);
        testDateBucketedSumsPipelinedToBucketScriptedAggs();
    }

    @SneakyThrows
    public void testPipelineParentAggs_whenDateBucketedSumsPipelinedToBucketScriptedAggs_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", false);
        testDateBucketedSumsPipelinedToBucketScriptedAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenDateBucketedSumsPipelinedToBucketSortAggs_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", true);
        testDateBucketedSumsPipelinedToBucketSortAggs();
    }

    @SneakyThrows
    public void testPipelineParentAggs_whenDateBucketedSumsPipelinedToBucketSortAggs_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", false);
        testDateBucketedSumsPipelinedToBucketSortAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenDateBucketedSumsPipelinedToCumulativeSumAggs_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", true);
        testDateBucketedSumsPipelinedToCumulativeSumAggs();
    }

    @SneakyThrows
    public void testPipelineParentAggs_whenDateBucketedSumsPipelinedToCumulativeSumAggs_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", false);
        testDateBucketedSumsPipelinedToCumulativeSumAggs();
    }

    private void testVariousQueries() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

            // test bool query and aggregation
            TermQueryBuilder termQueryBuilder1 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
            TermQueryBuilder termQueryBuilder2 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT5);

            BoolQueryBuilder boolAndAggsQueryBuilder = QueryBuilders.boolQuery().should(termQueryBuilder1).should(termQueryBuilder2);

            AggregationBuilder aggsBuilder = AggregationBuilders.dateHistogram(GENERIC_AGGREGATION_NAME)
                .calendarInterval(DateHistogramInterval.YEAR)
                .field(DATE_FIELD_1)
                .subAggregation(AggregationBuilders.sum(SUM_AGGREGATION_NAME).field(INTEGER_FIELD_1));

            BucketMetricsPipelineAggregationBuilder<AvgBucketPipelineAggregationBuilder> aggAvgBucket = PipelineAggregatorBuilders
                .avgBucket(BUCKETS_AGGREGATION_NAME_1, GENERIC_AGGREGATION_NAME + ">" + SUM_AGGREGATION_NAME);

            BucketMetricsPipelineAggregationBuilder<SumBucketPipelineAggregationBuilder> aggSumBucket = PipelineAggregatorBuilders
                .sumBucket(BUCKETS_AGGREGATION_NAME_2, GENERIC_AGGREGATION_NAME + ">" + SUM_AGGREGATION_NAME);

            BucketMetricsPipelineAggregationBuilder<MinBucketPipelineAggregationBuilder> aggMinBucket = PipelineAggregatorBuilders
                .minBucket(BUCKETS_AGGREGATION_NAME_3, GENERIC_AGGREGATION_NAME + ">" + SUM_AGGREGATION_NAME);

            BucketMetricsPipelineAggregationBuilder<MaxBucketPipelineAggregationBuilder> aggMaxBucket = PipelineAggregatorBuilders
                .maxBucket(BUCKETS_AGGREGATION_NAME_4, GENERIC_AGGREGATION_NAME + ">" + SUM_AGGREGATION_NAME);

            Map<String, Object> searchResponseAsMapAnngsBoolQuery = executeQueryAndGetAggsResults(
                List.of(aggsBuilder, aggAvgBucket, aggSumBucket, aggMinBucket, aggMaxBucket),
                boolAndAggsQueryBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
                3
            );

            assertResultsOfPipelineSumtoDateHistogramAggs(searchResponseAsMapAnngsBoolQuery);

            // test only aggregation without query (handled as match_all query)
            Map<String, Object> searchResponseAsMapAggsNoQuery = executeQueryAndGetAggsResults(
                List.of(aggsBuilder, aggAvgBucket),
                null,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
                6
            );

            assertResultsOfPipelineSumtoDateHistogramAggsForMatchAllQuery(searchResponseAsMapAggsNoQuery);

        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void assertResultsOfPipelineSumtoDateHistogramAggs(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
        assertNotNull(aggregations);

        double aggValue = getAggregationValue(aggregations, BUCKETS_AGGREGATION_NAME_1);
        assertEquals(3517.5, aggValue, DELTA_FOR_SCORE_ASSERTION);

        double sumValue = getAggregationValue(aggregations, BUCKETS_AGGREGATION_NAME_2);
        assertEquals(7035.0, sumValue, DELTA_FOR_SCORE_ASSERTION);

        double minValue = getAggregationValue(aggregations, BUCKETS_AGGREGATION_NAME_3);
        assertEquals(1234.0, minValue, DELTA_FOR_SCORE_ASSERTION);

        double maxValue = getAggregationValue(aggregations, BUCKETS_AGGREGATION_NAME_4);
        assertEquals(5801.0, maxValue, DELTA_FOR_SCORE_ASSERTION);

        List<Map<String, Object>> buckets = getAggregationBuckets(aggregations, GENERIC_AGGREGATION_NAME);
        assertNotNull(buckets);
        assertEquals(21, buckets.size());

        // check content of few buckets
        Map<String, Object> firstBucket = buckets.get(0);
        assertEquals(4, firstBucket.size());
        assertEquals("01/01/1995", firstBucket.get(BUCKET_AGG_KEY_AS_STRING));
        assertEquals(1, firstBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(1234.0, getAggregationValue(firstBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
        assertTrue(firstBucket.containsKey(KEY));

        Map<String, Object> secondBucket = buckets.get(1);
        assertEquals(4, secondBucket.size());
        assertEquals("01/01/1996", secondBucket.get(BUCKET_AGG_KEY_AS_STRING));
        assertEquals(0, secondBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(0.0, getAggregationValue(secondBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
        assertTrue(secondBucket.containsKey(KEY));

        Map<String, Object> lastBucket = buckets.get(buckets.size() - 1);
        assertEquals(4, lastBucket.size());
        assertEquals("01/01/2015", lastBucket.get(BUCKET_AGG_KEY_AS_STRING));
        assertEquals(2, lastBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(5801.0, getAggregationValue(lastBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
        assertTrue(lastBucket.containsKey(KEY));
    }

    private void assertResultsOfPipelineSumtoDateHistogramAggsForMatchAllQuery(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
        assertNotNull(aggregations);

        double aggValue = getAggregationValue(aggregations, BUCKETS_AGGREGATION_NAME_1);
        assertEquals(3764.5, aggValue, DELTA_FOR_SCORE_ASSERTION);

        List<Map<String, Object>> buckets = getAggregationBuckets(aggregations, GENERIC_AGGREGATION_NAME);
        assertNotNull(buckets);
        assertEquals(21, buckets.size());

        // check content of few buckets
        Map<String, Object> firstBucket = buckets.get(0);
        assertEquals(4, firstBucket.size());
        assertEquals("01/01/1995", firstBucket.get(BUCKET_AGG_KEY_AS_STRING));
        assertEquals(1, firstBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(1234.0, getAggregationValue(firstBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
        assertTrue(firstBucket.containsKey(KEY));

        Map<String, Object> secondBucket = buckets.get(1);
        assertEquals(4, secondBucket.size());
        assertEquals("01/01/1996", secondBucket.get(BUCKET_AGG_KEY_AS_STRING));
        assertEquals(0, secondBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(0.0, getAggregationValue(secondBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
        assertTrue(secondBucket.containsKey(KEY));

        Map<String, Object> lastBucket = buckets.get(buckets.size() - 1);
        assertEquals(4, lastBucket.size());
        assertEquals("01/01/2015", lastBucket.get(BUCKET_AGG_KEY_AS_STRING));
        assertEquals(2, lastBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(5801.0, getAggregationValue(lastBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
        assertTrue(lastBucket.containsKey(KEY));
    }

    private void testDateBucketedSumsPipelinedToBucketStatsAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

            AggregationBuilder aggDateHisto = AggregationBuilders.dateHistogram(GENERIC_AGGREGATION_NAME)
                .calendarInterval(DateHistogramInterval.YEAR)
                .field(DATE_FIELD_1)
                .subAggregation(AggregationBuilders.sum(SUM_AGGREGATION_NAME).field(INTEGER_FIELD_1));

            StatsBucketPipelineAggregationBuilder aggStatsBucket = PipelineAggregatorBuilders.statsBucket(
                BUCKETS_AGGREGATION_NAME_1,
                GENERIC_AGGREGATION_NAME + ">" + SUM_AGGREGATION_NAME
            );

            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                List.of(aggDateHisto, aggStatsBucket),
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);

            Map<String, Object> statsAggs = getAggregationValues(aggregations, BUCKETS_AGGREGATION_NAME_1);

            assertNotNull(statsAggs);

            assertEquals(3517.5, (Double) statsAggs.get("avg"), DELTA_FOR_SCORE_ASSERTION);
            assertEquals(7035.0, (Double) statsAggs.get("sum"), DELTA_FOR_SCORE_ASSERTION);
            assertEquals(1234.0, (Double) statsAggs.get("min"), DELTA_FOR_SCORE_ASSERTION);
            assertEquals(5801.0, (Double) statsAggs.get("max"), DELTA_FOR_SCORE_ASSERTION);
            assertEquals(2, (int) statsAggs.get("count"));
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testDateBucketedSumsPipelinedToBucketScriptedAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

            AggregationBuilder aggBuilder = AggregationBuilders.dateHistogram(DATE_AGGREGATION_NAME)
                .calendarInterval(DateHistogramInterval.YEAR)
                .field(DATE_FIELD_1)
                .subAggregations(
                    new AggregatorFactories.Builder().addAggregator(AggregationBuilders.sum(SUM_AGGREGATION_NAME).field(INTEGER_FIELD_1))
                        .addAggregator(
                            AggregationBuilders.filter(
                                GENERIC_AGGREGATION_NAME,
                                QueryBuilders.boolQuery()
                                    .should(
                                        QueryBuilders.boolQuery()
                                            .should(QueryBuilders.termQuery(KEYWORD_FIELD_1, KEYWORD_FIELD_1_VALUE))
                                            .should(QueryBuilders.termQuery(KEYWORD_FIELD_1, KEYWORD_FIELD_2_VALUE))
                                    )
                                    .should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(KEYWORD_FIELD_1)))
                            ).subAggregation(AggregationBuilders.sum(SUM_AGGREGATION_NAME_2).field(INTEGER_FIELD_PRICE))
                        )
                        .addPipelineAggregator(
                            PipelineAggregatorBuilders.bucketScript(
                                BUCKETS_AGGREGATION_NAME_1,
                                Map.of("docNum", GENERIC_AGGREGATION_NAME + ">" + SUM_AGGREGATION_NAME_2, "totalNum", SUM_AGGREGATION_NAME),
                                new Script("params.docNum / params.totalNum")
                            )
                        )
                );

            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);

            List<Map<String, Object>> buckets = getAggregationBuckets(aggregations, DATE_AGGREGATION_NAME);

            assertNotNull(buckets);
            assertEquals(21, buckets.size());

            // check content of few buckets
            // first bucket have all the aggs values
            Map<String, Object> firstBucket = buckets.get(0);
            assertEquals(6, firstBucket.size());
            assertEquals("01/01/1995", firstBucket.get(BUCKET_AGG_KEY_AS_STRING));
            assertEquals(1, firstBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals(0.1053, getAggregationValue(firstBucket, BUCKETS_AGGREGATION_NAME_1), DELTA_FOR_SCORE_ASSERTION);
            assertEquals(1234.0, getAggregationValue(firstBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
            assertTrue(firstBucket.containsKey(KEY));

            Map<String, Object> inBucketAggValues = getAggregationValues(firstBucket, GENERIC_AGGREGATION_NAME);
            assertNotNull(inBucketAggValues);
            assertEquals(1, inBucketAggValues.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals(130.0, getAggregationValue(inBucketAggValues, SUM_AGGREGATION_NAME_2), DELTA_FOR_SCORE_ASSERTION);

            // second bucket is empty
            Map<String, Object> secondBucket = buckets.get(1);
            assertEquals(5, secondBucket.size());
            assertEquals("01/01/1996", secondBucket.get(BUCKET_AGG_KEY_AS_STRING));
            assertEquals(0, secondBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertFalse(secondBucket.containsKey(BUCKETS_AGGREGATION_NAME_1));
            assertEquals(0.0, getAggregationValue(secondBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
            assertTrue(secondBucket.containsKey(KEY));

            Map<String, Object> inSecondBucketAggValues = getAggregationValues(secondBucket, GENERIC_AGGREGATION_NAME);
            assertNotNull(inSecondBucketAggValues);
            assertEquals(0, inSecondBucketAggValues.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals(0.0, getAggregationValue(inSecondBucketAggValues, SUM_AGGREGATION_NAME_2), DELTA_FOR_SCORE_ASSERTION);

            // last bucket has values
            Map<String, Object> lastBucket = buckets.get(buckets.size() - 1);
            assertEquals(6, lastBucket.size());
            assertEquals("01/01/2015", lastBucket.get(BUCKET_AGG_KEY_AS_STRING));
            assertEquals(2, lastBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals(0.0172, getAggregationValue(lastBucket, BUCKETS_AGGREGATION_NAME_1), DELTA_FOR_SCORE_ASSERTION);
            assertEquals(5801.0, getAggregationValue(lastBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
            assertTrue(lastBucket.containsKey(KEY));

            Map<String, Object> inLastBucketAggValues = getAggregationValues(lastBucket, GENERIC_AGGREGATION_NAME);
            assertNotNull(inLastBucketAggValues);
            assertEquals(1, inLastBucketAggValues.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals(100.0, getAggregationValue(inLastBucketAggValues, SUM_AGGREGATION_NAME_2), DELTA_FOR_SCORE_ASSERTION);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testDateBucketedSumsPipelinedToBucketSortAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

            AggregationBuilder aggBuilder = AggregationBuilders.dateHistogram(DATE_AGGREGATION_NAME)
                .calendarInterval(DateHistogramInterval.YEAR)
                .field(DATE_FIELD_1)
                .subAggregations(
                    new AggregatorFactories.Builder().addAggregator(AggregationBuilders.sum(SUM_AGGREGATION_NAME).field(INTEGER_FIELD_1))
                        .addPipelineAggregator(
                            PipelineAggregatorBuilders.bucketSort(
                                BUCKETS_AGGREGATION_NAME_1,
                                List.of(new FieldSortBuilder(SUM_AGGREGATION_NAME).order(SortOrder.DESC))
                            ).size(5)
                        )
                );

            QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .should(
                    QueryBuilders.boolQuery()
                        .should(QueryBuilders.termQuery(KEYWORD_FIELD_1, KEYWORD_FIELD_1_VALUE))
                        .should(QueryBuilders.termQuery(KEYWORD_FIELD_1, KEYWORD_FIELD_2_VALUE))
                )
                .should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(KEYWORD_FIELD_1)));

            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                List.of(aggBuilder),
                queryBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);

            List<Map<String, Object>> buckets = getAggregationBuckets(aggregations, DATE_AGGREGATION_NAME);

            assertNotNull(buckets);
            assertEquals(3, buckets.size());

            // check content of few buckets
            Map<String, Object> firstBucket = buckets.get(0);
            assertEquals(4, firstBucket.size());
            assertEquals("01/01/2015", firstBucket.get(BUCKET_AGG_KEY_AS_STRING));
            assertEquals(1, firstBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals(2345.0, getAggregationValue(firstBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
            assertTrue(firstBucket.containsKey(KEY));

            // second bucket is empty
            Map<String, Object> secondBucket = buckets.get(1);
            assertEquals(4, secondBucket.size());
            assertEquals("01/01/1995", secondBucket.get(BUCKET_AGG_KEY_AS_STRING));
            assertEquals(1, secondBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals(1234.0, getAggregationValue(secondBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
            assertTrue(secondBucket.containsKey(KEY));

            // last bucket has values
            Map<String, Object> lastBucket = buckets.get(buckets.size() - 1);
            assertEquals(4, lastBucket.size());
            assertEquals("01/01/2007", lastBucket.get(BUCKET_AGG_KEY_AS_STRING));
            assertEquals(1, lastBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals(0.0, getAggregationValue(lastBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
            assertTrue(lastBucket.containsKey(KEY));
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testDateBucketedSumsPipelinedToCumulativeSumAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

            AggregationBuilder aggBuilder = AggregationBuilders.dateHistogram(DATE_AGGREGATION_NAME)
                .calendarInterval(DateHistogramInterval.YEAR)
                .field(DATE_FIELD_1)
                .subAggregations(
                    new AggregatorFactories.Builder().addAggregator(AggregationBuilders.sum(SUM_AGGREGATION_NAME).field(INTEGER_FIELD_1))
                        .addPipelineAggregator(PipelineAggregatorBuilders.cumulativeSum(BUCKETS_AGGREGATION_NAME_1, SUM_AGGREGATION_NAME))
                );

            QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .should(
                    QueryBuilders.boolQuery()
                        .should(QueryBuilders.termQuery(KEYWORD_FIELD_1, KEYWORD_FIELD_1_VALUE))
                        .should(QueryBuilders.termQuery(KEYWORD_FIELD_1, KEYWORD_FIELD_2_VALUE))
                )
                .should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(KEYWORD_FIELD_1)));

            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                List.of(aggBuilder),
                queryBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);

            List<Map<String, Object>> buckets = getAggregationBuckets(aggregations, DATE_AGGREGATION_NAME);

            assertNotNull(buckets);
            assertEquals(21, buckets.size());

            // check content of few buckets
            Map<String, Object> firstBucket = buckets.get(0);
            assertEquals(5, firstBucket.size());
            assertEquals("01/01/1995", firstBucket.get(BUCKET_AGG_KEY_AS_STRING));
            assertEquals(1, firstBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals(1234.0, getAggregationValue(firstBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
            assertEquals(1234.0, getAggregationValue(firstBucket, BUCKETS_AGGREGATION_NAME_1), DELTA_FOR_SCORE_ASSERTION);
            assertTrue(firstBucket.containsKey(KEY));

            Map<String, Object> secondBucket = buckets.get(1);
            assertEquals(5, secondBucket.size());
            assertEquals("01/01/1996", secondBucket.get(BUCKET_AGG_KEY_AS_STRING));
            assertEquals(0, secondBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals(0.0, getAggregationValue(secondBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
            assertEquals(1234.0, getAggregationValue(secondBucket, BUCKETS_AGGREGATION_NAME_1), DELTA_FOR_SCORE_ASSERTION);
            assertTrue(secondBucket.containsKey(KEY));

            // last bucket is empty
            Map<String, Object> lastBucket = buckets.get(buckets.size() - 1);
            assertEquals(5, lastBucket.size());
            assertEquals("01/01/2015", lastBucket.get(BUCKET_AGG_KEY_AS_STRING));
            assertEquals(1, lastBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals(2345.0, getAggregationValue(lastBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
            assertEquals(3579.0, getAggregationValue(lastBucket, BUCKETS_AGGREGATION_NAME_1), DELTA_FOR_SCORE_ASSERTION);
            assertTrue(lastBucket.containsKey(KEY));
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

    void prepareResources(String indexName, String pipelineName) {
        initializeIndexIfNotExist(indexName);
        createSearchPipelineWithResultsPostProcessor(pipelineName);
    }
}
