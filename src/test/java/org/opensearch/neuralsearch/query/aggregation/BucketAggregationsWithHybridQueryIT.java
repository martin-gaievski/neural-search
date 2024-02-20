/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query.aggregation;

import lombok.SneakyThrows;
import org.junit.Before;
import org.opensearch.client.ResponseException;
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

public class BucketAggregationsWithHybridQueryIT extends BaseNeuralSearchIT {
    private static final String TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS =
        "test-neural-aggs-bucket-multi-doc-index-multiple-shards";
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
    private static final String TEST_DOC_TEXT6 = "She traveled because it cost the same as therapy and was a lot more enjoyable";
    private static final String TEST_TEXT_FIELD_NAME_1 = "test-text-field-1";
    private static final String TEST_NESTED_TYPE_FIELD_NAME_1 = "user";
    private static final String NESTED_FIELD_1 = "firstname";
    private static final String NESTED_FIELD_2 = "lastname";
    private static final String NESTED_FIELD_1_VALUE_1 = "john";
    private static final String NESTED_FIELD_2_VALUE_1 = "black";
    private static final String NESTED_FIELD_1_VALUE_2 = "frodo";
    private static final String NESTED_FIELD_2_VALUE_2 = "baggins";
    private static final String NESTED_FIELD_1_VALUE_3 = "mohammed";
    private static final String NESTED_FIELD_2_VALUE_3 = "ezab";
    private static final String NESTED_FIELD_1_VALUE_4 = "sun";
    private static final String NESTED_FIELD_2_VALUE_4 = "wukong";
    private static final String NESTED_FIELD_1_VALUE_5 = "vasilisa";
    private static final String NESTED_FIELD_2_VALUE_5 = "the wise";
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
    private static final String CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH = "search.concurrent_segment_search.enabled";

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
    public void testBucketAndNestedAggs_whenAdjacencyMatrix_thenSuccessful() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, false);
        testAdjacencyMatrixAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenAdjacencyMatrix_thenSuccessful() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, true);
        testAdjacencyMatrixAggs();
    }

    @SneakyThrows
    public void testBucketAndNestedAggs_whenDateRange_thenSuccessful() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, false);
        testDateRange();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenDateRange_thenSuccessful() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, true);
        testDateRange();
    }

    @SneakyThrows
    public void testBucketAndNestedAggs_whenDiversifiedSampler_thenSuccessful() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, false);
        testDiversifiedSampler();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenDiversifiedSampler_thenFail() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, true);

        /*AggregationBuilder aggsBuilder = AggregationBuilders.diversifiedSampler(GENERIC_AGGREGATION_NAME)
            .field(KEYWORD_FIELD_1)
            .shardSize(2)
            .subAggregation(AggregationBuilders.terms(BUCKETS_AGGREGATION_NAME_1).field(KEYWORD_FIELD_1));
        testAggregationWithExpectedFailure(aggsBuilder);*/
        testDiversifiedSampler();
    }

    @SneakyThrows
    public void testBucketAndNestedAggs_whenAvgNestedIntoFilter_thenSuccessful() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, false);
        testAvgNestedIntoFilter();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenAvgNestedIntoFilter_thenSuccessful() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, true);
        testAvgNestedIntoFilter();
    }

    @SneakyThrows
    public void testBucketAndNestedAggs_whenSumNestedIntoFilters_thenSuccessful() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, false);
        testSumNestedIntoFilters();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenSumNestedIntoFilters_thenSuccessful() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, true);
        testSumNestedIntoFilters();
    }

    @SneakyThrows
    public void testBucketAggs_whenGlobalAggUsedWithQuery_thenSuccessful() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, false);
        testGlobalAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenGlobalAggUsedWithQuery_thenSuccessful() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, true);
        testGlobalAggs();
    }

    @SneakyThrows
    public void testBucketAggs_whenHistogramAgg_thenSuccessful() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, false);
        testHistogramAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenHistogramAgg_thenSuccessful() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, true);
        testHistogramAggs();
    }

    @SneakyThrows
    public void testBucketAggs_whenNestedAgg_thenSuccessful() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, false);
        testNestedAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenNestedAgg_thenSuccessful() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, true);
        testNestedAggs();
    }

    @SneakyThrows
    public void testBucketAggs_whenSamplerAgg_thenSuccessful() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, false);
        testSampler();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenSamplerAgg_thenFail() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, true);

        /*AggregationBuilder aggsBuilder = AggregationBuilders.sampler(GENERIC_AGGREGATION_NAME)
            .shardSize(1)
            .subAggregation(AggregationBuilders.terms(BUCKETS_AGGREGATION_NAME_1).field(KEYWORD_FIELD_1));
        testAggregationWithExpectedFailure(aggsBuilder);*/
        testSampler();
    }

    @SneakyThrows
    public void testPipelineSiblingAggs_whenDateBucketedSumsPipelinedToBucketMinMaxSumAvgAggs_thenSuccessful() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, false);
        testDateBucketedSumsPipelinedToBucketMinMaxSumAvgAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenDateBucketedSumsPipelinedToBucketMinMaxSumAvgAggs_thenSuccessful() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, true);
        testDateBucketedSumsPipelinedToBucketMinMaxSumAvgAggs();
    }

    @SneakyThrows
    public void testPipelineSiblingAggs_whenDateBucketedSumsPipelinedToBucketStatsAggs_thenSuccessful() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, false);
        testDateBucketedSumsPipelinedToBucketStatsAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenDateBucketedSumsPipelinedToBucketStatsAggs_thenSuccessful() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, true);
        testDateBucketedSumsPipelinedToBucketStatsAggs();
    }

    @SneakyThrows
    public void testPipelineSiblingAggs_whenDateBucketedSumsPipelinedToBucketScriptAggs_thenSuccessful() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, false);
        testDateBucketedSumsPipelinedToBucketScriptedAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenDateBucketedSumsPipelinedToBucketScriptedAggs_thenSuccessful() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, true);
        testDateBucketedSumsPipelinedToBucketScriptedAggs();
    }

    @SneakyThrows
    public void testPipelineParentAggs_whenDateBucketedSumsPipelinedToBucketScriptedAggs_thenSuccessful() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, false);
        testDateBucketedSumsPipelinedToBucketScriptedAggs();
    }

    @SneakyThrows
    public void testMetricAggs_whenTermsAggs_thenSuccessful() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, false);
        testTermsAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenTermsAggs_thenSuccessful() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, true);
        testTermsAggs();
    }

    @SneakyThrows
    public void testMetricAggs_whenSignificantTermsAggs_thenSuccessful() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, false);
        testSignificantTermsAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenSignificantTermsAggs_thenSuccessful() {
        updateClusterSettings(CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH, true);
        testSignificantTermsAggs();
    }

    private void testAvgNestedIntoFilter() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

            AggregationBuilder aggsBuilder = AggregationBuilders.filter(
                GENERIC_AGGREGATION_NAME,
                QueryBuilders.rangeQuery(INTEGER_FIELD_1).lte(3000)
            ).subAggregation(AggregationBuilders.avg(AVG_AGGREGATION_NAME).field(INTEGER_FIELD_1));
            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);
            assertTrue(aggregations.containsKey(GENERIC_AGGREGATION_NAME));
            double avgValue = getAggregationValue(getAggregationValues(aggregations, GENERIC_AGGREGATION_NAME), AVG_AGGREGATION_NAME);
            assertEquals(1789.5, avgValue, DELTA_FOR_SCORE_ASSERTION);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testSumNestedIntoFilters() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

            AggregationBuilder aggsBuilder = AggregationBuilders.filters(
                GENERIC_AGGREGATION_NAME,
                QueryBuilders.rangeQuery(INTEGER_FIELD_1).lte(3000),
                QueryBuilders.termQuery(KEYWORD_FIELD_1, KEYWORD_FIELD_1_VALUE)
            ).otherBucket(true).subAggregation(AggregationBuilders.sum(SUM_AGGREGATION_NAME).field(INTEGER_FIELD_1));
            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);

            List<Map<String, Object>> buckets = getAggregationBuckets(aggregations, GENERIC_AGGREGATION_NAME);
            assertNotNull(buckets);
            assertEquals(3, buckets.size());

            Map<String, Object> firstBucket = buckets.get(0);
            assertEquals(2, firstBucket.size());
            assertEquals(2, firstBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals(3579.0, getAggregationValue(firstBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);

            Map<String, Object> secondBucket = buckets.get(1);
            assertEquals(2, secondBucket.size());
            assertEquals(1, secondBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals(1234.0, getAggregationValue(secondBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);

            Map<String, Object> thirdBucket = buckets.get(2);
            assertEquals(2, thirdBucket.size());
            assertEquals(1, thirdBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals(3456.0, getAggregationValue(thirdBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testGlobalAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

            TermQueryBuilder termQueryBuilder1 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
            TermQueryBuilder termQueryBuilder2 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT5);

            HybridQueryBuilder hybridQueryBuilderNeuralThenTerm = new HybridQueryBuilder();
            hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder1);
            hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder2);

            AggregationBuilder aggsBuilder = AggregationBuilders.global(GENERIC_AGGREGATION_NAME)
                .subAggregation(AggregationBuilders.sum(AVG_AGGREGATION_NAME).field(INTEGER_FIELD_1));

            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                List.of(aggsBuilder),
                hybridQueryBuilderNeuralThenTerm,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);
            assertTrue(aggregations.containsKey(GENERIC_AGGREGATION_NAME));
            double avgValue = getAggregationValue(getAggregationValues(aggregations, GENERIC_AGGREGATION_NAME), AVG_AGGREGATION_NAME);
            assertEquals(15058.0, avgValue, DELTA_FOR_SCORE_ASSERTION);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testHistogramAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

            AggregationBuilder aggsBuilder = AggregationBuilders.histogram(GENERIC_AGGREGATION_NAME)
                .field(INTEGER_FIELD_PRICE)
                .interval(100);

            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);

            List<Map<String, Object>> buckets = getAggregationBuckets(aggregations, GENERIC_AGGREGATION_NAME);
            assertNotNull(buckets);
            assertEquals(2, buckets.size());

            Map<String, Object> firstBucket = buckets.get(0);
            assertEquals(2, firstBucket.size());
            assertEquals(1, firstBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals(0.0, (Double) firstBucket.get(KEY), DELTA_FOR_SCORE_ASSERTION);

            Map<String, Object> secondBucket = buckets.get(1);
            assertEquals(2, secondBucket.size());
            assertEquals(2, secondBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals(100.0, (Double) secondBucket.get(KEY), DELTA_FOR_SCORE_ASSERTION);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testNestedAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

            AggregationBuilder aggsBuilder = AggregationBuilders.nested(GENERIC_AGGREGATION_NAME, TEST_NESTED_TYPE_FIELD_NAME_1)
                .subAggregation(
                    AggregationBuilders.terms(BUCKETS_AGGREGATION_NAME_1)
                        .field(String.join(".", TEST_NESTED_TYPE_FIELD_NAME_1, NESTED_FIELD_1))
                );

            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);

            Map<String, Object> nestedAgg = getAggregationValues(aggregations, GENERIC_AGGREGATION_NAME);
            assertNotNull(nestedAgg);

            assertEquals(3, nestedAgg.get(BUCKET_AGG_DOC_COUNT_FIELD));
            List<Map<String, Object>> buckets = getAggregationBuckets(nestedAgg, BUCKETS_AGGREGATION_NAME_1);

            assertNotNull(buckets);
            assertEquals(3, buckets.size());

            Map<String, Object> firstBucket = buckets.get(0);
            assertEquals(2, firstBucket.size());
            assertEquals(1, firstBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals(NESTED_FIELD_1_VALUE_2, firstBucket.get(KEY));

            Map<String, Object> secondBucket = buckets.get(1);
            assertEquals(2, secondBucket.size());
            assertEquals(1, secondBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals(NESTED_FIELD_1_VALUE_1, secondBucket.get(KEY));

            Map<String, Object> thirdBucket = buckets.get(2);
            assertEquals(2, thirdBucket.size());
            assertEquals(1, thirdBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals(NESTED_FIELD_1_VALUE_4, thirdBucket.get(KEY));
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testDateRange() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

            AggregationBuilder aggsBuilder = AggregationBuilders.dateRange(DATE_AGGREGATION_NAME)
                .field(DATE_FIELD_1)
                .format("MM-yyyy")
                .addRange("01-2014", "02-2024");
            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);
            List<Map<String, Object>> buckets = getAggregationBuckets(aggregations, DATE_AGGREGATION_NAME);
            assertNotNull(buckets);
            assertEquals(1, buckets.size());

            Map<String, Object> bucket = buckets.get(0);

            assertEquals(6, bucket.size());
            assertEquals("01-2014", bucket.get("from_as_string"));
            assertEquals(2, bucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals("02-2024", bucket.get("to_as_string"));
            assertTrue(bucket.containsKey("from"));
            assertTrue(bucket.containsKey("to"));
            assertTrue(bucket.containsKey(KEY));
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testDiversifiedSampler() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

            AggregationBuilder aggsBuilder = AggregationBuilders.diversifiedSampler(GENERIC_AGGREGATION_NAME)
                .field(KEYWORD_FIELD_1)
                .shardSize(2)
                .subAggregation(AggregationBuilders.terms(BUCKETS_AGGREGATION_NAME_1).field(KEYWORD_FIELD_1));

            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);

            Map<String, Object> aggValue = getAggregationValues(aggregations, GENERIC_AGGREGATION_NAME);
            assertEquals(2, aggValue.size());
            assertEquals(3, aggValue.get(BUCKET_AGG_DOC_COUNT_FIELD));
            Map<String, Object> nestedAggs = getAggregationValues(aggValue, BUCKETS_AGGREGATION_NAME_1);
            assertNotNull(nestedAggs);
            assertEquals(0, nestedAggs.get("doc_count_error_upper_bound"));
            List<Map<String, Object>> buckets = getAggregationBuckets(aggValue, BUCKETS_AGGREGATION_NAME_1);
            assertEquals(2, buckets.size());

            Map<String, Object> firstBucket = buckets.get(0);
            assertEquals(1, firstBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals("likeable", firstBucket.get(KEY));

            Map<String, Object> secondBucket = buckets.get(1);
            assertEquals(1, secondBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals("workable", secondBucket.get(KEY));
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testAdjacencyMatrixAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

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
            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);
            List<Map<String, Object>> buckets = getAggregationBuckets(aggregations, GENERIC_AGGREGATION_NAME);
            assertNotNull(buckets);
            assertEquals(2, buckets.size());
            Map<String, Object> grpA = buckets.get(0);
            assertEquals(1, grpA.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals("grpA", grpA.get(KEY));
            Map<String, Object> grpC = buckets.get(1);
            assertEquals(1, grpC.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals("grpC", grpC.get(KEY));
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testDateBucketedSumsPipelinedToBucketMinMaxSumAvgAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

            AggregationBuilder aggDateHisto = AggregationBuilders.dateHistogram(GENERIC_AGGREGATION_NAME)
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

            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                List.of(aggDateHisto, aggAvgBucket, aggSumBucket, aggMinBucket, aggMaxBucket),
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            assertResultsOfPipelineSumtoDateHistogramAggs(searchResponseAsMap);
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

    private void testSampler() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

            AggregationBuilder aggsBuilder = AggregationBuilders.sampler(GENERIC_AGGREGATION_NAME)
                .shardSize(2)
                .subAggregation(AggregationBuilders.terms(BUCKETS_AGGREGATION_NAME_1).field(KEYWORD_FIELD_1));

            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);

            Map<String, Object> aggValue = getAggregationValues(aggregations, GENERIC_AGGREGATION_NAME);
            assertEquals(2, aggValue.size());
            assertEquals(3, aggValue.get(BUCKET_AGG_DOC_COUNT_FIELD));
            Map<String, Object> nestedAggs = getAggregationValues(aggValue, BUCKETS_AGGREGATION_NAME_1);
            assertNotNull(nestedAggs);
            assertEquals(0, nestedAggs.get("doc_count_error_upper_bound"));
            List<Map<String, Object>> buckets = getAggregationBuckets(aggValue, BUCKETS_AGGREGATION_NAME_1);
            assertEquals(2, buckets.size());

            Map<String, Object> firstBucket = buckets.get(0);
            assertEquals(1, firstBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals("likeable", firstBucket.get(KEY));

            Map<String, Object> secondBucket = buckets.get(1);
            assertEquals(1, secondBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals("workable", secondBucket.get(KEY));
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testTermsAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

            AggregationBuilder aggsBuilder = AggregationBuilders.terms(GENERIC_AGGREGATION_NAME).field(KEYWORD_FIELD_1);
            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);
            List<Map<String, Object>> buckets = ((Map<String, List>) getAggregationValues(aggregations, GENERIC_AGGREGATION_NAME)).get(
                "buckets"
            );
            assertNotNull(buckets);
            assertEquals(2, buckets.size());
            Map<String, Object> firstBucket = buckets.get(0);
            assertEquals(1, firstBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals(KEYWORD_FIELD_3_VALUE, firstBucket.get(KEY));
            Map<String, Object> secondBucket = buckets.get(1);
            assertEquals(1, secondBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals(KEYWORD_FIELD_1_VALUE, secondBucket.get(KEY));
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testSignificantTermsAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

            AggregationBuilder aggsBuilder = AggregationBuilders.significantTerms(GENERIC_AGGREGATION_NAME).field(KEYWORD_FIELD_1);
            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);
            List<Map<String, Object>> buckets = getAggregationBuckets(aggregations, GENERIC_AGGREGATION_NAME);
            assertNotNull(buckets);

            Map<String, Object> significantTermsAggregations = getAggregationValues(aggregations, GENERIC_AGGREGATION_NAME);

            assertNotNull(significantTermsAggregations);
            assertEquals(3, (int) getAggregationValues(significantTermsAggregations, BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals(11, (int) getAggregationValues(significantTermsAggregations, "bg_count"));
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testAggregationWithExpectedFailure(final AggregationBuilder aggsBuilder) throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

            TermQueryBuilder termQueryBuilder1 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
            TermQueryBuilder termQueryBuilder2 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT5);

            HybridQueryBuilder hybridQueryBuilderNeuralThenTerm = new HybridQueryBuilder();
            hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder1);
            hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder2);

            ResponseException responseException = expectThrows(
                ResponseException.class,
                () -> search(
                    TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
                    hybridQueryBuilderNeuralThenTerm,
                    null,
                    10,
                    Map.of("search_pipeline", SEARCH_PIPELINE),
                    List.of(aggsBuilder)
                )
            );
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
                buildIndexConfiguration(
                    List.of(),
                    List.of(TEST_NESTED_TYPE_FIELD_NAME_1, NESTED_FIELD_1, NESTED_FIELD_2),
                    List.of(INTEGER_FIELD_1),
                    List.of(KEYWORD_FIELD_1),
                    List.of(DATE_FIELD_1),
                    3
                ),
                ""
            );

            addKnnDoc(
                indexName,
                "1",
                List.of(),
                List.of(),
                List.of(TEST_TEXT_FIELD_NAME_1),
                List.of(TEST_DOC_TEXT1),
                List.of(TEST_NESTED_TYPE_FIELD_NAME_1),
                List.of(Map.of(NESTED_FIELD_1, NESTED_FIELD_1_VALUE_1, NESTED_FIELD_2, NESTED_FIELD_2_VALUE_1)),
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
                List.of(TEST_NESTED_TYPE_FIELD_NAME_1),
                List.of(Map.of(NESTED_FIELD_1, NESTED_FIELD_1_VALUE_2, NESTED_FIELD_2, NESTED_FIELD_2_VALUE_2)),
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
                List.of(TEST_NESTED_TYPE_FIELD_NAME_1),
                List.of(Map.of(NESTED_FIELD_1, NESTED_FIELD_1_VALUE_3, NESTED_FIELD_2, NESTED_FIELD_2_VALUE_3)),
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
                List.of(TEST_NESTED_TYPE_FIELD_NAME_1),
                List.of(Map.of(NESTED_FIELD_1, NESTED_FIELD_1_VALUE_4, NESTED_FIELD_2, NESTED_FIELD_2_VALUE_4)),
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
                List.of(TEST_NESTED_TYPE_FIELD_NAME_1),
                List.of(Map.of(NESTED_FIELD_1, NESTED_FIELD_1_VALUE_5, NESTED_FIELD_2, NESTED_FIELD_2_VALUE_5)),
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
