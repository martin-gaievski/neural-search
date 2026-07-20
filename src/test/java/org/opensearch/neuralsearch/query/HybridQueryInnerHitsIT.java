/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.SneakyThrows;
import org.apache.lucene.search.join.ScoreMode;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.join.query.HasChildQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.search.sort.SortOrder;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_COMBINATION_METHOD;
import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_NORMALIZATION_METHOD;
import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_SCORE_ASSERTION;
import static org.opensearch.neuralsearch.util.TestUtils.RELATION_EQUAL_TO;
import static org.opensearch.neuralsearch.util.TestUtils.getMaxScore;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getNestedHits;
import static org.opensearch.neuralsearch.util.TestUtils.getTotalHits;
import static org.opensearch.neuralsearch.util.TestUtils.getValueByKey;

public class HybridQueryInnerHitsIT extends BaseNeuralSearchIT {
    private static final String TEST_MULTI_DOC_WITH_NESTED_FIELDS_SINGLE_SHARD_INDEX_NAME = "test-hybrid-index-nested-field-single-shard";
    private static final String TEST_MULTI_DOC_WITH_NESTED_FIELDS_MULTIPLE_SHARD_INDEX_NAME =
        "test-hybrid-index-nested-field-multiple-shard";
    private static final String TEST_MULTI_DOC_WITH_PARENT_CHILD_INDEX_NAME = "test-hybrid-index-parent-child-field";
    private static final String TEST_MULTI_DOC_WITH_NESTED_PARENT_CHILD_INDEX_NAME = "test-hybrid-index-nested-parent-child-field";

    private static final String TEST_NESTED_FIELD_NAME_1 = "user";
    private static final String TEST_USER_INNER_NAME_NESTED_FIELD = "name";
    private static final String TEST_USER_INNER_AGE_NESTED_FIELD = "age";
    private static final String TEST_NESTED_FIELD_NAME_2 = "location";
    private static final String TEST_LOCATION_INNER_STATE_NESTED_FIELD = "state";
    private static final String TEST_LOCATION_INNER_PLACE_NESTED_FIELD = "place";
    private static final String TEST_PARENT_CHILD_MY_JOIN_FIELD_FIELD = "my_join_field";
    private static final String TEST_PARENT_CHILD_TYPE_JOIN = "join";
    private static final String TEST_PARENT_CHILD_RELATION_FIELD_NAME_1 = "parent";
    private static final String TEST_PARENT_CHILD_RELATION_FIELD_NAME_2 = "child";
    private static final String TEST_PARENT_CHILD_TEXT_FIELD_NAME = "text";
    private static final String TEST_PARENT_CHILD_TEXT_FIELD_VALUE_1 = "This is a parent document";
    private static final String TEST_PARENT_CHILD_TEXT_FIELD_VALUE_2 = "This is a child document";
    private static final String TEST_PARENT_CHILD_INNER_HITS_FIELD_NAME = "child";
    private static final String NORMALIZATION_SEARCH_PIPELINE = "normalization-search-pipeline";

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
    public void testInnerHits_whenMultipleSubqueriesOnNestedFields_thenSuccessful() {
        testInnerHits_whenMultipleSubqueriesOnNestedFields_thenSuccessful(TEST_MULTI_DOC_WITH_NESTED_FIELDS_SINGLE_SHARD_INDEX_NAME);
        testInnerHits_whenMultipleSubqueriesOnNestedFields_thenSuccessful(TEST_MULTI_DOC_WITH_NESTED_FIELDS_MULTIPLE_SHARD_INDEX_NAME);
    }

    private void testInnerHits_whenMultipleSubqueriesOnNestedFields_thenSuccessful(String indexName) {
        initializeIndexIfNotExist(indexName);
        createSearchPipeline(NORMALIZATION_SEARCH_PIPELINE, DEFAULT_NORMALIZATION_METHOD, DEFAULT_COMBINATION_METHOD, Map.of());
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        NestedQueryBuilder nestedQueryBuilder1 = new NestedQueryBuilder("user", new MatchQueryBuilder("user.name", "John"), ScoreMode.Avg);
        nestedQueryBuilder1.innerHit(new InnerHitBuilder());
        NestedQueryBuilder nestedQueryBuilder2 = new NestedQueryBuilder(
            "location",
            new MatchQueryBuilder("location.state", "California"),
            ScoreMode.Avg
        );
        nestedQueryBuilder2.innerHit(new InnerHitBuilder());
        hybridQueryBuilder.add(nestedQueryBuilder1);
        hybridQueryBuilder.add(nestedQueryBuilder2);

        Map<String, Object> searchResponseAsMap = search(
            indexName,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", NORMALIZATION_SEARCH_PIPELINE),
            null
        );

        List<Object> hitsNestedList = getInnerHitsFromSearchHits(searchResponseAsMap);
        assertEquals(2, getHitCount(searchResponseAsMap));
        Map<String, Map<String, ArrayList<Integer>>> innerHitCountPerFieldName = getInnerHitsCountsOfNestedField(
            hitsNestedList,
            List.of(TEST_NESTED_FIELD_NAME_1, TEST_NESTED_FIELD_NAME_2)
        );
        assertEquals(2, innerHitCountPerFieldName.get(TEST_NESTED_FIELD_NAME_1).get("total").get(0).intValue());
        assertEquals(3, innerHitCountPerFieldName.get(TEST_NESTED_FIELD_NAME_2).get("total").get(0).intValue());
        assertEquals(1, innerHitCountPerFieldName.get(TEST_NESTED_FIELD_NAME_1).get("total").get(1).intValue());
        assertEquals(0, innerHitCountPerFieldName.get(TEST_NESTED_FIELD_NAME_2).get("total").get(1).intValue());
    }

    /**
     * Correctness (not just presence) check for fused-mode inner_hits: run the SAME two nested-field sub-queries, on the
     * SAME index/data, in classic mode (phase-results normalization pipeline) and in {@code mode: "fused"} (self-erase,
     * reading the same config from the index default pipeline), then assert the per-document inner_hits totals are
     * IDENTICAL between the two. Keyed by {@code _id} so parent ordering differences don't matter. This uses the classic
     * hybrid inner_hits behavior as the correctness oracle.
     */
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public void testInnerHits_whenFusedMode_thenMatchesClassicHybrid() {
        String index = TEST_MULTI_DOC_WITH_NESTED_FIELDS_MULTIPLE_SHARD_INDEX_NAME;
        initializeIndexIfNotExist(index);
        createSearchPipeline(NORMALIZATION_SEARCH_PIPELINE, DEFAULT_NORMALIZATION_METHOD, DEFAULT_COMBINATION_METHOD, Map.of());

        // classic hybrid with the two nested legs + inner_hits, via the named search pipeline (today's behavior)
        HybridQueryBuilder classic = new HybridQueryBuilder();
        classic.add(nestedUserJohn());
        classic.add(nestedLocationCalifornia());
        Map<String, Object> classicResp = search(index, classic, null, 10, Map.of("search_pipeline", NORMALIZATION_SEARCH_PIPELINE), null);
        Map<String, Map<String, Integer>> classicInnerHits = innerHitTotalsById(
            classicResp,
            List.of(TEST_NESTED_FIELD_NAME_1, TEST_NESTED_FIELD_NAME_2)
        );

        // sanity: the oracle really produced the documented-correct counts (order-independent)
        assertTrue(
            "classic oracle must have a doc with user=2, location=3",
            classicInnerHits.values().stream().anyMatch(m -> m.get(TEST_NESTED_FIELD_NAME_1) == 2 && m.get(TEST_NESTED_FIELD_NAME_2) == 3)
        );
        assertTrue(
            "classic oracle must have a doc with user=1, location=0",
            classicInnerHits.values().stream().anyMatch(m -> m.get(TEST_NESTED_FIELD_NAME_1) == 1 && m.get(TEST_NESTED_FIELD_NAME_2) == 0)
        );

        // same query in fused mode, reading the same config from the index default pipeline (only working source today)
        try {
            setIndexDefaultPipeline(index, NORMALIZATION_SEARCH_PIPELINE);
            HybridQueryBuilder fused = new HybridQueryBuilder().mode(HybridQueryBuilder.Mode.FUSED);
            fused.add(nestedUserJohn());
            fused.add(nestedLocationCalifornia());
            Map<String, Object> fusedResp = search(index, fused, 10);
            Map<String, Map<String, Integer>> fusedInnerHits = innerHitTotalsById(
                fusedResp,
                List.of(TEST_NESTED_FIELD_NAME_1, TEST_NESTED_FIELD_NAME_2)
            );

            assertEquals("fused and classic must return the same parent hits", classicInnerHits.keySet(), fusedInnerHits.keySet());
            assertEquals("fused inner_hits totals must be identical to classic hybrid, per document", classicInnerHits, fusedInnerHits);
        } finally {
            setIndexDefaultPipeline(index, "_none");
        }
    }

    /**
     * Correctness check for {@code has_child} inner_hits under fused mode: same {@code has_child} sub-query, same index,
     * classic vs fused, assert per-parent child inner_hits totals are identical.
     */
    @SneakyThrows
    public void testInnerHits_whenFusedMode_parentChild_thenMatchesClassicHybrid() {
        String index = TEST_MULTI_DOC_WITH_PARENT_CHILD_INDEX_NAME;
        initializeIndexIfNotExist(index);
        createSearchPipeline(NORMALIZATION_SEARCH_PIPELINE, DEFAULT_NORMALIZATION_METHOD, DEFAULT_COMBINATION_METHOD, Map.of());
        List<String> fields = List.of(TEST_PARENT_CHILD_INNER_HITS_FIELD_NAME);

        Map<String, Object> classicResp = search(
            index,
            hasChildTextChildQuery(),
            null,
            10,
            Map.of("search_pipeline", NORMALIZATION_SEARCH_PIPELINE),
            null
        );
        Map<String, Map<String, Integer>> classic = innerHitTotalsById(classicResp, fields);
        assertTrue(
            "oracle: a parent with child inner_hits total=1",
            classic.values().stream().anyMatch(m -> m.get(TEST_PARENT_CHILD_INNER_HITS_FIELD_NAME) == 1)
        );

        try {
            setIndexDefaultPipeline(index, NORMALIZATION_SEARCH_PIPELINE);
            Map<String, Object> fusedResp = search(index, hasChildTextChildQuery().mode(HybridQueryBuilder.Mode.FUSED), 10);
            Map<String, Map<String, Integer>> fused = innerHitTotalsById(fusedResp, fields);
            assertEquals("fused and classic must return the same parent hits", classic.keySet(), fused.keySet());
            assertEquals("fused has_child inner_hits totals must be identical to classic hybrid", classic, fused);
        } finally {
            setIndexDefaultPipeline(index, "_none");
        }
    }

    /**
     * Deepest inner_hits path: a {@code has_child} sub-query whose child query is itself a {@code nested} query, both
     * carrying inner_hits — so the parent's child inner_hits contain nested inner_hits. Exercises the recursive
     * inner-hit extraction. Classic vs fused, assert both the child total and the nested (user) total per parent match.
     */
    @SneakyThrows
    public void testInnerHits_whenFusedMode_nestedInsideParentChild_thenMatchesClassicHybrid() {
        String index = TEST_MULTI_DOC_WITH_NESTED_PARENT_CHILD_INDEX_NAME;
        initializeIndexIfNotExist(index);
        createSearchPipeline(NORMALIZATION_SEARCH_PIPELINE, DEFAULT_NORMALIZATION_METHOD, DEFAULT_COMBINATION_METHOD, Map.of());

        Map<String, Object> classicResp = search(
            index,
            hasChildWrappingNestedQuery(),
            null,
            10,
            Map.of("search_pipeline", NORMALIZATION_SEARCH_PIPELINE),
            null
        );
        Map<String, List<Integer>> classic = childThenNestedTotalsById(classicResp);
        assertTrue(
            "oracle: a parent with child total=1 and nested user total=1",
            classic.values().stream().anyMatch(v -> v.equals(List.of(1, 1)))
        );

        try {
            setIndexDefaultPipeline(index, NORMALIZATION_SEARCH_PIPELINE);
            Map<String, Object> fusedResp = search(index, hasChildWrappingNestedQuery().mode(HybridQueryBuilder.Mode.FUSED), 10);
            Map<String, List<Integer>> fused = childThenNestedTotalsById(fusedResp);
            assertEquals("fused and classic must return the same parent hits", classic.keySet(), fused.keySet());
            assertEquals("fused recursive child->nested inner_hits must be identical to classic hybrid", classic, fused);
        } finally {
            setIndexDefaultPipeline(index, "_none");
        }
    }

    /**
     * Inner_hits pagination + sort under fused mode: one leg's inner_hits uses {@code from:1}, the other sorts by
     * {@code _doc} DESC. Classic vs fused, comparing the ORDERED list of inner-hit {@code _source} per field per parent —
     * so equality proves pagination and sort are applied identically, not just that inner_hits exist.
     */
    @SneakyThrows
    public void testInnerHits_whenFusedMode_withSortingAndPagination_thenMatchesClassicHybrid() {
        String index = TEST_MULTI_DOC_WITH_NESTED_FIELDS_MULTIPLE_SHARD_INDEX_NAME;
        initializeIndexIfNotExist(index);
        createSearchPipeline(NORMALIZATION_SEARCH_PIPELINE, DEFAULT_NORMALIZATION_METHOD, DEFAULT_COMBINATION_METHOD, Map.of());
        List<String> fields = List.of(TEST_NESTED_FIELD_NAME_1, TEST_NESTED_FIELD_NAME_2);

        Map<String, Object> classicResp = search(
            index,
            sortedPaginatedNestedQuery(),
            null,
            10,
            Map.of("search_pipeline", NORMALIZATION_SEARCH_PIPELINE),
            null
        );
        Map<String, Map<String, List<Object>>> classic = innerHitSourcesById(classicResp, fields);

        try {
            setIndexDefaultPipeline(index, NORMALIZATION_SEARCH_PIPELINE);
            Map<String, Object> fusedResp = search(index, sortedPaginatedNestedQuery().mode(HybridQueryBuilder.Mode.FUSED), 10);
            Map<String, Map<String, List<Object>>> fused = innerHitSourcesById(fusedResp, fields);
            assertEquals("fused and classic must return the same parent hits", classic.keySet(), fused.keySet());
            assertEquals("fused inner_hits pagination+sort (ordered _source) must be identical to classic hybrid", classic, fused);
        } finally {
            setIndexDefaultPipeline(index, "_none");
        }
    }

    private HybridQueryBuilder hasChildTextChildQuery() {
        HasChildQueryBuilder hasChild = new HasChildQueryBuilder("child", new MatchQueryBuilder("text", "child"), ScoreMode.Avg);
        hasChild.innerHit(new InnerHitBuilder());
        HybridQueryBuilder hybrid = new HybridQueryBuilder();
        hybrid.add(hasChild);
        return hybrid;
    }

    private HybridQueryBuilder hasChildWrappingNestedQuery() {
        NestedQueryBuilder nested = new NestedQueryBuilder("user", new MatchQueryBuilder("user.name", "John"), ScoreMode.Avg);
        nested.innerHit(new InnerHitBuilder());
        HasChildQueryBuilder hasChild = new HasChildQueryBuilder("child", nested, ScoreMode.Avg);
        hasChild.innerHit(new InnerHitBuilder());
        HybridQueryBuilder hybrid = new HybridQueryBuilder();
        hybrid.add(hasChild);
        return hybrid;
    }

    private HybridQueryBuilder sortedPaginatedNestedQuery() {
        NestedQueryBuilder nested1 = new NestedQueryBuilder("user", new MatchQueryBuilder("user.name", "John"), ScoreMode.Avg);
        InnerHitBuilder userInnerHit = new InnerHitBuilder();
        userInnerHit.setFrom(1);
        nested1.innerHit(userInnerHit);
        NestedQueryBuilder nested2 = new NestedQueryBuilder(
            "location",
            new MatchQueryBuilder("location.state", "California"),
            ScoreMode.Avg
        );
        InnerHitBuilder locationInnerHit = new InnerHitBuilder();
        locationInnerHit.setSorts(createSortBuilders(Map.of("_doc", SortOrder.DESC), false));
        nested2.innerHit(locationInnerHit);
        HybridQueryBuilder hybrid = new HybridQueryBuilder();
        hybrid.add(nested1);
        hybrid.add(nested2);
        return hybrid;
    }

    /** Parent {@code _id} -> [child inner_hits total, summed nested (user) inner_hits total across child hits]. */
    @SuppressWarnings("unchecked")
    private Map<String, List<Integer>> childThenNestedTotalsById(Map<String, Object> response) {
        Map<String, Object> hitsMap = (Map<String, Object>) response.get("hits");
        List<Map<String, Object>> hits = (List<Map<String, Object>>) hitsMap.get("hits");
        Map<String, List<Integer>> byId = new HashMap<>();
        for (Map<String, Object> hit : hits) {
            String id = (String) hit.get("_id");
            Map<String, Object> innerHits = (Map<String, Object>) hit.get("inner_hits");
            assertNotNull("parent " + id + " must carry inner_hits", innerHits);
            Map<String, Object> childInner = (Map<String, Object>) innerHits.get(TEST_PARENT_CHILD_INNER_HITS_FIELD_NAME);
            assertNotNull("parent " + id + " missing child inner_hits", childInner);
            Map<String, Object> childHitsBlock = (Map<String, Object>) childInner.get("hits");
            int childTotal = (Integer) ((Map<String, Object>) childHitsBlock.get("total")).get("value");
            List<Map<String, Object>> childHits = (List<Map<String, Object>>) childHitsBlock.get("hits");
            int userTotalAcrossChildren = 0;
            for (Map<String, Object> childHit : childHits) {
                Map<String, Object> childInnerHits = (Map<String, Object>) childHit.get("inner_hits");
                if (childInnerHits != null && childInnerHits.get(TEST_NESTED_FIELD_NAME_1) != null) {
                    Map<String, Object> userBlock = (Map<String, Object>) ((Map<String, Object>) childInnerHits.get(
                        TEST_NESTED_FIELD_NAME_1
                    )).get("hits");
                    userTotalAcrossChildren += (Integer) ((Map<String, Object>) userBlock.get("total")).get("value");
                }
            }
            byId.put(id, List.of(childTotal, userTotalAcrossChildren));
        }
        return byId;
    }

    private NestedQueryBuilder nestedUserJohn() {
        NestedQueryBuilder nested = new NestedQueryBuilder("user", new MatchQueryBuilder("user.name", "John"), ScoreMode.Avg);
        nested.innerHit(new InnerHitBuilder());
        return nested;
    }

    private NestedQueryBuilder nestedLocationCalifornia() {
        NestedQueryBuilder nested = new NestedQueryBuilder(
            "location",
            new MatchQueryBuilder("location.state", "California"),
            ScoreMode.Avg
        );
        nested.innerHit(new InnerHitBuilder());
        return nested;
    }

    /** Map of parent {@code _id} -> {nested field name -> inner_hits total count}. Order-independent oracle key. */
    @SuppressWarnings("unchecked")
    private Map<String, Map<String, Integer>> innerHitTotalsById(Map<String, Object> response, List<String> fields) {
        Map<String, Object> hitsMap = (Map<String, Object>) response.get("hits");
        List<Map<String, Object>> hits = (List<Map<String, Object>>) hitsMap.get("hits");
        Map<String, Map<String, Integer>> byId = new HashMap<>();
        for (Map<String, Object> hit : hits) {
            String id = (String) hit.get("_id");
            Map<String, Object> innerHits = (Map<String, Object>) hit.get("inner_hits");
            assertNotNull("hit " + id + " must carry inner_hits", innerHits);
            Map<String, Integer> perField = new HashMap<>();
            for (String field : fields) {
                Map<String, Object> fieldInner = (Map<String, Object>) innerHits.get(field);
                assertNotNull("hit " + id + " missing inner_hits for field " + field, fieldInner);
                Map<String, Object> fieldHits = (Map<String, Object>) fieldInner.get("hits");
                Map<String, Object> total = (Map<String, Object>) fieldHits.get("total");
                perField.put(field, (Integer) total.get("value"));
            }
            byId.put(id, perField);
        }
        return byId;
    }

    /**
     * Per parent {@code _id} -> field -> ordered list of inner-hit {@code _source} maps (in returned order). Captures
     * inner_hits pagination ({@code from}/{@code size}) and sort, so classic-vs-fused equality proves those are applied
     * identically, not just that inner_hits are present.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Map<String, List<Object>>> innerHitSourcesById(Map<String, Object> response, List<String> fields) {
        Map<String, Object> hitsMap = (Map<String, Object>) response.get("hits");
        List<Map<String, Object>> hits = (List<Map<String, Object>>) hitsMap.get("hits");
        Map<String, Map<String, List<Object>>> byId = new HashMap<>();
        for (Map<String, Object> hit : hits) {
            String id = (String) hit.get("_id");
            Map<String, Object> innerHits = (Map<String, Object>) hit.get("inner_hits");
            assertNotNull("hit " + id + " must carry inner_hits", innerHits);
            Map<String, List<Object>> perField = new HashMap<>();
            for (String field : fields) {
                Map<String, Object> fieldInner = (Map<String, Object>) innerHits.get(field);
                assertNotNull("hit " + id + " missing inner_hits for field " + field, fieldInner);
                Map<String, Object> fieldHits = (Map<String, Object>) fieldInner.get("hits");
                List<Map<String, Object>> innerHitList = (List<Map<String, Object>>) fieldHits.get("hits");
                List<Object> sources = new ArrayList<>();
                for (Map<String, Object> innerHit : innerHitList) {
                    sources.add(innerHit.get("_source"));
                }
                perField.put(field, sources);
            }
            byId.put(id, perField);
        }
        return byId;
    }

    @SneakyThrows
    private void setIndexDefaultPipeline(String indexName, String pipelineId) {
        Request request = new Request("PUT", "/" + indexName + "/_settings");
        request.setJsonEntity("{\"index.search.default_pipeline\":\"" + pipelineId + "\"}");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    public void testInnerHits_whenMultipleSubqueriesOnParentChildFields_thenSuccessful() {
        initializeIndexIfNotExist(TEST_MULTI_DOC_WITH_PARENT_CHILD_INDEX_NAME);
        createSearchPipeline(NORMALIZATION_SEARCH_PIPELINE, DEFAULT_NORMALIZATION_METHOD, DEFAULT_COMBINATION_METHOD, Map.of());
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        HasChildQueryBuilder hasChildQueryBuilder = new HasChildQueryBuilder(
            "child",
            new MatchQueryBuilder("text", "child"),
            ScoreMode.Avg
        );
        hasChildQueryBuilder.innerHit(new InnerHitBuilder());
        hybridQueryBuilder.add(hasChildQueryBuilder);
        Map<String, Object> searchResponseAsMap = search(
            TEST_MULTI_DOC_WITH_PARENT_CHILD_INDEX_NAME,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", NORMALIZATION_SEARCH_PIPELINE),
            null
        );

        List<Object> hitsNestedList = getInnerHitsFromSearchHits(searchResponseAsMap);
        assertEquals(1, getHitCount(searchResponseAsMap));
        Map<String, Map<String, ArrayList<Integer>>> innerHitCountPerFieldName = getInnerHitsCountsOfNestedField(
            hitsNestedList,
            List.of(TEST_PARENT_CHILD_INNER_HITS_FIELD_NAME)
        );
        assertEquals(1, innerHitCountPerFieldName.get(TEST_PARENT_CHILD_INNER_HITS_FIELD_NAME).get("total").get(0).intValue());
    }

    @SneakyThrows
    public void testInnerHits_whenMultipleSubqueriesOnNestedAndParentChildFields_thenSuccessful() {
        initializeIndexIfNotExist(TEST_MULTI_DOC_WITH_NESTED_PARENT_CHILD_INDEX_NAME);
        createSearchPipeline(NORMALIZATION_SEARCH_PIPELINE, DEFAULT_NORMALIZATION_METHOD, DEFAULT_COMBINATION_METHOD, Map.of());
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder("user", new MatchQueryBuilder("user.name", "John"), ScoreMode.Avg);
        nestedQueryBuilder.innerHit(new InnerHitBuilder());
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        HasChildQueryBuilder hasChildQueryBuilder = new HasChildQueryBuilder("child", nestedQueryBuilder, ScoreMode.Avg);
        hasChildQueryBuilder.innerHit(new InnerHitBuilder());
        hybridQueryBuilder.add(hasChildQueryBuilder);
        Map<String, Object> searchResponseAsMap = search(
            TEST_MULTI_DOC_WITH_NESTED_PARENT_CHILD_INDEX_NAME,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", NORMALIZATION_SEARCH_PIPELINE),
            null
        );

        List<Object> hitsNestedList = getInnerHitsFromSearchHits(searchResponseAsMap);
        assertEquals(1, getHitCount(searchResponseAsMap));
        Map<String, Map<String, ArrayList<Integer>>> innerHitCountPerFieldName = getInnerHitsCountsOfNestedField(
            hitsNestedList,
            List.of(TEST_PARENT_CHILD_INNER_HITS_FIELD_NAME)
        );
        assertEquals(1, innerHitCountPerFieldName.get(TEST_PARENT_CHILD_INNER_HITS_FIELD_NAME).get("total").get(0).intValue());
        Map<String, Object> childInnerHit = (Map<String, Object>) hitsNestedList.get(0);
        Map<String, Object> childHit = (Map<String, Object>) childInnerHit.get(TEST_PARENT_CHILD_INNER_HITS_FIELD_NAME);
        List<Object> childInnerHits = getInnerHitsFromSearchHits(childHit);
        Map<String, Map<String, ArrayList<Integer>>> childInnerHitCountPerFieldName = getInnerHitsCountsOfNestedField(
            childInnerHits,
            List.of(TEST_NESTED_FIELD_NAME_1)
        );
        assertEquals(1, childInnerHitCountPerFieldName.get(TEST_NESTED_FIELD_NAME_1).get("total").get(0).intValue());
    }

    @SneakyThrows
    public void testInnerHits_withSortingAndPagination_thenSuccessful() {
        initializeIndexIfNotExist(TEST_MULTI_DOC_WITH_NESTED_FIELDS_MULTIPLE_SHARD_INDEX_NAME);
        createSearchPipeline(NORMALIZATION_SEARCH_PIPELINE, DEFAULT_NORMALIZATION_METHOD, DEFAULT_COMBINATION_METHOD, Map.of());
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        NestedQueryBuilder nestedQueryBuilder1 = new NestedQueryBuilder("user", new MatchQueryBuilder("user.name", "John"), ScoreMode.Avg);

        InnerHitBuilder innerHitBuilder = new InnerHitBuilder();
        innerHitBuilder.setFrom(1);
        nestedQueryBuilder1.innerHit(innerHitBuilder);
        NestedQueryBuilder nestedQueryBuilder2 = new NestedQueryBuilder(
            "location",
            new MatchQueryBuilder("location.state", "California"),
            ScoreMode.Avg
        );
        InnerHitBuilder innerHitBuilder1 = new InnerHitBuilder();
        innerHitBuilder1.setSorts(createSortBuilders(Map.of("_doc", SortOrder.DESC), false));
        nestedQueryBuilder2.innerHit(innerHitBuilder1);
        hybridQueryBuilder.add(nestedQueryBuilder1);
        hybridQueryBuilder.add(nestedQueryBuilder2);

        Map<String, Object> searchResponseAsMap = search(
            TEST_MULTI_DOC_WITH_NESTED_FIELDS_MULTIPLE_SHARD_INDEX_NAME,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", NORMALIZATION_SEARCH_PIPELINE),
            null
        );

        List<Object> hitsNestedList = getInnerHitsFromSearchHits(searchResponseAsMap);
        assertEquals(2, getHitCount(searchResponseAsMap));
        Map<String, Map<String, ArrayList<Integer>>> innerHitCountPerFieldName = getInnerHitsCountsOfNestedField(
            hitsNestedList,
            List.of(TEST_NESTED_FIELD_NAME_1, TEST_NESTED_FIELD_NAME_2)
        );
        assertEquals(1, innerHitCountPerFieldName.get(TEST_NESTED_FIELD_NAME_1).get("actual").get(0).intValue());
        assertEquals(3, innerHitCountPerFieldName.get(TEST_NESTED_FIELD_NAME_2).get("actual").get(0).intValue());
        assertEquals(0, innerHitCountPerFieldName.get(TEST_NESTED_FIELD_NAME_1).get("actual").get(1).intValue());
        assertEquals(0, innerHitCountPerFieldName.get(TEST_NESTED_FIELD_NAME_2).get("actual").get(1).intValue());

        Map<String, ArrayList<List<Object>>> sortsPerField = getInnerHitsSortValueOfNestedField(
            hitsNestedList,
            List.of(TEST_NESTED_FIELD_NAME_2)
        );

        ArrayList<List<Object>> locationSorts = sortsPerField.get(TEST_NESTED_FIELD_NAME_2);
        assertTrue(
            IntStream.range(0, locationSorts.size() - 1)
                .mapToObj(i -> new AbstractMap.SimpleEntry<>(locationSorts.get(i).get(0), locationSorts.get(i + 1).get(0)))
                .allMatch(pair -> ((Comparable<Object>) pair.getKey()).compareTo(pair.getValue()) > 0)
        );
    }

    @SneakyThrows
    public void testInnerHitsWithExplain_whenMultipleSubqueriesOnNestedFields_thenSuccessful() {
        initializeIndexIfNotExist(TEST_MULTI_DOC_WITH_NESTED_FIELDS_MULTIPLE_SHARD_INDEX_NAME);
        createSearchPipeline(
            NORMALIZATION_SEARCH_PIPELINE,
            DEFAULT_NORMALIZATION_METHOD,
            Map.of(),
            DEFAULT_COMBINATION_METHOD,
            Map.of(),
            true
        );
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        NestedQueryBuilder nestedQueryBuilder1 = new NestedQueryBuilder("user", new MatchQueryBuilder("user.name", "John"), ScoreMode.Max);
        nestedQueryBuilder1.innerHit(new InnerHitBuilder());
        NestedQueryBuilder nestedQueryBuilder2 = new NestedQueryBuilder(
            "location",
            new MatchQueryBuilder("location.state", "California"),
            ScoreMode.Max
        );
        nestedQueryBuilder2.innerHit(new InnerHitBuilder());
        hybridQueryBuilder.add(nestedQueryBuilder1);
        hybridQueryBuilder.add(nestedQueryBuilder2);

        Map<String, Object> searchResponseAsMap = search(
            TEST_MULTI_DOC_WITH_NESTED_FIELDS_MULTIPLE_SHARD_INDEX_NAME,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", NORMALIZATION_SEARCH_PIPELINE, "explain", Boolean.TRUE.toString()),
            null
        );

        List<Object> nestedHitsList = getInnerHitsFromSearchHits(searchResponseAsMap);
        Map<String, ArrayList<Double>> scoreOfInnerHits = getInnerHitsScoresPerFieldList(
            nestedHitsList,
            List.of(TEST_NESTED_FIELD_NAME_1, TEST_NESTED_FIELD_NAME_2)
        );

        Map<String, Map<String, ArrayList<Integer>>> innerHitCountPerFieldName = getInnerHitsCountsOfNestedField(
            nestedHitsList,
            List.of(TEST_NESTED_FIELD_NAME_1, TEST_NESTED_FIELD_NAME_2)
        );
        // Assert
        // basic sanity check for search hits
        assertEquals(2, getHitCount(searchResponseAsMap));
        assertTrue(getMaxScore(searchResponseAsMap).isPresent());
        float actualMaxScore = getMaxScore(searchResponseAsMap).get();
        assertTrue(actualMaxScore > 0);
        Map<String, Object> total = getTotalHits(searchResponseAsMap);
        assertNotNull(total.get("value"));
        assertEquals(2, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));

        // explain, hit 1
        List<Map<String, Object>> hitsNestedList = getNestedHits(searchResponseAsMap);
        Map<String, Object> searchHit1 = hitsNestedList.get(0);
        Map<String, Object> explanationForHit1 = getValueByKey(searchHit1, "_explanation");
        assertNotNull(explanationForHit1);
        assertEquals((double) searchHit1.get("_score"), (double) explanationForHit1.get("value"), DELTA_FOR_SCORE_ASSERTION);

        // top level explanation
        String expectedTopLevelDescription = "arithmetic_mean combination of:";
        assertEquals(expectedTopLevelDescription, explanationForHit1.get("description"));

        // Normalization explanation: one "min_max normalization of:" block per sub-query.
        // Both sub-queries are nested queries, so the index contains nested fields and OpenSearch core wraps the
        // hybrid query in a BooleanQuery. The explanation processor descends to the hybrid node, so each sub-query
        // gets its own normalization block (previously they were collapsed into a single block, see issue #1875).
        List<Map<String, Object>> hit1Details = getListOfValues(explanationForHit1, "details");
        assertEquals(2, hit1Details.size());

        // sub-query 1 (nested field "user") — min score in its result set, normalizes to the min_max floor
        Map<String, Object> hit1NormalizationBlock1 = hit1Details.get(0);
        assertEquals("min_max normalization of:", hit1NormalizationBlock1.get("description"));
        assertEquals(0.001f, (double) hit1NormalizationBlock1.get("value"), DELTA_FOR_SCORE_ASSERTION);
        List<Map<String, Object>> hit1SubQuery1Details = getListOfValues(hit1NormalizationBlock1, "details");
        assertEquals(1, hit1SubQuery1Details.size());
        Map<String, Object> hit1SubQuery1Child = hit1SubQuery1Details.get(0);
        assertEquals(
            scoreOfInnerHits.get(TEST_NESTED_FIELD_NAME_1).get(0),
            (double) hit1SubQuery1Child.get("value"),
            DELTA_FOR_SCORE_ASSERTION
        );
        assertEquals(
            "Score based on "
                + innerHitCountPerFieldName.get(TEST_NESTED_FIELD_NAME_1).get("total").get(0)
                + " child docs in range from 0 to 11, using score mode Max",
            hit1SubQuery1Child.get("description")
        );
        assertEquals(1, ((List) hit1SubQuery1Child.get("details")).size());

        // sub-query 2 (nested field "location") — max score in its result set, normalizes to 1.0
        Map<String, Object> hit1NormalizationBlock2 = hit1Details.get(1);
        assertEquals("min_max normalization of:", hit1NormalizationBlock2.get("description"));
        assertEquals(1.0f, (double) hit1NormalizationBlock2.get("value"), DELTA_FOR_SCORE_ASSERTION);
        List<Map<String, Object>> hit1SubQuery2Details = getListOfValues(hit1NormalizationBlock2, "details");
        assertEquals(1, hit1SubQuery2Details.size());
        Map<String, Object> hit1SubQuery2Child = hit1SubQuery2Details.get(0);
        assertEquals(
            scoreOfInnerHits.get(TEST_NESTED_FIELD_NAME_2).get(0),
            (double) hit1SubQuery2Child.get("value"),
            DELTA_FOR_SCORE_ASSERTION
        );
        assertEquals(
            "Score based on "
                + innerHitCountPerFieldName.get(TEST_NESTED_FIELD_NAME_2).get("total").get(0)
                + " child docs in range from 0 to 11, using score mode Max",
            hit1SubQuery2Child.get("description")
        );
        assertEquals(1, ((List) hit1SubQuery2Child.get("details")).size());
    }

    @SneakyThrows
    private void initializeIndexIfNotExist(String indexName) {
        Map<String, Map<String, String>> nestedFields = new HashMap<>();
        nestedFields.put(
            TEST_NESTED_FIELD_NAME_1,
            Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "text", TEST_USER_INNER_AGE_NESTED_FIELD, "integer")
        );
        nestedFields.put(
            TEST_NESTED_FIELD_NAME_2,
            Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "text", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "text")
        );
        if ((TEST_MULTI_DOC_WITH_NESTED_FIELDS_SINGLE_SHARD_INDEX_NAME.equals(indexName)
            && !indexExists(TEST_MULTI_DOC_WITH_NESTED_FIELDS_SINGLE_SHARD_INDEX_NAME))) {
            createIndexWithConfiguration(indexName, buildIndexConfiguration(Collections.emptyList(), nestedFields, 1), "");
            addNestedDocsToIndex(TEST_MULTI_DOC_WITH_NESTED_FIELDS_SINGLE_SHARD_INDEX_NAME);
        }

        if ((TEST_MULTI_DOC_WITH_NESTED_FIELDS_MULTIPLE_SHARD_INDEX_NAME.equals(indexName)
            && !indexExists(TEST_MULTI_DOC_WITH_NESTED_FIELDS_MULTIPLE_SHARD_INDEX_NAME))) {
            createIndexWithConfiguration(indexName, buildIndexConfiguration(Collections.emptyList(), nestedFields, 3), "");
            addNestedDocsToIndex(TEST_MULTI_DOC_WITH_NESTED_FIELDS_MULTIPLE_SHARD_INDEX_NAME);
        }

        if (TEST_MULTI_DOC_WITH_PARENT_CHILD_INDEX_NAME.equals(indexName) && !indexExists(TEST_MULTI_DOC_WITH_PARENT_CHILD_INDEX_NAME)) {
            createIndexWithConfiguration(
                indexName,
                buildIndexConfiguration(
                    Collections.emptyList(),
                    Collections.emptyMap(),
                    List.of(List.of(TEST_PARENT_CHILD_MY_JOIN_FIELD_FIELD, TEST_PARENT_CHILD_TYPE_JOIN)),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    3
                ),
                ""
            );
            addParentChildDocsToIndex(TEST_MULTI_DOC_WITH_PARENT_CHILD_INDEX_NAME);
        }

        if (TEST_MULTI_DOC_WITH_NESTED_PARENT_CHILD_INDEX_NAME.equals(indexName)
            && !indexExists(TEST_MULTI_DOC_WITH_NESTED_PARENT_CHILD_INDEX_NAME)) {
            createIndexWithConfiguration(
                indexName,
                buildIndexConfiguration(
                    Collections.emptyList(),
                    nestedFields,
                    List.of(List.of(TEST_PARENT_CHILD_MY_JOIN_FIELD_FIELD, TEST_PARENT_CHILD_TYPE_JOIN)),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    3
                ),
                ""
            );
            addNestedAndParentChildDocsToIndex(TEST_MULTI_DOC_WITH_NESTED_PARENT_CHILD_INDEX_NAME);
        }
    }

    private void addNestedDocsToIndex(final String testMultiDocIndexName) {
        addKnnDoc(
            testMultiDocIndexName,
            "1",
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(TEST_NESTED_FIELD_NAME_1, TEST_NESTED_FIELD_NAME_2),
            Map.of(
                TEST_NESTED_FIELD_NAME_1,
                Arrays.asList(
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "John Alder", TEST_USER_INNER_AGE_NESTED_FIELD, "50"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "John snow", TEST_USER_INNER_AGE_NESTED_FIELD, "23"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Harry Styles", TEST_USER_INNER_AGE_NESTED_FIELD, "20"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Michael Jackson", TEST_USER_INNER_AGE_NESTED_FIELD, "67"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Marry Jane", TEST_USER_INNER_AGE_NESTED_FIELD, "90"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Tom Hanks", TEST_USER_INNER_AGE_NESTED_FIELD, "5")
                ),
                TEST_NESTED_FIELD_NAME_2,
                Arrays.asList(
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "California", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "San Diego"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "North Carolina", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Charlotte"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "California", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Los Angeles"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "New York", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "New York"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Oregon", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Portland"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "California", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Fresno")
                )
            )
        );
        addKnnDoc(
            testMultiDocIndexName,
            "2",
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(TEST_NESTED_FIELD_NAME_1, TEST_NESTED_FIELD_NAME_2),
            Map.of(
                TEST_NESTED_FIELD_NAME_1,
                Arrays.asList(
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "John Carry", TEST_USER_INNER_AGE_NESTED_FIELD, "34"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Dwayne Rock", TEST_USER_INNER_AGE_NESTED_FIELD, "28"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Leonardo Di Caprio", TEST_USER_INNER_AGE_NESTED_FIELD, "22"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Jack Sparrow", TEST_USER_INNER_AGE_NESTED_FIELD, "47"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Will Smith", TEST_USER_INNER_AGE_NESTED_FIELD, "45"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Brad Pitt", TEST_USER_INNER_AGE_NESTED_FIELD, "39")
                ),
                TEST_NESTED_FIELD_NAME_2,
                Arrays.asList(
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Illinois", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Chicago"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Texas", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Dallas"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Arizona", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Phoenix"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Florida", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Orlando"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Virginia", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Redmond"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Washington", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Seattle")
                )
            )
        );

        assertEquals(2, getDocCount(testMultiDocIndexName));
    }

    private void addParentChildDocsToIndex(final String testMultiDocIndexName) {
        indexTheDocument(
            testMultiDocIndexName,
            "1",
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(TEST_PARENT_CHILD_TEXT_FIELD_NAME),
            List.of(TEST_PARENT_CHILD_TEXT_FIELD_VALUE_1),
            Collections.emptyList(),
            Map.of(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(TEST_PARENT_CHILD_MY_JOIN_FIELD_FIELD),
            List.of(TEST_PARENT_CHILD_RELATION_FIELD_NAME_1),
            null
        );

        indexTheDocument(
            testMultiDocIndexName,
            "2",
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(TEST_PARENT_CHILD_TEXT_FIELD_NAME),
            List.of(TEST_PARENT_CHILD_TEXT_FIELD_VALUE_2),
            Collections.emptyList(),
            Map.of(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(TEST_PARENT_CHILD_MY_JOIN_FIELD_FIELD),
            List.of(TEST_PARENT_CHILD_RELATION_FIELD_NAME_2),
            "1"
        );
    }

    private void addNestedAndParentChildDocsToIndex(final String testMultiDocIndexName) {
        indexTheDocument(
            testMultiDocIndexName,
            "1",
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(TEST_PARENT_CHILD_TEXT_FIELD_NAME),
            List.of(TEST_PARENT_CHILD_TEXT_FIELD_VALUE_1),
            List.of(TEST_NESTED_FIELD_NAME_1, TEST_NESTED_FIELD_NAME_2),
            Map.of(
                TEST_NESTED_FIELD_NAME_1,
                Arrays.asList(
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "John Alder", TEST_USER_INNER_AGE_NESTED_FIELD, "50"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "John snow", TEST_USER_INNER_AGE_NESTED_FIELD, "23"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Harry Styles", TEST_USER_INNER_AGE_NESTED_FIELD, "20"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Michael Jackson", TEST_USER_INNER_AGE_NESTED_FIELD, "67"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Marry Jane", TEST_USER_INNER_AGE_NESTED_FIELD, "90"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Tom Hanks", TEST_USER_INNER_AGE_NESTED_FIELD, "5")
                ),
                TEST_NESTED_FIELD_NAME_2,
                Arrays.asList(
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "California", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "San Diego"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "North Carolina", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Charlotte"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "California", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Los Angeles"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "New York", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "New York"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Oregon", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Portland"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "California", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Fresno")
                )
            ),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(TEST_PARENT_CHILD_MY_JOIN_FIELD_FIELD),
            List.of(TEST_PARENT_CHILD_RELATION_FIELD_NAME_1),
            null
        );

        indexTheDocument(
            testMultiDocIndexName,
            "2",
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(TEST_PARENT_CHILD_TEXT_FIELD_NAME),
            List.of(TEST_PARENT_CHILD_TEXT_FIELD_VALUE_2),
            List.of(TEST_NESTED_FIELD_NAME_1, TEST_NESTED_FIELD_NAME_2),
            Map.of(
                TEST_NESTED_FIELD_NAME_1,
                Arrays.asList(
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "John Carry", TEST_USER_INNER_AGE_NESTED_FIELD, "34"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Dwayne Rock", TEST_USER_INNER_AGE_NESTED_FIELD, "28"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Leonardo Di Caprio", TEST_USER_INNER_AGE_NESTED_FIELD, "22"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Jack Sparrow", TEST_USER_INNER_AGE_NESTED_FIELD, "47"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Will Smith", TEST_USER_INNER_AGE_NESTED_FIELD, "45"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Brad Pitt", TEST_USER_INNER_AGE_NESTED_FIELD, "39")
                ),
                TEST_NESTED_FIELD_NAME_2,
                Arrays.asList(
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Illinois", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Chicago"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Texas", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Dallas"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Arizona", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Phoenix"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Florida", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Orlando"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Virginia", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Redmond"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Washington", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Seattle")
                )
            ),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(TEST_PARENT_CHILD_MY_JOIN_FIELD_FIELD),
            List.of(TEST_PARENT_CHILD_RELATION_FIELD_NAME_2),
            "1"
        );
    }

    @SneakyThrows
    public void testInnerHits_whenMultipleSubqueriesOnNestedFields_statsEnabled_thenSuccessful() {
        enableStats();

        testInnerHits_whenMultipleSubqueriesOnNestedFields_thenSuccessful(TEST_MULTI_DOC_WITH_NESTED_FIELDS_SINGLE_SHARD_INDEX_NAME);
        testInnerHits_whenMultipleSubqueriesOnNestedFields_thenSuccessful(TEST_MULTI_DOC_WITH_NESTED_FIELDS_MULTIPLE_SHARD_INDEX_NAME);

        // Get stats
        String responseBody = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>());
        Map<String, Object> stats = parseInfoStatsResponse(responseBody);
        Map<String, Object> allNodesStats = parseAggregatedNodeStatsResponse(responseBody);

        // Parse json to get stats
        assertEquals(2, getNestedValue(allNodesStats, EventStatName.HYBRID_QUERY_REQUESTS));
        assertEquals(2, getNestedValue(allNodesStats, EventStatName.HYBRID_QUERY_INNER_HITS_REQUESTS));

        disableStats();
    }
}
