/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.hamcrest.Matchers.containsString;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchType;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.search.slice.SliceBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;

import lombok.SneakyThrows;

public class CandidateScopeTests extends OpenSearchTestCase {

    private static final String INDEX = "test-index";
    private static final QueryBuilder LEG = new MatchQueryBuilder("text", "hello");

    // ---- the guard: the classification table must cover both request classes, exactly ----

    /**
     * The reason this class exists. Round 1 and round 2 must agree on which documents are candidates, and the silent way
     * to break that is for a request field to reach a leg (or fail to) because nobody considered it. So every declared
     * field of both request classes must carry an explicit disposition, and this test fails the build until it does — a
     * core upgrade that adds a search-request field cannot land unexamined.
     */
    public void testEveryRequestFieldIsClassified() {
        Set<String> unclassified = new TreeSet<>();
        for (Field field : instanceFields(SearchRequest.class)) {
            String key = CandidateScope.key(CandidateScope.SEARCH_REQUEST, field.getName());
            if (CandidateScope.CLASSIFICATION.containsKey(key) == false) {
                unclassified.add(key);
            }
        }
        for (Field field : instanceFields(SearchSourceBuilder.class)) {
            String key = CandidateScope.key(CandidateScope.SEARCH_SOURCE, field.getName());
            if (CandidateScope.CLASSIFICATION.containsKey(key) == false) {
                unclassified.add(key);
            }
        }

        assertTrue(
            "New search-request field(s) "
                + unclassified
                + " are not classified in CandidateScope. Decide whether each one changes WHICH documents a leg returns "
                + "or HOW they score: if it does, propagate it (or reject the request); if it does not, mark it "
                + "NOT_PROPAGATED with the reason. Do not delete this assertion.",
            unclassified.isEmpty()
        );
    }

    /**
     * The other direction: an entry naming a field core has renamed or removed would leave the table looking complete
     * while silently classifying nothing.
     */
    public void testNoStaleClassificationEntries() {
        Set<String> declared = new TreeSet<>();
        for (Field field : instanceFields(SearchRequest.class)) {
            declared.add(CandidateScope.key(CandidateScope.SEARCH_REQUEST, field.getName()));
        }
        for (Field field : instanceFields(SearchSourceBuilder.class)) {
            declared.add(CandidateScope.key(CandidateScope.SEARCH_SOURCE, field.getName()));
        }

        Set<String> stale = new TreeSet<>(CandidateScope.CLASSIFICATION.keySet());
        stale.removeAll(declared);

        assertTrue(
            "CandidateScope classifies field(s) " + stale + " that no longer exist in core — remove or rename them.",
            stale.isEmpty()
        );
    }

    public void testEveryClassificationStatesAReason() {
        for (Map.Entry<String, CandidateScope.Classification> entry : CandidateScope.CLASSIFICATION.entrySet()) {
            assertNotNull(entry.getKey(), entry.getValue().disposition());
            assertFalse("[" + entry.getKey() + "] must state why its disposition is correct", entry.getValue().reason().isBlank());
        }
    }

    private List<Field> instanceFields(Class<?> type) {
        List<Field> fields = new ArrayList<>();
        for (Field field : type.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers()) || field.isSynthetic()) {
                continue;
            }
            fields.add(field);
        }
        assertFalse(
            "reflection found no instance fields on " + type.getSimpleName() + " — the guard would pass vacuously",
            fields.isEmpty()
        );
        return fields;
    }

    // ---- PROPAGATED: what defines the candidate set must reach every leg ----

    public void testPropagatedFieldsReachTheLeg() {
        SliceBuilder slice = new SliceBuilder("_id", 1, 4);
        TermQueryBuilder postFilter = new TermQueryBuilder("grp", "a");
        SearchRequest request = new SearchRequest(INDEX).indicesOptions(IndicesOptions.lenientExpandOpen())
            .routing("r1")
            .preference("_local")
            .searchType(SearchType.DFS_QUERY_THEN_FETCH)
            .allowPartialSearchResults(false)
            .source(new SearchSourceBuilder().postFilter(postFilter).slice(slice).timeout(TimeValue.timeValueSeconds(7)));
        request.setMaxConcurrentShardRequests(3);
        request.setPreFilterShardSize(64);
        request.setCancelAfterTimeInterval(TimeValue.timeValueSeconds(11));

        SearchRequest leg = CandidateScope.from(request).newLegRequest(LEG, 50);

        assertArrayEquals(new String[] { INDEX }, leg.indices());
        assertEquals(IndicesOptions.lenientExpandOpen(), leg.indicesOptions());
        assertEquals("round 2 only searches routed shards", "r1", leg.routing());
        assertEquals("legs and round 2 must hit the same shard copies", "_local", leg.preference());
        assertEquals("dfs changes term stats, and so the window", SearchType.DFS_QUERY_THEN_FETCH, leg.searchType());
        assertEquals(Boolean.FALSE, leg.allowPartialSearchResults());
        assertEquals(3, leg.getMaxConcurrentShardRequestsRaw());
        assertEquals(Integer.valueOf(64), leg.getPreFilterShardSize());
        assertEquals(TimeValue.timeValueSeconds(11), leg.getCancelAfterTimeInterval());
        assertEquals(TimeValue.timeValueSeconds(7), leg.source().timeout());
        assertEquals("round 2 post-filters its window, so the leg must too", postFilter, leg.source().postFilter());
        assertEquals("round 2 returns only the slice", slice, leg.source().slice());
    }

    public void testUnsetFieldsAreLeftUnsetOnTheLeg() {
        // An unset value must not be forced onto a leg: the leg has to resolve the same default the outer request would.
        SearchRequest leg = CandidateScope.from(new SearchRequest(INDEX)).newLegRequest(LEG, 50);

        assertNull(leg.routing());
        assertNull(leg.preference());
        assertNull("unset → each leg resolves the cluster default (true) at execution", leg.allowPartialSearchResults());
        assertEquals("0 is core's 'unset' for max_concurrent_shard_requests", 0, leg.getMaxConcurrentShardRequestsRaw());
        assertNull(leg.getPreFilterShardSize());
        assertNull(leg.getCancelAfterTimeInterval());
        assertNull(leg.source().timeout());
        assertNull(leg.source().postFilter());
        assertNull(leg.source().slice());
        assertNull(leg.source().pointInTimeBuilder());
    }

    public void testAllowPartialSearchResultsPropagatesEitherExplicitValue() {
        // false in particular must reach the legs: that is what makes a leg with a failing shard fail outright, which
        // HybridFusionOrchestrator#groupLegHits turns into a whole-request failure instead of a re-normalized ranking.
        for (boolean explicit : new boolean[] { true, false }) {
            SearchRequest request = new SearchRequest(INDEX).allowPartialSearchResults(explicit);

            SearchRequest leg = CandidateScope.from(request).newLegRequest(LEG, 50);

            assertEquals(explicit, leg.allowPartialSearchResults());
        }
    }

    public void testPointInTimePropagatesWithoutExtendingKeepAlive() {
        SearchRequest request = new SearchRequest(INDEX).source(
            new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder("pit-id-42").setKeepAlive(TimeValue.timeValueMinutes(5)))
        );

        SearchRequest leg = CandidateScope.from(request).newLegRequest(LEG, 50);

        assertEquals("all legs and round 2 must read one immutable view", "pit-id-42", leg.source().pointInTimeBuilder().getId());
        assertNull("a leg never extends the PIT keep-alive", leg.source().pointInTimeBuilder().getKeepAlive());
    }

    public void testScopeIsCapturedOnceAndReusedByEveryLeg() {
        // The scope is captured before the fan-out, so mutating the request afterwards cannot change what a leg inherits.
        SearchRequest request = new SearchRequest(INDEX).routing("r1");
        CandidateScope scope = CandidateScope.from(request);
        request.routing("r2");

        assertEquals("r1", scope.newLegRequest(LEG, 50).routing());
        assertEquals("r1", scope.newLegRequest(new TermQueryBuilder("text", "place"), 50).routing());
    }

    // ---- OVERRIDDEN: fused mode dictates the leg's own shape ----

    public void testOverriddenFieldsAreSetByFusedModeNotInherited() {
        SearchRequest request = new SearchRequest(INDEX).pipeline("norm-pipeline")
            .source(
                new SearchSourceBuilder().query(new TermQueryBuilder("text", "outer"))
                    .from(30)
                    .size(10)
                    .trackTotalHits(true)
                    .fetchSource(true)
                    .aggregation(AggregationBuilders.terms("t").field("f"))
                    .highlighter(new HighlightBuilder().field("text"))
                    .searchPipelineSource(Map.of("response_processors", List.of()))
            );

        SearchRequest leg = CandidateScope.from(request).newLegRequest(LEG, 50);

        assertEquals("a leg runs its own sub-query", LEG, leg.source().query());
        assertEquals("a leg returns exactly the candidate window", 50, leg.source().size());
        assertEquals("paging is a round-2 concern", 0, leg.source().from());
        assertFalse("legs are id-only", leg.source().fetchSource().fetchSource());
        assertEquals("legs disable totals; the Tail supplies them", Integer.valueOf(-1), leg.source().trackTotalHitsUpTo());
        assertNull("aggregations run once, in round 2", leg.source().aggregations());
        assertNull("highlighting runs once, in round 2", leg.source().highlighter());
        assertNull("an inline pipeline body is not applied per leg", leg.source().searchPipelineSource());
        assertEquals("processors run once for the user request, not once per leg", SearchPipelineService.NOOP_PIPELINE_ID, leg.pipeline());
    }

    // ---- REJECTED: shapes fused mode cannot answer correctly, refused before the fan-out ----

    public void testRejectsTerminateAfter() {
        SearchRequest request = new SearchRequest(INDEX).source(new SearchSourceBuilder().terminateAfter(100));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CandidateScope.from(request));

        assertThat(e.getMessage(), containsString("[hybrid] query [fusion] does not support [terminate_after]"));
        assertThat(e.getMessage(), containsString("counts every match in docid order"));
    }

    public void testRejectsIndicesBoost() {
        SearchRequest request = new SearchRequest(INDEX).source(new SearchSourceBuilder().indexBoost(INDEX, 2.0f));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CandidateScope.from(request));

        assertThat(e.getMessage(), containsString("does not support [indices_boost]"));
    }

    /**
     * Both shapes a user can write. The map form is only reachable by parsing a request body — core exposes no setter for
     * it, which is also why fused mode rejects derived fields instead of propagating them onto the leg source.
     */
    @SneakyThrows
    public void testRejectsDerivedFieldsInBothForms() {
        XContentParser parser = createParser(
            JsonXContent.jsonXContent,
            "{\"derived\":{\"d\":{\"type\":\"keyword\",\"script\":\"emit('x')\"}}}"
        );
        SearchRequest mapForm = new SearchRequest(INDEX).source(SearchSourceBuilder.fromXContent(parser));
        SearchRequest listForm = new SearchRequest(INDEX).source(
            new SearchSourceBuilder().derivedField("d", "keyword", new Script("emit('x')"))
        );

        for (SearchRequest request : List.of(mapForm, listForm)) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CandidateScope.from(request));
            assertThat(e.getMessage(), containsString("does not support [derived]"));
            assertThat(e.getMessage(), containsString("would silently rewrite to match_none"));
        }
    }

    public void testRejectsCrossClusterSearch() {
        // A remote hit's _index carries the cluster alias, which no shard of the remote index matches, so the
        // _index-qualified Top clauses would silently drop every remote document.
        SearchRequest request = new SearchRequest("local-index", "remote-cluster:other-index");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CandidateScope.from(request));

        assertThat(e.getMessage(), containsString("does not support [cross-cluster search]"));
    }

    public void testPlainRequestIsAccepted() {
        // The rejections must test for values the user actually supplied — an ordinary request must not trip any of them.
        CandidateScope.from(new SearchRequest(INDEX).source(new SearchSourceBuilder().query(LEG).size(10)));
        CandidateScope.from(new SearchRequest(INDEX));
    }

    // ---- FORCES_TAIL: a non-_score sort ranks over the match set, not the window ----

    public void testSortDiscardsFusedRanking() {
        assertFalse("no sort → the fused score is the ranking", CandidateScope.sortDiscardsFusedRanking(new SearchSourceBuilder()));
        assertFalse("null source", CandidateScope.sortDiscardsFusedRanking(null));
        assertFalse(
            "_score sort IS the fused ranking",
            CandidateScope.sortDiscardsFusedRanking(new SearchSourceBuilder().sort(SortBuilders.scoreSort()))
        );
        assertTrue(
            "a field sort ranks by the field, so round 2 must see the full union",
            CandidateScope.sortDiscardsFusedRanking(new SearchSourceBuilder().sort(SortBuilders.fieldSort("price").order(SortOrder.ASC)))
        );
        assertTrue(
            "_doc order likewise discards the fused ranking",
            CandidateScope.sortDiscardsFusedRanking(new SearchSourceBuilder().sort(SortBuilders.fieldSort("_doc")))
        );
        assertTrue(
            "a field sort anywhere in the sort list counts, not just first",
            CandidateScope.sortDiscardsFusedRanking(
                new SearchSourceBuilder().sort(SortBuilders.scoreSort()).sort(SortBuilders.fieldSort("price"))
            )
        );
    }
}
