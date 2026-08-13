/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.List;
import java.util.Map;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.test.OpenSearchTestCase;

public class HybridFusionOrchestratorTests extends OpenSearchTestCase {

    private static final String INDEX = "test-index";

    private FusionSpec minMaxArithmetic() {
        return new FusionSpec(
            FusionSpec.TECHNIQUE_ARITHMETIC_MEAN,
            FusionSpec.NORMALIZATION_MIN_MAX,
            FusionSpec.DEFAULT_RANK_CONSTANT,
            new float[0]
        );
    }

    /** One MultiSearch item wrapping a SearchResponse whose hits carry the given (_id -> score) pairs. */
    private MultiSearchResponse.Item legItem(Map<String, Float> idToScore) {
        SearchHit[] hits = new SearchHit[idToScore.size()];
        int i = 0;
        for (Map.Entry<String, Float> e : idToScore.entrySet()) {
            SearchHit hit = new SearchHit(i, e.getKey(), Map.of(), Map.of());
            hit.score(e.getValue());
            hits[i++] = hit;
        }
        SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponseSections sections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        SearchResponse response = new SearchResponse(sections, null, 1, 1, 0, 10, ShardSearchFailure.EMPTY_ARRAY, null);
        return new MultiSearchResponse.Item(response, null);
    }

    private MultiSearchResponse.Item failedItem() {
        return new MultiSearchResponse.Item(null, new RuntimeException("leg boom"));
    }

    /** A SUCCESSFUL MultiSearch item that lost a shard under allow_partial=true: HTTP 200, fewer hits, non-empty
     *  shardFailures (isFailure()==false). Models a partially-degraded leg. */
    private MultiSearchResponse.Item partialLegItem(Map<String, Float> idToScore) {
        SearchHit[] hits = new SearchHit[idToScore.size()];
        int i = 0;
        for (Map.Entry<String, Float> e : idToScore.entrySet()) {
            SearchHit hit = new SearchHit(i, e.getKey(), Map.of(), Map.of());
            hit.score(e.getValue());
            hits[i++] = hit;
        }
        SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponseSections sections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        ShardSearchFailure[] failures = new ShardSearchFailure[] { new ShardSearchFailure(new RuntimeException("shard down")) };
        // totalShards=2, successful=1, skipped=0, one shard failure → partial but SUCCESSFUL item.
        SearchResponse response = new SearchResponse(sections, null, 2, 1, 0, 10, failures, null);
        return new MultiSearchResponse.Item(response, null);
    }

    private MultiSearchResponse multiSearch(MultiSearchResponse.Item... items) {
        return new MultiSearchResponse(items, 10L);
    }

    // ---- buildLegMultiSearch ----

    public void testBuildLegMultiSearch_perLegSourceShape() {
        SearchRequest request = new SearchRequest(INDEX);
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));

        MultiSearchRequest ms = HybridFusionOrchestrator.buildLegMultiSearch(request, legs, 50);

        assertEquals(2, ms.requests().size());
        for (SearchRequest leg : ms.requests()) {
            SearchSourceBuilder source = leg.source();
            assertEquals(50, source.size());
            assertFalse(source.fetchSource().fetchSource());
            assertEquals(SearchPipelineService.NOOP_PIPELINE_ID, leg.pipeline());
            // Unset on the request → left unset on the leg so each resolves the cluster default (true) at execution.
            assertNull(leg.allowPartialSearchResults());
        }
    }

    public void testBuildLegMultiSearch_whenAllowPartialExplicitlySet_thenPropagatedToLegs() {
        // An explicit request-level value is honored by the legs. Notably false must reach them: that is what makes a
        // leg with a failing shard fail outright, which groupLegHits turns into a whole-request failure.
        for (boolean explicit : new boolean[] { true, false }) {
            SearchRequest request = new SearchRequest(INDEX).allowPartialSearchResults(explicit);
            List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));

            MultiSearchRequest ms = HybridFusionOrchestrator.buildLegMultiSearch(request, legs, 50);

            for (SearchRequest leg : ms.requests()) {
                assertEquals(explicit, leg.allowPartialSearchResults());
            }
        }
    }

    public void testBuildLegMultiSearch_whenRequestHasPit_thenPassedToEveryLeg() {
        // A user-supplied PIT must reach every leg so all legs (and round 2) read one immutable view instead of N+1
        // independent reader instants. keepAlive is left unset on legs so the PIT's original keep-alive governs.
        SearchRequest request = new SearchRequest(INDEX).source(
            new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder("pit-id-42").setKeepAlive(TimeValue.timeValueMinutes(5)))
        );
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));

        MultiSearchRequest ms = HybridFusionOrchestrator.buildLegMultiSearch(request, legs, 50);

        assertEquals(2, ms.requests().size());
        for (SearchRequest leg : ms.requests()) {
            assertNotNull("each leg must carry the PIT", leg.source().pointInTimeBuilder());
            assertEquals("pit-id-42", leg.source().pointInTimeBuilder().getId());
            assertNull("legs must not extend the PIT keep-alive", leg.source().pointInTimeBuilder().getKeepAlive());
        }
    }

    public void testBuildLegMultiSearch_whenNoPit_thenLegsHaveNone() {
        SearchRequest request = new SearchRequest(INDEX);
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"));

        MultiSearchRequest ms = HybridFusionOrchestrator.buildLegMultiSearch(request, legs, 50);

        assertNull(ms.requests().get(0).source().pointInTimeBuilder());
    }

    // ---- buildFusedQuery: Top+Tail / Top-only / match_none ----

    public void testBuildFusedQuery_topLevelWithAggs_keepsTail() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f, "2", 0.5f)), legItem(Map.of("2", 0.8f, "3", 0.4f)));
        SearchSourceBuilder source = new SearchSourceBuilder().aggregation(
            org.opensearch.search.aggregations.AggregationBuilders.terms("t").field("f")
        );

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(source, ms, legs, minMaxArithmetic(), 10);

        assertTrue(fused instanceof HybridFusionQueryBuilder);
        BoolQueryBuilder self = ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery();
        assertEquals("union of {1,2,3} scored in Top", 3, self.should().size());
        assertEquals("aggs → Tail retained", 1, self.filter().size());
    }

    public void testBuildFusedQuery_topLevelPlainTopK_topOnly() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)), legItem(Map.of("2", 0.8f)));
        // track_total_hits:false, no aggs/highlight/explain → plain top-K, Tail dropped.
        SearchSourceBuilder source = new SearchSourceBuilder().trackTotalHits(false);

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(source, ms, legs, minMaxArithmetic(), 10);

        BoolQueryBuilder self = ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery();
        assertEquals(2, self.should().size());
        assertEquals("plain top-K → no Tail", 0, self.filter().size());
    }

    public void testBuildFusedQuery_tailDecisionIsDepthIndependent() {
        // The Tail decision comes from the REQUEST alone, never from whether this hybrid is top-level or nested: the
        // fused query self-erases the same way at any depth, and an enclosing clause simply intersects it. So with aggs
        // present the Tail is retained — nesting no longer silently downgrades agg/total_hits accuracy.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)), legItem(Map.of("2", 0.8f)));
        SearchSourceBuilder source = new SearchSourceBuilder().aggregation(
            org.opensearch.search.aggregations.AggregationBuilders.terms("t").field("f")
        );

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(source, ms, legs, minMaxArithmetic(), 10);

        BoolQueryBuilder self = ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery();
        assertEquals("aggs → Tail retained regardless of nesting depth", 1, self.filter().size());
    }

    public void testBuildFusedQuery_emptyResult_matchNone() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of()));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(new SearchSourceBuilder(), ms, legs, minMaxArithmetic(), 10);

        assertTrue(fused instanceof MatchNoneQueryBuilder);
    }

    public void testBuildFusedQuery_windowCapsRankedDocs() {
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f, "2", 0.8f, "3", 0.7f, "4", 0.6f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(
            new SearchSourceBuilder().trackTotalHits(false),
            ms,
            legs,
            minMaxArithmetic(),
            2
        );

        BoolQueryBuilder self = ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery();
        assertEquals("window=2 caps the Top to 2 docs", 2, self.should().size());
    }

    // ---- leg failure: a wholly-failed leg fails fast, a partially-degraded leg is fused ----

    public void testBuildFusedQuery_whenAnyLegFailed_thenFailsFast() {
        // A wholly-failed leg (all shards down / non-partial error -> Item.isFailure) fails the whole request — fusing
        // over a missing leg would silently change the ranking function. (A merely partial leg degrades instead — see
        // testBuildFusedQuery_whenLegPartiallyFailed_thenFused.) The failing leg's index is reported, cause chained.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f, "2", 0.5f)), failedItem());

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> HybridFusionOrchestrator.buildFusedQuery(
                new SearchSourceBuilder().trackTotalHits(false),
                ms,
                legs,
                minMaxArithmetic(),
                10
            )
        );
        assertTrue("reports the failing leg index", e.getMessage().contains("fused-mode sub-query 1 failed"));
        assertNotNull("chains the leg failure as cause", e.getCause());
        assertTrue(e.getCause().getMessage().contains("leg boom"));
    }

    public void testBuildFusedQuery_whenAllLegsFailed_thenFailsFast() {
        // All legs failing also fails fast — on the first failed leg (index 0).
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(failedItem(), failedItem());

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> HybridFusionOrchestrator.buildFusedQuery(new SearchSourceBuilder(), ms, legs, minMaxArithmetic(), 10)
        );
        assertTrue(e.getMessage().contains("fused-mode sub-query 0 failed"));
        assertNotNull(e.getCause());
    }

    public void testBuildFusedQuery_whenLegPartiallyFailed_thenFusedWithWarning() {
        // A leg that lost some shards under allow_partial_search_results=true is a SUCCESSFUL item with fewer hits — it
        // is fused (degrade, matching OpenSearch's default), not rejected. groupLegHits only hard-fails a wholly-failed
        // item, so a partial-but-successful leg flows through — but emits a Warning header naming it, because per-leg
        // normalization means the degraded leg can shift the fused ranking, not just drop docs.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(partialLegItem(Map.of("1", 0.9f)), legItem(Map.of("2", 0.8f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(
            new SearchSourceBuilder().trackTotalHits(false),
            ms,
            legs,
            minMaxArithmetic(),
            10
        );

        assertTrue(fused instanceof HybridFusionQueryBuilder);
        assertEquals(
            "both legs' docs fused despite one leg's partial shard failure",
            2,
            ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery().should().size()
        );
        assertWarnings(
            "[hybrid] fused-mode sub-query [0] returned partial results (shard failures); fused scores were computed "
                + "over an incomplete result set, so ranking may differ from a complete run"
        );
    }

    public void testBuildFusedQuery_whenNoLegDegraded_thenNoWarning() {
        // Clean legs must not emit a warning (OpenSearchTestCase fails the test on any unasserted warning).
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)), legItem(Map.of("2", 0.8f)));

        HybridFusionOrchestrator.buildFusedQuery(new SearchSourceBuilder().trackTotalHits(false), ms, legs, minMaxArithmetic(), 10);
    }

    // ---- knn/neural leg materialized as Ids in the Tail (no second ANN walk) ----

    public void testBuildFusedQuery_knnLeg_materializedAsIdsInTail() {
        // A leg whose writeable name is a materializable one ("knn") — its Lucene match set IS its returned top-k, so
        // legQueriesForTail rewrites it to an IdsQuery in the Tail rather than re-walking the ANN graph. Using a
        // minimal MatchQuery wrapper reporting name "knn" keeps the test off KNN-internal construction/validation.
        QueryBuilder knnLeg = new MatchQueryBuilder("vec", "q") {
            @Override
            public String getWriteableName() {
                return "knn";
            }
        };
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), knnLeg);
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)), legItem(Map.of("2", 0.8f, "3", 0.7f)));
        // aggregation forces Tail retention so we can inspect leg materialization.
        SearchSourceBuilder source = new SearchSourceBuilder().aggregation(
            org.opensearch.search.aggregations.AggregationBuilders.terms("t").field("f")
        );

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(source, ms, legs, minMaxArithmetic(), 10);

        BoolQueryBuilder self = ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery();
        BoolQueryBuilder tail = (BoolQueryBuilder) self.filter().get(0);
        assertEquals(2, tail.should().size());
        // lexical leg stays a real query; knn/neural leg is materialized as an IdsQuery of its returned hits.
        long idsClauses = tail.should().stream().filter(q -> q instanceof IdsQueryBuilder).count();
        assertEquals("knn leg materialized as IdsQuery", 1, idsClauses);
    }

    // ---- weighted combination + highlight/totals tail triggers (explain/profile do NOT trigger the Tail) ----

    public void testBuildFusedQuery_withPerLegWeights_fusesWithoutError() {
        // Weighted arithmetic mean: exercises weightsParams() building the combination technique from FusionSpec weights.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), new TermQueryBuilder("text", "place"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f, "2", 0.5f)), legItem(Map.of("2", 0.8f, "3", 0.4f)));
        FusionSpec weighted = new FusionSpec(
            FusionSpec.TECHNIQUE_ARITHMETIC_MEAN,
            FusionSpec.NORMALIZATION_MIN_MAX,
            FusionSpec.DEFAULT_RANK_CONSTANT,
            new float[] { 0.7f, 0.3f }
        );

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(
            new SearchSourceBuilder().trackTotalHits(false),
            ms,
            legs,
            weighted,
            10
        );

        assertTrue(fused instanceof HybridFusionQueryBuilder);
        assertEquals(3, ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery().should().size());
    }

    public void testBuildFusedQuery_explainDoesNotTriggerTail() {
        // explain/profile no longer force the Tail: fusion is computed on the coordinator (Top is constant_score(ids)),
        // so the Lucene tree has no fusion breakdown to explain and the Tail would only re-execute legs. With
        // track_total_hits:false there is nothing else needing the Tail, so the query is Top-only.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)));
        SearchSourceBuilder source = new SearchSourceBuilder().trackTotalHits(false).explain(true);

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(source, ms, legs, minMaxArithmetic(), 10);

        assertEquals("explain alone → no Tail", 0, ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery().filter().size());
    }

    public void testBuildFusedQuery_profileDoesNotTriggerTail() {
        // Same as explain: profiling the self-erased query would only time a redundant re-execution of legs that already
        // ran in the fan-out, so profile is not a Tail trigger. With track_total_hits:false the query is Top-only.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)));
        SearchSourceBuilder source = new SearchSourceBuilder().trackTotalHits(false).profile(true);

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(source, ms, legs, minMaxArithmetic(), 10);

        assertEquals("profile alone → no Tail", 0, ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery().filter().size());
    }

    public void testBuildFusedQuery_defaultTrackTotalHits_keepsTailForAccurateCount() {
        // No aggs/explain and track_total_hits left at default → wantsTotalsBeyondWindow keeps the Tail for the count.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f, "2", 0.5f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(new SearchSourceBuilder(), ms, legs, minMaxArithmetic(), 10);

        assertEquals("default totals → Tail retained", 1, ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery().filter().size());
    }

    public void testBuildFusedQuery_nullSource_keepsTail() {
        // A null source (defensive) is treated as "wants totals" → Tail retained.
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"));
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)));

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(null, ms, legs, minMaxArithmetic(), 10);

        assertEquals(1, ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery().filter().size());
    }

    public void testBuildFusedQuery_neuralNamedLeg_materializedAsIds() {
        // "neural" is also a materializable name → its leg is materialized to IdsQuery in the Tail (not re-walked).
        QueryBuilder neuralLeg = new MatchQueryBuilder("vec", "q") {
            @Override
            public String getWriteableName() {
                return "neural";
            }
        };
        List<QueryBuilder> legs = List.of(new MatchQueryBuilder("text", "hello"), neuralLeg);
        MultiSearchResponse ms = multiSearch(legItem(Map.of("1", 0.9f)), legItem(Map.of("2", 0.8f)));
        SearchSourceBuilder source = new SearchSourceBuilder().aggregation(
            org.opensearch.search.aggregations.AggregationBuilders.terms("t").field("f")
        );

        QueryBuilder fused = HybridFusionOrchestrator.buildFusedQuery(source, ms, legs, minMaxArithmetic(), 10);

        BoolQueryBuilder tail = (BoolQueryBuilder) ((HybridFusionQueryBuilder) fused).buildSelfErasedQuery().filter().get(0);
        long idsClauses = tail.should().stream().filter(q -> q instanceof IdsQueryBuilder).count();
        assertEquals("neural leg materialized as IdsQuery", 1, idsClauses);
    }
}
