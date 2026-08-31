/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.common.MinClusterVersionUtil.MINIMAL_SUPPORTED_VERSION_FUSED_MODE_IN_HYBRID_QUERY;
import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.DEFAULT_MAX_FUSION_LEG_SEARCHES;
import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.MAX_FUSION_LEG_SEARCHES;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.Version;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryCoordinatorContext;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.Rewriteable;
import org.opensearch.index.query.TemplateQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.script.Script;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.search.sort.NestedSortBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.pipeline.SearchPipelineMetadata;
import org.opensearch.transport.client.Client;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;

import lombok.SneakyThrows;

/**
 * How much a fused ({@code fusion}) hybrid query is allowed to fan out, and how much it actually does.
 *
 * <p>Two properties are pinned here, and they only make sense together:
 *
 * <ul>
 *   <li><b>Each fused hybrid fans out once.</b> A nested fused hybrid is reachable twice on the coordinator — as a leg of
 *       the enclosing query, and again in the enclosing query's Tail, which keeps the original leg builders. Executing it
 *       both times made cost {@code 2^(depth+1) - 2} leg sub-searches for a body that grows linearly, so a handful of
 *       bytes bought an exponential fan-out (and past depth ~8, core's {@code MAX_REWRITE_ROUNDS} turned it into a 500).
 *       The Tail now rewrites under a match-set marker, where a fused hybrid contributes {@code bool{should: legs}}
 *       instead of firing them: cost is linear in the hybrids the body spells out, which is what makes a static count of
 *       them meaningful.</li>
 *   <li><b>That count is capped</b> by {@code plugins.neural_search.hybrid.fusion.max_leg_searches}, per request, over
 *       every fused hybrid in the body — nested or side by side.</li>
 * </ul>
 *
 * <p>Fan-out is measured where it is paid: the number of {@code multiSearch} calls this coordinator makes while the query
 * is rewritten to completion, and the number of leg requests in each.
 */
public class HybridQueryFusedFanOutTests extends OpenSearchQueryTestCase {

    private static final String TEXT_FIELD_NAME = "field";
    private static final String INDEX_NAME = "test-index";
    /** A nested path, for the two shapes that need one: a sort's nested filter, and a [nested] query. */
    private static final String NESTED_PATH = "nested";

    /** The cluster service the current {@link #initClusterUtil} call installed, so a test can swap only the resolver. */
    private ClusterService clusterService;

    /** What a coordinator paid to rewrite one request: one entry in {@code legCountPerMultiSearch} per fan-out. */
    private record FanOut(List<Integer> legCountPerMultiSearch, QueryBuilder finalQuery) {
        int multiSearches() {
            return legCountPerMultiSearch.size();
        }

    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        initClusterUtil(null);
    }

    // ------------------------------------------------ fan-out shape ------------------------------------------------

    /**
     * The regression this class exists for: nesting must not multiply fan-out. Each depth is a chain of fused hybrids,
     * every level holding the level below plus a term leg — a body that grows by one clause per level. Before the Tail was
     * rewritten for its match set, this coordinator issued one MultiSearch per level (and the whole tree paid
     * {@code 2^(depth+1) - 2} leg searches); now the root fans out once and the nested levels are executed exactly once
     * each, as their parent's leg sub-search.
     */
    @SneakyThrows
    public void testNestedFusedHybrids_whenRewrittenToCompletion_thenCoordinatorFansOutOnce() {
        for (int depth = 1; depth <= 6; depth++) {
            FanOut fanOut = drive(request(nestedChain(depth)));
            assertEquals("depth " + depth + " must cost one MultiSearch on this coordinator", 1, fanOut.multiSearches());
            assertEquals("and it carries this hybrid's own legs, nothing more", List.of(2), fanOut.legCountPerMultiSearch());
        }
    }

    /**
     * The same, for a fused hybrid the enclosing query cannot see through as a direct leg: it sits inside a {@code bool}
     * inside the leg. The marker is carried by the rewrite context rather than applied to the leg list, so container depth
     * makes no difference — which is the reason it is a context and not a substitution at Tail-build time.
     */
    @SneakyThrows
    public void testNestedFusedHybridInsideAContainerLeg_thenCoordinatorStillFansOutOnce() {
        HybridQueryBuilder inner = fused(new MatchQueryBuilder(TEXT_FIELD_NAME, "hello"), QueryBuilders.termQuery(TEXT_FIELD_NAME, "kw"));
        QueryBuilder containerLeg = QueryBuilders.boolQuery().must(inner).filter(QueryBuilders.termQuery(TEXT_FIELD_NAME, "gate"));
        HybridQueryBuilder outer = fused(containerLeg, QueryBuilders.termQuery(TEXT_FIELD_NAME, "kw"));

        FanOut fanOut = drive(request(outer));

        assertEquals(1, fanOut.multiSearches());
        assertEquals(List.of(2), fanOut.legCountPerMultiSearch());
    }

    /**
     * What the Tail ends up matching. The nested fused hybrid is replaced by a {@code bool} of its own legs, which is the
     * set it matches: a fused hybrid compiles to {@code bool{should: Top, filter: Tail}} with no minimum_should_match, so
     * its matches are its Tail's — the union of its legs. Scores are lost and must be: this only ever appears inside the
     * enclosing Tail's non-scoring {@code filter}.
     */
    @SneakyThrows
    public void testTail_whenLegIsAFusedHybrid_thenItBecomesTheUnionOfThatLegsSubQueries() {
        MatchQueryBuilder innerMatch = new MatchQueryBuilder(TEXT_FIELD_NAME, "hello");
        QueryBuilder innerTerm = QueryBuilders.termQuery(TEXT_FIELD_NAME, "inner");
        HybridQueryBuilder inner = fused(innerMatch, innerTerm);
        QueryBuilder outerTerm = QueryBuilders.termQuery(TEXT_FIELD_NAME, "outer");

        FanOut fanOut = drive(request(fused(inner, outerTerm)));

        assertTrue("the request self-erases into the fused query", fanOut.finalQuery() instanceof HybridFusionQueryBuilder);
        BoolQueryBuilder selfErased = ((HybridFusionQueryBuilder) fanOut.finalQuery()).buildSelfErasedQuery();
        assertEquals("the Tail is the single filter clause", 1, selfErased.filter().size());
        BoolQueryBuilder tail = (BoolQueryBuilder) selfErased.filter().get(0);
        assertEquals("one Tail clause per leg", 2, tail.should().size());
        assertEquals("the non-hybrid leg is kept as itself", outerTerm, tail.should().get(1));
        BoolQueryBuilder substituted = (BoolQueryBuilder) tail.should().get(0);
        assertEquals("the nested hybrid contributes its legs", List.of(innerMatch, innerTerm), substituted.should());
        assertNull("as a plain union — no minimum_should_match to satisfy", substituted.minimumShouldMatch());
        assertEquals("and no fused hybrid is left to execute", 0, HybridQueryBuilder.countFusedLegSearches(fanOut.finalQuery()));
    }

    /**
     * The mechanism on its own: under a match-set context a fused hybrid resolves to its legs' union without registering
     * any async action. This is the assertion that fails if the early return in {@code doRewriteFused} is dropped, whether
     * or not any Tail is built.
     */
    @SneakyThrows
    public void testFusedHybrid_whenRewrittenForItsMatchSetOnly_thenNoFanOutIsRegistered() {
        MatchQueryBuilder match = new MatchQueryBuilder(TEXT_FIELD_NAME, "hello");
        QueryBuilder term = QueryBuilders.termQuery(TEXT_FIELD_NAME, "kw");
        HybridQueryBuilder hybrid = fused(match, term);
        List<BiConsumer<Client, ActionListener<?>>> registered = new ArrayList<>();
        QueryCoordinatorContext coordinatorContext = coordinatorContext(request(hybrid), registered);

        QueryBuilder rewritten = hybrid.rewrite(MatchSetRewriteContext.wrap(coordinatorContext));

        assertEquals(new BoolQueryBuilder().should(match).should(term), rewritten);
        assertTrue("a match set costs no sub-search", registered.isEmpty());
    }

    /** The marker is not applied where no fan-out can happen, and never stacks on itself. */
    public void testMatchSetContext_whenThereIsNothingToMark_thenTheContextIsUnchanged() {
        QueryRewriteContext plainContext = mock(QueryRewriteContext.class);
        assertSame("a shard-side rewrite has no coordinator context to wrap", plainContext, MatchSetRewriteContext.wrap(plainContext));
        assertFalse(MatchSetRewriteContext.isMatchSetOnly(plainContext));

        QueryRewriteContext marked = MatchSetRewriteContext.wrap(coordinatorContext(request(fused()), new ArrayList<>()));
        assertTrue(MatchSetRewriteContext.isMatchSetOnly(marked));
        assertSame("a Tail inside a Tail is already marked", marked, MatchSetRewriteContext.wrap(marked));
    }

    // ------------------------------------------------ the budget ------------------------------------------------

    /** The counted unit: declared legs, summed over every fused hybrid in the request, and nothing else. */
    public void testCountFusedLegSearches_countsDeclaredLegsOfEveryFusedHybrid() {
        assertEquals(0, HybridQueryBuilder.countFusedLegSearches(null));
        assertEquals("a non-hybrid query declares none", 0, HybridQueryBuilder.countFusedLegSearches(QueryBuilders.matchAllQuery()));

        HybridQueryBuilder classic = new HybridQueryBuilder();
        classic.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, "a")).add(QueryBuilders.termQuery(TEXT_FIELD_NAME, "b"));
        assertEquals("classic hybrid runs no leg sub-search at all", 0, HybridQueryBuilder.countFusedLegSearches(classic));

        assertEquals("one level, one leg per sub-query", 2, HybridQueryBuilder.countFusedLegSearches(nestedChain(1)));
        assertEquals("a chain of three pays for all three", 6, HybridQueryBuilder.countFusedLegSearches(nestedChain(3)));

        QueryBuilder siblings = QueryBuilders.boolQuery().must(nestedChain(1)).should(nestedChain(2));
        assertEquals(
            "siblings in one request are summed — the request is what fans out",
            6,
            HybridQueryBuilder.countFusedLegSearches(siblings)
        );

        QueryBuilder insideContainer = QueryBuilders.functionScoreQuery(QueryBuilders.constantScoreQuery(nestedChain(1)));
        assertEquals(
            "a hybrid inside containers that expose their children is counted",
            2,
            HybridQueryBuilder.countFusedLegSearches(insideContainer)
        );
    }

    /** The ceiling is derived from the per-query leg limit, not chosen independently, and cannot be set below it. */
    public void testLegSearchBudgetSetting_isTiedToTheLegLimit() {
        assertEquals(
            HybridQueryBuilder.MAX_NUMBER_OF_SUB_QUERIES * HybridQueryBuilder.MAX_NUMBER_OF_SUB_QUERIES,
            DEFAULT_MAX_FUSION_LEG_SEARCHES
        );
        assertEquals(Integer.valueOf(DEFAULT_MAX_FUSION_LEG_SEARCHES), MAX_FUSION_LEG_SEARCHES.getDefault(Settings.EMPTY));

        Settings belowFloor = Settings.builder()
            .put(MAX_FUSION_LEG_SEARCHES.getKey(), HybridQueryBuilder.MAX_NUMBER_OF_SUB_QUERIES - 1)
            .build();
        IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> MAX_FUSION_LEG_SEARCHES.get(belowFloor));
        assertThat(
            "a ceiling under the legs a single hybrid may declare would reject a plain fused query",
            error.getMessage(),
            containsString("must be >= " + HybridQueryBuilder.MAX_NUMBER_OF_SUB_QUERIES)
        );
    }

    /**
     * Every shape the default budget admits is accepted, and settles inside core's rewrite-round budget. The widths span the
     * per-query leg limit, so the depths span the shape the default is chosen to admit —
     * {@link HybridQueryBuilder#MAX_NUMBER_OF_SUB_QUERIES} levels of full-width nesting — down to a 25-level single-leg
     * chain.
     *
     * <p>Depth is what makes the round budget interesting. Rewrite descends one level per round, and core allows
     * {@link Rewriteable#MAX_REWRITE_ROUNDS} for the whole request, so a nested fused hybrid substituted level by level
     * would spend a round per level and the deepest chain here would fail with "too many rewrite rounds" — an internal
     * error on a body inside its own declared budget. {@link #drive} runs core's loop, so that is a real failure here and
     * not a soft assertion.
     */
    @SneakyThrows
    public void testEveryShapeTheDefaultBudgetAdmits_isAcceptedAndSettlesInCoresRewriteRounds() {
        for (int width = 1; width <= HybridQueryBuilder.MAX_NUMBER_OF_SUB_QUERIES; width++) {
            int depth = DEFAULT_MAX_FUSION_LEG_SEARCHES / width;
            String shape = width + " legs x " + depth + " levels";
            HybridQueryBuilder query = nestedChain(depth, width);
            assertEquals(shape, depth * width, HybridQueryBuilder.countFusedLegSearches(query));
            assertTrue(shape + " must be within the budget", depth * width <= DEFAULT_MAX_FUSION_LEG_SEARCHES);

            FanOut fanOut = drive(request(query));

            assertEquals(shape + ": this coordinator fans out once", 1, fanOut.multiSearches());
            assertEquals(shape + ": carrying this level's legs only", List.of(width), fanOut.legCountPerMultiSearch());
        }
    }

    /** One over the budget is a 400 before anything is fanned out. */
    @SneakyThrows
    public void testRequestOverTheBudget_isRejectedBeforeAnyFanOut() {
        int overBudget = DEFAULT_MAX_FUSION_LEG_SEARCHES / 2 + 1;
        QueryBuilder query = nestedChain(overBudget);
        List<BiConsumer<Client, ActionListener<?>>> registered = new ArrayList<>();
        QueryCoordinatorContext coordinatorContext = coordinatorContext(request(query), registered);

        IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> query.rewrite(coordinatorContext));

        assertThat(error.getMessage(), containsString("declares " + (2 * overBudget) + " leg sub-searches"));
        assertThat(error.getMessage(), containsString(MAX_FUSION_LEG_SEARCHES.getKey()));
        assertThat(error.getMessage(), containsString("(" + DEFAULT_MAX_FUSION_LEG_SEARCHES + ")"));
        assertTrue("rejected before the fan-out it is meant to prevent", registered.isEmpty());
    }

    /** Sibling hybrids are one budget, and the budget is the live cluster setting. */
    @SneakyThrows
    public void testBudget_readsTheLiveClusterSetting() {
        QueryBuilder siblings = QueryBuilders.boolQuery().must(nestedChain(1)).should(nestedChain(2));
        assertEquals(6, HybridQueryBuilder.countFusedLegSearches(siblings));

        initClusterUtil(Settings.builder().put(MAX_FUSION_LEG_SEARCHES.getKey(), 5).build());
        IllegalArgumentException error = expectThrows(
            IllegalArgumentException.class,
            () -> siblings.rewrite(coordinatorContext(request(siblings), new ArrayList<>()))
        );
        assertThat(error.getMessage(), containsString("declares 6 leg sub-searches"));
        assertThat("the message quotes the value in force, not the default", error.getMessage(), containsString("(5)"));

        initClusterUtil(Settings.builder().put(MAX_FUSION_LEG_SEARCHES.getKey(), 6).build());
        FanOut fanOut = drive(request(siblings));
        assertEquals("raising the setting admits the same body", 2, fanOut.multiSearches());
        assertEquals("both siblings fan out, each once", List.of(2, 2), fanOut.legCountPerMultiSearch());
    }

    // ---------------------------------------------- position guard ----------------------------------------------

    /**
     * A body's {@code query} is not the only thing core rewrites against the coordinator context: {@code post_filter},
     * aggregations, sorts, {@code rescore} and {@code highlight} are rewritten alongside it, and fused mode would fan out
     * from any of them — while the budget above counts the request's {@code query} alone, so those placements were admitted
     * at a counted budget of zero. Worse, a leg sub-search inherits the request's {@code post_filter}, so a fused hybrid
     * there is copied onto every leg it creates and re-enters the rewrite with the same body. Both are refused now, before
     * a single leg is dispatched.
     */
    @SneakyThrows
    public void testFusedHybridOutsideTheRequestQuery_isRefusedBeforeAnyFanOut() {
        Map<String, Function<HybridQueryBuilder, SearchSourceBuilder>> positions = new LinkedHashMap<>();
        positions.put("post_filter", hybrid -> requestQuery().postFilter(hybrid));
        positions.put("aggregation filter", hybrid -> requestQuery().aggregation(AggregationBuilders.filter("probe", hybrid)));
        positions.put("rescore", hybrid -> requestQuery().addRescorer(new QueryRescorerBuilder(hybrid)));
        positions.put(
            "highlight query",
            hybrid -> requestQuery().highlighter(
                new HighlightBuilder().field(new HighlightBuilder.Field(TEXT_FIELD_NAME).highlightQuery(hybrid))
            )
        );
        positions.put(
            "sort",
            hybrid -> requestQuery().sort(
                SortBuilders.fieldSort(TEXT_FIELD_NAME).setNestedSort(new NestedSortBuilder(NESTED_PATH).setFilter(hybrid))
            )
        );

        for (Map.Entry<String, Function<HybridQueryBuilder, SearchSourceBuilder>> position : positions.entrySet()) {
            SearchSourceBuilder source = position.getValue().apply(nestedChain(1));
            SearchRequest searchRequest = new SearchRequest(INDEX_NAME).source(source);
            List<BiConsumer<Client, ActionListener<?>>> registered = new ArrayList<>();
            QueryCoordinatorContext coordinatorContext = coordinatorContext(searchRequest, registered);

            IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> source.rewrite(coordinatorContext));

            assertThat(position.getKey(), error.getMessage(), containsString("must be part of the request's [query]"));
            assertThat(position.getKey() + ": the message names the way out", error.getMessage(), containsString("[bool]"));
            assertTrue(position.getKey() + ": refused before the fan-out it would have created", registered.isEmpty());
        }
    }

    /**
     * The one shape where "nothing was dispatched" does not follow from the refusal alone: a legitimate fused hybrid in the
     * {@code query} and another in {@code post_filter}. Core rewrites the {@code query} first
     * ({@code SearchSourceBuilder#rewrite}), so the legitimate one registers its fan-out before the refusal fires — which is
     * safe only because registering is not dispatching: {@link org.opensearch.index.query.Rewriteable#rewriteAndFetch} runs a
     * round's registered actions after that round's {@code rewrite} returns, so a throw inside the round discards them. That
     * round is what this pins. A refusal deferred to a later round would let the first round complete and dispatch.
     */
    @SneakyThrows
    public void testFusedHybridInTheQueryAndInPostFilter_isRefusedInTheSameRoundThatRegistered() {
        SearchSourceBuilder source = new SearchSourceBuilder().query(nestedChain(1)).postFilter(nestedChain(1));
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME).source(source);
        List<BiConsumer<Client, ActionListener<?>>> registered = new ArrayList<>();
        QueryCoordinatorContext coordinatorContext = coordinatorContext(searchRequest, registered);

        IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> source.rewrite(coordinatorContext));

        assertThat(error.getMessage(), containsString("must be part of the request's [query]"));
        assertEquals("the query's own fan-out was registered in the round that then threw", 1, registered.size());
    }

    /**
     * A rescore is confined to the fused window, which is the request's ranking only when the hybrid is the request's own
     * {@code query}. Composed into another query the window is a different set from what the request ranks, so the
     * confinement would silently misapply the rescore — inert over a {@code must_not}'s survivors, partial over a
     * {@code should}'s siblings, the intersection of two windows for two fused hybrids. Each such shape is refused, and
     * before any fan-out: the guard runs ahead of the leg registration, so nothing is dispatched. A top-level fused hybrid
     * with a rescore — the supported case — is admitted and fans out as usual (asserted separately below).
     */
    @SneakyThrows
    public void testFusedHybridComposedWithOtherClausesAndARescore_isRefusedBeforeAnyFanOut() {
        QueryRescorerBuilder rescorer = new QueryRescorerBuilder(new MatchQueryBuilder(TEXT_FIELD_NAME, "hello"));
        Map<String, QueryBuilder> composedPositions = new LinkedHashMap<>();
        composedPositions.put("must_not", QueryBuilders.boolQuery().mustNot(nestedChain(1)));
        composedPositions.put(
            "should sibling",
            QueryBuilders.boolQuery().should(nestedChain(1)).should(new MatchQueryBuilder(TEXT_FIELD_NAME, "phone"))
        );
        composedPositions.put("two fused hybrids", QueryBuilders.boolQuery().should(nestedChain(1)).should(nestedChain(1)));

        for (Map.Entry<String, QueryBuilder> position : composedPositions.entrySet()) {
            SearchSourceBuilder source = new SearchSourceBuilder().query(position.getValue()).addRescorer(rescorer);
            List<BiConsumer<Client, ActionListener<?>>> registered = new ArrayList<>();
            QueryCoordinatorContext coordinatorContext = coordinatorContext(new SearchRequest(INDEX_NAME).source(source), registered);

            IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> source.rewrite(coordinatorContext));

            assertThat(
                position.getKey(),
                error.getMessage(),
                containsString("only supported when the [hybrid] is the request's own [query]")
            );
            assertTrue(position.getKey() + ": refused before the fan-out it would have created", registered.isEmpty());
        }
    }

    /**
     * The supported shape the refusal above carves out: the fused hybrid is the request's own {@code query}, with a
     * rescore over it. It is admitted and fans out once — the confinement's window is exactly the request's ranking here, so
     * there is nothing to refuse.
     */
    @SneakyThrows
    public void testTopLevelFusedHybridWithARescore_isAdmittedAndFansOut() {
        SearchSourceBuilder source = new SearchSourceBuilder().query(nestedChain(1))
            .addRescorer(new QueryRescorerBuilder(new MatchQueryBuilder(TEXT_FIELD_NAME, "hello")));
        List<Integer> legCounts = driveWholeSource(new SearchRequest(INDEX_NAME).source(source));
        assertEquals("the top-level fused hybrid with a rescore fans out once", List.of(2), legCounts);
    }

    /**
     * The same refusal when there is no {@code query} for the hybrid to be part of. A body may carry only a
     * {@code post_filter}, or only aggregations, and core rewrites those against this context all the same — so what an
     * absent query means has to be decided rather than assumed. It means refused: admitting a hybrid because nothing
     * contradicts it is how the unbounded case gets back in, and here there is no query to bound it with at all.
     */
    @SneakyThrows
    public void testFusedHybridInABodyWithNoRequestQuery_isRefusedBeforeAnyFanOut() {
        Map<String, Function<HybridQueryBuilder, SearchSourceBuilder>> bodies = new LinkedHashMap<>();
        bodies.put("post_filter only", hybrid -> new SearchSourceBuilder().postFilter(hybrid));
        bodies.put("aggregations only", hybrid -> new SearchSourceBuilder().aggregation(AggregationBuilders.filter("probe", hybrid)));

        for (Map.Entry<String, Function<HybridQueryBuilder, SearchSourceBuilder>> body : bodies.entrySet()) {
            SearchSourceBuilder source = body.getValue().apply(nestedChain(1));
            SearchRequest searchRequest = new SearchRequest(INDEX_NAME).source(source);
            List<BiConsumer<Client, ActionListener<?>>> registered = new ArrayList<>();
            QueryCoordinatorContext coordinatorContext = coordinatorContext(searchRequest, registered);

            IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> source.rewrite(coordinatorContext));

            assertThat(body.getKey(), error.getMessage(), containsString("must be part of the request's [query]"));
            assertTrue(body.getKey() + ": refused before the fan-out it would have created", registered.isEmpty());
        }
    }

    /**
     * And when the request carries no source at all — a {@code SearchRequest} may hold none, which is why every read of it in
     * core is null-checked. Same answer for the same reason: fused mode is refused rather than fanning out against a body no
     * guard can then count. Spelled with the no-argument constructor deliberately, since {@code new SearchRequest(index)}
     * installs an empty source and would exercise the absent-query case above instead.
     */
    @SneakyThrows
    public void testFusedHybrid_whenTheRequestHasNoSource_isRefusedBeforeAnyFanOut() {
        QueryBuilder query = nestedChain(1);
        SearchRequest sourceless = new SearchRequest();
        assertNull("the shape under test is a request with no body", sourceless.source());
        List<BiConsumer<Client, ActionListener<?>>> registered = new ArrayList<>();
        QueryCoordinatorContext coordinatorContext = coordinatorContext(sourceless, registered);

        IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> query.rewrite(coordinatorContext));

        assertThat(error.getMessage(), containsString("must be part of the request's [query]"));
        assertTrue("refused before any fan-out", registered.isEmpty());
    }

    /** Inside the request's query, at any depth a query builder exposes, is admitted — the shape users actually write. */
    @SneakyThrows
    public void testFusedHybridNestedInsideTheRequestQuery_isAdmitted() {
        BoolQueryBuilder inBool = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery(TEXT_FIELD_NAME, "kw")).should(nestedChain(1));

        FanOut fanOut = drive(request(inBool));

        assertEquals("a fused hybrid nested in a bool still fans out", 1, fanOut.multiSearches());
        assertEquals(List.of(2), fanOut.legCountPerMultiSearch());
    }

    /**
     * "At any depth a query builder exposes" would be the whole qualification if the walk trusted the builders, and core's
     * do not agree: {@code boosting} and {@code script_score} hand their inner query to {@code accept} without recursing
     * into it, so a walk that only followed {@code visit} would see one level under either and nothing below. Both of this
     * guard's walks descend on their own instead, so those levels are counted and admitted like any other — and they have
     * to be, because the same walk is the budget counter: a level it cannot see is a level whose legs are free.
     * {@code nested}, which does recurse, is the contrast that the descent does not double-count what a builder already
     * walked.
     */
    @SneakyThrows
    public void testFusedHybridUnderAQueryThatDoesNotRecurse_isCountedAndAdmittedAtEveryDepth() {
        Map<String, Function<QueryBuilder, QueryBuilder>> containers = new LinkedHashMap<>();
        containers.put("boosting", inner -> QueryBuilders.boostingQuery(inner, QueryBuilders.matchAllQuery()).negativeBoost(0.5f));
        containers.put("script_score", inner -> QueryBuilders.scriptScoreQuery(inner, new Script("_score")));

        for (Map.Entry<String, Function<QueryBuilder, QueryBuilder>> container : containers.entrySet()) {
            assertEquals(
                container.getKey() + " exposes its immediate inner query",
                2,
                HybridQueryBuilder.countFusedLegSearches(container.getValue().apply(nestedChain(1)))
            );
            assertEquals(
                container.getKey() + " exposes nothing under that, and the walk descends for it",
                2,
                HybridQueryBuilder.countFusedLegSearches(container.getValue().apply(QueryBuilders.boolQuery().should(nestedChain(1))))
            );
            assertEquals(
                container.getKey() + ": and keeps descending, however deep the body nests below it",
                6,
                HybridQueryBuilder.countFusedLegSearches(container.getValue().apply(QueryBuilders.boolQuery().should(nestedChain(3))))
            );

            for (int depth = 1; depth <= 2; depth++) {
                QueryBuilder body = depth == 1
                    ? container.getValue().apply(nestedChain(1))
                    : container.getValue().apply(QueryBuilders.boolQuery().should(nestedChain(1)));

                FanOut fanOut = drive(request(body));

                assertEquals(container.getKey() + " at depth " + depth + ": admitted, and fans out once", 1, fanOut.multiSearches());
                assertEquals(container.getKey() + " at depth " + depth, List.of(2), fanOut.legCountPerMultiSearch());
                assertEquals(
                    container.getKey() + " at depth " + depth + ": and survives the rewrite as the enclosing query",
                    container.getKey(),
                    fanOut.finalQuery().getName()
                );
            }
        }

        assertEquals(
            "boosting exposes its negative clause too, so a fused hybrid written there is counted as well",
            4,
            HybridQueryBuilder.countFusedLegSearches(QueryBuilders.boostingQuery(nestedChain(1), nestedChain(1)).negativeBoost(0.5f))
        );

        assertEquals(
            "nested recurses on its own, and the descent does not count what it already walked twice",
            2,
            HybridQueryBuilder.countFusedLegSearches(
                QueryBuilders.nestedQuery(NESTED_PATH, QueryBuilders.boolQuery().should(nestedChain(1)), ScoreMode.Avg)
            )
        );
    }

    /**
     * The ceiling counts the request, so it has to see the whole request. A builder that exposes a child without recursing
     * into it truncates a {@code visit} walk one level below itself, and the budget reads that as an absence of legs rather
     * than as an unknown — while every leg sub-search re-roots the same walk at its own leg, where the next level is the
     * root and trivially reachable. So a chain of fused hybrids each written one level under a {@code boosting} was
     * admitted at a count of {@code 2 x width} whatever its depth and then served {@code width} leg sub-searches per level:
     * the ceiling held per nesting level instead of per request, which is the evasion this guard exists to prevent.
     * Pinned against the same chain written without the wrappers, which must count and be refused identically.
     */
    @SneakyThrows
    public void testFusedHybridChainUnderQueriesThatDoNotRecurse_isCappedLikeThePlainChain() {
        int width = HybridQueryBuilder.MAX_NUMBER_OF_SUB_QUERIES;
        for (int depth = 1; depth <= 6; depth++) {
            int declared = depth * width;
            assertEquals(
                "the plain chain declares one level's legs per level",
                declared,
                HybridQueryBuilder.countFusedLegSearches(nestedChain(depth, width))
            );
            assertEquals(
                "and wrapping every level in a query that does not recurse cannot change that number",
                declared,
                HybridQueryBuilder.countFusedLegSearches(boostingChain(depth, width))
            );
        }

        HybridQueryBuilder overBudget = boostingChain(DEFAULT_MAX_FUSION_LEG_SEARCHES / width + 1, width);
        List<BiConsumer<Client, ActionListener<?>>> registered = new ArrayList<>();
        IllegalArgumentException error = expectThrows(
            IllegalArgumentException.class,
            () -> overBudget.rewrite(coordinatorContext(request(overBudget), registered))
        );
        assertThat(error.getMessage(), containsString("declares " + (DEFAULT_MAX_FUSION_LEG_SEARCHES + width) + " leg sub-searches"));
        assertTrue("refused before the first level was dispatched", registered.isEmpty());

        FanOut atTheCeiling = drive(request(boostingChain(DEFAULT_MAX_FUSION_LEG_SEARCHES / width, width)));
        assertEquals("the deepest chain the ceiling admits still fans out once from this coordinator", 1, atTheCeiling.multiSearches());
        assertEquals(List.of(width), atTheCeiling.legCountPerMultiSearch());
    }

    /**
     * The same evasion in the shape that made it worth a guard rather than a note: every leg of every level wrapped, so the
     * body's fan-out grows as {@code width + width² + … + width^depth} while a truncated count stayed at
     * {@code width + width²} — four levels of four legs read 20 and served 340. Counted in full now, so the ceiling refuses
     * it at the depth it stops being affordable.
     */
    @SneakyThrows
    public void testBranchingFusedHybridsUnderQueriesThatDoNotRecurse_areCountedInFull() {
        assertEquals(4, HybridQueryBuilder.countFusedLegSearches(branchingChain(1, 4)));
        assertEquals(20, HybridQueryBuilder.countFusedLegSearches(branchingChain(2, 4)));
        assertEquals("the depth that read 20 and served 84", 84, HybridQueryBuilder.countFusedLegSearches(branchingChain(3, 4)));
        assertEquals("and the one that read 20 and served 340", 340, HybridQueryBuilder.countFusedLegSearches(branchingChain(4, 4)));

        HybridQueryBuilder branching = branchingChain(3, 4);
        List<BiConsumer<Client, ActionListener<?>>> registered = new ArrayList<>();
        IllegalArgumentException error = expectThrows(
            IllegalArgumentException.class,
            () -> branching.rewrite(coordinatorContext(request(branching), registered))
        );
        assertThat(error.getMessage(), containsString("declares 84 leg sub-searches"));
        assertTrue("refused before any of it was dispatched", registered.isEmpty());
    }

    /**
     * A {@code template} query is the other builder that carries a whole query without exposing it: it holds an unparsed
     * map, and unlike {@code wrapper} it returns the builder it parses <i>without</i> rewriting it, so a fused hybrid
     * inside one reaches this guard a rewrite round later. The request's {@code source()} still holds the {@code template}
     * then — core installs the rewritten source only once the whole loop has finished — so the hybrid is unreachable, and
     * uncounted for the same reason, which is what makes refusing it the fail-closed answer rather than a lost shape.
     */
    @SneakyThrows
    public void testFusedHybridInsideATemplateQuery_isRefusedAndWasNeverCounted() {
        TemplateQueryBuilder template = new TemplateQueryBuilder(
            Map.of("hybrid", Map.of("queries", List.of(Map.of("match_all", Map.of())), "fusion", fusionConfig()))
        );
        assertEquals(
            "a template carries its inner query as a map, which no walk can count",
            0,
            HybridQueryBuilder.countFusedLegSearches(template)
        );

        HybridQueryBuilder parsedOutOfTheTemplate = nestedChain(1);
        List<BiConsumer<Client, ActionListener<?>>> registered = new ArrayList<>();
        QueryCoordinatorContext coordinatorContext = coordinatorContext(request(template), registered);

        IllegalArgumentException error = expectThrows(
            IllegalArgumentException.class,
            () -> parsedOutOfTheTemplate.rewrite(coordinatorContext)
        );

        assertThat(error.getMessage(), containsString("must be part of the request's [query]"));
        assertThat("the message names the carriers that can hide it", error.getMessage(), containsString("[template]"));
        assertTrue("refused before any fan-out", registered.isEmpty());
    }

    /**
     * A fused hybrid that is a leg of another one stays admissible: its leg sub-search carries it as that request's own
     * query, which is where the guard looks. Pinned separately because a guard reading only the original request would
     * refuse every nested fused hybrid instead — the shape this class's first test exists for.
     */
    @SneakyThrows
    public void testLegSubSearch_carriesItsLegAsItsOwnQuery_soANestedFusedLegIsStillAdmitted() {
        HybridQueryBuilder outer = nestedChain(2);
        List<BiConsumer<Client, ActionListener<?>>> registered = new ArrayList<>();
        outer.rewrite(coordinatorContext(request(outer), registered));
        assertEquals("the outer hybrid fans out once", 1, registered.size());

        List<MultiSearchRequest> multiSearches = new ArrayList<>();
        Client client = mock(Client.class);
        doAnswer(invocation -> multiSearches.add(invocation.getArgument(0))).when(client).multiSearch(any(), any());
        registered.getFirst().accept(client, ActionListener.wrap(response -> {}, e -> fail("leg fan-out failed: " + e.getMessage())));

        SearchRequest legRequest = multiSearches.getFirst().requests().getFirst();
        assertTrue("the inner fused hybrid is its leg request's query", legRequest.source().query() instanceof HybridQueryBuilder);

        List<BiConsumer<Client, ActionListener<?>>> legRegistered = new ArrayList<>();
        legRequest.source().query().rewrite(coordinatorContext(legRequest, legRegistered));

        assertEquals("and it fans out its own legs from there", 1, legRegistered.size());
    }

    // ---------------------------------------------- version guard ----------------------------------------------

    /**
     * Fused mode on a cluster that cannot run round 2 everywhere is refused before anything is fanned out. Round 2 is a
     * {@code hybrid_fusion} query, a type a node predating fused mode cannot resolve at all — it fails deserializing the
     * shard request, and a shard failure under the default {@code allow_partial_search_results} is a 200 with those shards'
     * documents silently missing. So the cost of being wrong here is a wrong answer, not an error.
     */
    @SneakyThrows
    public void testFusedMode_whenAnyNodeIsBelowTheMinimumVersion_isRejectedBeforeAnyFanOut() {
        initClusterUtil(null, Version.V_3_7_0);
        QueryBuilder query = nestedChain(1);
        List<BiConsumer<Client, ActionListener<?>>> registered = new ArrayList<>();
        QueryCoordinatorContext coordinatorContext = coordinatorContext(request(query), registered);

        IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> query.rewrite(coordinatorContext));

        assertThat(
            error.getMessage(),
            containsString("on version [" + MINIMAL_SUPPORTED_VERSION_FUSED_MODE_IN_HYBRID_QUERY + "] or later")
        );
        assertThat("the message quotes what the cluster actually is", error.getMessage(), containsString("is [" + Version.V_3_7_0 + "]"));
        assertTrue("refused before the fan-out whose results no node could be asked to fuse", registered.isEmpty());
    }

    /**
     * The boundary is inclusive: a cluster exactly at the minimum runs fused mode. Spelled as the literal rather than as
     * {@code MINIMAL_SUPPORTED_VERSION_FUSED_MODE_IN_HYBRID_QUERY} so it pins the version fused mode ships in — against
     * the constant this assertion holds by construction and a bump would go unnoticed.
     */
    @SneakyThrows
    public void testFusedMode_whenEveryNodeIsExactlyAtTheMinimumVersion_thenItFansOut() {
        initClusterUtil(null, Version.V_3_8_0);

        FanOut fanOut = drive(request(nestedChain(1)));

        assertEquals(1, fanOut.multiSearches());
        assertEquals(List.of(2), fanOut.legCountPerMultiSearch());
    }

    /**
     * The refusal sits above the {@code SearchRequest} cast on purpose. {@code _explain} and {@code _validate/query} rewrite
     * on the coordinator with an {@code ExplainRequest}/{@code ValidateQueryRequest} and are then dispatched <i>still
     * fused</i> to the node holding the document — where the {@code fusion} field is gated off the wire, so an old node
     * answers for a classic hybrid instead. Refusing below the cast would leave that shape silently wrong.
     */
    @SneakyThrows
    public void testFusedMode_whenTheRequestIsNotASearch_thenItIsStillRefusedOnALaggingCluster() {
        initClusterUtil(null, Version.V_3_7_0);
        QueryBuilder query = nestedChain(1);
        QueryCoordinatorContext coordinatorContext = mock(QueryCoordinatorContext.class);
        when(coordinatorContext.convertToCoordinatorContext()).thenReturn(coordinatorContext);
        when(coordinatorContext.getSearchRequest()).thenReturn(mock(org.opensearch.action.IndicesRequest.class));

        IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> query.rewrite(coordinatorContext));

        assertThat(error.getMessage(), containsString("requires all nodes in the cluster to be on version"));
    }

    // ------------------------------------------- request-shape refusals -------------------------------------------

    /**
     * Where the request-shape refusal sits. {@code CandidateScope.from} runs ahead of everything that resolves index
     * metadata — the fusion-config lookup reads the targeted indices' default pipelines, the window ceiling reads their
     * {@code max_result_window} — because a literal {@code cluster:index} expression dies in that resolution with
     * {@code no such index}, a message that says nothing about fused mode. This models resolution failing exactly there and
     * asserts fused mode's own explanation is what the user gets.
     */
    @SneakyThrows
    public void testCrossClusterRequest_isRefusedBeforeIndexMetadataIsResolved() {
        initClusterUtilWhereIndexResolutionFails();
        QueryBuilder query = nestedChain(1);
        List<BiConsumer<Client, ActionListener<?>>> registered = new ArrayList<>();
        SearchRequest crossCluster = new SearchRequest(INDEX_NAME, "remote-cluster:" + INDEX_NAME).source(
            new SearchSourceBuilder().query(query)
        );

        IllegalArgumentException error = expectThrows(
            IllegalArgumentException.class,
            () -> query.rewrite(coordinatorContext(crossCluster, registered))
        );

        assertThat(error.getMessage(), containsString("does not support [cross-cluster search]"));
        assertTrue("refused before any leg is fanned out", registered.isEmpty());

        // Non-vacuity: with resolution failing the same way, a request that gets past this refusal really does reach it and
        // die there — so the assertion above is about ordering, not about resolution being unreachable in this harness.
        QueryBuilder localOnly = nestedChain(1);
        SearchRequest localRequest = request(localOnly);
        expectThrows(IndexNotFoundException.class, () -> localOnly.rewrite(coordinatorContext(localRequest, new ArrayList<>())));
    }

    /**
     * A rescore over a fused hybrid settles normally, and a fused hybrid <i>inside</i> the rescore query never reaches
     * confinement at all.
     *
     * <p>Confinement is installed while core rewrites the request's <i>query</i>, strictly before it snapshots the rescorer
     * list ({@link FusedRescoreScope}). A fused hybrid living in the rescore query rewrites during that snapshot instead, so
     * its own installation would write into a list core has already read past: its placeholders could never be visited, and
     * {@link FusedRescoreScope#requireReachedTheExecutedRequest} would turn that into a server error <i>after</i> the legs
     * had run. It does not get that far — the position guard refuses the shape from the body, before any fan-out, which is
     * what keeps that fail-closed path out of reach through this entry point. Pinned here because it is the one placement
     * whose refusal this class depends on rather than merely shares.
     */
    @SneakyThrows
    public void testFusedHybridInsideARescoreQuery_neverReachesConfinement() {
        HybridQueryBuilder hybrid = fused(new MatchQueryBuilder(TEXT_FIELD_NAME, "hello"), QueryBuilders.termQuery(TEXT_FIELD_NAME, "kw"));
        SearchSourceBuilder source = new SearchSourceBuilder().query(new MatchQueryBuilder(TEXT_FIELD_NAME, "hello"))
            .addRescorer(new QueryRescorerBuilder(hybrid));
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME).source(source);
        List<BiConsumer<Client, ActionListener<?>>> registered = new ArrayList<>();
        QueryCoordinatorContext coordinatorContext = coordinatorContext(searchRequest, registered);

        IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> source.rewrite(coordinatorContext));

        assertThat(error.getMessage(), containsString("must be part of the request's [query]"));
        assertTrue("refused before the fan-out whose window could never be applied", registered.isEmpty());

        // The placement that is supported, and what this class is about: the same hybrid as the request's query, with a
        // rescore over it, fans out and settles — so what the refusal above rules out is where the hybrid sits, nothing else
        // about a body that carries both a fused hybrid and a rescore.
        HybridQueryBuilder asTheQuery = fused(
            new MatchQueryBuilder(TEXT_FIELD_NAME, "hello"),
            QueryBuilders.termQuery(TEXT_FIELD_NAME, "kw")
        );
        List<Integer> legCounts = driveWholeSource(
            new SearchRequest(INDEX_NAME).source(
                new SearchSourceBuilder().query(asTheQuery)
                    .addRescorer(new QueryRescorerBuilder(new MatchQueryBuilder(TEXT_FIELD_NAME, "hello")))
            )
        );
        assertEquals(List.of(2), legCounts);
    }

    /** Scoped to fused mode: classic hybrid is answered by every version that can parse it, and stays version-free. */
    @SneakyThrows
    public void testClassicHybrid_onTheSameLaggingCluster_isUnaffected() {
        initClusterUtil(null, Version.V_3_7_0);
        HybridQueryBuilder classic = new HybridQueryBuilder();
        classic.add(new MatchQueryBuilder(TEXT_FIELD_NAME, "hello"));
        classic.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, "kw"));
        List<BiConsumer<Client, ActionListener<?>>> registered = new ArrayList<>();

        QueryBuilder rewritten = classic.rewrite(coordinatorContext(request(classic), registered));

        assertNotNull(rewritten);
        assertTrue("classic hybrid does not fan out at all", registered.isEmpty());
    }

    // ------------------------------------------------ harness ------------------------------------------------

    /** A chain {@code depth} fused hybrids deep, each holding the level below plus one term leg: 2 legs per level. */
    private HybridQueryBuilder nestedChain(final int depth) {
        return nestedChain(depth, 2);
    }

    /**
     * A chain {@code depth} fused hybrids deep and {@code width} legs wide: every level holds the level below as its first
     * leg, padded with term legs, so the body grows by {@code width} clauses per level and declares {@code depth × width}
     * leg sub-searches in total.
     */
    private HybridQueryBuilder nestedChain(final int depth, final int width) {
        HybridQueryBuilder query = null;
        for (int level = 0; level < depth; level++) {
            List<QueryBuilder> legs = new ArrayList<>();
            legs.add(query == null ? new MatchQueryBuilder(TEXT_FIELD_NAME, "hello") : query);
            for (int leg = 1; leg < width; leg++) {
                legs.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, "kw" + leg));
            }
            query = fused(legs.toArray(new QueryBuilder[0]));
        }
        return query;
    }

    /**
     * The same chain as {@link #nestedChain(int, int)}, with every level but the outermost written one level under a
     * {@code boosting} query: a builder that hands its inner query to {@code accept} without recursing into it. The body
     * declares exactly what the plain chain declares — {@code depth × width} leg sub-searches.
     */
    private HybridQueryBuilder boostingChain(final int depth, final int width) {
        HybridQueryBuilder query = null;
        for (int level = 0; level < depth; level++) {
            List<QueryBuilder> legs = new ArrayList<>();
            legs.add(
                query == null
                    ? new MatchQueryBuilder(TEXT_FIELD_NAME, "hello")
                    : QueryBuilders.boostingQuery(query, QueryBuilders.matchAllQuery()).negativeBoost(0.5f)
            );
            for (int leg = 1; leg < width; leg++) {
                legs.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, "kw" + leg));
            }
            query = fused(legs.toArray(new QueryBuilder[0]));
        }
        return query;
    }

    /** The branching form of {@link #boostingChain}: every leg of every level carries the level below, so the body
     * declares {@code width + width² + … + width^depth} leg sub-searches. */
    private HybridQueryBuilder branchingChain(final int depth, final int width) {
        List<QueryBuilder> legs = new ArrayList<>();
        for (int leg = 0; leg < width; leg++) {
            legs.add(
                depth <= 1
                    ? QueryBuilders.termQuery(TEXT_FIELD_NAME, "kw" + leg)
                    : QueryBuilders.boostingQuery(branchingChain(depth - 1, width), QueryBuilders.matchAllQuery()).negativeBoost(0.5f)
            );
        }
        return fused(legs.toArray(new QueryBuilder[0]));
    }

    private HybridQueryBuilder fused(final QueryBuilder... legs) {
        HybridQueryBuilder hybrid = new HybridQueryBuilder();
        for (QueryBuilder leg : legs) {
            hybrid.add(leg);
        }
        hybrid.fusion(fusionConfig());
        return hybrid;
    }

    private Map<String, Object> fusionConfig() {
        return new HashMap<>(
            Map.of("normalization", Map.of("technique", "min_max"), "combination", Map.of("technique", "arithmetic_mean"))
        );
    }

    private SearchRequest request(final QueryBuilder query) {
        return new SearchRequest(INDEX_NAME).source(new SearchSourceBuilder().query(query));
    }

    /** A source whose {@code query} is something other than the hybrid under test, for the positions that are not it. */
    private SearchSourceBuilder requestQuery() {
        return new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
    }

    private QueryCoordinatorContext coordinatorContext(
        final SearchRequest searchRequest,
        final List<BiConsumer<Client, ActionListener<?>>> registered
    ) {
        QueryCoordinatorContext coordinatorContext = mock(QueryCoordinatorContext.class);
        when(coordinatorContext.convertToCoordinatorContext()).thenReturn(coordinatorContext);
        when(coordinatorContext.getSearchRequest()).thenReturn(searchRequest);
        doAnswer(invocation -> registered.add(invocation.getArgument(0))).when(coordinatorContext).registerAsyncAction(any());
        return coordinatorContext;
    }

    /**
     * Rewrite the request's query to completion through core's own {@code Rewriteable.rewriteAndFetch}, recording each
     * MultiSearch the coordinator issues. Core drives the loop deliberately: it is what counts rewrite rounds, and a query
     * that needs more than {@link Rewriteable#MAX_REWRITE_ROUNDS} of them fails the request with an internal error rather
     * than running — so any test using this harness also pins that this query settles inside that budget.
     */
    @SneakyThrows
    private FanOut drive(final SearchRequest searchRequest) {
        List<Integer> legCountPerMultiSearch = new ArrayList<>();
        QueryCoordinatorContext coordinatorContext = fanningOutContext(searchRequest, legCountPerMultiSearch);

        AtomicReference<QueryBuilder> settled = new AtomicReference<>();
        Rewriteable.rewriteAndFetch(searchRequest.source().query(), coordinatorContext, ActionListener.wrap(settled::set, e -> {
            throw new AssertionError("rewrite failed: " + e.getMessage(), e);
        }));
        assertNotNull("the rewrite never completed", settled.get());
        return new FanOut(legCountPerMultiSearch, settled.get());
    }

    /**
     * As {@link #drive}, but driving the whole {@code source}, which is what a request carrying a rescore needs: fused mode's
     * confinement is resolved by the pass core takes over the <i>rescorer list</i>, so driving the query alone leaves the
     * placeholders unvisited and the request fails closed. Reports the fan-out only — what the dispatched source contains is
     * pinned in {@link FusedRescoreScopeTests}, against core's own rewrite rather than this harness.
     */
    @SneakyThrows
    private List<Integer> driveWholeSource(final SearchRequest searchRequest) {
        List<Integer> legCountPerMultiSearch = new ArrayList<>();
        QueryCoordinatorContext coordinatorContext = fanningOutContext(searchRequest, legCountPerMultiSearch);

        AtomicReference<SearchSourceBuilder> settled = new AtomicReference<>();
        Rewriteable.rewriteAndFetch(searchRequest.source(), coordinatorContext, ActionListener.wrap(settled::set, e -> {
            throw new AssertionError("rewrite failed: " + e.getMessage(), e);
        }));
        assertNotNull("the rewrite never completed", settled.get());
        return legCountPerMultiSearch;
    }

    /** A coordinator context that answers every fan-out registered on it from {@link #multiSearchingClient}, round by round. */
    private QueryCoordinatorContext fanningOutContext(final SearchRequest searchRequest, final List<Integer> legCountPerMultiSearch) {
        List<BiConsumer<Client, ActionListener<?>>> registered = new ArrayList<>();
        QueryCoordinatorContext coordinatorContext = coordinatorContext(searchRequest, registered);
        Client client = multiSearchingClient(legCountPerMultiSearch);
        when(coordinatorContext.hasAsyncActions()).thenAnswer(invocation -> registered.isEmpty() == false);
        doAnswer(invocation -> {
            List<BiConsumer<Client, ActionListener<?>>> thisRound = new ArrayList<>(registered);
            registered.clear();
            for (BiConsumer<Client, ActionListener<?>> action : thisRound) {
                action.accept(client, ActionListener.wrap(response -> {}, e -> fail("leg fan-out failed: " + e.getMessage())));
            }
            ActionListener<Object> roundListener = invocation.getArgument(0);
            roundListener.onResponse(null);
            return null;
        }).when(coordinatorContext).executeAsyncActions(any());
        return coordinatorContext;
    }

    /** Answers every leg with the same two hits, and records how many legs each MultiSearch carried. */
    private Client multiSearchingClient(final List<Integer> legCountPerMultiSearch) {
        Client client = mock(Client.class);
        doAnswer(invocation -> {
            MultiSearchRequest multiSearchRequest = invocation.getArgument(0);
            legCountPerMultiSearch.add(multiSearchRequest.requests().size());
            MultiSearchResponse.Item[] items = new MultiSearchResponse.Item[multiSearchRequest.requests().size()];
            for (int leg = 0; leg < items.length; leg++) {
                items[leg] = legItem();
            }
            ActionListener<MultiSearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(new MultiSearchResponse(items, 10L));
            return null;
        }).when(client).multiSearch(any(), any());
        return client;
    }

    private MultiSearchResponse.Item legItem() {
        SearchHit[] hits = new SearchHit[] { hit(0, "1", 0.9f), hit(1, "2", 0.5f) };
        SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponseSections sections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        return new MultiSearchResponse.Item(new SearchResponse(sections, null, 1, 1, 0, 10, null, null), null);
    }

    /** A leg hit as the coordinator sees one: its {@code _index} comes from the shard target the response was read from. */
    private SearchHit hit(final int docId, final String id, final float score) {
        SearchHit hit = new SearchHit(docId, id, Map.of(), Map.of());
        hit.score(score);
        hit.shard(new SearchShardTarget("node-1", new ShardId(new Index(INDEX_NAME, "uuid-1"), 0), null, OriginalIndices.NONE));
        return hit;
    }

    /** A cluster every node of which is on the current version — the fused rewrite's precondition. */
    private void initClusterUtil(final Settings clusterSettings) {
        initClusterUtil(clusterSettings, Version.CURRENT);
    }

    /**
     * A cluster the fused rewrite can read: a minimum node version, concrete indices for the window check, and cluster
     * settings for the budget. {@code null} settings means nothing is configured, so the budget falls back to the
     * setting's own default.
     */
    private void initClusterUtil(final Settings clusterSettings, final Version minNodeVersion) {
        Metadata metadata = mock(Metadata.class);
        ClusterState clusterState = mock(ClusterState.class);
        clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(clusterState.getMetadata()).thenReturn(metadata);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(clusterState.getNodes()).thenReturn(nodes);
        when(nodes.getMinNodeVersion()).thenReturn(minNodeVersion);
        when(metadata.custom(SearchPipelineMetadata.TYPE)).thenReturn(new SearchPipelineMetadata(Map.of()));
        if (clusterSettings != null) {
            when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(clusterSettings, Set.of(MAX_FUSION_LEG_SEARCHES)));
        }
        Index index = new Index(INDEX_NAME, "uuid-1");
        Settings indexSettings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.version.created", Version.CURRENT.id)
            .build();
        when(metadata.index(index)).thenReturn(IndexMetadata.builder(INDEX_NAME).settings(indexSettings).build());
        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(ClusterState.class), any(org.opensearch.action.IndicesRequest.class))).thenReturn(
            new Index[] { index }
        );
        NeuralSearchClusterUtil.instance().initialize(clusterService, resolver);
    }

    /**
     * The same cluster, except index resolution fails the way core's does for a literal expression it cannot look up —
     * which is what a {@code cluster:index} expression hits when there is no such remote alias.
     */
    private void initClusterUtilWhereIndexResolutionFails() {
        initClusterUtil(null);
        IndexNameExpressionResolver failing = mock(IndexNameExpressionResolver.class);
        when(failing.concreteIndices(any(ClusterState.class), any(org.opensearch.action.IndicesRequest.class))).thenThrow(
            new IndexNotFoundException("cluster:index")
        );
        NeuralSearchClusterUtil.instance().initialize(clusterService, failing);
    }
}
