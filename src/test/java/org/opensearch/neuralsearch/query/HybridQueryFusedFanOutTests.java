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
import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.DEFAULT_MAX_FUSION_LEG_SEARCHES;
import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.MAX_FUSION_LEG_SEARCHES;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import org.apache.lucene.search.TotalHits;
import org.opensearch.Version;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryCoordinatorContext;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.Rewriteable;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
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
        List<BiConsumer<Client, ActionListener<?>>> registered = new ArrayList<>();
        QueryCoordinatorContext coordinatorContext = coordinatorContext(searchRequest, registered);
        List<Integer> legCountPerMultiSearch = new ArrayList<>();
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

        AtomicReference<QueryBuilder> settled = new AtomicReference<>();
        Rewriteable.rewriteAndFetch(searchRequest.source().query(), coordinatorContext, ActionListener.wrap(settled::set, e -> {
            throw new AssertionError("rewrite failed: " + e.getMessage(), e);
        }));
        assertNotNull("the rewrite never completed", settled.get());
        return new FanOut(legCountPerMultiSearch, settled.get());
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

    private SearchHit hit(final int docId, final String id, final float score) {
        SearchHit hit = new SearchHit(docId, id, Map.of(), Map.of());
        hit.score(score);
        return hit;
    }

    /**
     * A cluster the fused rewrite can read: concrete indices for the window check, and cluster settings for the budget.
     * {@code null} settings means nothing is configured, so the budget falls back to the setting's own default.
     */
    private void initClusterUtil(final Settings clusterSettings) {
        Metadata metadata = mock(Metadata.class);
        ClusterState clusterState = mock(ClusterState.class);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(clusterState.getMetadata()).thenReturn(metadata);
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
}
