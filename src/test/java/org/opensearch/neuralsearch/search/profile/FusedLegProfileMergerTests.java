/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.profile;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.neuralsearch.query.ext.RerankSearchExtBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.pipeline.ProcessorExecutionDetail;
import org.opensearch.search.profile.NetworkTime;
import org.opensearch.search.profile.ProfileResult;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.search.profile.aggregation.AggregationProfileShardResult;
import org.opensearch.search.profile.fetch.FetchProfileShardResult;
import org.opensearch.search.profile.query.CollectorResult;
import org.opensearch.search.profile.query.QueryProfileShardResult;
import org.opensearch.search.suggest.Suggest;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit coverage for the two shaping rules the merger applies: the response's own entries are relabelled rather than left
 * to read as the user's query, and a leg's fetch section is emptied because a leg only fetches {@code _id}.
 */
public class FusedLegProfileMergerTests extends OpenSearchTestCase {

    private static final String SHARD_KEY = "[node][index][0]";

    public void testGetMergedResponse_whenNoLegReported_thenResponseReturnedUntouched() {
        FusedLegProfileMerger merger = new FusedLegProfileMerger();
        SearchResponse response = responseWithProfile(Map.of(SHARD_KEY, shardResult(2)));

        assertTrue("nothing was collected", merger.isEmpty());
        assertSame("an unprofiled or classic response must not be rebuilt", response, merger.getMergedResponse(response));
        assertEquals("and its keys must not be relabelled", Set.of(SHARD_KEY), response.getProfileResults().keySet());
    }

    public void testGetMergedResponse_whenLegReported_thenRoundTwoIsRelabelledAndLegIsTagged() {
        FusedLegProfileMerger merger = new FusedLegProfileMerger();
        merger.forHybrid("hybrid_0").accept(1, Map.of(SHARD_KEY, shardResult(3)));

        SearchResponse merged = merger.getMergedResponse(responseWithProfile(Map.of(SHARD_KEY, shardResult(2))));

        assertEquals(
            "round 2 keeps its tree under a label saying it is the rewritten query, not the user's",
            Set.of(SHARD_KEY + "[fused:rewrite]", SHARD_KEY + "[fused:hybrid_0.leg_1]"),
            merged.getProfileResults().keySet()
        );
    }

    public void testGetMergedResponse_whenLegReported_thenLegFetchIsStrippedAndTheRestIsKept() {
        FusedLegProfileMerger merger = new FusedLegProfileMerger();
        merger.forHybrid("hybrid_0").accept(0, Map.of(SHARD_KEY, shardResult(3)));

        Map<String, ProfileShardResult> merged = merger.getMergedResponse(responseWithProfile(Map.of(SHARD_KEY, shardResult(2))))
            .getProfileResults();

        ProfileShardResult leg = merged.get(SHARD_KEY + "[fused:hybrid_0.leg_0]");
        assertEquals("a leg only fetches the _id it contributes", List.of(), leg.getFetchProfileResult().getFetchProfileResults());
        assertEquals("the aggregation section is untouched", 3, leg.getAggregationProfileResults().getProfileResults().size());
        assertEquals("and so is the network time", 3L, leg.getNetworkTime().getInboundNetworkTime());

        ProfileShardResult roundTwo = merged.get(SHARD_KEY + "[fused:rewrite]");
        assertEquals(
            "the user's own fetch is the one that matters, and it is kept",
            2,
            roundTwo.getFetchProfileResult().getFetchProfileResults().size()
        );
    }

    /**
     * A nested fused hybrid's leg profiles arrive already tagged by the inner request, so the outer tag has to go in front
     * for the label to read as a path from the user's query down. That applies to the inner {@code rewrite} entry too.
     */
    public void testForHybrid_whenLegKeyIsAlreadyTagged_thenOuterTagGoesFirst() {
        FusedLegProfileMerger merger = new FusedLegProfileMerger();
        merger.forHybrid("hybrid_0")
            .accept(0, Map.of(SHARD_KEY + "[fused:rewrite]", shardResult(1), SHARD_KEY + "[fused:hybrid_0.leg_1]", shardResult(1)));

        Map<String, ProfileShardResult> merged = merger.getMergedResponse(responseWithProfile(Map.of(SHARD_KEY, shardResult(1))))
            .getProfileResults();

        assertEquals(
            Set.of(
                SHARD_KEY + "[fused:rewrite]",
                SHARD_KEY + "[fused:hybrid_0.leg_0][fused:rewrite]",
                SHARD_KEY + "[fused:hybrid_0.leg_0][fused:hybrid_0.leg_1]"
            ),
            merged.keySet()
        );
    }

    public void testForHybrid_whenLegReportedNoProfile_thenNothingIsCollected() {
        FusedLegProfileMerger merger = new FusedLegProfileMerger();
        merger.forHybrid("hybrid_0").accept(0, null);
        assertTrue("a leg that reported no profile must not force a rebuild", merger.isEmpty());
    }

    public void testGetMergedResponse_whenResponseHasNoProfileSection_thenOnlyLegsAreReported() {
        FusedLegProfileMerger merger = new FusedLegProfileMerger();
        merger.forHybrid("hybrid_0").accept(0, Map.of(SHARD_KEY, shardResult(1)));

        Map<String, ProfileShardResult> merged = merger.getMergedResponse(responseWithProfile(null)).getProfileResults();

        assertEquals(Set.of(SHARD_KEY + "[fused:hybrid_0.leg_0]"), merged.keySet());
    }

    /** Two fused hybrids in one request are two handles off one merger, and their legs must not collide. */
    public void testForHybrid_whenTwoHybridsReportTheSameLegIndex_thenEachKeepsItsOwnLabel() {
        FusedLegProfileMerger merger = new FusedLegProfileMerger();
        merger.forHybrid("hybrid_0").accept(0, Map.of(SHARD_KEY, shardResult(1)));
        merger.forHybrid("hybrid_1").accept(0, Map.of(SHARD_KEY, shardResult(1)));

        Map<String, ProfileShardResult> merged = merger.getMergedResponse(responseWithProfile(Map.of(SHARD_KEY, shardResult(1))))
            .getProfileResults();

        assertEquals(
            Set.of(SHARD_KEY + "[fused:rewrite]", SHARD_KEY + "[fused:hybrid_0.leg_0]", SHARD_KEY + "[fused:hybrid_1.leg_0]"),
            merged.keySet()
        );
    }

    /** The query section is the whole point of a leg entry, so stripping the fetch section must leave it intact. */
    public void testGetMergedResponse_whenLegReported_thenItsQuerySectionIsKeptWhole() {
        FusedLegProfileMerger merger = new FusedLegProfileMerger();
        merger.forHybrid("hybrid_0").accept(0, Map.of(SHARD_KEY, shardResult(2)));

        Map<String, ProfileShardResult> merged = merger.getMergedResponse(responseWithProfile(Map.of(SHARD_KEY, shardResult(2))))
            .getProfileResults();

        List<QueryProfileShardResult> query = merged.get(SHARD_KEY + "[fused:hybrid_0.leg_0]").getQueryProfileResults();
        assertEquals("one search per leg", 1, query.size());
        assertEquals("the leg's own query tree", 2, query.get(0).getQueryResults().size());
        assertEquals("type_0", query.get(0).getQueryResults().get(0).getQueryName());
        assertEquals("rewrite time is part of the leg's cost", 11L, query.get(0).getRewriteTime());
        assertEquals("and so is its collector", "leg_collector", query.get(0).getCollectorResult().getName());
    }

    /**
     * Merging rebuilds the response around a new profile section, which means every other section is copied by hand. A
     * field dropped there would silently change an answer the search already got right, so pin every one of them — built
     * through the widest constructors, with a distinct non-default value each, so that dropping any single one fails here.
     */
    public void testGetMergedResponse_whenRebuilt_thenEverySectionOutsideTheProfileIsPreserved() {
        FusedLegProfileMerger merger = new FusedLegProfileMerger();
        merger.forHybrid("hybrid_0").accept(0, Map.of(SHARD_KEY, shardResult(1)));

        SearchResponse response = responseWithEverything("scroll-id", null);
        SearchResponse merged = merger.getMergedResponse(response);

        // SearchResponseSections, all of it but the profile results the merge exists to replace
        assertSame("hits are the answer and must survive verbatim", response.getHits(), merged.getHits());
        assertEquals(42L, merged.getHits().getTotalHits().value());
        assertEquals(1.5f, merged.getHits().getMaxScore(), 0.0f);
        assertSame("aggregations are handed over, not rebuilt", response.getAggregations(), merged.getAggregations());
        assertSame("a suggest section belongs to the search, not to the profile", response.getSuggest(), merged.getSuggest());
        assertTrue(merged.isTimedOut());
        assertEquals(Boolean.TRUE, merged.isTerminatedEarly());
        assertEquals(3, merged.getNumReducePhases());
        assertEquals(
            "ext sections are what a pipeline answers with",
            response.getInternalResponse().getSearchExtBuilders(),
            merged.getInternalResponse().getSearchExtBuilders()
        );
        assertEquals(
            "and so is the processor execution detail a verbose pipeline request asked for",
            response.getInternalResponse().getProcessorResult(),
            merged.getInternalResponse().getProcessorResult()
        );

        // SearchResponse's own fields
        assertEquals("scroll-id", merged.getScrollId());
        assertEquals(5, merged.getTotalShards());
        assertEquals(4, merged.getSuccessfulShards());
        assertEquals(1, merged.getSkippedShards());
        assertEquals("took is the user's latency, not the merge's", 17L, merged.getTook().millis());
        assertEquals("phase_took is the per-phase half of the same accounting", Map.of("query", 11L), phaseTookMap(merged));
        assertEquals("a partial answer stays partial", 1, merged.getFailedShards());
        assertSame(response.getShardFailures()[0], merged.getShardFailures()[0]);
        assertSame(SearchResponse.Clusters.EMPTY, merged.getClusters());
    }

    /**
     * {@code pointInTimeId} shares the assertion above's fixture but not its response: core asserts a response carries at
     * most one of {@code scrollId} and {@code pointInTimeId}, so the two can only be pinned one per response.
     */
    public void testGetMergedResponse_whenRebuilt_thenThePointInTimeIdIsPreserved() {
        FusedLegProfileMerger merger = new FusedLegProfileMerger();
        merger.forHybrid("hybrid_0").accept(0, Map.of(SHARD_KEY, shardResult(1)));

        SearchResponse merged = merger.getMergedResponse(responseWithEverything(null, "pit-id"));

        assertEquals("pit-id", merged.pointInTimeId());
        assertNull("and the scroll id it excludes stays absent", merged.getScrollId());
    }

    /**
     * The rebuild above is only complete while it calls the widest constructor core offers, and core widens a response by
     * <i>adding</i> a constructor rather than changing one — the 7 → 8 → 9 and 8 → 9 → 10 ladders these two classes already
     * carry are the evidence. So a new field arrives as a wider constructor, not as a compile error in the merger, and
     * nothing above would fail. This is the assertion that does fail, and it names the fix.
     */
    public void testGetMergedResponse_whenCoreWidensAResponse_thenTheRebuildIsToldToCarryTheNewField() {
        assertEquals(
            "core widened InternalSearchResponse: FusedLegProfileMerger#getMergedResponse must pass the new field(s) too",
            9,
            widestPublicConstructorArity(InternalSearchResponse.class)
        );
        assertEquals(
            "core widened SearchResponse: FusedLegProfileMerger#getMergedResponse must pass the new field(s) too",
            10,
            widestPublicConstructorArity(SearchResponse.class)
        );
    }

    private static int widestPublicConstructorArity(final Class<?> type) {
        return Arrays.stream(type.getConstructors()).mapToInt(Constructor::getParameterCount).max().orElse(0);
    }

    private static Map<String, Long> phaseTookMap(final SearchResponse response) {
        return Objects.isNull(response.getPhaseTook()) ? null : response.getPhaseTook().getPhaseTookMap();
    }

    /**
     * A response with a distinct non-default value in every field the rebuild has to carry, through the widest constructor
     * each class offers. Only one of {@code scrollId} / {@code pointInTimeId} may be set, which core asserts.
     */
    private static SearchResponse responseWithEverything(final String scrollId, final String pointInTimeId) {
        SearchHits hits = new SearchHits(new SearchHit[] { new SearchHit(1) }, new TotalHits(42L, TotalHits.Relation.EQUAL_TO), 1.5f);
        InternalSearchResponse sections = new InternalSearchResponse(
            hits,
            InternalAggregations.EMPTY,
            // core's Suggest sorts the list it is handed in place, so it cannot be an immutable one
            new Suggest(new ArrayList<>()),
            new SearchProfileShardResults(Map.of(SHARD_KEY, shardResult(1))),
            true,
            true,
            3,
            List.of(new RerankSearchExtBuilder(Map.of("query_text", "cat"))),
            List.of(new ProcessorExecutionDetail("normalization-processor"))
        );
        return new SearchResponse(
            sections,
            scrollId,
            5,
            4,
            1,
            17L,
            new SearchResponse.PhaseTook(Map.of("query", 11L)),
            new ShardSearchFailure[] { new ShardSearchFailure(new IllegalStateException("shard 4 is unavailable")) },
            SearchResponse.Clusters.EMPTY,
            pointInTimeId
        );
    }

    /**
     * The coordinator's own entry: what fused mode spent fanning the legs out and fusing them, which no shard entry can
     * report because it happens before the first search phase starts.
     */
    public void testForHybridTiming_whenTimingsPublished_thenTheCoordinatorEntryIsSynthesized() {
        FusedLegProfileMerger merger = new FusedLegProfileMerger();
        merger.forHybridTiming("hybrid_0").accept(timings());

        assertFalse("a published coordinator entry is worth a rebuild on its own", merger.isEmpty());
        Map<String, ProfileShardResult> merged = merger.getMergedResponse(responseWithProfile(Map.of(SHARD_KEY, shardResult(1))))
            .getProfileResults();
        assertEquals(
            "keyed off a stand-in for the node, because the coordinator's fusion is not a shard's work",
            Set.of(SHARD_KEY + "[fused:rewrite]", "[coordinator][fused:hybrid_0]"),
            merged.keySet()
        );

        QueryProfileShardResult query = merged.get("[coordinator][fused:hybrid_0]").getQueryProfileResults().get(0);
        assertEquals("no Lucene rewrite happens on the coordinator", 0L, query.getRewriteTime());
        assertEquals("HybridFusionCombiner", query.getCollectorResult().getName());
        assertEquals("the collector slot carries the fusion subtotal", 40L + 50L + 60L + 70L, query.getCollectorResult().getTime());

        ProfileResult node = query.getQueryResults().get(0);
        assertEquals(
            "not HybridQuery: a coordinator span must not invite subtraction from a shard-local one",
            "FusedHybridQuery",
            node.getQueryName()
        );
        assertEquals("2 legs, window 11, min_max / arithmetic_mean", node.getLuceneDescription());
        assertEquals("the node's time is the whole coordinator span for this hybrid", 20L + 30L + 40L + 50L + 60L + 70L, node.getTime());
        assertEquals("the phases are leaves", List.of(), node.getProfiledChildren());
    }

    /** The breakdown is what a reader adds up, so pin both its keys and that they sum to the node's own time. */
    public void testForHybridTiming_whenTimingsPublished_thenTheBreakdownPhasesSumToTheNodeTime() {
        FusedLegProfileMerger merger = new FusedLegProfileMerger();
        merger.forHybridTiming("hybrid_0").accept(timings());

        ProfileResult node = merger.getMergedResponse(responseWithProfile(null))
            .getProfileResults()
            .get("[coordinator][fused:hybrid_0]")
            .getQueryProfileResults()
            .get(0)
            .getQueryResults()
            .get(0);

        Map<String, Long> breakdown = node.getTimeBreakdown();
        assertEquals(
            List.of("fan_out_build", "fan_out_wait", "window_merge", "fuse_scores", "rank_window", "substitute_build"),
            new ArrayList<>(breakdown.keySet())
        );
        assertEquals(Long.valueOf(20L), breakdown.get("fan_out_build"));
        assertEquals(Long.valueOf(30L), breakdown.get("fan_out_wait"));
        assertEquals(Long.valueOf(40L), breakdown.get("window_merge"));
        assertEquals(Long.valueOf(50L), breakdown.get("fuse_scores"));
        assertEquals(Long.valueOf(60L), breakdown.get("rank_window"));
        assertEquals(Long.valueOf(70L), breakdown.get("substitute_build"));
        assertEquals("nothing is counted twice", node.getTime(), breakdown.values().stream().mapToLong(Long::longValue).sum());
    }

    /** {@code debug} carries what is not a duration — including each leg's own {@code took} and timeout flag. */
    public void testForHybridTiming_whenTimingsPublished_thenDebugCarriesTheShapeOfTheWork() {
        FusedLegProfileMerger merger = new FusedLegProfileMerger();
        merger.forHybridTiming("hybrid_0").accept(timings());

        Map<String, Object> debug = merger.getMergedResponse(responseWithProfile(null))
            .getProfileResults()
            .get("[coordinator][fused:hybrid_0]")
            .getQueryProfileResults()
            .get(0)
            .getQueryResults()
            .get(0)
            .getDebugInfo();

        assertEquals(11, debug.get("window_size"));
        assertEquals(9, debug.get("ranked_docs"));
        assertEquals(true, debug.get("tail_built"));
        assertEquals(
            List.of(
                Map.of("leg", 0, "took_in_millis", 3L, "hits", 11, "timed_out", false),
                Map.of("leg", 1, "took_in_millis", 5L, "hits", 4, "timed_out", true)
            ),
            debug.get("legs")
        );
    }

    /** Core renders both of these unconditionally, so the coordinator entry has to carry them as empty rather than absent. */
    public void testForHybridTiming_whenTimingsPublished_thenTheAggregationAndFetchSectionsAreEmpty() {
        FusedLegProfileMerger merger = new FusedLegProfileMerger();
        merger.forHybridTiming("hybrid_0").accept(timings());

        ProfileShardResult coordinator = merger.getMergedResponse(responseWithProfile(null))
            .getProfileResults()
            .get("[coordinator][fused:hybrid_0]");
        assertEquals(List.of(), coordinator.getAggregationProfileResults().getProfileResults());
        assertEquals(List.of(), coordinator.getFetchProfileResult().getFetchProfileResults());
        assertEquals("the coordinator does not talk to itself over the network", 0L, coordinator.getNetworkTime().getInboundNetworkTime());
    }

    public void testForHybridTiming_whenNothingWasPublished_thenNothingIsCollected() {
        FusedLegProfileMerger merger = new FusedLegProfileMerger();
        merger.forHybridTiming("hybrid_0").accept(null);
        assertTrue("an unprofiled hybrid must not force a rebuild", merger.isEmpty());
    }

    /**
     * A nested fused hybrid's coordinator entry arrives inside its leg's response already keyed {@code [coordinator][...]},
     * and has to compose into the label path exactly like a leg's shard entry — otherwise the inner fusion's cost reads as
     * the outer one's.
     */
    public void testForHybrid_whenALegReportsItsOwnCoordinatorEntry_thenItIsRetaggedIntoThePath() {
        FusedLegProfileMerger merger = new FusedLegProfileMerger();
        merger.forHybridTiming("hybrid_0").accept(timings());
        merger.forHybrid("hybrid_0").accept(0, Map.of("[coordinator][fused:hybrid_0]", shardResult(1)));

        Map<String, ProfileShardResult> merged = merger.getMergedResponse(responseWithProfile(Map.of(SHARD_KEY, shardResult(1))))
            .getProfileResults();

        assertEquals(
            Set.of(SHARD_KEY + "[fused:rewrite]", "[coordinator][fused:hybrid_0]", "[coordinator][fused:hybrid_0.leg_0][fused:hybrid_0]"),
            merged.keySet()
        );
    }

    /** Two fused hybrids in one request each get their own coordinator entry, and must not overwrite each other. */
    public void testForHybridTiming_whenTwoHybridsPublish_thenEachKeepsItsOwnEntry() {
        FusedLegProfileMerger merger = new FusedLegProfileMerger();
        merger.forHybridTiming("hybrid_0").accept(timings());
        merger.forHybridTiming("hybrid_1").accept(timings());

        Map<String, ProfileShardResult> merged = merger.getMergedResponse(responseWithProfile(null)).getProfileResults();

        assertEquals(Set.of("[coordinator][fused:hybrid_0]", "[coordinator][fused:hybrid_1]"), merged.keySet());
    }

    /** Recognizable, distinct spans per phase, so a phase reported under the wrong key is visible. */
    private static FusedCoordinatorTimings timings() {
        FusedCoordinatorTimings timings = new FusedCoordinatorTimings().fanOutBuildNanos(20L)
            .fanOutWaitNanos(30L)
            .windowMergeNanos(40L)
            .fuseScoresNanos(50L)
            .rankWindowNanos(60L)
            .substituteBuildNanos(70L)
            .windowSize(11)
            .rankedDocs(9)
            .tailBuilt(true)
            .normalizationTechnique("min_max")
            .combinationTechnique("arithmetic_mean");
        timings.addLeg(0, 3L, 11, false);
        timings.addLeg(1, 5L, 4, true);
        return timings;
    }

    /**
     * A shard result with a {@code nodes}-node query tree, {@code nodes} fetch nodes, {@code nodes} aggregation nodes and
     * a recognizable network time.
     */
    private static ProfileShardResult shardResult(final int nodes) {
        List<ProfileResult> tree = new ArrayList<>();
        for (int i = 0; i < nodes; i++) {
            tree.add(new ProfileResult("type_" + i, "description_" + i, Map.of(), Map.of(), i, List.of()));
        }
        QueryProfileShardResult query = new QueryProfileShardResult(
            tree,
            11L,
            new CollectorResult("leg_collector", "search_top_hits", 13L, List.of())
        );
        return new ProfileShardResult(
            List.of(query),
            new AggregationProfileShardResult(tree),
            new FetchProfileShardResult(tree),
            new NetworkTime(nodes, nodes)
        );
    }

    private static SearchResponse responseWithProfile(final Map<String, ProfileShardResult> profiles) {
        InternalSearchResponse sections = new InternalSearchResponse(
            SearchHits.empty(),
            InternalAggregations.EMPTY,
            null,
            Objects.isNull(profiles) ? null : new SearchProfileShardResults(profiles),
            false,
            null,
            1
        );
        return new SearchResponse(sections, null, 1, 1, 0, 7L, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }
}
