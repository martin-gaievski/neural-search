/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.profile;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.profile.NetworkTime;
import org.opensearch.search.profile.ProfileResult;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.search.profile.aggregation.AggregationProfileShardResult;
import org.opensearch.search.profile.fetch.FetchProfileShardResult;
import org.opensearch.search.profile.query.CollectorResult;
import org.opensearch.search.profile.query.QueryProfileShardResult;
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
     * field dropped there would silently change an answer the search already got right, so pin them all.
     */
    public void testGetMergedResponse_whenRebuilt_thenEverySectionOutsideTheProfileIsPreserved() {
        FusedLegProfileMerger merger = new FusedLegProfileMerger();
        merger.forHybrid("hybrid_0").accept(0, Map.of(SHARD_KEY, shardResult(1)));

        SearchHits hits = new SearchHits(new SearchHit[] { new SearchHit(1) }, new TotalHits(42L, TotalHits.Relation.EQUAL_TO), 1.5f);
        InternalSearchResponse sections = new InternalSearchResponse(
            hits,
            InternalAggregations.EMPTY,
            null,
            new SearchProfileShardResults(Map.of(SHARD_KEY, shardResult(1))),
            true,
            true,
            3
        );
        SearchResponse response = new SearchResponse(
            sections,
            "scroll-id",
            5,
            4,
            1,
            17L,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );

        SearchResponse merged = merger.getMergedResponse(response);

        assertEquals("hits are the answer and must survive verbatim", 42L, merged.getHits().getTotalHits().value());
        assertEquals(1.5f, merged.getHits().getMaxScore(), 0.0f);
        assertSame("aggregations are handed over, not rebuilt", response.getAggregations(), merged.getAggregations());
        assertEquals(Boolean.TRUE, merged.isTerminatedEarly());
        assertTrue(merged.isTimedOut());
        assertEquals(3, merged.getNumReducePhases());
        assertEquals("scroll-id", merged.getScrollId());
        assertEquals(5, merged.getTotalShards());
        assertEquals(4, merged.getSuccessfulShards());
        assertEquals(1, merged.getSkippedShards());
        assertEquals("took is the user's latency, not the merge's", 17L, merged.getTook().millis());
        assertSame(SearchResponse.Clusters.EMPTY, merged.getClusters());
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
