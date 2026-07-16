/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import lombok.Value;

/**
 * JVM-local registry that carries per-document boost-tier information from the shard-side hybrid collector to the
 * coordinator-side {@link NormalizationProcessorWorkflow}, keyed by {@link SearchShard}.
 *
 * <p><b>POC / single-node only.</b> This is the same pattern as the reverted {@code HybridScoreRegistry}
 * (PR #1369): it only works when shard collection and the coordinator normalization step run in the same JVM,
 * i.e. a single-node cluster. It is <b>not</b> safe for multi-node deployments, where the per-doc tier must instead
 * ride the shard-&gt;coordinator score envelope. It exists so the POC can demonstrate the feature end-to-end on a
 * single node without the (much larger) envelope/merger/wire work.
 *
 * <p>The {@link SearchShard} key (index, shardId, nodeId) disambiguates entries without relying on thread identity,
 * so it survives the query-phase-thread to coordinator-thread hop within one JVM.
 */
public final class HybridBoostTierRegistry {

    private static final Map<SearchShard, ShardTiers> REGISTRY = new ConcurrentHashMap<>();

    private HybridBoostTierRegistry() {}

    /**
     * Per-shard boost-tier payload: a doc-id -&gt; tier map (tier 0 = highest priority) and the number of conditions.
     */
    @Value
    public static class ShardTiers {
        Map<Integer, Integer> docIdToTier;
        int numConditions;
    }

    /**
     * Publish the tier map for a shard (called from the shard-side collector manager after collection).
     */
    public static void put(final SearchShard searchShard, final Map<Integer, Integer> docIdToTier, final int numConditions) {
        REGISTRY.put(searchShard, new ShardTiers(docIdToTier, numConditions));
    }

    /**
     * Retrieve and remove the tier map for a shard (called from the coordinator workflow). Returns {@code null} if
     * no entry was published for that shard (e.g. the query had no boost conditions).
     */
    public static ShardTiers takeAndClear(final SearchShard searchShard) {
        return REGISTRY.remove(searchShard);
    }

    /**
     * Test/utility hook to clear all entries.
     */
    public static void clear() {
        REGISTRY.clear();
    }
}
