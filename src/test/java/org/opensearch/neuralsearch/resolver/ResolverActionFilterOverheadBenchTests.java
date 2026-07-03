/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.resolver;

import org.opensearch.action.search.SearchAction;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Microbenchmark for the per-query CPU overhead the {@link ResolverActionFilter} adds to searches that are
 * NOT resolver queries — i.e. the synchronous work every ordinary search pays on the coordinator: the action/type
 * gate ({@code SearchAction.NAME.equals} + instanceof) plus {@link ResolverOrchestrator#collectMarkers} (the
 * recursive bool-tree walk that finds no markers and returns).
 *
 * <p>Not a JUnit assertion test in spirit — it prints ns/op so we can quote the actual per-query delay. Runs a
 * warmup then a timed loop over three representative query shapes: a leaf match, a small 3-clause bool, and a deep
 * nested bool (~50 leaf clauses). Reported figure is the coordinator-side, once-per-request cost (not per shard/doc).
 */
public class ResolverActionFilterOverheadBenchTests extends OpenSearchTestCase {

    private static final int WARMUP = 200_000;
    private static final int ITERS = 2_000_000;

    /** The exact synchronous work apply() does for a non-resolver query, up to the chain.proceed decision.
     *  Mirrors the optimized ResolverActionFilter.apply(): action check + not-a-resolver + (only for a top-level
     *  bool) the collectMarkers walk. Leaf / non-bool queries skip collectMarkers entirely (no allocation). */
    private static boolean filterGate(String action, QueryBuilder query) {
        if (SearchAction.NAME.equals(action) == false) {
            return false;
        }
        if (query instanceof ResolverQueryBuilder) {
            return true;
        }
        if (query instanceof org.opensearch.index.query.BoolQueryBuilder) {
            return ResolverOrchestrator.collectMarkers(query).isEmpty() == false;
        }
        return false;
    }

    private double benchNsPerOp(String label, QueryBuilder q) {
        long sink = 0;
        for (int i = 0; i < WARMUP; i++) {
            sink += filterGate(SearchAction.NAME, q) ? 1 : 0;
        }
        long t0 = System.nanoTime();
        for (int i = 0; i < ITERS; i++) {
            sink += filterGate(SearchAction.NAME, q) ? 1 : 0;
        }
        long elapsed = System.nanoTime() - t0;
        double nsPerOp = (double) elapsed / ITERS;
        // sink prevents dead-code elimination
        logger.info("OVERHEAD {} -> {} ns/op (sink={})", label, String.format(java.util.Locale.ROOT, "%.1f", nsPerOp), sink);
        return nsPerOp;
    }

    private static QueryBuilder smallBool() {
        return new BoolQueryBuilder().must(new MatchQueryBuilder("title", "a"))
            .should(new MatchQueryBuilder("body", "b"))
            .filter(new TermQueryBuilder("category", "c"));
    }

    /** ~50-leaf nested bool: 5 top clauses, each a bool of 10 leaves — exercises the recursive walk + per-node alloc. */
    private static QueryBuilder deepBool() {
        BoolQueryBuilder root = new BoolQueryBuilder();
        for (int i = 0; i < 5; i++) {
            BoolQueryBuilder child = new BoolQueryBuilder();
            for (int j = 0; j < 10; j++) {
                child.should(new MatchQueryBuilder("f" + j, "v" + j));
            }
            root.must(child);
        }
        return root;
    }

    public void testActionFilterNonResolverOverhead() {
        // leaf query: not a bool, so collect() returns in O(1) after one instanceof
        double leaf = benchNsPerOp("leaf-match", new MatchQueryBuilder("title", "hello world"));
        double small = benchNsPerOp("small-bool-3clause", smallBool());
        double deep = benchNsPerOp("deep-bool-~50leaf", deepBool());
        // sanity: all should be well under a microsecond-scale ceiling for the leaf case; we just assert they ran
        // and are finite (the real output is the logged ns/op numbers).
        assertTrue("leaf overhead should be tiny (<2000 ns/op even on a slow box)", leaf < 2000.0);
        assertTrue(small > 0 && deep > 0);
    }
}
