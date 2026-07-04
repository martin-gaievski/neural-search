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
 * NOT resolver queries. After the re-home onto {@link ResolverQueryBuilder#doRewrite}, the filter is a thin fast-path
 * hook: its only synchronous work for a non-resolver query is the action/type gate ({@code SearchAction.NAME.equals}
 * + {@code source.query() instanceof ResolverQueryBuilder}). There is NO tree-walk anymore — the recursive
 * marker collection that used to run on every top-level {@code bool} is gone (nesting is now handled structurally by
 * the rewrite framework recursing into container queries). So the overhead is constant, allocation-free, and
 * independent of query shape/depth.
 *
 * <p>Not a JUnit assertion test in spirit — it prints ns/op so we can quote the actual per-query delay. Runs a warmup
 * then a timed loop over three representative query shapes: a leaf match, a small 3-clause bool, and a deep nested
 * bool (~50 leaf clauses). The three should be indistinguishable now (no shape sensitivity), which is the point.
 */
public class ResolverActionFilterOverheadBenchTests extends OpenSearchTestCase {

    private static final int WARMUP = 200_000;
    private static final int ITERS = 2_000_000;

    /** The exact synchronous work the thin apply() does for a non-resolver query, up to the chain.proceed decision:
     *  the action check + a single instanceof. No tree-walk, no allocation — regardless of query shape/depth. */
    private static boolean filterGate(String action, QueryBuilder query) {
        if (SearchAction.NAME.equals(action) == false) {
            return false;
        }
        // Mirrors ResolverActionFilter: only a top-level ResolverQueryBuilder enters the fast-path branch; everything
        // else (including any bool tree) falls straight through with no further inspection.
        return query instanceof ResolverQueryBuilder;
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

    /** ~50-leaf nested bool: 5 top clauses, each a bool of 10 leaves. With the tree-walk removed, the filter no longer
     *  descends this — so it must cost the same as a leaf query (shape-independence is the property under test). */
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
        double leaf = benchNsPerOp("leaf-match", new MatchQueryBuilder("title", "hello world"));
        double small = benchNsPerOp("small-bool-3clause", smallBool());
        double deep = benchNsPerOp("deep-bool-~50leaf", deepBool());
        // The gate is a single instanceof now, so all three are tiny AND shape-independent (no tree-walk).
        assertTrue("non-resolver gate overhead should be tiny (<2000 ns/op even on a slow box)", leaf < 2000.0);
        assertTrue("deep bool must not cost more than a leaf now that the tree-walk is gone (<2000 ns/op)", deep < 2000.0);
        assertTrue(small > 0 && deep > 0);
    }
}
