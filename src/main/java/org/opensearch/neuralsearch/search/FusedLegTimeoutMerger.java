/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

/**
 * Coordinator-side {@code timed_out} for fused ({@code fusion}) hybrid queries: makes a leg that a soft {@code timeout}
 * truncated visible on the response.
 *
 * <p>A fused hybrid propagates the request's {@code timeout} to each of its legs, and a leg that exceeds it returns the
 * candidates it had collected rather than failing. The fusion window is then narrower than a complete run's, so the
 * ranking is computed from an incomplete candidate set — and without this, the response says nothing about it. Classic
 * hybrid has no such gap: its sub-queries run shard-side inside the search phases, so core's own counters see the
 * truncation and set the response's {@code timed_out} itself. Fused mode's legs are a separate in-process
 * {@code MultiSearch} issued during the coordinator rewrite, and the per-leg responses that know are consumed there and
 * discarded.
 *
 * <p>Request-scoped, like {@code FusedLegProfileMerger}: created by {@link HybridQuerySearchRequestFilter}, handed to every
 * fused {@code hybrid} the request fans out, published to by the leg fan-out callback, read once when the response comes
 * back. Unlike that one it is attached whether or not the request asked to be profiled — an incomplete answer has to be
 * reported as one either way, and the profile entry's per-leg {@code timed_out} under {@code debug} is a diagnostic for
 * someone already looking, not a flag a client can act on.
 *
 * <p>Across legs and across hybrids the signal is a logical OR, and only ever sets the flag: it means "some part of this
 * answer is incomplete", which is what core's own {@code timed_out} means, and a leg that finished cannot make an
 * incomplete answer complete. The two neighbouring per-leg signals are deliberately <b>not</b> reported, because they
 * cannot be composed the same way — N legs search the same shards while a response carries exactly one set of
 * shard counters and one {@code terminated_early}, so there is no union of them that is true of the response as a whole.
 *
 * <p>Published on the leg-MultiSearch response thread and read on the response-listener thread; {@code volatile} for that
 * hand-off, which is ordered anyway by the rewrite completing before the search phases start.
 */
public final class FusedLegTimeoutMerger {

    /** Whether any leg of any fused hybrid in this request reported a soft timeout. Never cleared once set. */
    private volatile boolean anyLegTimedOut;

    /** A handle for one fused hybrid, handed to its {@code HybridQueryBuilder} and published to once its legs are back. */
    public interface LegTimeoutConsumer {
        void accept(boolean legTimedOut);
    }

    /**
     * The consumer to attach to a fused hybrid. Publishing {@code false} is the normal case and records nothing, so a
     * hybrid whose legs all completed cannot undo a sibling hybrid's truncation.
     */
    public LegTimeoutConsumer consumer() {
        return legTimedOut -> {
            if (legTimedOut) {
                anyLegTimedOut = true;
            }
        };
    }

    public boolean anyLegTimedOut() {
        return anyLegTimedOut;
    }
}
