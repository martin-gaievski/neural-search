/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.resolver;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * POC — a transparent Lucene {@link Query} wrapper that behaves EXACTLY like its delegate (the resolver's Top+Tail
 * {@code BooleanQuery}) for scoring/matching, but additionally carries the raw per-leg scores keyed by {@code _id}.
 *
 * <p>Purpose: on the resolver's standard (shard-fanout) path the query self-erases into a {@link RankDocsQueryBuilder}
 * that is serialized to the data nodes and executed there; the raw scores must reach the DATA-NODE fetch phase to be
 * attached to hits. {@link org.opensearch.search.fetch.FetchContext#query()} exposes only the Lucene {@link Query},
 * not the builder, so we smuggle the payload on the Query itself. A {@code FetchSubPhase} walks the executed query
 * tree, finds this wrapper, and reads {@link #rawScoresById()} — a same-JVM read of state that was transported inside
 * the query object (NOT a coordinator-only side channel), so it is multi-node-safe.
 *
 * <p>All execution is delegated verbatim, so this wrapper does not change scoring, matching, or rewriting behavior.
 */
public final class RawScoreCarryingQuery extends Query {

    private final Query delegate;
    private final Map<String, float[]> rawScoresById;

    public RawScoreCarryingQuery(Query delegate, Map<String, float[]> rawScoresById) {
        this.delegate = Objects.requireNonNull(delegate);
        this.rawScoresById = rawScoresById;
    }

    public Map<String, float[]> rawScoresById() {
        return rawScoresById;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        // Return THIS (never unwrap) so the carrier stays the stable top-level query through IndexSearcher's rewrite
        // loop and is therefore visible to the fetch phase via FetchContext.query(). If we returned the rewritten
        // delegate (or a fresh wrapper), core's rewrite/optimization could collapse a transparent delegating query
        // and strip the payload (observed: some shards saw a bare TermQuery at fetch). The delegate is rewritten
        // lazily in createWeight instead, so scoring/matching are unchanged.
        return this;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return delegate.rewrite(searcher).createWeight(searcher, scoreMode, boost);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        delegate.visit(visitor);
    }

    @Override
    public String toString(String field) {
        return delegate.toString(field);
    }

    // Identity-based equality on purpose: two carriers with the SAME delegate but DIFFERENT payloads must NOT be
    // treated as equal, or the node query cache could substitute one for the other and strip/ swap the payload.
    @Override
    public boolean equals(Object obj) {
        return this == obj;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }
}
