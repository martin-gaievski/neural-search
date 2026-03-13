/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

/**
 * Lucene Query implementation for the "fusion" query type. Holds sub-queries and delegates
 * to FusionQueryWeight for shard-level RRF scoring.
 */
public final class FusionQuery extends Query implements Iterable<Query> {

    private final List<Query> subQueries;

    public FusionQuery(List<Query> subQueries) {
        Objects.requireNonNull(subQueries, "sub-queries must not be null");
        if (subQueries.isEmpty()) {
            throw new IllegalArgumentException("sub-queries must not be empty");
        }
        this.subQueries = new ArrayList<>(subQueries);
    }

    public List<Query> getSubQueries() {
        return Collections.unmodifiableList(subQueries);
    }

    @Override
    public Iterator<Query> iterator() {
        return getSubQueries().iterator();
    }

    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder("FusionQuery(");
        for (int i = 0; i < subQueries.size(); i++) {
            Query subquery = subQueries.get(i);
            buffer.append(subquery.toString(field));
            if (i < subQueries.size() - 1) {
                buffer.append(" | ");
            }
        }
        buffer.append(")");
        return buffer.toString();
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        if (subQueries.isEmpty()) {
            return new MatchNoDocsQuery("empty FusionQuery");
        }
        boolean changed = false;
        List<Query> rewrittenSubQueries = new ArrayList<>(subQueries.size());
        for (Query subQuery : subQueries) {
            Query rewritten = subQuery.rewrite(indexSearcher);
            if (rewritten != subQuery) {
                changed = true;
            }
            rewrittenSubQueries.add(rewritten);
        }
        if (changed) {
            return new FusionQuery(rewrittenSubQueries);
        }
        return super.rewrite(indexSearcher);
    }

    @Override
    public void visit(QueryVisitor queryVisitor) {
        QueryVisitor v = queryVisitor.getSubVisitor(BooleanClause.Occur.SHOULD, this);
        for (Query q : subQueries) {
            q.visit(v);
        }
    }

    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) && subQueries.equals(((FusionQuery) other).subQueries);
    }

    @Override
    public int hashCode() {
        return 31 * classHash() + subQueries.hashCode();
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new FusionQueryWeight(this, searcher, scoreMode, boost);
    }
}
