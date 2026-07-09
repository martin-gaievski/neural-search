/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.resolver;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.opensearch.common.document.DocumentField;
import org.opensearch.search.SearchHit;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.FetchSubPhaseProcessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.neuralsearch.resolver.ResolverOrchestrator.SUB_QUERY_SCORES_FIELD_NAME;

/**
 * POC — data-node fetch sub-phase for the resolver's STANDARD (shard-fanout) path. It attaches the raw per-leg
 * scores that {@link RankDocsQueryBuilder} carried into the executed query (via {@link RawScoreCarryingQuery}).
 *
 * <p><b>Multi-node correctness</b>: the payload rode the (NamedWriteable) {@code RankDocsQueryBuilder} to this data
 * node and is read here in the SAME JVM that runs the fetch — there is no coordinator-only side channel (the flaw
 * that reverted hybrid PR #1369). Association is by the hit's stable {@code _id} ({@link SearchHit#getId()}), which
 * is valid at fetch time on the owning node.
 *
 * <p>Registered via {@code getFetchSubPhases}; it is a no-op (returns {@code null}) for any query that does not
 * contain a {@link RawScoreCarryingQuery}, so it costs nothing for non-resolver / opt-out requests.
 */
public class RawSubQueryScoresFetchSubPhase implements FetchSubPhase {

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) throws IOException {
        Map<String, float[]> rawById = findRawScores(fetchContext.query());
        if (rawById == null) {
            return null; // not a resolver-standard-path request with sub_query_scores enabled — skip entirely
        }
        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {
                // no per-segment state; association is purely by _id
            }

            @Override
            public void process(HitContext hitContext) {
                SearchHit hit = hitContext.hit();
                String id = hit.getId();
                if (id == null) {
                    return;
                }
                float[] raw = rawById.get(id);
                if (raw == null) {
                    return;
                }
                List<Object> values = new ArrayList<>(raw.length);
                for (float score : raw) {
                    values.add(score);
                }
                hit.setDocumentField(SUB_QUERY_SCORES_FIELD_NAME, new DocumentField(SUB_QUERY_SCORES_FIELD_NAME, values));
            }
        };
    }

    /** Recursively locate the {@link RawScoreCarryingQuery} in the executed query tree (core may wrap the resolver
     *  query in a BooleanQuery for aliases/DLS/nested), returning its payload or null if absent. */
    private static Map<String, float[]> findRawScores(Query query) {
        if (query instanceof RawScoreCarryingQuery carrier) {
            return carrier.rawScoresById();
        }
        if (query instanceof BoostQuery boostQuery) {
            return findRawScores(boostQuery.getQuery());
        }
        if (query instanceof ConstantScoreQuery constantScoreQuery) {
            return findRawScores(constantScoreQuery.getQuery());
        }
        if (query instanceof BooleanQuery booleanQuery) {
            for (BooleanClause clause : booleanQuery.clauses()) {
                Map<String, float[]> found = findRawScores(clause.query());
                if (found != null) {
                    return found;
                }
            }
        }
        return null;
    }
}
