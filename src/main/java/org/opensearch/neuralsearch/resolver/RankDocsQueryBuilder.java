/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.resolver;

import org.apache.lucene.search.Query;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Internal query produced by the {@link ResolverProcessor} after coordinator-level RRF fusion.
 * Realizes the "Top + Tail" pattern (cf. Elasticsearch's {@code RankDocsQuery}) using standard
 * OpenSearch query builders:
 *
 * <ul>
 *   <li><b>Top</b> — one {@code constant_score(ids: [id])^rrfScore} clause per ranked document.
 *       These are the scoring {@code should} clauses, so the fused window is returned in RRF order.</li>
 *   <li><b>Tail</b> — a {@code bool{ should: [sourceQuery...] }} added as a non-scoring
 *       {@code filter}. It matches the FULL set of documents any sub-query matched, so
 *       {@code total_hits} and aggregations cover all matches (not just the ranked window),
 *       and the highlighter can extract the sub-queries' terms.</li>
 * </ul>
 *
 * <p>Because the tail is a filter, non-window documents match with score 0 and sort below the
 * RRF-scored window — so a request with {@code size <= rank_window_size} returns exactly the fused
 * window, while aggregations/total-hits see everything.
 *
 * <p>This query is created internally and is not parseable from a search request.
 */
public class RankDocsQueryBuilder extends AbstractQueryBuilder<RankDocsQueryBuilder> {

    public static final String NAME = "rank_docs";

    private final String[] ids;
    private final float[] scores;
    private final List<QueryBuilder> sourceQueries;

    public RankDocsQueryBuilder(String[] ids, float[] scores, List<QueryBuilder> sourceQueries) {
        this.ids = ids;
        this.scores = scores;
        this.sourceQueries = sourceQueries == null ? new ArrayList<>() : sourceQueries;
    }

    public RankDocsQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.ids = in.readStringArray();
        this.scores = in.readFloatArray();
        this.sourceQueries = in.readNamedWriteableList(QueryBuilder.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeStringArray(ids);
        out.writeFloatArray(scores);
        out.writeNamedWriteableList(sourceQueries);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        boolean changed = false;
        List<QueryBuilder> rewritten = new ArrayList<>(sourceQueries.size());
        for (QueryBuilder q : sourceQueries) {
            QueryBuilder r = q.rewrite(queryRewriteContext);
            rewritten.add(r);
            changed |= r != q;
        }
        if (changed) {
            RankDocsQueryBuilder rewrittenBuilder = new RankDocsQueryBuilder(ids, scores, rewritten);
            rewrittenBuilder.boost(boost());
            rewrittenBuilder.queryName(queryName());
            return rewrittenBuilder;
        }
        return this;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        BoolQueryBuilder composite = new BoolQueryBuilder();
        // Top: ranked docs carry their pre-computed RRF scores (scoring clauses).
        for (int i = 0; i < ids.length; i++) {
            composite.should(new ConstantScoreQueryBuilder(new IdsQueryBuilder().addIds(ids[i])).boost(scores[i]));
        }
        // Tail: all source-query matches as a non-scoring filter -> total hits and aggregations
        // cover the full match set, and highlighting has the sub-queries' terms available.
        if (sourceQueries.isEmpty() == false) {
            BoolQueryBuilder tail = new BoolQueryBuilder();
            for (QueryBuilder q : sourceQueries) {
                tail.should(q);
            }
            composite.filter(tail);
        }
        return composite.toQuery(context);
    }

    @Override
    protected boolean doEquals(RankDocsQueryBuilder other) {
        return Arrays.equals(ids, other.ids) && Arrays.equals(scores, other.scores) && Objects.equals(sourceQueries, other.sourceQueries);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(Arrays.hashCode(ids), Arrays.hashCode(scores), sourceQueries);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        // Internal query; representation is informational only.
        builder.startObject(NAME);
        builder.field("rank_docs_count", ids.length);
        builder.endObject();
    }

    public static RankDocsQueryBuilder fromXContent(XContentParser parser) {
        throw new UnsupportedOperationException(
            "[" + NAME + "] is created internally by the resolver processor and cannot be parsed from a request"
        );
    }
}
