/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.resolver;

import org.apache.lucene.search.Query;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * POC marker query for the Resolver framework (Phase 1).
 *
 * <p>This query is NOT executed at the shard level. It is a coordinator-level marker that carries
 * a hybrid-search intent (a list of sub-queries plus RRF fusion parameters). The companion
 * {@link ResolverProcessor} search request processor detects this query on the coordinator, fires
 * the sub-queries as parallel independent searches (MultiSearch), fuses the globally-merged results
 * with Reciprocal Rank Fusion, and rewrites the request into a standard query carrying the fused
 * scores. By the time the query phase runs, this marker is gone.
 *
 * <p>This mirrors the "resolver / retriever" architecture: coordinator-level (global) RRF, then a
 * self-erasing rewrite into a standard query so explain / profile / aggregations work natively.
 *
 * <p>REST shape (the resolver request processor must be in the search pipeline):
 * <pre>
 * {
 *   "query": {
 *     "resolver": {
 *       "technique": "rrf",
 *       "queries": [
 *         { "match":  { "title": "neural search" } },
 *         { "neural": { "passage_embedding": { "query_text": "neural search", "model_id": "..." } } }
 *       ],
 *       "rank_constant": 60,
 *       "rank_window_size": 100
 *     }
 *   }
 * }
 * </pre>
 *
 * <p>POC scope: only the {@code rrf} technique is supported; production adds {@code linear},
 * {@code rescorer}, weighted fusion, PIT-based snapshot consistency, and rich per-leg explain.
 */
public class ResolverQueryBuilder extends AbstractQueryBuilder<ResolverQueryBuilder> {

    public static final String NAME = "resolver";

    public static final String TECHNIQUE_RRF = "rrf";
    public static final int DEFAULT_RANK_CONSTANT = 60;
    public static final int DEFAULT_RANK_WINDOW_SIZE = 100;
    public static final int MIN_SUB_QUERIES = 2;

    private static final String QUERIES_FIELD = "queries";
    private static final String TECHNIQUE_FIELD = "technique";
    private static final String RANK_CONSTANT_FIELD = "rank_constant";
    private static final String RANK_WINDOW_SIZE_FIELD = "rank_window_size";

    private final List<QueryBuilder> queries;
    private final String technique;
    private final int rankConstant;
    private final int rankWindowSize;

    public ResolverQueryBuilder(List<QueryBuilder> queries, String technique, int rankConstant, int rankWindowSize) {
        this.queries = queries == null ? new ArrayList<>() : queries;
        this.technique = technique;
        this.rankConstant = rankConstant;
        this.rankWindowSize = rankWindowSize;
    }

    public ResolverQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.queries = in.readNamedWriteableList(QueryBuilder.class);
        this.technique = in.readString();
        this.rankConstant = in.readVInt();
        this.rankWindowSize = in.readVInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableList(queries);
        out.writeString(technique);
        out.writeVInt(rankConstant);
        out.writeVInt(rankWindowSize);
    }

    public List<QueryBuilder> queries() {
        return queries;
    }

    public String technique() {
        return technique;
    }

    public int rankConstant() {
        return rankConstant;
    }

    public int rankWindowSize() {
        return rankWindowSize;
    }

    public static ResolverQueryBuilder fromXContent(XContentParser parser) throws IOException {
        List<QueryBuilder> queries = new ArrayList<>();
        String technique = TECHNIQUE_RRF;
        int rankConstant = DEFAULT_RANK_CONSTANT;
        int rankWindowSize = DEFAULT_RANK_WINDOW_SIZE;
        float boost = DEFAULT_BOOST;
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (QUERIES_FIELD.equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        queries.add(parseInnerQueryBuilder(parser));
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] unknown array field [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (RANK_CONSTANT_FIELD.equals(currentFieldName)) {
                    rankConstant = parser.intValue();
                } else if (RANK_WINDOW_SIZE_FIELD.equals(currentFieldName)) {
                    rankWindowSize = parser.intValue();
                } else if (TECHNIQUE_FIELD.equals(currentFieldName)) {
                    technique = parser.text();
                } else if (BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] unknown field [" + currentFieldName + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] unexpected token [" + token + "]");
            }
        }

        if (queries.size() < MIN_SUB_QUERIES) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "[%s] requires at least %d sub-queries in [%s]", NAME, MIN_SUB_QUERIES, QUERIES_FIELD)
            );
        }
        if (technique == null || technique.equalsIgnoreCase(TECHNIQUE_RRF) == false) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "[%s] POC only supports technique [%s], got [%s]", NAME, TECHNIQUE_RRF, technique)
            );
        }
        if (rankConstant <= 0) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "[%s] %s must be > 0", NAME, RANK_CONSTANT_FIELD));
        }
        if (rankWindowSize <= 0) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "[%s] %s must be > 0", NAME, RANK_WINDOW_SIZE_FIELD));
        }

        ResolverQueryBuilder queryBuilder = new ResolverQueryBuilder(
            queries,
            technique.toLowerCase(Locale.ROOT),
            rankConstant,
            rankWindowSize
        );
        queryBuilder.boost(boost);
        queryBuilder.queryName(queryName);
        return queryBuilder;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray(QUERIES_FIELD);
        for (QueryBuilder query : queries) {
            query.toXContent(builder, params);
        }
        builder.endArray();
        builder.field(TECHNIQUE_FIELD, technique);
        builder.field(RANK_CONSTANT_FIELD, rankConstant);
        builder.field(RANK_WINDOW_SIZE_FIELD, rankWindowSize);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        // A resolver is coordinator-level orchestration; it must never reach a shard. If it does,
        // the resolver request processor was not applied to the search pipeline.
        throw new IllegalStateException(
            "["
                + NAME
                + "] query must be processed by the '"
                + ResolverProcessor.TYPE
                + "' search request processor on the coordinator. Add it to your search pipeline "
                + "(for example: POST /index/_search?search_pipeline=resolver_pipeline) so the resolver "
                + "can orchestrate its sub-queries before the query phase."
        );
    }

    @Override
    protected boolean doEquals(ResolverQueryBuilder other) {
        return Objects.equals(queries, other.queries)
            && Objects.equals(technique, other.technique)
            && rankConstant == other.rankConstant
            && rankWindowSize == other.rankWindowSize;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(queries, technique, rankConstant, rankWindowSize);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
