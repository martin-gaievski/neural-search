/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.lucene.search.Query;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * Query builder for the "fusion" query type. Performs shard-level RRF (Reciprocal Rank Fusion)
 * normalization and combination, producing standard flat TopDocs. No search pipeline needed.
 */
@Log4j2
@Getter
@NoArgsConstructor
public final class FusionQueryBuilder extends AbstractQueryBuilder<FusionQueryBuilder> {

    public static final String NAME = "fusion";
    private static final ParseField QUERIES_FIELD = new ParseField("queries");
    public static final int MAX_NUMBER_OF_SUB_QUERIES = 3;

    private final List<QueryBuilder> queries = new ArrayList<>();

    public FusionQueryBuilder(StreamInput in) throws IOException {
        super(in);
        queries.addAll(in.readNamedWriteableList(QueryBuilder.class));
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableList(queries);
    }

    /**
     * Add a sub-query to this fusion query.
     */
    public FusionQueryBuilder add(QueryBuilder queryBuilder) {
        Objects.requireNonNull(queryBuilder, "inner fusion query clause cannot be null");
        queries.add(queryBuilder);
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray(QUERIES_FIELD.getPreferredName());
        for (QueryBuilder queryBuilder : queries) {
            queryBuilder.toXContent(builder, params);
        }
        builder.endArray();
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext queryShardContext) throws IOException {
        Collection<Query> queryCollection = toQueries(queries, queryShardContext);
        if (queryCollection.isEmpty()) {
            return Queries.newMatchNoDocsQuery(String.format(Locale.ROOT, "no clauses for %s query", NAME));
        }
        return new FusionQuery(new ArrayList<>(queryCollection));
    }

    /**
     * Parse fusion query from XContent.
     *
     * Example:
     * {
     *   "query": {
     *     "fusion": {
     *       "queries": [
     *         {"match": {"title": "neural search"}},
     *         {"term": {"category": "technology"}}
     *       ]
     *     }
     *   }
     * }
     */
    public static FusionQueryBuilder fromXContent(XContentParser parser) throws IOException {
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        final List<QueryBuilder> queries = new ArrayList<>();
        String queryName = null;
        String currentFieldName = null;
        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (QUERIES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    while (token != XContentParser.Token.END_ARRAY) {
                        if (queries.size() == MAX_NUMBER_OF_SUB_QUERIES) {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                String.format(Locale.ROOT, "max number of sub-queries exceeded for [%s] query", NAME)
                            );
                        }
                        queries.add(parseInnerQueryBuilder(parser));
                        token = parser.nextToken();
                    }
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        String.format(Locale.ROOT, "field [%s] is not supported by [%s] query", currentFieldName, NAME)
                    );
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (QUERIES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queries.add(parseInnerQueryBuilder(parser));
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        String.format(Locale.ROOT, "field [%s] is not supported by [%s] query", currentFieldName, NAME)
                    );
                }
            } else {
                if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        String.format(Locale.ROOT, "field [%s] is not supported by [%s] query", currentFieldName, NAME)
                    );
                }
            }
        }

        if (queries.isEmpty()) {
            throw new ParsingException(
                parser.getTokenLocation(),
                String.format(Locale.ROOT, "[%s] requires 'queries' field with at least one clause", NAME)
            );
        }

        FusionQueryBuilder fusionQueryBuilder = new FusionQueryBuilder();
        fusionQueryBuilder.queryName(queryName);
        fusionQueryBuilder.boost(boost);
        for (QueryBuilder query : queries) {
            fusionQueryBuilder.add(query);
        }
        return fusionQueryBuilder;
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
        FusionQueryBuilder newBuilder = new FusionQueryBuilder();
        boolean changed = false;
        for (QueryBuilder query : queries) {
            QueryBuilder result = query.rewrite(queryShardContext);
            if (result != query) {
                changed = true;
            }
            newBuilder.add(result);
        }
        if (changed) {
            newBuilder.queryName(queryName);
            newBuilder.boost(boost);
            return newBuilder;
        }
        return this;
    }

    @Override
    protected boolean doEquals(FusionQueryBuilder other) {
        return Objects.equals(queries, other.queries);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(queries);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    private Collection<Query> toQueries(Collection<QueryBuilder> queryBuilders, QueryShardContext context) throws QueryShardException {
        return queryBuilders.stream().map(qb -> {
            try {
                return qb.rewrite(context).toQuery(context);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }
}
