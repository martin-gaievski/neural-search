/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.Query;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.neuralsearch.processor.HybridFusedAggregationsResponseProcessor;
import org.opensearch.neuralsearch.processor.HybridFusedProfileResponseProcessor;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.PipelinedRequest;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.common.SetOnce;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.InnerHitContextBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryCoordinatorContext;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.index.query.QueryBuilderVisitor;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;

import static org.opensearch.neuralsearch.common.MinClusterVersionUtil.isClusterOnOrAfterMinReqVersionForPaginationInHybridQuery;
import static org.opensearch.neuralsearch.common.MinClusterVersionUtil.isClusterOnOrAfterMinReqVersionForFusedModeInHybridQuery;

/**
 * Class abstract creation of a Query type "hybrid". Hybrid query will allow execution of multiple sub-queries and
 * collects score for each of those sub-query.
 */
@Log4j2
@Getter
@Setter
@Accessors(chain = true, fluent = true)
@NoArgsConstructor
public final class HybridQueryBuilder extends AbstractQueryBuilder<HybridQueryBuilder> {
    public static final String NAME = "hybrid";

    private static final ParseField QUERIES_FIELD = new ParseField("queries");
    private static final ParseField FILTER_FIELD = new ParseField("filter");
    private static final ParseField PAGINATION_DEPTH_FIELD = new ParseField("pagination_depth");
    private static final ParseField MODE_FIELD = new ParseField("mode");
    private static final ParseField RANK_WINDOW_SIZE_FIELD = new ParseField("rank_window_size");
    private static final ParseField FUSION_FIELD = new ParseField("fusion");

    private final List<QueryBuilder> queries = new ArrayList<>();

    private Integer paginationDepth;

    /**
     * Combination mode. {@link Mode#PIPELINE} (default) is classic hybrid: the query builds a Lucene {@link HybridQuery}
     * and a normalization search pipeline combines the per-sub-query scores between the query and fetch phases.
     * {@link Mode#FUSED} self-erases at the coordinator into a standard query (see {@link #doRewrite}); it reads the
     * SAME normalization/combination config from the attached search pipeline, so an existing hybrid user adopts it by
     * adding one token with zero migration.
     */
    private Mode mode = Mode.PIPELINE;

    /** Fused-mode candidate window: how many top docs each leg contributes to fusion. Ignored in pipeline mode. */
    private Integer rankWindowSize;

    /**
     * Fused-mode inline fusion config (Option X precedence step 1): the raw {@code fusion} block from the query body,
     * mirroring the normalization-processor JSON shape verbatim ({@code normalization}/{@code combination} clauses).
     * When present it wins over the attached search pipeline. Null = fall back to the pipeline (Option P path).
     */
    private Map<String, Object> fusion;

    /**
     * Transient result of the fused-mode coordinator-rewrite orchestration: the standard query this query self-erases
     * into once the leg {@code MultiSearch} completes. Populated by the async action registered in {@link #doRewrite};
     * read on the next rewrite round. NEVER serialized and excluded from {@link #doEquals}/{@link #doHashCode}.
     */
    private java.util.function.Supplier<QueryBuilder> fusedSupplier;

    public static final int MAX_NUMBER_OF_SUB_QUERIES = 5;
    private static final int LOWER_BOUND_OF_PAGINATION_DEPTH = 0;
    private static final int DEFAULT_RANK_WINDOW_SIZE = 100;

    /** Hybrid query combination mode. */
    public enum Mode {
        /** Classic hybrid: Lucene HybridQuery + normalization search pipeline (default; byte-identical to pre-mode). */
        PIPELINE,
        /** Coordinator self-erase into a standard query; reads fusion config from the attached pipeline (zero-migration). */
        FUSED;

        static Mode fromString(String value) {
            if (value == null) {
                return PIPELINE;
            }
            switch (value.toLowerCase(Locale.ROOT)) {
                case "pipeline":
                    return PIPELINE;
                case "fused":
                    return FUSED;
                default:
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "[%s] query [%s] must be one of [pipeline, fused], got [%s]", NAME, "mode", value)
                    );
            }
        }

        String wireValue() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    // Error message templates for reuse across REST and gRPC paths
    public static final String ERROR_MSG_QUERIES_REQUIRED = "[%s] requires 'queries' field with at least one clause";
    public static final String ERROR_MSG_MAX_QUERIES_EXCEEDED = "Number of sub-queries exceeds maximum supported by [%s] query";
    public static final String ERROR_MSG_BOOST_NOT_SUPPORTED = "[%s] query does not support [%s]";
    public static final String ERROR_MSG_FILTER_MUST_BE_QUERY_OBJECT = "[%s] query's [%s] field must be a query object";

    public HybridQueryBuilder(StreamInput in) throws IOException {
        super(in);
        queries.addAll(readQueries(in));
        if (isClusterOnOrAfterMinReqVersionForPaginationInHybridQuery()) {
            paginationDepth = in.readOptionalInt();
        }
        // Fused-mode fields are gated: only read them when every node in the cluster can write them, else the stream
        // would misalign. mode defaults to PIPELINE (classic, byte-identical to the pre-mode wire form).
        if (isClusterOnOrAfterMinReqVersionForFusedModeInHybridQuery()) {
            mode = Mode.fromString(in.readOptionalString());
            rankWindowSize = in.readOptionalInt();
            if (in.readBoolean()) {
                fusion = in.readMap();
            }
        }
    }

    /**
     * Serialize this query object into input stream
     * @param out stream that we'll be used for serialization
     * @throws IOException
     */
    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        writeQueries(out, queries);
        if (isClusterOnOrAfterMinReqVersionForPaginationInHybridQuery()) {
            out.writeOptionalInt(paginationDepth);
        }
        if (isClusterOnOrAfterMinReqVersionForFusedModeInHybridQuery()) {
            out.writeOptionalString(mode == null ? null : mode.wireValue());
            out.writeOptionalInt(rankWindowSize);
            out.writeBoolean(fusion != null);
            if (fusion != null) {
                out.writeMap(fusion);
            }
        }
    }

    /**
     * Add one sub-query
     * @param queryBuilder
     * @return
     */
    public HybridQueryBuilder add(QueryBuilder queryBuilder) {
        if (queryBuilder == null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "inner %s query clause cannot be null", NAME));
        }
        queries.add(queryBuilder);
        return this;
    }

    /**
     * Function to support filter on HybridQueryBuilder filter.
     * If the filter is null, then we do nothing and return.
     * Otherwise, we push down the filter to queries list.
     * @param filter the filter parameter
     * @return HybridQueryBuilder itself
     */
    public QueryBuilder filter(QueryBuilder filter) {
        if (validateFilterParams(filter) == false) {
            return this;
        }
        ListIterator<QueryBuilder> iterator = queries.listIterator();
        while (iterator.hasNext()) {
            QueryBuilder query = iterator.next();
            // set the query again because query.filter(filter) can return new query.
            iterator.set(query.filter(filter));
        }
        return this;
    }

    /**
     * Create builder object with a content of this hybrid query
     * @param builder
     * @param params
     * @throws IOException
     */
    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray(QUERIES_FIELD.getPreferredName());
        for (QueryBuilder queryBuilder : queries) {
            queryBuilder.toXContent(builder, params);
        }
        builder.endArray();
        // TODO https://github.com/opensearch-project/neural-search/issues/1097
        if (Objects.nonNull(paginationDepth)) {
            builder.field(PAGINATION_DEPTH_FIELD.getPreferredName(), paginationDepth);
        }
        if (mode == Mode.FUSED) {
            builder.field(MODE_FIELD.getPreferredName(), mode.wireValue());
            if (Objects.nonNull(rankWindowSize)) {
                builder.field(RANK_WINDOW_SIZE_FIELD.getPreferredName(), rankWindowSize);
            }
            if (Objects.nonNull(fusion)) {
                builder.field(FUSION_FIELD.getPreferredName(), fusion);
            }
        }
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    /** Fused-mode candidate window (how many top docs each leg contributes), defaulting when unset. */
    private int effectiveRankWindowSize() {
        return rankWindowSize == null ? DEFAULT_RANK_WINDOW_SIZE : rankWindowSize;
    }

    /**
     * Create query object for current hybrid query using shard context
     * @param queryShardContext context object that used to create hybrid query
     * @return hybrid query object
     * @throws IOException
     */
    @Override
    protected Query doToQuery(QueryShardContext queryShardContext) throws IOException {
        // Fused mode self-erases into a standard query at the coordinator rewrite and must never reach a shard. If it
        // does, the coordinator rewrite did not run — fail loudly rather than silently building the pipeline-mode query.
        if (mode == Mode.FUSED) {
            throw new IllegalStateException(
                String.format(
                    Locale.ROOT,
                    "[%s] query mode=fused is coordinator-only: it must self-erase into a standard query during the "
                        + "coordinator rewrite (see doRewrite) and must not reach a shard",
                    NAME
                )
            );
        }
        Collection<Query> queryCollection = toQueries(queries, queryShardContext);
        if (queryCollection.isEmpty()) {
            return Queries.newMatchNoDocsQuery(String.format(Locale.ROOT, "no clauses for %s query", NAME));
        }
        validatePaginationDepth(paginationDepth, queryShardContext);
        HybridQueryContext hybridQueryContext = HybridQueryContext.builder().paginationDepth(paginationDepth).build();
        return new HybridQuery(queryCollection, hybridQueryContext);
    }

    /**
     * Creates HybridQueryBuilder from xContent.
     * Example of a json for Hybrid Query:
     * {
     *      "query": {
     *          "hybrid": {
     *              "queries": [
     *                  {
     *                      "neural": {
     *                          "text_knn": {
     *                                    "query_text": "Hello world",
     *                                    "model_id": "dcsdcasd",
     *                                    "k": 10
     *                                }
     *                            }
     *                   },
     *                   {
     *                      "term": {
     *                          "text": "keyword"
     *                       }
     *                    }
     *               ],
     *               "filter": {
     *                  "term": {
     *                      "text": "keyword"
     *                  }
     *               }
     *          }
     *     }
     * }
     *
     * To combine multiple filter clauses, wrap them in a bool query:
     * {
     *     "filter": {
     *         "bool": {
     *             "must": [
     *                 {"term": {"field1": "value1"}},
     *                 {"term": {"field2": "value2"}}
     *             ]
     *         }
     *     }
     * }
     *
     * @param parser parser that has been initialized with the query content
     * @return new instance of HybridQueryBuilder
     * @throws IOException
     */
    public static HybridQueryBuilder fromXContent(XContentParser parser) throws IOException {
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        Integer paginationDepth = null;
        Mode mode = Mode.PIPELINE;
        Integer rankWindowSize = null;
        Map<String, Object> fusion = null;
        final List<QueryBuilder> queries = new ArrayList<>();
        QueryBuilder filter = null;
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (QUERIES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queries.add(parseInnerQueryBuilder(parser));
                } else if (FILTER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    filter = parseInnerQueryBuilder(parser);
                } else if (FUSION_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    fusion = parser.map();
                } else {
                    throwUnsupportedFieldParsingException(parser, currentFieldName);
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (QUERIES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    while (token != XContentParser.Token.END_ARRAY) {
                        if (queries.size() == MAX_NUMBER_OF_SUB_QUERIES) {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                String.format(Locale.ROOT, ERROR_MSG_MAX_QUERIES_EXCEEDED, NAME)
                            );
                        }
                        queries.add(parseInnerQueryBuilder(parser));
                        token = parser.nextToken();
                    }
                } else if (FILTER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    throwUnsupportedFilterParsingException(parser);
                } else {
                    throwUnsupportedFieldParsingException(parser, currentFieldName);
                }
            } else {
                if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                    // regular boost functionality is not supported, user should use score normalization methods to manipulate with scores
                    if (boost != DEFAULT_BOOST) {
                        log.error("[{}] query does not support provided value {} for [{}]", NAME, boost, BOOST_FIELD);
                        throw new ParsingException(parser.getTokenLocation(), "[{}] query does not support [{}]", NAME, BOOST_FIELD);
                    }
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else if (PAGINATION_DEPTH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    paginationDepth = parser.intValue();
                } else if (MODE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    mode = Mode.fromString(parser.text());
                } else if (RANK_WINDOW_SIZE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    rankWindowSize = parser.intValue();
                } else if (FILTER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    throwUnsupportedFilterParsingException(parser);
                } else {
                    throwUnsupportedFieldParsingException(parser, currentFieldName);
                }
            }
        }

        if (queries.isEmpty()) {
            throw new ParsingException(parser.getTokenLocation(), String.format(Locale.ROOT, ERROR_MSG_QUERIES_REQUIRED, NAME));
        }

        validateModeParams(parser, mode, paginationDepth, rankWindowSize);
        if (mode != Mode.FUSED && fusion != null) {
            throw new ParsingException(
                parser.getTokenLocation(),
                String.format(Locale.ROOT, "[%s] query [%s] is only supported in mode [fused]", NAME, FUSION_FIELD.getPreferredName())
            );
        }

        HybridQueryBuilder compoundQueryBuilder = new HybridQueryBuilder();
        compoundQueryBuilder.queryName(queryName);
        compoundQueryBuilder.boost(boost);
        compoundQueryBuilder.mode(mode);
        compoundQueryBuilder.rankWindowSize(rankWindowSize);
        compoundQueryBuilder.fusion(fusion);
        if (isClusterOnOrAfterMinReqVersionForPaginationInHybridQuery()) {
            compoundQueryBuilder.paginationDepth(paginationDepth);
        }

        boolean hasInnerHits = false;
        for (QueryBuilder query : queries) {
            if (filter == null) {
                compoundQueryBuilder.add(query);
            } else {
                compoundQueryBuilder.add(query.filter(filter));
            }

            // Check if children have inner hits for stats
            if (hasInnerHits == false) {
                Map<String, InnerHitContextBuilder> innerHits = new HashMap<>();
                InnerHitContextBuilder.extractInnerHits(query, innerHits);
                hasInnerHits = innerHits.isEmpty() == false;
            }
        }

        boolean hasFilter = filter != null;
        boolean hasPagination = paginationDepth != null;
        updateQueryStats(hasFilter, hasPagination, hasInnerHits);
        return compoundQueryBuilder;
    }

    protected QueryBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
        if (mode == Mode.FUSED) {
            return doRewriteFused(queryShardContext);
        }
        HybridQueryBuilder newBuilder = new HybridQueryBuilder();
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
            if (isClusterOnOrAfterMinReqVersionForPaginationInHybridQuery()) {
                newBuilder.paginationDepth(paginationDepth);
            }
            return newBuilder;
        } else {
            return this;
        }
    }

    /**
     * Fused-mode coordinator self-erase. Runs ONLY at the coordinator rewrite (where
     * {@link QueryRewriteContext#convertToCoordinatorContext()} is non-null); on the shards it is a no-op so
     * {@link #doToQuery} stays the safety net. Two-pass lifecycle driven by {@code Rewriteable.rewriteAndFetch}:
     * <ol>
     *   <li><b>Round 1</b>: resolve the fusion config from the attached search pipeline (same source as classic hybrid),
     *       fire the sub-queries as a parallel {@code MultiSearch} via {@link QueryRewriteContext#registerAsyncAction},
     *       and return a copy carrying a {@link SetOnce}-backed supplier.</li>
     *   <li><b>Round 2</b>: return the fused standard query ({@link HybridFusionQuery} or {@code match_none}).</li>
     * </ol>
     * Because {@code bool}/{@code dis_max}/{@code function_score} recurse rewrite into their children, a nested fused
     * hybrid self-orchestrates from here too.
     */
    private QueryBuilder doRewriteFused(QueryRewriteContext queryRewriteContext) throws IOException {
        // Round 2: the async self-erase already produced the standard query — swap to it (or stay put until it lands).
        if (fusedSupplier != null) {
            QueryBuilder fused = fusedSupplier.get();
            return fused == null ? this : fused;
        }
        QueryCoordinatorContext coordinatorContext = queryRewriteContext.convertToCoordinatorContext();
        if (coordinatorContext == null) {
            return this;
        }
        if ((coordinatorContext.getSearchRequest() instanceof SearchRequest) == false) {
            return this;
        }
        SearchRequest searchRequest = (SearchRequest) coordinatorContext.getSearchRequest();
        // Option X precedence: an inline `fusion` block on the query body wins; else resolve the config from the
        // attached search pipeline (inline body / named param / index default), reproducing core's precedence. Fail
        // fast if neither yields a normalization/score-ranker config rather than emitting unfused scores.
        FusionSpec fusion = this.fusion != null ? FusionSpec.fromInlineFusion(this.fusion) : FusionConfigResolver.resolve(searchRequest);
        if (fusion == null) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "[%s] query mode=fused requires a normalization or score-ranker processor in the attached search "
                        + "pipeline (inline body, ?search_pipeline=, or index.search.default_pipeline), or an inline fusion "
                        + "block; none was found",
                    NAME
                )
            );
        }
        // The Tail decision is depth-independent (evaluated inside buildFusedQuery from the request source), so the
        // fused query keeps accurate totals/aggregations whether this hybrid is top-level or nested.
        int window = effectiveRankWindowSize();
        List<QueryBuilder> legs = queries;
        // When the outer request profiles, profile the legs too and capture their per-sub-query profiles below — the
        // self-erased outer query only profiles the constant_score(ids) Top + Tail filter, never the sub-query scoring.
        boolean profile = searchRequest.source() != null && searchRequest.source().profile();
        SetOnce<QueryBuilder> fused = new SetOnce<>();
        queryRewriteContext.registerAsyncAction(
            (client, listener) -> client.multiSearch(
                HybridFusionOrchestrator.buildLegMultiSearch(searchRequest, legs, window, profile),
                ActionListener.wrap(multiSearchResponse -> {
                    try {
                        fused.set(
                            HybridFusionOrchestrator.buildFusedQuery(searchRequest.source(), multiSearchResponse, legs, fusion, window)
                        );
                        if (profile) {
                            captureLegProfiles(searchRequest, multiSearchResponse);
                        }
                        captureAggregationLeg(searchRequest, multiSearchResponse, legs.size());
                        listener.onResponse(null);
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }, listener::onFailure)
            )
        );
        HybridQueryBuilder marker = new HybridQueryBuilder();
        for (QueryBuilder query : queries) {
            marker.add(query);
        }
        marker.queryName(queryName);
        marker.boost(boost);
        marker.mode(Mode.FUSED);
        marker.rankWindowSize(rankWindowSize);
        marker.fusion(this.fusion);
        marker.fusedSupplier = fused::get;
        return marker;
    }

    /**
     * Store the per-leg profiles captured from a profiled leg MultiSearch into the request-scoped
     * {@link PipelineProcessingContext}, so the system-generated {@link HybridFusedProfileResponseProcessor} can merge
     * them into the final response's profile section. The coordinator rewrite runs against the {@link PipelinedRequest},
     * whose context is the same instance the response processor later reads (mirrors how the explanation payload is
     * threaded through the context). No-op when the request is not pipelined, has no context, or no leg was profiled.
     */
    /**
     * Stash the <b>aggregation leg</b>'s aggregations and total hits into the request-scoped context so the
     * system-generated {@link HybridFusedAggregationsResponseProcessor} can swap them into the final response. The agg
     * leg runs the sub-queries in place per shard (see {@code HybridFusionOrchestrator#buildAggregationLegSource}), so
     * its aggregation match set is the true leg union — this is what removes the KNN multi-shard and {@code min_score}
     * aggregation undercounts of the Tail-based path. No-op when the request is not pipelined, has no context, or no
     * aggregation leg ran (no aggregations on the request, or the leg failed) — in which case the Tail-based
     * aggregations stand.
     */
    private static void captureAggregationLeg(SearchRequest searchRequest, MultiSearchResponse multiSearchResponse, int legCount) {
        if ((searchRequest instanceof PipelinedRequest) == false) {
            return;
        }
        PipelineProcessingContext requestContext = ((PipelinedRequest) searchRequest).getPipelineProcessingContext();
        if (requestContext == null) {
            return;
        }
        MultiSearchResponse.Item aggItem = HybridFusionOrchestrator.aggregationLegResponse(multiSearchResponse, legCount);
        if (aggItem == null) {
            return;
        }
        org.opensearch.action.search.SearchResponse aggResponse = aggItem.getResponse();
        if (aggResponse.getAggregations() instanceof org.opensearch.search.aggregations.InternalAggregations) {
            requestContext.setAttribute(HybridFusedAggregationsResponseProcessor.AGG_LEG_AGGS_CONTEXT_KEY, aggResponse.getAggregations());
        }
        if (aggResponse.getHits() != null && aggResponse.getHits().getTotalHits() != null) {
            requestContext.setAttribute(
                HybridFusedAggregationsResponseProcessor.AGG_LEG_TOTAL_HITS_CONTEXT_KEY,
                aggResponse.getHits().getTotalHits()
            );
        }
    }

    private static void captureLegProfiles(SearchRequest searchRequest, MultiSearchResponse multiSearchResponse) {
        if ((searchRequest instanceof PipelinedRequest) == false) {
            return;
        }
        PipelineProcessingContext requestContext = ((PipelinedRequest) searchRequest).getPipelineProcessingContext();
        if (requestContext == null) {
            return;
        }
        Map<String, ProfileShardResult> legProfiles = HybridFusionOrchestrator.collectLegProfiles(multiSearchResponse);
        if (legProfiles.isEmpty() == false) {
            requestContext.setAttribute(HybridFusedProfileResponseProcessor.LEG_PROFILES_CONTEXT_KEY, legProfiles);
        }
    }

    /**
     * Indicates whether some other QueryBuilder object of the same type is "equal to" this one.
     * @param obj
     * @return true if objects are equal
     */
    @Override
    protected boolean doEquals(HybridQueryBuilder obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        EqualsBuilder equalsBuilder = new EqualsBuilder();
        equalsBuilder.append(queries, obj.queries);
        equalsBuilder.append(paginationDepth, obj.paginationDepth);
        equalsBuilder.append(mode, obj.mode);
        equalsBuilder.append(rankWindowSize, obj.rankWindowSize);
        equalsBuilder.append(fusion, obj.fusion);
        return equalsBuilder.isEquals();
    }

    /**
     * Create hash code for current hybrid query builder object
     * @return hash code
     */
    @Override
    protected int doHashCode() {
        return Objects.hash(queries, paginationDepth, mode, rankWindowSize, fusion);
    }

    /**
     * Returns the name of the writeable object
     * @return
     */
    @Override
    public String getWriteableName() {
        return NAME;
    }

    private List<QueryBuilder> readQueries(StreamInput in) throws IOException {
        return in.readNamedWriteableList(QueryBuilder.class);
    }

    private void writeQueries(StreamOutput out, List<? extends QueryBuilder> queries) throws IOException {
        out.writeNamedWriteableList(queries);
    }

    private Collection<Query> toQueries(Collection<QueryBuilder> queryBuilders, QueryShardContext context) throws QueryShardException {
        List<Query> queries = queryBuilders.stream().map(qb -> {
            try {
                return qb.rewrite(context).toQuery(context);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());
        return queries;
    }

    /**
     * Parse-time validation of the mode / fused-mode fields:
     * <ul>
     *   <li>{@code rank_window_size} is a fused-mode-only knob — reject it in pipeline mode.</li>
     *   <li>{@code pagination_depth} is consumed only by the shard-side Lucene {@link HybridQuery} that fused mode never
     *       builds, so it is inert in fused mode — reject it rather than silently ignore.</li>
     *   <li>{@code rank_window_size} must be positive.</li>
     * </ul>
     */
    private static void validateModeParams(XContentParser parser, Mode mode, Integer paginationDepth, Integer rankWindowSize) {
        if (mode != Mode.FUSED && rankWindowSize != null) {
            throw new ParsingException(
                parser.getTokenLocation(),
                String.format(
                    Locale.ROOT,
                    "[%s] query [%s] is only supported in mode [fused]",
                    NAME,
                    RANK_WINDOW_SIZE_FIELD.getPreferredName()
                )
            );
        }
        if (mode == Mode.FUSED && paginationDepth != null) {
            throw new ParsingException(
                parser.getTokenLocation(),
                String.format(
                    Locale.ROOT,
                    "[%s] query [%s] is not supported in mode [fused]; fused mode pages over its [%s] window instead",
                    NAME,
                    PAGINATION_DEPTH_FIELD.getPreferredName(),
                    RANK_WINDOW_SIZE_FIELD.getPreferredName()
                )
            );
        }
        if (rankWindowSize != null && rankWindowSize <= 0) {
            throw new ParsingException(
                parser.getTokenLocation(),
                String.format(Locale.ROOT, "[%s] query [%s] must be greater than 0", NAME, RANK_WINDOW_SIZE_FIELD.getPreferredName())
            );
        }
    }

    private static void validatePaginationDepth(final Integer paginationDepth, final QueryShardContext queryShardContext) {
        if (Objects.isNull(paginationDepth)) {
            return;
        }
        if (paginationDepth < LOWER_BOUND_OF_PAGINATION_DEPTH) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "pagination_depth should be greater than %s", LOWER_BOUND_OF_PAGINATION_DEPTH)
            );
        }
        // compare pagination depth with OpenSearch setting index.max_result_window
        // see https://opensearch.org/docs/latest/install-and-configure/configuring-opensearch/index-settings/
        int maxResultWindowIndexSetting = queryShardContext.getIndexSettings().getMaxResultWindow();
        if (paginationDepth > maxResultWindowIndexSetting) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "pagination_depth should be less than or equal to %s setting",
                    IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey()
                )
            );
        }
    }

    /**
     * visit method to parse the HybridQueryBuilder by a visitor
     */
    @Override
    public void visit(QueryBuilderVisitor visitor) {
        visitor.accept(this);
        // getChildVisitor of NeuralSearchQueryVisitor return this.
        // therefore any argument can be passed. Here we have used Occcur.MUST as an argument.
        QueryBuilderVisitor subVisitor = visitor.getChildVisitor(Occur.MUST);
        for (QueryBuilder subQueryBuilder : queries) {
            subQueryBuilder.visit(subVisitor);
        }
    }

    /**
     * Extracts the inner hits from the hybrid query tree structure.
     * While it extracts inner hits, child inner hits are inlined into the inner hit builder they belong to.
     * This implementation handles inner hits for all sub-queries within the hybrid query.
     *
     * @param innerHits the map to collect inner hit contexts, where the key is the inner hit name
     *                   and the value is the corresponding inner hit context builder
     */
    @Override
    protected void extractInnerHitBuilders(Map<String, InnerHitContextBuilder> innerHits) {
        for (QueryBuilder queryBuilder : queries) {
            InnerHitContextBuilder.extractInnerHits(queryBuilder, innerHits);
        }
    }

    public static void updateQueryStats(boolean hasFilter, boolean hasPagination, boolean hasInnerHits) {
        EventStatsManager.increment(EventStatName.HYBRID_QUERY_REQUESTS);
        if (hasFilter) {
            EventStatsManager.increment(EventStatName.HYBRID_QUERY_FILTER_REQUESTS);
        }
        if (hasPagination) {
            EventStatsManager.increment(EventStatName.HYBRID_QUERY_PAGINATION_REQUESTS);
        }
        if (hasInnerHits) {
            EventStatsManager.increment(EventStatName.HYBRID_QUERY_INNER_HITS_REQUESTS);
        }
    }

    private static void throwUnsupportedFilterParsingException(XContentParser parser) {
        throw new ParsingException(
            parser.getTokenLocation(),
            String.format(Locale.ROOT, ERROR_MSG_FILTER_MUST_BE_QUERY_OBJECT, NAME, FILTER_FIELD.getPreferredName())
        );
    }

    private static void throwUnsupportedFieldParsingException(XContentParser parser, String fieldName) {
        log.error(String.format(Locale.ROOT, "[%s] query does not support [%s]", NAME, fieldName));
        throw new ParsingException(parser.getTokenLocation(), String.format(Locale.ROOT, "Field is not supported by [%s] query", NAME));
    }
}
