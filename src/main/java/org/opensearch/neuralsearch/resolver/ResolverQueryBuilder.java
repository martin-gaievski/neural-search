/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.resolver;

import org.apache.lucene.search.Query;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.SetOnce;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryCoordinatorContext;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * POC marker query for the Resolver framework (Phase 1).
 *
 * <p>Coordinator-level marker carrying a hybrid-search intent: a list of sub-queries plus a
 * (normalization, combination) fusion spec. The {@link ResolverActionFilter} detects it on the
 * coordinator (pipeline-free), fires the sub-queries as parallel
 * independent searches, fuses the globally-merged results, and rewrites the request into a standard
 * query. By the time the query phase runs, this marker is gone.
 *
 * <p>Supported fusion pairs:
 * <ul>
 *   <li><b>RRF</b> — {@code combination.technique = rrf} (rank-based; no normalization).</li>
 *   <li><b>min_max + arithmetic mean</b> — {@code normalization.technique = min_max} +
 *       {@code combination.technique = arithmetic_mean} (score-based; optional per-leg weights).</li>
 *   <li><b>z_score + arithmetic mean</b> — {@code normalization.technique = z_score} +
 *       {@code combination.technique = arithmetic_mean} (POC v2 adaptive-fusion #1; DBSF-style per-query
 *       distribution normalization — each leg normalized by its own returned-score mean/std).</li>
 * </ul>
 *
 * <p>REST shape (Option B — mirrors the hybrid normalization/combination model and ES retrievers):
 * <pre>
 * // RRF
 * { "resolver": { "queries": [ ... ], "rank_window_size": 100,
 *                 "combination": { "technique": "rrf", "parameters": { "rank_constant": 60 } } } }
 *
 * // min_max + arithmetic mean (optionally weighted)
 * { "resolver": { "queries": [ ... ], "rank_window_size": 100,
 *                 "normalization": { "technique": "min_max" },
 *                 "combination":   { "technique": "arithmetic_mean", "parameters": { "weights": [0.6, 0.4] } } } }
 * </pre>
 *
 * <p>The legacy flat form ({@code "technique": "rrf", "rank_constant": 60}) is still accepted.
 */
public class ResolverQueryBuilder extends AbstractQueryBuilder<ResolverQueryBuilder> {

    public static final String NAME = "resolver";

    // Combination techniques
    public static final String TECHNIQUE_RRF = "rrf";
    public static final String TECHNIQUE_ARITHMETIC_MEAN = "arithmetic_mean";
    // Normalization techniques
    public static final String NORMALIZATION_NONE = "none";
    public static final String NORMALIZATION_MIN_MAX = "min_max";
    // z_score (DBSF-style, POC v2 adaptive-fusion #1): normalize each leg by its OWN returned-score distribution
    // (mean mu, sample std sigma) — a per-query, unsupervised, label-free normalizer. Unlike min_max (which is
    // range-sensitive to a single outlier), z_score adapts to each query's per-leg score spread. Only meaningful
    // with the score-based arithmetic_mean combination (rank-based RRF ignores scores).
    public static final String NORMALIZATION_Z_SCORE = "z_score";
    // Candidate-collection strategies (how the legs' results reach the coordinator for fusion)
    public static final String COLLECTION_COORDINATOR = "coordinator"; // each leg = one standalone search reduced to global top-K (default)
    public static final String COLLECTION_PER_SHARD = "per_shard";     // each leg collected per shard, fused over num_shards x depth
                                                                       // (min_max only)

    public static final int DEFAULT_RANK_CONSTANT = 60;
    public static final int DEFAULT_RANK_WINDOW_SIZE = 100;
    public static final int MIN_SUB_QUERIES = 2;
    /** Sentinel: candidate_depth defaults to rank_window_size when unset (mirrors hybrid using size when pagination_depth is unset). */
    public static final int CANDIDATE_DEPTH_UNSET = -1;

    private static final String QUERIES_FIELD = "queries";
    private static final String TECHNIQUE_FIELD = "technique";
    private static final String RANK_CONSTANT_FIELD = "rank_constant";
    private static final String RANK_WINDOW_SIZE_FIELD = "rank_window_size";
    private static final String NORMALIZATION_FIELD = "normalization";
    private static final String COMBINATION_FIELD = "combination";
    private static final String PARAMETERS_FIELD = "parameters";
    private static final String WEIGHTS_FIELD = "weights";
    private static final String COLLECTION_FIELD = "collection";
    private static final String CANDIDATE_DEPTH_FIELD = "candidate_depth";

    private final List<QueryBuilder> queries;
    private final String technique;        // combination technique: rrf | arithmetic_mean
    private final String normalization;    // normalization technique: none | min_max
    private final int rankConstant;        // RRF only
    private final int rankWindowSize;
    private final float[] weights;         // arithmetic_mean only; empty => unweighted
    private final String collection;       // candidate collection: coordinator | per_shard
    private final int candidateDepth;      // per-shard local top-K depth; CANDIDATE_DEPTH_UNSET => rankWindowSize

    /**
     * Transient result of the coordinator-rewrite async orchestration: the standard query this marker self-erases into
     * once the leg {@code MultiSearch} completes (a {@link RankDocsQueryBuilder}, or a {@code match_none} when the fused
     * set is empty). Populated by the async action registered in {@link #doRewrite}; read on the next rewrite round to
     * finish the self-erase. NEVER serialized (the wire form is always the parsed marker) and intentionally excluded
     * from {@link #doEquals}/{@link #doHashCode} identity — it is orchestration state, not part of the query's identity.
     */
    private final Supplier<QueryBuilder> fusedSupplier;

    /** Legacy 4-arg constructor (RRF, no normalization, unweighted, coordinator collection). */
    public ResolverQueryBuilder(List<QueryBuilder> queries, String technique, int rankConstant, int rankWindowSize) {
        this(queries, technique, NORMALIZATION_NONE, rankConstant, rankWindowSize, new float[0]);
    }

    /** 6-arg constructor (coordinator collection, default candidate_depth). */
    public ResolverQueryBuilder(
        List<QueryBuilder> queries,
        String technique,
        String normalization,
        int rankConstant,
        int rankWindowSize,
        float[] weights
    ) {
        this(queries, technique, normalization, rankConstant, rankWindowSize, weights, COLLECTION_COORDINATOR, CANDIDATE_DEPTH_UNSET);
    }

    public ResolverQueryBuilder(
        List<QueryBuilder> queries,
        String technique,
        String normalization,
        int rankConstant,
        int rankWindowSize,
        float[] weights,
        String collection,
        int candidateDepth
    ) {
        this(queries, technique, normalization, rankConstant, rankWindowSize, weights, collection, candidateDepth, null);
    }

    /** Full constructor incl. the transient {@link #fusedSupplier} (set only by {@link #doRewrite}'s async self-erase). */
    private ResolverQueryBuilder(
        List<QueryBuilder> queries,
        String technique,
        String normalization,
        int rankConstant,
        int rankWindowSize,
        float[] weights,
        String collection,
        int candidateDepth,
        Supplier<QueryBuilder> fusedSupplier
    ) {
        this.queries = queries == null ? new ArrayList<>() : queries;
        this.technique = technique;
        this.normalization = normalization == null ? NORMALIZATION_NONE : normalization;
        this.rankConstant = rankConstant;
        this.rankWindowSize = rankWindowSize;
        this.weights = weights == null ? new float[0] : weights;
        this.collection = collection == null ? COLLECTION_COORDINATOR : collection;
        this.candidateDepth = candidateDepth;
        this.fusedSupplier = fusedSupplier;
    }

    public ResolverQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.queries = in.readNamedWriteableList(QueryBuilder.class);
        this.technique = in.readString();
        this.normalization = in.readString();
        this.rankConstant = in.readVInt();
        this.rankWindowSize = in.readVInt();
        this.weights = in.readFloatArray();
        this.collection = in.readString();
        this.candidateDepth = in.readInt();
        this.fusedSupplier = null; // never serialized — the wire form is always the parsed marker
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableList(queries);
        out.writeString(technique);
        out.writeString(normalization);
        out.writeVInt(rankConstant);
        out.writeVInt(rankWindowSize);
        out.writeFloatArray(weights);
        out.writeString(collection);
        out.writeInt(candidateDepth);
    }

    public List<QueryBuilder> queries() {
        return queries;
    }

    public String technique() {
        return technique;
    }

    public String normalization() {
        return normalization;
    }

    public int rankConstant() {
        return rankConstant;
    }

    public int rankWindowSize() {
        return rankWindowSize;
    }

    public float[] weights() {
        return weights;
    }

    public String collection() {
        return collection;
    }

    /** Per-shard candidate depth; falls back to {@link #rankWindowSize()} when unset. */
    public int candidateDepth() {
        return candidateDepth == CANDIDATE_DEPTH_UNSET ? rankWindowSize : candidateDepth;
    }

    /** True when this marker requests per-shard candidate collection (only honoured for min_max+arithmetic_mean). */
    public boolean isPerShardCollection() {
        return COLLECTION_PER_SHARD.equals(collection) && TECHNIQUE_ARITHMETIC_MEAN.equals(technique);
    }

    @SuppressWarnings("unchecked")
    public static ResolverQueryBuilder fromXContent(XContentParser parser) throws IOException {
        List<QueryBuilder> queries = new ArrayList<>();
        String combination = null;               // set from flat "technique" or "combination.technique"
        String normalization = null;             // set from "normalization.technique"
        int rankConstant = DEFAULT_RANK_CONSTANT;
        int rankWindowSize = DEFAULT_RANK_WINDOW_SIZE;
        float[] weights = new float[0];
        String collection = COLLECTION_COORDINATOR;
        int candidateDepth = CANDIDATE_DEPTH_UNSET;
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
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (NORMALIZATION_FIELD.equals(currentFieldName)) {
                    Object t = parser.map().get(TECHNIQUE_FIELD);
                    if (t != null) {
                        normalization = t.toString();
                    }
                } else if (COMBINATION_FIELD.equals(currentFieldName)) {
                    Map<String, Object> combinationMap = parser.map();
                    Object t = combinationMap.get(TECHNIQUE_FIELD);
                    if (t != null) {
                        combination = t.toString();
                    }
                    Object params = combinationMap.get(PARAMETERS_FIELD);
                    if (params instanceof Map) {
                        Map<String, Object> p = (Map<String, Object>) params;
                        if (p.get(RANK_CONSTANT_FIELD) instanceof Number) {
                            rankConstant = ((Number) p.get(RANK_CONSTANT_FIELD)).intValue();
                        }
                        if (p.get(WEIGHTS_FIELD) instanceof List) {
                            List<?> w = (List<?>) p.get(WEIGHTS_FIELD);
                            weights = new float[w.size()];
                            for (int i = 0; i < w.size(); i++) {
                                weights[i] = ((Number) w.get(i)).floatValue();
                            }
                        }
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] unknown object field [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (RANK_CONSTANT_FIELD.equals(currentFieldName)) {
                    rankConstant = parser.intValue();
                } else if (RANK_WINDOW_SIZE_FIELD.equals(currentFieldName)) {
                    rankWindowSize = parser.intValue();
                } else if (COLLECTION_FIELD.equals(currentFieldName)) {
                    collection = parser.text();
                } else if (CANDIDATE_DEPTH_FIELD.equals(currentFieldName)) {
                    candidateDepth = parser.intValue();
                } else if (TECHNIQUE_FIELD.equals(currentFieldName)) {
                    combination = parser.text();
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

        if (combination == null) {
            combination = TECHNIQUE_RRF;
        }
        combination = combination.toLowerCase(Locale.ROOT);
        // arithmetic_mean without an explicit normalization defaults to min_max (raw-score AM is unsafe across leg scales).
        if (normalization == null) {
            normalization = TECHNIQUE_ARITHMETIC_MEAN.equals(combination) ? NORMALIZATION_MIN_MAX : NORMALIZATION_NONE;
        }
        normalization = normalization.toLowerCase(Locale.ROOT);
        collection = collection == null ? COLLECTION_COORDINATOR : collection.toLowerCase(Locale.ROOT);

        validate(queries, combination, normalization, rankConstant, rankWindowSize, weights, collection, candidateDepth, parser);

        ResolverQueryBuilder queryBuilder = new ResolverQueryBuilder(
            queries,
            combination,
            normalization,
            rankConstant,
            rankWindowSize,
            weights,
            collection,
            candidateDepth
        );
        queryBuilder.boost(boost);
        queryBuilder.queryName(queryName);
        return queryBuilder;
    }

    private static void validate(
        List<QueryBuilder> queries,
        String combination,
        String normalization,
        int rankConstant,
        int rankWindowSize,
        float[] weights,
        String collection,
        int candidateDepth,
        XContentParser parser
    ) {
        if (queries.size() < MIN_SUB_QUERIES) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "[%s] requires at least %d sub-queries in [%s]", NAME, MIN_SUB_QUERIES, QUERIES_FIELD)
            );
        }
        if (TECHNIQUE_RRF.equals(combination) == false && TECHNIQUE_ARITHMETIC_MEAN.equals(combination) == false) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "[%s] POC supports combination techniques [%s, %s], got [%s]",
                    NAME,
                    TECHNIQUE_RRF,
                    TECHNIQUE_ARITHMETIC_MEAN,
                    combination
                )
            );
        }
        if (NORMALIZATION_NONE.equals(normalization) == false
            && NORMALIZATION_MIN_MAX.equals(normalization) == false
            && NORMALIZATION_Z_SCORE.equals(normalization) == false) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "[%s] POC supports normalization techniques [%s, %s, %s], got [%s]",
                    NAME,
                    NORMALIZATION_NONE,
                    NORMALIZATION_MIN_MAX,
                    NORMALIZATION_Z_SCORE,
                    normalization
                )
            );
        }
        // z_score normalizes by each leg's score distribution, so it is only meaningful with the score-based
        // arithmetic_mean combination (rank-based RRF ignores scores). Reject the incoherent pairing rather than
        // silently ignoring it. (Mirrors OpenSearch's "z_score supports only arithmetic_mean".)
        if (NORMALIZATION_Z_SCORE.equals(normalization) && TECHNIQUE_ARITHMETIC_MEAN.equals(combination) == false) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "[%s] normalization [%s] is only supported with combination technique [%s], got [%s]",
                    NAME,
                    NORMALIZATION_Z_SCORE,
                    TECHNIQUE_ARITHMETIC_MEAN,
                    combination
                )
            );
        }
        if (rankConstant <= 0) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "[%s] %s must be > 0", NAME, RANK_CONSTANT_FIELD));
        }
        if (rankWindowSize <= 0) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "[%s] %s must be > 0", NAME, RANK_WINDOW_SIZE_FIELD));
        }
        if (weights.length > 0 && weights.length != queries.size()) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "[%s] %s length (%d) must match the number of sub-queries (%d)",
                    NAME,
                    WEIGHTS_FIELD,
                    weights.length,
                    queries.size()
                )
            );
        }
        if (COLLECTION_COORDINATOR.equals(collection) == false && COLLECTION_PER_SHARD.equals(collection) == false) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "[%s] %s must be one of [%s, %s], got [%s]",
                    NAME,
                    COLLECTION_FIELD,
                    COLLECTION_COORDINATOR,
                    COLLECTION_PER_SHARD,
                    collection
                )
            );
        }
        if (candidateDepth != CANDIDATE_DEPTH_UNSET && candidateDepth <= 0) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "[%s] %s must be > 0", NAME, CANDIDATE_DEPTH_FIELD));
        }
        // Coherence: per_shard collection is only meaningful for the score-based min_max+arithmetic_mean fusion
        // (RRF is rank-based and already at parity), and candidate_depth only applies to per_shard collection.
        // Reject incoherent combinations rather than silently ignoring the knob.
        if (COLLECTION_PER_SHARD.equals(collection) && TECHNIQUE_ARITHMETIC_MEAN.equals(combination) == false) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "[%s] %s=%s is only supported with combination technique [%s], got [%s]",
                    NAME,
                    COLLECTION_FIELD,
                    COLLECTION_PER_SHARD,
                    TECHNIQUE_ARITHMETIC_MEAN,
                    combination
                )
            );
        }
        if (candidateDepth != CANDIDATE_DEPTH_UNSET && COLLECTION_PER_SHARD.equals(collection) == false) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "[%s] %s only applies when %s=%s",
                    NAME,
                    CANDIDATE_DEPTH_FIELD,
                    COLLECTION_FIELD,
                    COLLECTION_PER_SHARD
                )
            );
        }
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray(QUERIES_FIELD);
        for (QueryBuilder query : queries) {
            query.toXContent(builder, params);
        }
        builder.endArray();
        builder.field(RANK_WINDOW_SIZE_FIELD, rankWindowSize);
        if (COLLECTION_COORDINATOR.equals(collection) == false) {
            builder.field(COLLECTION_FIELD, collection);
        }
        if (candidateDepth != CANDIDATE_DEPTH_UNSET) {
            builder.field(CANDIDATE_DEPTH_FIELD, candidateDepth);
        }
        if (NORMALIZATION_NONE.equals(normalization) == false) {
            builder.startObject(NORMALIZATION_FIELD).field(TECHNIQUE_FIELD, normalization).endObject();
        }
        builder.startObject(COMBINATION_FIELD);
        builder.field(TECHNIQUE_FIELD, technique);
        builder.startObject(PARAMETERS_FIELD);
        if (TECHNIQUE_RRF.equals(technique)) {
            builder.field(RANK_CONSTANT_FIELD, rankConstant);
        }
        if (weights.length > 0) {
            builder.array(WEIGHTS_FIELD, weights);
        }
        builder.endObject();
        builder.endObject();
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    /**
     * Coordinator-level self-erasing orchestration (mirrors {@code NeuralQueryBuilder}/{@code NeuralSparseQueryBuilder}'s
     * async-inference rewrite). Runs ONLY at the coordinator rewrite (where {@link QueryRewriteContext#convertToCoordinatorContext()}
     * is non-null); on the shards it is a no-op so {@link #doToQuery} stays the safety net for a marker that slips through.
     *
     * <p>Two-pass lifecycle, driven by {@code Rewriteable.rewriteAndFetch}:
     * <ol>
     *   <li><b>Round 1</b> (supplier absent): fire the legs as a parallel {@code MultiSearch} via
     *       {@link QueryRewriteContext#registerAsyncAction} and return a NEW marker carrying a {@link SetOnce}-backed
     *       supplier. Returning a distinct object drives another rewrite round after the async action drains.</li>
     *   <li><b>Round 2</b> (supplier present and populated): return the fused standard query
     *       ({@link RankDocsQueryBuilder} Top-only, or {@code match_none}); the marker is now gone from the tree.</li>
     * </ol>
     *
     * <p>Because the container query builders ({@code bool}/{@code dis_max}/{@code function_score}/{@code constant_score})
     * recurse {@code rewrite()} into their children, a nested marker self-orchestrates from here too — no bespoke tree-walk.
     * The Top-only shape (no Tail) means an enclosing {@code bool} filter intersects the fused window at the query phase
     * (fuse-then-filter), rather than being pushed into each leg. Accurate-totals / aggregations / explain / highlight on a
     * TOP-LEVEL resolver are served by the {@link RankDocsQueryBuilder} Tail: those requests skip the fast path, and the
     * conditional Tail is added below whenever the request needs the full match set.
     *
     * <p>Idempotency / termination: once the {@code SetOnce} is populated the returned query is a plain standard builder
     * (never a resolver marker), so it cannot loop; a still-pending supplier returns {@code this} to make no progress until
     * the async round completes. Both keep the whole-tree rewrite within {@code Rewriteable.MAX_REWRITE_ROUNDS}.
     */
    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        // Round 2: the async self-erase already produced the standard query — swap to it (or stay put until it lands).
        if (fusedSupplier != null) {
            QueryBuilder fused = fusedSupplier.get();
            return fused == null ? this : fused;
        }
        // Only orchestrate at the coordinator rewrite; on shards / base contexts this is a no-op.
        QueryCoordinatorContext coordinatorContext = queryRewriteContext.convertToCoordinatorContext();
        if (coordinatorContext == null) {
            return this;
        }
        // The runtime request on the _search path is a PipelinedRequest (extends SearchRequest); _explain/_validate pass
        // other IndicesRequest types. Guard the cast — only a SearchRequest exposes source()/routing()/preference().
        if ((coordinatorContext.getSearchRequest() instanceof SearchRequest) == false) {
            return this;
        }
        SearchRequest searchRequest = (SearchRequest) coordinatorContext.getSearchRequest();
        // Whether this marker is the whole query (=> conditional Tail for accurate totals/aggs/etc.) or nested inside a
        // container (=> Top-only; an enclosing filter intersects at the query phase). Determined by reference identity
        // against the request's top-level query on this first round, before any rewrite has replaced it.
        boolean topLevel = searchRequest.source() != null && searchRequest.source().query() == this;

        // Compute the per-shard collection plan ONCE and thread it into both the build and the reduce (single-plan
        // invariant), exactly as the ActionFilter path did.
        ResolverOrchestrator.CollectionPlan plan = ResolverOrchestrator.planCollection(searchRequest, this);
        SetOnce<QueryBuilder> fused = new SetOnce<>();
        ResolverQueryBuilder self = this;
        queryRewriteContext.registerAsyncAction(
            (client, listener) -> client.multiSearch(
                ResolverOrchestrator.buildLegMultiSearch(searchRequest, self, plan),
                ActionListener.wrap(multiSearchResponse -> {
                    try {
                        fused.set(ResolverOrchestrator.buildFusedQuery(searchRequest.source(), multiSearchResponse, self, plan, topLevel));
                        listener.onResponse(null);
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }, listener::onFailure)
            )
        );
        return new ResolverQueryBuilder(
            queries,
            technique,
            normalization,
            rankConstant,
            rankWindowSize,
            weights,
            collection,
            candidateDepth,
            fused::get
        );
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        // A resolver is coordinator-level orchestration; it self-erases into a standard query at the coordinator rewrite
        // and must never reach a shard. If it does, the coordinator rewrite did not run (e.g. a code path that bypasses
        // Rewriteable.rewriteAndFetch) — fail loudly rather than silently mis-scoring.
        throw new IllegalStateException(
            "["
                + NAME
                + "] query is coordinator-only: it must be self-erased into a standard query during the coordinator "
                + "rewrite (see doRewrite) and must not reach a shard."
        );
    }

    @Override
    protected boolean doEquals(ResolverQueryBuilder other) {
        return Objects.equals(queries, other.queries)
            && Objects.equals(technique, other.technique)
            && Objects.equals(normalization, other.normalization)
            && rankConstant == other.rankConstant
            && rankWindowSize == other.rankWindowSize
            && Arrays.equals(weights, other.weights)
            && Objects.equals(collection, other.collection)
            && candidateDepth == other.candidateDepth;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(
            queries,
            technique,
            normalization,
            rankConstant,
            rankWindowSize,
            Arrays.hashCode(weights),
            collection,
            candidateDepth
        );
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
