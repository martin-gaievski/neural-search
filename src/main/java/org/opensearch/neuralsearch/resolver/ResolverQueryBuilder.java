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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * POC marker query for the Resolver framework (Phase 1).
 *
 * <p>Coordinator-level marker carrying a hybrid-search intent: a list of sub-queries plus a
 * (normalization, combination) fusion spec. The {@link ResolverActionFilter} (pipeline-free) or the
 * optional {@link ResolverProcessor} detects it on the coordinator, fires the sub-queries as parallel
 * independent searches, fuses the globally-merged results, and rewrites the request into a standard
 * query. By the time the query phase runs, this marker is gone.
 *
 * <p>Supported fusion pairs:
 * <ul>
 *   <li><b>RRF</b> — {@code combination.technique = rrf} (rank-based; no normalization).</li>
 *   <li><b>min_max + arithmetic mean</b> — {@code normalization.technique = min_max} +
 *       {@code combination.technique = arithmetic_mean} (score-based; optional per-leg weights).</li>
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

    public static final int DEFAULT_RANK_CONSTANT = 60;
    public static final int DEFAULT_RANK_WINDOW_SIZE = 100;
    public static final int MIN_SUB_QUERIES = 2;

    private static final String QUERIES_FIELD = "queries";
    private static final String TECHNIQUE_FIELD = "technique";
    private static final String RANK_CONSTANT_FIELD = "rank_constant";
    private static final String RANK_WINDOW_SIZE_FIELD = "rank_window_size";
    private static final String NORMALIZATION_FIELD = "normalization";
    private static final String COMBINATION_FIELD = "combination";
    private static final String PARAMETERS_FIELD = "parameters";
    private static final String WEIGHTS_FIELD = "weights";

    private final List<QueryBuilder> queries;
    private final String technique;        // combination technique: rrf | arithmetic_mean
    private final String normalization;    // normalization technique: none | min_max
    private final int rankConstant;        // RRF only
    private final int rankWindowSize;
    private final float[] weights;         // arithmetic_mean only; empty => unweighted

    /** Legacy 4-arg constructor (RRF, no normalization, unweighted). */
    public ResolverQueryBuilder(List<QueryBuilder> queries, String technique, int rankConstant, int rankWindowSize) {
        this(queries, technique, NORMALIZATION_NONE, rankConstant, rankWindowSize, new float[0]);
    }

    public ResolverQueryBuilder(
        List<QueryBuilder> queries,
        String technique,
        String normalization,
        int rankConstant,
        int rankWindowSize,
        float[] weights
    ) {
        this.queries = queries == null ? new ArrayList<>() : queries;
        this.technique = technique;
        this.normalization = normalization == null ? NORMALIZATION_NONE : normalization;
        this.rankConstant = rankConstant;
        this.rankWindowSize = rankWindowSize;
        this.weights = weights == null ? new float[0] : weights;
    }

    public ResolverQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.queries = in.readNamedWriteableList(QueryBuilder.class);
        this.technique = in.readString();
        this.normalization = in.readString();
        this.rankConstant = in.readVInt();
        this.rankWindowSize = in.readVInt();
        this.weights = in.readFloatArray();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableList(queries);
        out.writeString(technique);
        out.writeString(normalization);
        out.writeVInt(rankConstant);
        out.writeVInt(rankWindowSize);
        out.writeFloatArray(weights);
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

    @SuppressWarnings("unchecked")
    public static ResolverQueryBuilder fromXContent(XContentParser parser) throws IOException {
        List<QueryBuilder> queries = new ArrayList<>();
        String combination = null;               // set from flat "technique" or "combination.technique"
        String normalization = null;             // set from "normalization.technique"
        int rankConstant = DEFAULT_RANK_CONSTANT;
        int rankWindowSize = DEFAULT_RANK_WINDOW_SIZE;
        float[] weights = new float[0];
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

        validate(queries, combination, normalization, rankConstant, rankWindowSize, weights, parser);

        ResolverQueryBuilder queryBuilder = new ResolverQueryBuilder(
            queries,
            combination,
            normalization,
            rankConstant,
            rankWindowSize,
            weights
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
        if (NORMALIZATION_NONE.equals(normalization) == false && NORMALIZATION_MIN_MAX.equals(normalization) == false) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "[%s] POC supports normalization techniques [%s, %s], got [%s]",
                    NAME,
                    NORMALIZATION_NONE,
                    NORMALIZATION_MIN_MAX,
                    normalization
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

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        // A resolver is coordinator-level orchestration; it must never reach a shard.
        throw new IllegalStateException(
            "["
                + NAME
                + "] query must be the top-level query (or nested in a bool) so the coordinator can orchestrate "
                + "its sub-queries before the query phase; it is handled by the resolver ActionFilter on the "
                + "coordinator and must not reach a shard."
        );
    }

    @Override
    protected boolean doEquals(ResolverQueryBuilder other) {
        return Objects.equals(queries, other.queries)
            && Objects.equals(technique, other.technique)
            && Objects.equals(normalization, other.normalization)
            && rankConstant == other.rankConstant
            && rankWindowSize == other.rankWindowSize
            && Arrays.equals(weights, other.weights);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(queries, technique, normalization, rankConstant, rankWindowSize, Arrays.hashCode(weights));
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
