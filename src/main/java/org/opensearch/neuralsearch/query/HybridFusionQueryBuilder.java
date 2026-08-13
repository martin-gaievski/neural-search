/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.search.Query;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.InnerHitContextBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;

/**
 * Internal query produced by {@link HybridQueryBuilder} when the resolver (fused) mode is enabled via the {@code fusion}
 * parameter, after coordinator-level fusion. It realizes the "Top + Tail" pattern using standard OpenSearch query
 * builders, so all downstream search features (sort, collapse, aggregations, pagination, highlight, min_score) operate
 * on a plain query with no hybrid-specific special-casing:
 *
 * <ul>
 *   <li><b>Top</b> — one {@code constant_score(...)^fusedScore} clause per ranked document. These are the scoring
 *       {@code should} clauses, so the fused window is returned in fused-score order.</li>
 *   <li><b>Tail</b> — a {@code bool{ should: [tailQuery...] }} added as a non-scoring {@code filter}. It matches the
 *       full set of documents any sub-query matched, so {@code total_hits} and aggregations cover all matches (not just
 *       the ranked window) and the highlighter has the sub-queries' terms available. Included only when the request
 *       needs the full match set; omitted (Top-only) for plain top-K.</li>
 * </ul>
 *
 * <p><b>Document identity.</b> A ranked doc is addressed by {@code _id}, optionally qualified by its {@code _index}.
 * {@code _id} is unique only within an index, so for a multi-index search two different documents can share an
 * {@code _id}; qualifying the Top clause with an {@code _index} filter keeps them distinct and stops one doc's fused
 * score from being applied to the other. Index qualification is opt-in per instance ({@code indices} may be null) so a
 * single-index search keeps the leaner {@code ids}-only clause and its original performance profile.
 *
 * <p><b>inner_hits registration is decoupled from the Tail.</b> inner_hits are computed in the fetch phase from the
 * registered {@link InnerHitContextBuilder}s (per returned parent doc), not from whatever ran in the query phase — so a
 * leg only needs to be <i>registered</i>, never <i>executed</i>, for its inner_hits to be returned. {@code tailQueries}
 * is therefore what gets executed, while {@code innerHitsQueries} is what gets registered; the two are populated
 * independently, which lets a Top-only query still return leg inner_hits without paying to re-run the legs.
 *
 * <p>This query is created internally by the coordinator self-erase and is never parseable from a search request. Its
 * wire form needs no version gate: the whole query type is new in the same version that introduced fused mode, and a
 * node predating that version cannot resolve this {@code NamedWriteable} name at all — so it fails on the query name
 * long before reading any field.
 */
public class HybridFusionQueryBuilder extends AbstractQueryBuilder<HybridFusionQueryBuilder> {

    public static final String NAME = "hybrid_fusion";

    /** Metadata field used to disambiguate same-{@code _id} docs across indices. */
    private static final String INDEX_FIELD = "_index";

    private final String[] ids;
    /** Parallel to {@code ids}; null when the search targets a single index and needs no qualification. */
    private final String[] indices;
    private final float[] scores;
    /** Legs executed in the query phase as the non-scoring Tail (empty for a Top-only query). */
    private final List<QueryBuilder> tailQueries;
    /** Legs registered for fetch-phase inner_hits extraction; never executed by this query. */
    private final List<QueryBuilder> innerHitsQueries;

    public HybridFusionQueryBuilder(
        String[] ids,
        String[] indices,
        float[] scores,
        List<QueryBuilder> tailQueries,
        List<QueryBuilder> innerHitsQueries
    ) {
        this.ids = ids;
        this.indices = indices;
        this.scores = scores;
        this.tailQueries = Objects.isNull(tailQueries) ? new ArrayList<>() : tailQueries;
        this.innerHitsQueries = Objects.isNull(innerHitsQueries) ? new ArrayList<>() : innerHitsQueries;
    }

    /** Single-index convenience: no index qualification, and the Tail legs are also the inner_hits source. */
    public HybridFusionQueryBuilder(String[] ids, float[] scores, List<QueryBuilder> tailQueries) {
        this(ids, null, scores, tailQueries, tailQueries);
    }

    public HybridFusionQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.ids = in.readStringArray();
        this.indices = in.readOptionalStringArray();
        this.scores = in.readFloatArray();
        this.tailQueries = in.readNamedWriteableList(QueryBuilder.class);
        this.innerHitsQueries = in.readNamedWriteableList(QueryBuilder.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeStringArray(ids);
        out.writeOptionalStringArray(indices);
        out.writeFloatArray(scores);
        out.writeNamedWriteableList(tailQueries);
        out.writeNamedWriteableList(innerHitsQueries);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        boolean changed = false;
        List<QueryBuilder> rewrittenTail = new ArrayList<>(tailQueries.size());
        for (QueryBuilder q : tailQueries) {
            QueryBuilder r = q.rewrite(queryRewriteContext);
            rewrittenTail.add(r);
            changed |= r != q;
        }
        // The inner_hits sources must be rewritten too — they are not executed, but the fetch phase builds each
        // inner-hit context from the registered builder, so an un-rewritten builder would be a different definition.
        List<QueryBuilder> rewrittenInnerHits = new ArrayList<>(innerHitsQueries.size());
        for (QueryBuilder q : innerHitsQueries) {
            QueryBuilder r = q.rewrite(queryRewriteContext);
            rewrittenInnerHits.add(r);
            changed |= r != q;
        }
        if (changed) {
            HybridFusionQueryBuilder rewrittenBuilder = new HybridFusionQueryBuilder(
                ids,
                indices,
                scores,
                rewrittenTail,
                rewrittenInnerHits
            );
            rewrittenBuilder.boost(boost());
            rewrittenBuilder.queryName(queryName());
            return rewrittenBuilder;
        }
        return this;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        return buildSelfErasedQuery().toQuery(context);
    }

    /**
     * Build the self-erased {@code bool} query (Top + optional Tail) as a standard {@link BoolQueryBuilder}, before it
     * is compiled to a Lucene query. Kept separate from {@link #doToQuery} so the structural contract — one scoring
     * {@code should} per ranked id, and a single non-scoring {@code filter} Tail iff any Tail query is present — is
     * unit-testable without a shard context.
     */
    BoolQueryBuilder buildSelfErasedQuery() {
        // Top: constant_score(...)^fusedScore per ranked doc — the scoring should-clauses that return the fused window
        // in fused-score order.
        BoolQueryBuilder composite = new BoolQueryBuilder();
        for (int i = 0; i < ids.length; i++) {
            composite.should(new ConstantScoreQueryBuilder(rankedDocQuery(i)).boost(scores[i]));
        }
        // Tail: all leg matches as a non-scoring filter -> total hits and aggregations cover the full match set, and
        // highlighting has the sub-queries' terms available. Non-window docs match at score 0 and sort below the fused
        // window, so a request with size <= window_size returns exactly the fused window.
        if (tailQueries.isEmpty() == false) {
            BoolQueryBuilder tail = new BoolQueryBuilder();
            for (QueryBuilder q : tailQueries) {
                tail.should(q);
            }
            composite.filter(tail);
        }
        return composite;
    }

    /**
     * Address one ranked document: by {@code _id} alone, or {@code _id} intersected with its {@code _index} when the
     * fused set spans indices (where {@code _id} is not unique on its own).
     */
    private QueryBuilder rankedDocQuery(int position) {
        IdsQueryBuilder idQuery = new IdsQueryBuilder().addIds(ids[position]);
        if (Objects.isNull(indices) || Objects.isNull(indices[position])) {
            return idQuery;
        }
        return new BoolQueryBuilder().filter(idQuery).filter(new TermQueryBuilder(INDEX_FIELD, indices[position]));
    }

    /**
     * Recurse into the registered inner_hits sources so that inner_hits declared on a leg (e.g. a {@code nested} or
     * {@code has_child} sub-query) are registered and fetched per returned parent hit. Mirrors
     * {@link HybridQueryBuilder#extractInnerHitBuilders}. Without this override the self-erased query would silently drop
     * leg-level inner_hits, because {@link AbstractQueryBuilder}'s default implementation is a no-op and the coordinator
     * self-erase replaces the original {@code hybrid} builder with this one before the shard extracts inner_hits.
     *
     * <p>This reads {@code innerHitsQueries}, which is independent of the executed Tail — so inner_hits keep working on a
     * Top-only query. KNN/neural legs materialized to ids are deliberately not part of that list (they cannot carry
     * inner_hits), so the un-materialized leg builders are the ones registered here.
     */
    @Override
    protected void extractInnerHitBuilders(Map<String, InnerHitContextBuilder> innerHits) {
        for (QueryBuilder innerHitsQuery : innerHitsQueries) {
            InnerHitContextBuilder.extractInnerHits(innerHitsQuery, innerHits);
        }
    }

    @Override
    protected boolean doEquals(HybridFusionQueryBuilder other) {
        return Arrays.equals(ids, other.ids)
            && Arrays.equals(indices, other.indices)
            && Arrays.equals(scores, other.scores)
            && Objects.equals(tailQueries, other.tailQueries)
            && Objects.equals(innerHitsQueries, other.innerHitsQueries);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(Arrays.hashCode(ids), Arrays.hashCode(indices), Arrays.hashCode(scores), tailQueries, innerHitsQueries);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        // Internal query; representation is informational only.
        builder.startObject(NAME);
        builder.field("fused_docs_count", ids.length);
        builder.endObject();
    }

    public static HybridFusionQueryBuilder fromXContent(XContentParser parser) {
        throw new UnsupportedOperationException(
            String.format(
                Locale.ROOT,
                "[%s] is created internally by the hybrid query fused mode and cannot be parsed from a request",
                NAME
            )
        );
    }
}
