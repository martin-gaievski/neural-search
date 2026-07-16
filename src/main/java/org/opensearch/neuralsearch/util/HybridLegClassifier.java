/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.BoostingQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.ScriptScoreQueryBuilder;
import org.opensearch.knn.index.query.KNNQueryBuilder;
import org.opensearch.neuralsearch.query.ModelInferenceQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralKNNQueryBuilder;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * Classifies a single sub-query ("leg") of a hybrid query as {@link Verdict#LEXICAL} (keyword / BM25),
 * {@link Verdict#SEMANTIC} (vector / neural / learned-sparse), {@link Verdict#MIXED} (genuinely blends both)
 * or {@link Verdict#UNKNOWN} (unrecognized or too deeply nested to classify safely).
 *
 * <p>This runs PRE-QUERY on the original {@link QueryBuilder} tree (before the coordinator rewrite turns
 * {@code neural} into a k-NN query and {@code neural_sparse} into token queries), which is the only point where the
 * query-type identity is still available. It is a pure, synchronous function of the query tree — it needs no cluster
 * state, no field mappings and no client calls.
 *
 * <h2>Design: retrieval-dominance fold</h2>
 * A leg is classified by its <b>retrieval / match-set driver</b> — i.e. what determines which documents are retrieved —
 * NOT by what computes the graded score. The motivating consumer (a conditional lexical-only rewrite) asks
 * "if I keep the lexical parts and drop the semantic parts, do I retrieve the same documents?", which is a question
 * about retrieval, not scoring. Two consequences:
 * <ul>
 *   <li>{@code script_score{query: match}} is {@link Verdict#LEXICAL}: the match set is BM25; the script only re-scores.</li>
 *   <li>{@code function_score{query: neural, boost_mode: REPLACE}} is {@link Verdict#SEMANTIC}: {@code REPLACE} changes the
 *       score, not which documents match.</li>
 * </ul>
 *
 * <p>Compound legs are folded recursively over each container's <b>primary (retrieval-driving) children</b> using a
 * 4-value join lattice: {@code LEXICAL ⊔ SEMANTIC = MIXED}; {@code UNKNOWN} is absorbing (any unrecognized child makes the
 * whole leg {@code UNKNOWN}); {@code MIXED ⊔ x = MIXED} for {@code x != UNKNOWN}. The join is commutative, associative and
 * idempotent, so fold order is irrelevant.
 *
 * <p>A subtle case: a semantic query placed in a bool {@code filter}/{@code must_not} clause contributes no score but still
 * <b>constrains which documents are retrieved</b>. Such a constraint "poisons" the leg to {@link Verdict#MIXED} so the
 * consumer will not silently produce a lexical result set that still carries a vector constraint. A lexical / structural
 * filter (the common {@code HybridQueryBuilder.filter()} push-down shape) is ignored.
 *
 * <p>The failure mode is fail-safe: unrecognized types are {@link Verdict#UNKNOWN} (never silently {@link Verdict#LEXICAL}),
 * and callers are expected to no-op on {@link Verdict#MIXED}/{@link Verdict#UNKNOWN} rather than guess.
 */
@Log4j2
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class HybridLegClassifier {

    /**
     * Classification of a hybrid sub-query with respect to its retrieval driver.
     */
    public enum Verdict {
        /** Retrieval is driven purely by keyword / lexical clauses (match, term, range, ...). */
        LEXICAL,
        /** Retrieval is driven purely by semantic clauses (neural, neural_sparse, neural_knn, knn). */
        SEMANTIC,
        /** Retrieval genuinely blends lexical and semantic drivers; no single side owns the match set. */
        MIXED,
        /** The leg (or a child it depends on) is an unrecognized type, or nesting exceeded {@link #MAX_DEPTH}. */
        UNKNOWN;

        /**
         * Join in the classification lattice: LEXICAL ⊔ SEMANTIC = MIXED, UNKNOWN is absorbing,
         * MIXED ⊔ x = MIXED for x != UNKNOWN. Commutative, associative, idempotent.
         */
        Verdict join(final Verdict other) {
            if (this == UNKNOWN || other == UNKNOWN) {
                return UNKNOWN;
            }
            if (this == other) {
                return this;
            }
            // the two remaining possibilities are {LEXICAL,SEMANTIC} or one side already MIXED
            return MIXED;
        }
    }

    /**
     * Closed set of query {@code NAME}s that identify a semantic leaf. The {@code instanceof} checks below already cover
     * the compiled neural-search types; this name set additionally catches the raw k-NN {@code "knn"} query and acts as a
     * safety net if the k-NN plugin class is ever absent from the classpath at runtime.
     */
    static final Set<String> SEMANTIC_QUERY_NAMES = Set.of(
        "neural",        // NeuralQueryBuilder.NAME
        "neural_sparse", // NeuralSparseQueryBuilder.NAME (also SparseAnnQueryBuilder.NAME)
        "neural_knn",    // NeuralKNNQueryBuilder.NAME
        "knn"            // org.opensearch.knn.index.query.KNNQueryBuilder.NAME
    );

    /**
     * Maximum recursion depth before a leg is declared {@link Verdict#UNKNOWN}. Real hybrid legs nest only a couple of
     * levels; this is a fail-safe backstop against pathological or adversarial nesting.
     */
    static final int MAX_DEPTH = 10;

    /**
     * Classify a single hybrid leg.
     *
     * @param leg the sub-query builder as authored in the request (pre-rewrite); may be {@code null}
     * @return the retrieval-driver classification; {@link Verdict#UNKNOWN} when {@code leg} is {@code null}
     */
    public static Verdict classify(final QueryBuilder leg) {
        return classify(leg, 0);
    }

    private static Verdict classify(final QueryBuilder qb, final int depth) {
        if (qb == null) {
            return Verdict.UNKNOWN;
        }
        if (depth > MAX_DEPTH) {
            log.debug("hybrid leg classification bailed out at depth {} for query type [{}]", depth, qb.getWriteableName());
            return Verdict.UNKNOWN;
        }

        // 1. Semantic leaf: neural / neural_sparse (marker), neural_knn (no marker), raw knn / drift safety net.
        if (isSemanticLeaf(qb)) {
            return Verdict.SEMANTIC;
        }

        // 2. Containers: fold over the primary (retrieval-driving) children only.
        if (qb instanceof BoolQueryBuilder) {
            return classifyBool((BoolQueryBuilder) qb, depth);
        }
        if (qb instanceof DisMaxQueryBuilder) {
            // all disjuncts are co-primary (Occur.SHOULD)
            return foldJoin(((DisMaxQueryBuilder) qb).innerQueries(), depth);
        }
        if (qb instanceof ConstantScoreQueryBuilder) {
            // the sole inner query IS the match set (scored as a constant)
            return classify(((ConstantScoreQueryBuilder) qb).innerQuery(), depth + 1);
        }
        if (qb instanceof FunctionScoreQueryBuilder) {
            // boost_mode (incl. REPLACE) only re-scores; query() is the match set
            return classify(((FunctionScoreQueryBuilder) qb).query(), depth + 1);
        }
        if (qb instanceof ScriptScoreQueryBuilder) {
            // the script only re-scores; query() is the match set. Script text is opaque and not classifiable.
            return classify(((ScriptScoreQueryBuilder) qb).query(), depth + 1);
        }
        if (qb instanceof NestedQueryBuilder) {
            return classify(((NestedQueryBuilder) qb).query(), depth + 1);
        }
        if (qb instanceof BoostingQueryBuilder) {
            // positive drives retrieval; negative only demotes and is intentionally ignored
            return classify(((BoostingQueryBuilder) qb).positiveQuery(), depth + 1);
        }

        // 3. Recognized lexical leaf, else unknown (never assume lexical).
        if (isLexicalLeaf(qb)) {
            return Verdict.LEXICAL;
        }
        return Verdict.UNKNOWN;
    }

    /**
     * Bool: classify by the primary retrieval group (must if present, else should). A semantic (or unknown) query in a
     * non-scoring filter/must_not clause still constrains retrieval, so it is joined in as a poison; a lexical/structural
     * filter is ignored.
     */
    private static Verdict classifyBool(final BoolQueryBuilder bool, final int depth) {
        final List<QueryBuilder> primary = bool.must().isEmpty() ? bool.should() : bool.must();
        if (primary.isEmpty()) {
            // no scoring/retrieval clauses (e.g. filter-only bool) — nothing to base a lexical/semantic call on
            return Verdict.UNKNOWN;
        }
        Verdict verdict = foldJoin(primary, depth);

        // Non-scoring clauses do not contribute score, but a SEMANTIC/UNKNOWN one still constrains which docs match.
        for (final QueryBuilder constraint : concat(bool.filter(), bool.mustNot())) {
            final Verdict constraintVerdict = classify(constraint, depth + 1);
            if (constraintVerdict == Verdict.SEMANTIC || constraintVerdict == Verdict.UNKNOWN) {
                verdict = verdict.join(constraintVerdict);
            }
            // a purely lexical/structural filter (term, range, bool-of-those, the filter push-down shape) is ignored
        }
        return verdict;
    }

    private static Verdict foldJoin(final List<QueryBuilder> children, final int depth) {
        if (children == null || children.isEmpty()) {
            return Verdict.UNKNOWN;
        }
        Verdict acc = null;
        for (final QueryBuilder child : children) {
            final Verdict childVerdict = classify(child, depth + 1);
            acc = (acc == null) ? childVerdict : acc.join(childVerdict);
        }
        return acc;
    }

    /**
     * True if the query is a semantic leaf. {@code instanceof ModelInferenceQueryBuilder} alone is insufficient — it
     * covers {@code neural} and {@code neural_sparse} (both extend {@code AbstractNeuralQueryBuilder}) but NOT
     * {@code neural_knn} (which does not implement the marker) nor the raw k-NN {@code knn} query.
     */
    static boolean isSemanticLeaf(final QueryBuilder qb) {
        return qb instanceof ModelInferenceQueryBuilder            // neural + neural_sparse
            || qb instanceof NeuralKNNQueryBuilder                  // neural_knn (no marker interface)
            || qb instanceof KNNQueryBuilder                        // raw k-NN query
            || SEMANTIC_QUERY_NAMES.contains(qb.getWriteableName()); // drift safety net / classpath fallback
    }

    /**
     * True if the query is a recognized lexical leaf (a non-semantic, non-container query type). Anything not recognized
     * here stays {@link Verdict#UNKNOWN} rather than being assumed lexical.
     */
    static boolean isLexicalLeaf(final QueryBuilder qb) {
        return LEXICAL_QUERY_NAMES.contains(qb.getWriteableName());
    }

    /**
     * Recognized core keyword / full-text query {@code NAME}s that count as lexical leaves. This is an allow-list on
     * purpose: an unrecognized type is {@link Verdict#UNKNOWN}, never silently lexical.
     */
    static final Set<String> LEXICAL_QUERY_NAMES = Set.of(
        // full-text
        "match",
        "match_phrase",
        "match_phrase_prefix",
        "match_bool_prefix",
        "multi_match",
        "combined_fields",
        "common",
        "query_string",
        "simple_query_string",
        // term-level
        "term",
        "terms",
        "terms_set",
        "prefix",
        "wildcard",
        "regexp",
        "fuzzy",
        "range",
        "intervals",
        "distance_feature",
        // span queries (all lexical/positional over analyzed text)
        "span_term",
        "span_near",
        "span_or",
        "span_first",
        "span_not",
        "span_within",
        "span_containing",
        "span_multi",
        "span_field_masking",
        // match-set / structural leaves that carry no semantic retrieval
        "match_all",
        "match_none",
        "exists",
        "ids"
    );

    private static List<QueryBuilder> concat(final List<QueryBuilder> a, final List<QueryBuilder> b) {
        final List<QueryBuilder> combined = new ArrayList<>((a == null ? 0 : a.size()) + (b == null ? 0 : b.size()));
        if (a != null) {
            combined.addAll(a);
        }
        if (b != null) {
            combined.addAll(b);
        }
        return combined;
    }
}
