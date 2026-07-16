/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.logging.HeaderWarning;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.BoostingQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.MatchBoolPrefixQueryBuilder;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.ScriptScoreQueryBuilder;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.neuralsearch.query.AbstractNeuralQueryBuilder;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.util.HybridLegClassifier;
import org.opensearch.neuralsearch.util.HybridLegClassifier.Verdict;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;

/**
 * A pre-query {@link SearchRequestProcessor} that conditionally rewrites a top-level {@code hybrid} query into a
 * lexical-only (non-hybrid) query when the user's query text matches a configured dictionary.
 *
 * <p>This is the "simple case" consumer of {@link HybridLegClassifier}: when a query word is in the dictionary, the
 * lexical leg(s) win outright (semantic legs are dropped) — which yields a true pure-BM25 result set with correct
 * {@code total_hits}/aggregations/pagination, and needs no changes to the score-combination path.
 *
 * <h2>Behavior</h2>
 * <ol>
 *   <li>No-op (return the request unchanged) unless the top-level query is a {@link HybridQueryBuilder}.</li>
 *   <li>Extract the user query text from the hybrid legs and test it against the dictionary. No match ⇒ no-op.</li>
 *   <li>On a match, classify each leg with {@link HybridLegClassifier}: keep {@link Verdict#LEXICAL} legs, drop
 *       {@link Verdict#SEMANTIC} legs. If any leg is {@link Verdict#MIXED} or {@link Verdict#UNKNOWN}, decline the
 *       rewrite entirely (fail-safe no-op) rather than guess.</li>
 *   <li>Collapse the surviving lexical legs to a NON-hybrid query: 0 lexical ⇒ no-op; 1 ⇒ that leg bare;
 *       &gt;1 ⇒ a {@code bool} query with each lexical leg as a {@code should} clause. Collapsing to a non-hybrid
 *       query is required so the fixed pipeline combination weights are never validated against a shrunken leg count.</li>
 * </ol>
 *
 * <p>Scope: this MVP supports an inline dictionary and a token/phrase match mode. Dictionary hot-reload from a
 * config-file, richer rule actions (graded reweight, pinned-head tiering) and an explicit lexical-leg {@code _name}
 * override are deliberately out of scope here and tracked in the design docs.
 */
@Log4j2
@Getter
public class HybridDictionaryRewriteProcessor extends AbstractProcessor implements SearchRequestProcessor {

    /** Key to reference this processor type from a search pipeline. */
    public static final String TYPE = "hybrid_dictionary_rewrite";

    /** How the query text is matched against the dictionary. */
    public enum MatchMode {
        /** Any whitespace-delimited, lower-cased token of the query text is present in the dictionary. */
        ANY_TOKEN,
        /** The full lower-cased, trimmed query text equals a dictionary entry. */
        PHRASE;

        static MatchMode fromString(final String value) {
            if (value == null) {
                return ANY_TOKEN;
            }
            switch (value.toLowerCase(Locale.ROOT)) {
                case "any_token":
                    return ANY_TOKEN;
                case "phrase":
                    return PHRASE;
                default:
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "[%s] unsupported match_mode [%s], expected any_token|phrase", TYPE, value)
                    );
            }
        }
    }

    private static final int MAX_TEXT_SCAN_DEPTH = 10;

    /** Dictionary entries, pre-normalized to lower case. */
    private final Set<String> dictionary;
    private final MatchMode matchMode;

    HybridDictionaryRewriteProcessor(
        final String tag,
        final String description,
        final boolean ignoreFailure,
        final Set<String> dictionary,
        final MatchMode matchMode
    ) {
        super(tag, description, ignoreFailure);
        this.dictionary = dictionary;
        this.matchMode = matchMode;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public SearchRequest processRequest(final SearchRequest searchRequest) {
        final SearchSourceBuilder source = searchRequest == null ? null : searchRequest.source();
        if (source == null || source.query() == null) {
            return searchRequest; // empty body / no query — nothing to do
        }
        if (!(source.query() instanceof HybridQueryBuilder)) {
            return searchRequest; // hybrid is always top level; other queries are not our concern
        }
        final HybridQueryBuilder hybrid = (HybridQueryBuilder) source.query();

        // 1. Does the user query text match the dictionary?
        if (!matchesDictionary(hybrid)) {
            return searchRequest;
        }

        // 2. Classify each leg; keep lexical, drop semantic, decline on mixed/unknown.
        final List<QueryBuilder> lexicalLegs = new ArrayList<>();
        for (int i = 0; i < hybrid.queries().size(); i++) {
            final QueryBuilder leg = hybrid.queries().get(i);
            final Verdict verdict = HybridLegClassifier.classify(leg);
            if (verdict == Verdict.LEXICAL) {
                lexicalLegs.add(leg);
            } else if (verdict == Verdict.MIXED || verdict == Verdict.UNKNOWN) {
                // Cannot faithfully extract a lexical-only query from a blended/unrecognized leg — decline, do not guess.
                declineRewrite(
                    String.format(
                        Locale.ROOT,
                        "hybrid leg [%d] (query type [%s]) classified [%s], which cannot be safely reduced to a lexical query",
                        i,
                        leg == null ? "null" : leg.getWriteableName(),
                        verdict
                    )
                );
                return searchRequest;
            }
            // SEMANTIC leg is simply dropped
        }

        if (lexicalLegs.isEmpty()) {
            declineRewrite("hybrid query matched the dictionary but has no lexical leg to keep");
            return searchRequest;
        }

        // 3. Collapse to a NON-hybrid query so the score-combination path is bypassed entirely.
        source.query(collapseToNonHybrid(lexicalLegs));
        return searchRequest;
    }

    /**
     * Emit an observable "matched but declined" signal. A dictionary rule fired but the rewrite could not be applied
     * safely, so the request is left as a normal hybrid query. This is surfaced as a response Warning header (visible to
     * managed-service customers who cannot read node logs) in addition to a debug log line.
     */
    private void declineRewrite(final String reason) {
        final String message = String.format(Locale.ROOT, "[%s] skipped lexical rewrite: %s", TYPE, reason);
        log.debug(message);
        HeaderWarning.addWarning(message);
    }

    private QueryBuilder collapseToNonHybrid(final List<QueryBuilder> lexicalLegs) {
        if (lexicalLegs.size() == 1) {
            return lexicalLegs.get(0);
        }
        final BoolQueryBuilder bool = new BoolQueryBuilder();
        lexicalLegs.forEach(bool::should);
        return bool;
    }

    private boolean matchesDictionary(final HybridQueryBuilder hybrid) {
        final Set<String> queryTexts = new LinkedHashSet<>();
        for (final QueryBuilder leg : hybrid.queries()) {
            collectQueryText(leg, queryTexts, 0);
        }
        for (final String rawText : queryTexts) {
            final String normalized = rawText.toLowerCase(Locale.ROOT).trim();
            if (normalized.isEmpty()) {
                continue;
            }
            if (matchMode == MatchMode.PHRASE) {
                if (dictionary.contains(normalized)) {
                    return true;
                }
            } else {
                for (final String token : normalized.split("\\s+")) {
                    if (dictionary.contains(token)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Collect user-facing query text from a leg's text-bearing leaves, recursing through the same containers the
     * classifier understands. Best-effort and null-safe: an unrecognized type simply contributes no text.
     */
    private void collectQueryText(final QueryBuilder qb, final Set<String> out, final int depth) {
        if (qb == null || depth > MAX_TEXT_SCAN_DEPTH) {
            return;
        }
        // Text-bearing leaves
        if (qb instanceof AbstractNeuralQueryBuilder) {
            addIfPresent(out, ((AbstractNeuralQueryBuilder<?>) qb).queryText()); // neural + neural_sparse; safe getter
            return;
        }
        if (qb instanceof MatchQueryBuilder) {
            addIfPresent(out, ((MatchQueryBuilder) qb).value());
            return;
        }
        if (qb instanceof MatchPhraseQueryBuilder) {
            addIfPresent(out, ((MatchPhraseQueryBuilder) qb).value());
            return;
        }
        if (qb instanceof MatchPhrasePrefixQueryBuilder) {
            addIfPresent(out, ((MatchPhrasePrefixQueryBuilder) qb).value());
            return;
        }
        if (qb instanceof MatchBoolPrefixQueryBuilder) {
            addIfPresent(out, ((MatchBoolPrefixQueryBuilder) qb).value());
            return;
        }
        if (qb instanceof MultiMatchQueryBuilder) {
            addIfPresent(out, ((MultiMatchQueryBuilder) qb).value());
            return;
        }
        if (qb instanceof QueryStringQueryBuilder) {
            addIfPresent(out, ((QueryStringQueryBuilder) qb).queryString());
            return;
        }
        if (qb instanceof SimpleQueryStringBuilder) {
            addIfPresent(out, ((SimpleQueryStringBuilder) qb).value());
            return;
        }
        if (qb instanceof TermQueryBuilder) {
            addIfPresent(out, ((TermQueryBuilder) qb).value());
            return;
        }
        // Containers: recurse (mirror the classifier's traversal)
        if (qb instanceof BoolQueryBuilder) {
            final BoolQueryBuilder bool = (BoolQueryBuilder) qb;
            collectFrom(bool.must(), out, depth);
            collectFrom(bool.should(), out, depth);
            return;
        }
        if (qb instanceof DisMaxQueryBuilder) {
            collectFrom(((DisMaxQueryBuilder) qb).innerQueries(), out, depth);
            return;
        }
        if (qb instanceof ConstantScoreQueryBuilder) {
            collectQueryText(((ConstantScoreQueryBuilder) qb).innerQuery(), out, depth + 1);
            return;
        }
        if (qb instanceof FunctionScoreQueryBuilder) {
            collectQueryText(((FunctionScoreQueryBuilder) qb).query(), out, depth + 1);
            return;
        }
        if (qb instanceof ScriptScoreQueryBuilder) {
            collectQueryText(((ScriptScoreQueryBuilder) qb).query(), out, depth + 1);
            return;
        }
        if (qb instanceof NestedQueryBuilder) {
            collectQueryText(((NestedQueryBuilder) qb).query(), out, depth + 1);
            return;
        }
        if (qb instanceof BoostingQueryBuilder) {
            collectQueryText(((BoostingQueryBuilder) qb).positiveQuery(), out, depth + 1);
        }
    }

    private void collectFrom(final List<QueryBuilder> children, final Set<String> out, final int depth) {
        if (children == null) {
            return;
        }
        for (final QueryBuilder child : children) {
            collectQueryText(child, out, depth + 1);
        }
    }

    private static void addIfPresent(final Set<String> out, final Object value) {
        if (value != null) {
            final String text = value.toString();
            if (!text.isEmpty()) {
                out.add(text);
            }
        }
    }

    public static class Factory implements Processor.Factory<SearchRequestProcessor> {
        private static final String DICTIONARY = "dictionary";
        private static final String MATCH_MODE = "match_mode";

        @Override
        public HybridDictionaryRewriteProcessor create(
            final Map<String, Processor.Factory<SearchRequestProcessor>> processorFactories,
            final String tag,
            final String description,
            final boolean ignoreFailure,
            final Map<String, Object> config,
            final PipelineContext pipelineContext
        ) {
            final List<String> terms = ConfigurationUtils.readOptionalList(TYPE, tag, config, DICTIONARY);
            if (terms == null || terms.isEmpty()) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "[%s] requires a non-empty [%s] list of terms", TYPE, DICTIONARY)
                );
            }
            final Set<String> dictionary = terms.stream()
                .filter(Objects::nonNull)
                .map(term -> term.toLowerCase(Locale.ROOT).trim())
                .filter(term -> !term.isEmpty())
                .collect(Collectors.toUnmodifiableSet());
            if (dictionary.isEmpty()) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "[%s] [%s] contained no usable terms", TYPE, DICTIONARY));
            }
            final MatchMode matchMode = MatchMode.fromString(ConfigurationUtils.readOptionalStringProperty(TYPE, tag, config, MATCH_MODE));
            return new HybridDictionaryRewriteProcessor(tag, description, ignoreFailure, dictionary, matchMode);
        }
    }
}
