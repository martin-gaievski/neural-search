/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.logging.HeaderWarning;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.InnerHitContextBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.knn.index.query.KNNQueryBuilder;
import org.opensearch.neuralsearch.fusion.CoordinatorScoreFusion;
import org.opensearch.neuralsearch.fusion.ScalarNormalizer;
import org.opensearch.neuralsearch.fusion.ScalarNormalizers;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationFactory;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationUtil;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Coordinator-side machinery for the resolver (fused) mode: fan the sub-query legs out as a parallel {@code MultiSearch},
 * then fuse the leg hits into the standard query the {@code hybrid} query self-erases into ({@link HybridFusionQueryBuilder},
 * or {@code match_none} when nothing fused). All methods are static and take the {@link SearchRequest} /
 * {@link MultiSearchResponse} explicitly so the class holds no state.
 *
 * <p>Fusion arithmetic is NOT reimplemented here — it delegates to {@link CoordinatorScoreFusion}, the shared core that
 * classic hybrid also calls, so fused-mode relevance matches classic for the same hit set. Current scope: {@code min_max}
 * normalization + {@code arithmetic_mean} combination (the caller rejects other techniques at rewrite for now).
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class HybridFusionOrchestrator {

    private static final ScoreCombinationFactory SCORE_COMBINATION_FACTORY = new ScoreCombinationFactory();
    /** Separator for the composite _index+_id fusion key. Never parsed back — see computeRankedDocs. */
    private static final String KEY_SEPARATOR = "#";
    /** The {@code _name} key as it appears in a rendered query — see {@link #anyLegNamed}. */
    private static final String QUERY_NAME_KEY = String.format(Locale.ROOT, "\"%s\":", AbstractQueryBuilder.NAME_FIELD.getPreferredName());

    /**
     * Build the leg MultiSearch: one standalone search per sub-query, each reduced to the global top-{@code windowSize}.
     *
     * <p>What each leg inherits from the user's request is decided in exactly one place — {@link CandidateScope}, which
     * classifies every field of {@link SearchRequest} and {@link SearchSourceBuilder} as propagated, overridden,
     * rejected, Tail-forcing, or deliberately dropped, with the reason recorded next to it. This method only assembles
     * the per-leg requests it produces; it holds no propagation policy of its own, so no request field can reach a leg
     * (or fail to) by omission here.
     *
     * <p>Note on ANN legs: {@code size} sets how many hits a leg returns, but an ANN leg's retrieval depth is bounded by
     * its own {@code k} (collected per shard), not by {@code size} — a {@code knn}/{@code neural} leg with a small or
     * default {@code k} (10) contributes at most that many candidates regardless of {@code windowSize}. This matches
     * classic hybrid, which likewise never rewrites {@code k}; for a full window, set {@code k >= window_size} on the
     * sub-query. We deliberately do NOT rewrite {@code k} here — it would diverge from classic and has no analog for
     * radial knn ({@code min_score}/{@code max_distance} have no {@code k}).
     */
    static MultiSearchRequest buildLegMultiSearch(CandidateScope scope, List<QueryBuilder> legs, int windowSize) {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        for (QueryBuilder leg : legs) {
            multiSearchRequest.add(scope.newLegRequest(leg, windowSize));
        }
        return multiSearchRequest;
    }

    /**
     * Fuse the leg results into the standard query the fused-mode hybrid self-erases into — a {@link HybridFusionQueryBuilder}
     * (Top + conditional Tail), or a {@link MatchNoneQueryBuilder} when nothing fused. Pure: returns the query and
     * mutates nothing.
     *
     * <p>The Tail (non-scoring {@code bool{should: legs}} surfacing the full match set) is included when the request needs
     * it: aggregations, highlighting, a non-{@code _score} sort, collapse group expansion, or totals beyond the window —
     * see {@link #needsTail} for the list and for what deliberately does not trigger it. Since a request that sets none of
     * those still wants an accurate {@code total_hits}, the Tail is present by default and Top-only is the opt-out.
     *
     * <p>The Tail decision is <b>depth-independent</b>: it is derived from the request alone, not from whether this
     * hybrid is the whole query or nested inside a container. A nested fused query still self-erases into
     * {@code bool{Top + Tail}}, and an enclosing clause simply intersects that (fuse-then-filter), so aggregations and
     * {@code total_hits} stay correct at any nesting depth. This deliberately avoids inferring nesting from the query
     * instance (a reference-identity check against {@code source().query()} would silently drop the Tail — and with it
     * agg/total_hits accuracy — for any request-rewrite layer that clones the query first).
     */
    static QueryBuilder buildFusedQuery(
        SearchSourceBuilder source,
        MultiSearchResponse multiSearchResponse,
        List<QueryBuilder> legs,
        FusionSpec fusion,
        int windowSize
    ) {
        MultiSearchResponse.Item[] items = multiSearchResponse.getResponses();
        SearchHit[][] legHits = groupLegHits(items, legs.size());
        RankedDocs ranked = computeRankedDocs(legHits, fusion, windowSize);
        if (ranked.ids().length == 0) {
            return new MatchNoneQueryBuilder();
        }
        boolean tailNeeded = needsTail(source, ranked.ids().length);
        // A leg's _name only reaches matched_queries if its builder is converted on the shard, and the Tail is the only
        // thing that converts legs. When the Tail is not built, carry the same leg forms for registration alone: the fetch
        // phase re-evaluates every named query from its own weights, so nothing has to execute for one to be reported.
        boolean namesOnly = tailNeeded == false && anyLegNamed(legs);
        List<QueryBuilder> legsInTailForm = tailNeeded || namesOnly ? legQueriesForTail(legs, legHits) : List.of();
        // inner_hits are registered from the legs themselves, independent of whether the Tail executes them.
        return new HybridFusionQueryBuilder(
            ranked.ids(),
            ranked.indices(),
            ranked.scores(),
            tailNeeded ? legsInTailForm : List.of(),
            innerHitsLegs(legs),
            namesOnly ? legsInTailForm : List.of()
        );
    }

    /**
     * Reduce the raw MultiSearch items into a per-leg array of hits (one item per leg). A wholly-failed leg (all shards
     * down or a non-partial error → {@code Item.isFailure()}) fails the whole request — fusing over a missing leg would
     * silently change the ranking function, not merely return fewer docs. A leg that only lost some shards under
     * {@code allow_partial_search_results=true} comes back as a successful item with fewer hits and is fused as-is —
     * matching OpenSearch's default partial-results behavior. Because normalization is per-leg, that degraded leg shifts
     * its own min/max, so the fused <i>ranking</i> can differ from a complete run rather than merely losing docs; a
     * response {@code Warning} header names the affected legs so the degradation is not silent. Under an effective
     * {@code allow_partial_search_results=false} the leg itself fails, which the check below turns into a whole-request
     * failure — so honoring that flag needs no separate handling here.
     *
     * <p>Every leg's hits enter fusion through this method and nowhere else, which makes it the one place to assert the
     * per-hit {@code _index} invariant both the Top and the Tail depend on — see {@link #requireHitsCarryTheirIndex}.
     */
    private static SearchHit[][] groupLegHits(MultiSearchResponse.Item[] items, int legCount) {
        if (items.length != legCount) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "[hybrid] expected %d leg sub-search responses but got %d", legCount, items.length)
            );
        }
        SearchHit[][] legHits = new SearchHit[legCount][];
        List<Integer> degradedLegs = new ArrayList<>();
        for (int leg = 0; leg < legCount; leg++) {
            MultiSearchResponse.Item item = items[leg];
            if (item.isFailure()) {
                // Carry the leg's own status instead of inventing one. A malformed query bound is the user's 400, a
                // queue rejection is a retryable 429, a cluster block is a 403 — and ExceptionsHelper.status has no
                // IllegalStateException case, so wrapping in one turned every leg failure into a 500 with the real
                // status buried under caused_by: a client's retry-on-429 never fired and a bad request read as a
                // server bug. SearchPhaseExecutionException#status derives from its shard failures and
                // OpenSearchException#status unwraps transport wrappers, so the leg's own failure carries the right
                // code with no unwrapping here. The message is passed with no format args, so LoggerMessageFormat
                // returns it verbatim and a stray {} inside the leg's message cannot mangle it.
                throw new OpenSearchStatusException(
                    String.format(Locale.ROOT, "[hybrid] fused-mode sub-query %d failed: %s", leg, item.getFailureMessage()),
                    ExceptionsHelper.status(item.getFailure()),
                    item.getFailure()
                );
            }
            // Shard failures (not successful<total) — skipped/can-match shards are not failures. Read the array rather
            // than getFailedShards(), which dereferences it unguarded.
            ShardSearchFailure[] shardFailures = item.getResponse().getShardFailures();
            if (Objects.nonNull(shardFailures) && shardFailures.length > 0) {
                degradedLegs.add(leg);
            }
            legHits[leg] = requireHitsCarryTheirIndex(item.getResponse().getHits().getHits(), leg);
        }
        warnOnDegradedLegs(degradedLegs);
        return legHits;
    }

    /**
     * Assert the property everything downstream relies on: every leg hit carries its {@code _index}. Fusion keys documents
     * by {@code _index} + {@code _id} and round 2 addresses them the same way, so a hit without an index can neither be
     * kept apart from a same-{@code _id} document in a sibling index nor addressed without also matching it.
     *
     * <p>An {@link IllegalStateException} rather than a fallback, on both counts: a coordinator sets a hit's index from its
     * shard target, so a hit missing one means the leg response was not produced the way a search response is; and the only
     * fallback available — {@code _id}-only addressing — merges distinct documents for the whole window, since its clauses
     * are qualified as a set, not per hit.
     */
    private static SearchHit[] requireHitsCarryTheirIndex(final SearchHit[] hits, final int leg) {
        for (SearchHit hit : hits) {
            if (Objects.isNull(hit.getIndex())) {
                throw new IllegalStateException(
                    String.format(
                        Locale.ROOT,
                        "[hybrid] fused-mode sub-query %d returned a hit [_id: %s] with no [_index]; fused documents are "
                            + "identified and addressed by [_index] plus [_id], so this hit cannot be fused",
                        leg,
                        hit.getId()
                    )
                );
            }
        }
        return hits;
    }

    /**
     * Surface partially-degraded legs as a response {@code Warning} header. Uses the same mechanism as deprecation
     * warnings, emitted from the coordinator rewrite's async callback so it rides the request's thread context onto the
     * response.
     */
    private static void warnOnDegradedLegs(final List<Integer> degradedLegs) {
        if (degradedLegs.isEmpty()) {
            return;
        }
        HeaderWarning.addWarning(
            String.format(
                Locale.ROOT,
                "[hybrid] fused-mode sub-quer%s %s returned partial results (shard failures); fused scores were computed "
                    + "over an incomplete result set, so ranking may differ from a complete run",
                degradedLegs.size() == 1 ? "y" : "ies",
                degradedLegs
            )
        );
    }

    /**
     * Fuse via the shared {@link CoordinatorScoreFusion} core, then rank by fused score and cut to the window. Converts
     * the coordinator's {@code SearchHit[][]} view into the per-leg key→score maps the shared core consumes; a leg that
     * matched nothing contributes an empty map (groupLegHits fails fast on failures, so every slot is non-null).
     *
     * <p>Documents are keyed by {@code _index} + {@code _id}, not {@code _id} alone: {@code _id} is unique only within an
     * index, so across indices two different documents can share one and fusion would otherwise combine their scores as
     * if they were one document. The composite key is built with a separator but is never parsed back — an {@code _id}
     * may itself contain the separator, so the original identity is carried in a side map instead. To fusion and to every
     * normalizer the key stays opaque.
     */
    private static RankedDocs computeRankedDocs(SearchHit[][] legHits, FusionSpec fusion, int windowSize) {
        List<Map<String, Float>> legRawScores = new ArrayList<>(legHits.length);
        Map<String, SearchHit> identityByKey = new HashMap<>();
        for (SearchHit[] hits : legHits) {
            Map<String, Float> byKey = new LinkedHashMap<>();
            for (SearchHit hit : hits) {
                String key = documentKey(hit);
                byKey.put(key, hit.getScore());
                identityByKey.putIfAbsent(key, hit);
            }
            legRawScores.add(byKey);
        }
        ScoreCombinationTechnique combination = SCORE_COMBINATION_FACTORY.createCombination(
            fusion.combinationTechnique(),
            weightsParams(fusion.weights())
        );
        // Normalization is resolved by name, so widening technique support is a new ScalarNormalizer plus one entry in
        // ScalarNormalizers — no change here. The caller already rejected out-of-scope techniques at rewrite.
        ScalarNormalizer normalizer = ScalarNormalizers.forTechnique(fusion.normalizationTechnique());
        Map<String, Float> combined = CoordinatorScoreFusion.fuse(legRawScores, normalizer, combination);
        return toRankedDocs(combined, identityByKey, windowSize);
    }

    /**
     * Fusion key for a hit: its {@code _index}, the separator, and its {@code _id}. Every leg hit carries an index —
     * {@link #requireHitsCarryTheirIndex} asserts it as the hits enter — so the key is always qualified.
     *
     * <p>Limitation in custom routing. {@code _index} + {@code _id} is not a total identity when custom routing is used:
     * the same {@code _id} can be written to different shards of one index under different routing values, giving two
     * genuinely distinct documents that share this key and are therefore fused as one.
     *
     * <p>Adding the routing value to the key would not fix it. Reading it is not the obstacle — round 2's matching surface
     * is: the self-erased query addresses documents by {@code _id} and an {@code _index} term, with no way to express
     * routing, so two docs split apart in the key resolve to the same clause, and Lucene folds identical SHOULD clauses by
     * <i>summing</i> their boosts — both would come back scored as the sum, which is worse than fusing them as one. This
     * is a limitation of how documents are addressed, not of how they are keyed.
     */
    private static String documentKey(SearchHit hit) {
        return hit.getIndex() + KEY_SEPARATOR + hit.getId();
    }

    /**
     * Fail fast on a malformed {@code weights} array BEFORE the leg fan-out fires — otherwise a bad weights array (out
     * of range, not summing to 1.0, or the wrong count) wastes a full N-leg MultiSearch before {@link ScoreCombinationUtil}
     * errors in the async callback. Constructing the combination technique here reuses core's existing
     * {@code validateParams}/{@code validateWeights} range-and-sum checks with no duplication; the count-vs-legs check
     * (only enforced later, in {@code combine()}) is added explicitly so a mismatch also fails before the fan-out.
     *
     * @param fusion resolved fusion config (inline or pipeline)
     * @param legCount number of sub-query legs (weights, when supplied, must match this)
     */
    static void validateFusionParams(final FusionSpec fusion, final int legCount) {
        // Triggers ScoreCombinationUtil.validateParams + getWeights -> validateWeights (range 0.0..1.0 and sum == 1.0).
        SCORE_COMBINATION_FACTORY.createCombination(fusion.combinationTechnique(), weightsParams(fusion.weights()));
        float[] weights = fusion.weights();
        if (weights.length != 0 && weights.length != legCount) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "number of weights [%d] must match number of sub-queries [%d] in hybrid query",
                    weights.length,
                    legCount
                )
            );
        }
    }

    private static Map<String, Object> weightsParams(float[] weights) {
        if (Objects.isNull(weights) || weights.length == 0) {
            return Map.of();
        }
        List<Double> weightsList = new ArrayList<>(weights.length);
        for (float weight : weights) {
            weightsList.add((double) weight);
        }
        return Map.of(ScoreCombinationUtil.PARAM_NAME_WEIGHTS, weightsList);
    }

    /**
     * Rank the fused scores, cut to the window, and resolve each key back to its {@code (_index, _id)} identity via the
     * side map — never by parsing the key.
     *
     * <p>Every ranked document is addressed by its {@code _index} as well as its {@code _id}, unconditionally. The window
     * is not evidence about the request: a multi-index search whose window happens to be filled from one index still runs
     * round 2 against every requested index, where a sibling index's same-{@code _id} document would match the bare
     * {@code ids} clause and inherit that document's fused score. Deciding qualification from the window was exactly that
     * bug — fusion keys documents by {@code _index + _id} (see {@link #documentKey}), and addressing them by {@code _id}
     * alone merges back together what keying had correctly separated.
     *
     * <p>Qualifying always is free rather than a trade: {@code _index} is a constant field, so on the shard's own index
     * the added filter is a MatchAll that {@code BooleanQuery.rewrite} removes — the clause collapses to exactly the
     * {@code constant_score(ids)} it would have been — and on any other index's shard the all-FILTER bool has a
     * MatchNoDocs required clause and collapses away entirely. Measured post-rewrite, the qualified Top presents the same
     * number of clauses to Lucene's ceiling as the unqualified one.
     *
     * <p>The returned {@code indices} array is fully populated, with no null holes: every key came from a hit whose
     * {@code _index} {@link #requireHitsCarryTheirIndex} already asserted, and the side map is built from those same hits.
     * Nothing here tolerates a missing index on purpose — see that method for why a fallback would be worse.
     */
    private static RankedDocs toRankedDocs(Map<String, Float> scoresByKey, Map<String, SearchHit> identityByKey, int windowSize) {
        List<Map.Entry<String, Float>> ranked = new ArrayList<>(scoresByKey.entrySet());
        ranked.sort(Comparator.<Map.Entry<String, Float>>comparingDouble(e -> -e.getValue()).thenComparing(Map.Entry::getKey));
        if (ranked.size() > windowSize) {
            ranked = ranked.subList(0, windowSize);
        }
        String[] ids = new String[ranked.size()];
        String[] indices = new String[ranked.size()];
        float[] scores = new float[ranked.size()];
        for (int i = 0; i < ranked.size(); i++) {
            String key = ranked.get(i).getKey();
            // Resolved through the side map, never by parsing the composite key — an _id may contain the separator.
            SearchHit hit = identityByKey.get(key);
            ids[i] = hit.getId();
            indices[i] = hit.getIndex();
            scores[i] = ranked.get(i).getValue();
        }
        return new RankedDocs(ids, indices, scores);
    }

    /** The sub-query legs in their Tail form. groupLegHits fails fast on any leg failure, so every leg is present here
     *  (legHits.length == legs.size(), no null slots). A kNN/neural leg retrieves a bounded candidate set rather than a
     *  term-defined one, so it is materialized as the documents it already retrieved rather than re-walking the HNSW graph
     *  in the Tail purely to count; other legs are used as-is. See {@link #isMaterializableLeg} for the bound this relies
     *  on and the one configuration where it under-counts. */
    private static List<QueryBuilder> legQueriesForTail(List<QueryBuilder> legs, SearchHit[][] legHits) {
        List<QueryBuilder> tail = new ArrayList<>(legs.size());
        for (int legIndex = 0; legIndex < legs.size(); legIndex++) {
            QueryBuilder leg = legs.get(legIndex);
            if (isMaterializableLeg(leg) == false) {
                tail.add(leg);
                continue;
            }
            // A materialized leg answers to its own _name: matched_queries is reported from the names registered while
            // this query is converted, and the substitute is a fresh builder that would otherwise carry none — so a named
            // kNN/neural leg would silently lose the field even with the Tail present. What it then reports is the
            // documents the leg returned, which is the same bound materialization already accepts for the match set.
            // Under include_named_queries_score the reported value is the substitute's score rather than the ANN
            // similarity: the shard never sees the vector query, and re-running it for a reporting field is exactly the
            // graph walk materialization exists to avoid. For the same reason only the leg's own name is inherited — a
            // _name nested inside the leg (on a knn filter, say) has no clause left here to be registered against.
            tail.add(materializedLeg(legHits[legIndex]).queryName(leg.queryName()));
        }
        return tail;
    }

    /**
     * Whether any leg carries a query name, at any depth. Used only to decide whether a Top-only query has to carry the
     * leg forms for name registration at all — the registration itself is unconditional once they are carried.
     *
     * <p><b>Deliberately over-inclusive.</b> There is no exact generic descent available: of all the core query builders
     * only {@code bool} overrides {@code visit(QueryBuilderVisitor)}, so a visitor walk (like a shallow
     * {@code queryName() != null} check) is blind to a {@code _name} under {@code nested}, {@code function_score},
     * {@code dis_max}, {@code constant_score} or a {@code knn} filter. The rendered form is checked instead, where
     * {@link AbstractQueryBuilder#printBoostAndQueryName} puts {@code _name} whenever one is set. A false positive costs
     * one registration nobody reads; a false negative would silently drop the field, which is the defect being fixed.
     */
    private static boolean anyLegNamed(List<QueryBuilder> legs) {
        for (QueryBuilder leg : legs) {
            if (Objects.nonNull(leg.queryName()) || rendersQueryName(leg)) {
                return true;
            }
        }
        return false;
    }

    private static boolean rendersQueryName(QueryBuilder leg) {
        // toXContent opens and closes the query's own object, so the builder is used at its root position: wrapping it in
        // another object makes every render fail, and this method's fail-open would then carry every leg unconditionally.
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            leg.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return builder.toString().contains(QUERY_NAME_KEY);
        } catch (IOException e) {
            // A leg that cannot be rendered is one whose names cannot be ruled out. Carry it: an extra registration is
            // cheap, and the alternative is exactly the silent loss this method exists to prevent.
            return true;
        }
    }

    /**
     * A materialized leg addresses the documents it returned the same way the Top addresses ranked documents — through
     * the shared {@link HybridFusionQueryBuilder#addressDocuments}, by {@code _index} and {@code _id} together.
     *
     * <p>The Tail is a {@code filter}, so it decides the match set. Addressing it by {@code _id} alone made every
     * same-{@code _id} sibling document in another index part of that set: it was counted into {@code total_hits}, fed
     * every aggregation bucket, and came back to the user as a score-0 hit — inflating precisely the numbers the Tail
     * exists to make correct. This is not the same defect as the Top's, and qualifying the Top did not fix it: the Tail was
     * built from the raw leg hits without reference to the ranked window's resolved indices at all.
     *
     * <p>Hits are grouped by index — one qualified clause per index, OR-ed — rather than one clause per hit, so a
     * single-index search still presents a single clause and, since {@code _index} is a constant field whose filter
     * rewrites to MatchAll on its own shard and MatchNoDocs elsewhere, the post-rewrite leaf count per leg is unchanged.
     *
     * <p>An empty leg is returned as an explicit {@code match_none}. {@code bool{should: []}} compiles to
     * {@code MatchAllDocsQuery}, so an ANN leg that matched nothing would otherwise flip to matching <i>every</i> document
     * in the Tail. Today's bare ids query avoids that only by accident — core rewrites an empty {@link IdsQueryBuilder} to
     * {@code match_none} — and that accident does not survive wrapping the leg in a bool, so state the guard here.
     */
    private static QueryBuilder materializedLeg(SearchHit[] hits) {
        if (hits.length == 0) {
            return new MatchNoneQueryBuilder();
        }
        // Insertion-ordered, so a leg's clauses come out in the order its hits arrived — deterministic for the same
        // response. Every hit carries an _index by the invariant requireHitsCarryTheirIndex asserts, so there is no
        // unqualified group to address by _id alone.
        Map<String, List<String>> idsByIndex = new LinkedHashMap<>();
        for (SearchHit hit : hits) {
            idsByIndex.computeIfAbsent(hit.getIndex(), index -> new ArrayList<>()).add(hit.getId());
        }
        List<QueryBuilder> perIndex = new ArrayList<>(idsByIndex.size());
        for (Map.Entry<String, List<String>> group : idsByIndex.entrySet()) {
            perIndex.add(HybridFusionQueryBuilder.addressDocuments(group.getKey(), group.getValue().toArray(new String[0])));
        }
        if (perIndex.size() == 1) {
            return perIndex.get(0);
        }
        BoolQueryBuilder inAnyIndex = new BoolQueryBuilder();
        perIndex.forEach(inAnyIndex::should);
        return inAnyIndex;
    }

    /**
     * Legs whose match set is bounded by their own retrieval depth rather than by the data, so re-running them in the Tail
     * would be a redundant ANN pass (kNN/neural re-walk the HNSW graph). Only such legs are safe to replace with a direct
     * address of the already-retrieved hits. A leg whose match set is NOT so bounded — e.g. {@code neural_sparse},
     * whose match set is every doc containing a query token (far larger than the window) — must NOT be materialized, or
     * the Tail would drop the rest and undercount total_hits/aggregations (and re-running it is cheap: no graph to walk).
     *
     * <p><b>Known limitation — materialization is exact only when the leg was not truncated.</b> A materialized leg stands
     * for the documents it <i>returned</i>, which is {@code min(matches, window_size)}: {@code newLegRequest} sets the
     * leg's {@code size} to the window. The leg's real match set is up to its own {@code k} per shard, which fused mode
     * deliberately does not rewrite (see {@link #buildLegMultiSearch}), so with {@code k} × shards greater than
     * {@code window_size} the leg matched documents it never returned, and the Tail — a {@code filter}, hence the match set
     * — leaves them out of {@code total_hits} and out of every aggregation bucket. Classic hybrid counts them, so this is
     * an under-count relative to classic in exactly that configuration. Default {@code k} (10) is below the default
     * {@code window_size} (100), which is why the common case is exact; a leg that returned fewer than {@code window_size}
     * hits is exact by construction, since it was not truncated. Raising {@code window_size} to at least {@code k} ×
     * shards restores an exact count. The fix — using the original leg in the Tail when a materialized leg came back full —
     * is deferred: it costs a second ANN walk in precisely the case a user chose a deep {@code k} for.
     */
    private static boolean isMaterializableLeg(QueryBuilder leg) {
        String name = leg.getWriteableName();
        return KNNQueryBuilder.NAME.equals(name) || NeuralQueryBuilder.NAME.equals(name) || NeuralKNNQueryBuilder.NAME.equals(name);
    }

    /**
     * Single source of truth for whether a fused query needs the executed Tail (the non-scoring
     * {@code bool{should: legs}}): aggregations or highlighting need the full match set in the query phase, an accurate
     * index-wide {@code total_hits} beyond the fused window needs the legs counted, a sort that is not by
     * {@code _score} ranks over the match set rather than the fused window, and {@code collapse.inner_hits} expands each
     * group over the match set (see {@link CandidateScope.Disposition#FORCES_TAIL}) — with
     * Top only, such a request would sort, or expand a group over, an arbitrary window-sized subset of its matches.
     *
     * <p>Deliberately NOT triggers:
     * <ul>
     *   <li>{@code explain} — the Tail cannot recover a fused explanation: the fused score is arithmetic the coordinator
     *       already did, and the clause carrying it into round 2 is a childless {@code constant_score} with nothing to
     *       descend into (the Tail explains as a non-scoring filter either way). Describing the fusion belongs on the
     *       response side, where classic hybrid puts it too — tracked as a fused-mode parity follow-up, see
     *       {@link CandidateScope.Disposition#NOT_PROPAGATED}.</li>
     *   <li>{@code profile} — the tree covers round 2, which is what executes under it, and only a leg's hits are read
     *       from its response, so a leg tree has nowhere to go. Forcing the Tail would not add the legs' own timings: it
     *       would time a fresh re-execution of them and change the very execution being measured. A materialized leg
     *       appears there as the id/index filter it was reduced to, not as the ANN query it came from.</li>
     *   <li>leg {@code inner_hits} — inner_hits are built in the fetch phase from the <i>registered</i> inner-hit
     *       contexts per returned parent doc, so the leg never has to be executed for them to be returned. They are
     *       carried separately (see {@link #innerHitsLegs}), which keeps a Top-only query cheap without losing them.
     *       This is unlike {@code collapse.inner_hits} above, which is not a fetch-phase expansion of the returned
     *       document at all but a whole extra search per group, and so does depend on what round 2 matches.</li>
     *   <li>a leg carrying {@code _name} — {@code matched_queries} is built in the fetch phase from weights the phase
     *       creates itself for each <i>registered</i> named query, so a named leg has to be converted, never executed.
     *       Forcing the Tail would buy a reporting field with a full match-set execution across every shard. The leg
     *       forms are carried for registration alone instead (see {@link #buildFusedQuery} and
     *       {@code HybridFusionQueryBuilder#registerNamedOnlyQueries}), which keeps a Top-only query Top-only.</li>
     * </ul>
     */
    private static boolean needsTail(SearchSourceBuilder source, int numRankedDocs) {
        if (Objects.nonNull(source) && (Objects.nonNull(source.aggregations()) || Objects.nonNull(source.highlighter()))) {
            return true;
        }
        if (CandidateScope.sortDiscardsFusedRanking(source) || CandidateScope.collapseExpandsGroups(source)) {
            return true;
        }
        return wantsTotalsBeyondWindow(source, numRankedDocs);
    }

    /**
     * The legs that declare {@code inner_hits}, in their original (un-materialized) form, for fetch-phase registration.
     * Only legs actually carrying an inner_hits definition are kept, so the common case registers nothing. Note these are
     * the original leg builders rather than the Tail's possibly id-materialized form — a kNN/neural leg materialized to
     * ids has no inner_hits definition left to extract.
     */
    private static List<QueryBuilder> innerHitsLegs(List<QueryBuilder> legs) {
        List<QueryBuilder> withInnerHits = new ArrayList<>();
        for (QueryBuilder leg : legs) {
            Map<String, InnerHitContextBuilder> innerHits = new HashMap<>();
            InnerHitContextBuilder.extractInnerHits(leg, innerHits);
            if (innerHits.isEmpty() == false) {
                withInnerHits.add(leg);
            }
        }
        return withInnerHits;
    }

    private static boolean wantsTotalsBeyondWindow(SearchSourceBuilder source, int numRankedDocs) {
        if (Objects.isNull(source)) {
            return true;
        }
        Integer trackTotalHitsUpTo = source.trackTotalHitsUpTo();
        return Objects.isNull(trackTotalHitsUpTo) || trackTotalHitsUpTo > numRankedDocs;
    }

    /** The fused window in score order. {@code indices} is parallel to {@code ids} and fully populated — every entry names
     *  the index the document was found in, which is what lets round 2 address it unambiguously. */
    private record RankedDocs(String[] ids, String[] indices, float[] scores) {
    }
}
