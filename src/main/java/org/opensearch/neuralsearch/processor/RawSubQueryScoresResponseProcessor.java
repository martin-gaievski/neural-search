/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.document.DocumentField;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.SearchResponseProcessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.neuralsearch.plugin.NeuralSearch.RAW_SUBQUERY_SCORES_KEY;
import static org.opensearch.neuralsearch.plugin.NeuralSearch.RAW_SUBQUERY_COMBINED_KEY;

/**
 * POC — coordinator-side response processor that attaches the RAW per-sub-query scores (pre-normalization,
 * pre-combination) captured by {@link NormalizationProcessorWorkflow} to each returned hit as a
 * {@code fields.hybridization_sub_query_scores} float array.
 *
 * <p><b>Why this is multi-node-safe</b> (unlike the reverted PR #1369): the scores are both captured and attached
 * in the COORDINATOR JVM. The workflow stashes a per-shard, final-order-aligned {@code List<float[]>} on the
 * request-scoped {@link PipelineProcessingContext} (never a JVM-static map), and this processor — a
 * {@link SearchResponseProcessor} which also runs on the coordinator, after the fetch results are merged —
 * reads it back and attaches it. There is no data-node fetch-phase read, so there is no cross-JVM invisibility.
 *
 * <p><b>Association key</b>: hits are grouped by their serialized {@link SearchShard} and consumed with a
 * per-shard positional counter — the same key the shipped {@code hybrid_score_explanation} processor uses. It does
 * NOT use {@link SearchHit#docId()} (which is transient and resets to -1 after transport, the trap that would
 * silently drop the field on remote-node hits).
 */
@Getter
@AllArgsConstructor
@Log4j2
public class RawSubQueryScoresResponseProcessor implements SearchResponseProcessor {

    public static final String TYPE = "hybridization_sub_query_scores";
    public static final String FIELD_NAME = "hybridization_sub_query_scores";

    private final String description;
    private final String tag;
    private final boolean ignoreFailure;

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) {
        return processResponse(request, response, null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public SearchResponse processResponse(
        final SearchRequest request,
        final SearchResponse response,
        final PipelineProcessingContext requestContext
    ) {
        if (Objects.isNull(requestContext)
            || Objects.isNull(requestContext.getAttribute(RAW_SUBQUERY_SCORES_KEY))
            || Objects.isNull(requestContext.getAttribute(RAW_SUBQUERY_COMBINED_KEY))) {
            return response; // not a hybrid-normalization request, or feature not engaged — no-op
        }
        Map<SearchShard, List<float[]>> orderedByShard = (Map<SearchShard, List<float[]>>) requestContext.getAttribute(
            RAW_SUBQUERY_SCORES_KEY
        );
        Map<SearchShard, float[]> combinedByShard = (Map<SearchShard, float[]>) requestContext.getAttribute(RAW_SUBQUERY_COMBINED_KEY);

        SearchHit[] hits = response.getHits().getHits();
        // Per-shard MONOTONIC forward cursor. Both the page hits and each shard's stored list are combined-score
        // descending, so we advance the cursor to the first stored entry whose combined score matches the hit's
        // _score. This correctly skips the leading entries paginated away by a global `from` offset (a naive 0-based
        // position mis-attributes under from>0 on multi-shard). We do NOT use searchHit.docId() — it is transient
        // and -1 after transport (the trap that reverted PR #1369); _score + per-shard order is transport-stable.
        Map<SearchShard, Integer> perShardCursor = new HashMap<>();
        for (SearchHit hit : hits) {
            SearchShardTarget shardTarget = hit.getShard();
            if (Objects.isNull(shardTarget)) {
                continue;
            }
            SearchShard searchShard = SearchShard.createSearchShard(shardTarget);
            List<float[]> ordered = orderedByShard.get(searchShard);
            float[] combined = combinedByShard.get(searchShard);
            if (Objects.isNull(ordered) || Objects.isNull(combined)) {
                continue;
            }
            int cursor = perShardCursor.getOrDefault(searchShard, 0);
            // advance to the stored entry matching this hit's combined score (handles from>0 skip + ties)
            while (cursor < combined.length && Float.compare(combined[cursor], hit.getScore()) != 0) {
                cursor++;
            }
            if (cursor >= ordered.size()) {
                perShardCursor.put(searchShard, cursor);
                continue; // no match (defensive) — leave field absent rather than mis-attribute
            }
            float[] raw = ordered.get(cursor);
            perShardCursor.put(searchShard, cursor + 1); // next hit for this shard resumes after this entry
            if (Objects.isNull(raw)) {
                continue;
            }
            List<Object> asList = new ArrayList<>(raw.length);
            for (float score : raw) {
                asList.add(score);
            }
            hit.setDocumentField(FIELD_NAME, new DocumentField(FIELD_NAME, asList));
        }
        return response;
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
