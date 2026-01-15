/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.resultboost;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.search.ScoreDoc;
import org.opensearch.search.SearchHit;

import java.util.Map;

/**
 * Applies post-normalization result boosts to search results.
 *
 * The boost is applied AFTER:
 * - Score normalization (e.g., min-max)
 * - Score combination (e.g., arithmetic_mean)
 *
 * And BEFORE:
 * - Collapse (if enabled)
 * - Final result sorting
 *
 * This allows boosted documents to exceed the normalized score ceiling (1.0)
 * and rank above naturally relevant results.
 */
@Log4j2
public class ResultBooster {

    /**
     * Apply boosts to search hits based on their document IDs.
     * This modifies the scores in the searchHits array and returns the new max score.
     *
     * @param searchHits The search hits to potentially boost
     * @param boostMap Map of document ID to boost configuration
     * @return The new maximum score after boosting
     */
    public float applyBoosts(SearchHit[] searchHits, Map<String, DocumentBoost> boostMap) {
        if (searchHits == null || searchHits.length == 0 || boostMap == null || boostMap.isEmpty()) {
            return 0.0f;
        }

        float maxScore = Float.MIN_VALUE;
        int boostsApplied = 0;

        for (SearchHit hit : searchHits) {
            if (hit == null) {
                continue;
            }

            String docId = hit.getId();
            if (docId == null) {
                continue;
            }

            DocumentBoost boost = boostMap.get(docId);
            if (boost != null) {
                float originalScore = hit.getScore();
                float boostedScore = applyBoost(originalScore, boost);
                hit.score(boostedScore);
                boostsApplied++;

                log.debug(
                    "Applied boost to document {}: {} -> {} (factor={}, type={})",
                    docId,
                    originalScore,
                    boostedScore,
                    boost.getFactor(),
                    boost.getType()
                );
            }

            if (!Float.isNaN(hit.getScore()) && hit.getScore() > maxScore) {
                maxScore = hit.getScore();
            }
        }

        if (boostsApplied > 0) {
            log.info("Applied {} result boosts", boostsApplied);
        }

        return maxScore == Float.MIN_VALUE ? 0.0f : maxScore;
    }

    /**
     * Apply boost to a map of docId to scores (used during score combination phase).
     * This is for multi-shard scenarios where we need to apply boost before final sorting.
     *
     * @param combinedScoresByDocId Map of Lucene doc ID to combined score
     * @param docIdToStringId Map of Lucene doc ID to String document ID
     * @param boostMap Map of String document ID to boost configuration
     */
    public void applyBoostsToScoreMap(
        Map<Integer, Float> combinedScoresByDocId,
        Map<Integer, String> docIdToStringId,
        Map<String, DocumentBoost> boostMap
    ) {

        if (combinedScoresByDocId == null || docIdToStringId == null || boostMap == null || boostMap.isEmpty()) {
            return;
        }

        int boostsApplied = 0;

        for (Map.Entry<Integer, Float> entry : combinedScoresByDocId.entrySet()) {
            Integer luceneDocId = entry.getKey();
            String stringDocId = docIdToStringId.get(luceneDocId);

            if (stringDocId != null) {
                DocumentBoost boost = boostMap.get(stringDocId);
                if (boost != null) {
                    float originalScore = entry.getValue();
                    float boostedScore = applyBoost(originalScore, boost);
                    entry.setValue(boostedScore);
                    boostsApplied++;

                    log.debug(
                        "Applied boost to lucene doc {}: {} -> {} (factor={}, type={})",
                        luceneDocId,
                        originalScore,
                        boostedScore,
                        boost.getFactor(),
                        boost.getType()
                    );
                }
            }
        }

        if (boostsApplied > 0) {
            log.info("Applied {} result boosts during score combination", boostsApplied);
        }
    }

    /**
     * Apply boost to a single score based on boost configuration.
     *
     * @param originalScore The original combined score
     * @param boost The boost configuration
     * @return The boosted score
     */
    public float applyBoost(float originalScore, DocumentBoost boost) {
        if (boost == null) {
            return originalScore;
        }

        switch (boost.getType()) {
            case MULTIPLICATIVE:
                return originalScore * boost.getFactor();
            case ADDITIVE:
                return originalScore + boost.getFactor();
            default:
                log.warn("Unknown boost type: {}, using multiplicative", boost.getType());
                return originalScore * boost.getFactor();
        }
    }

    /**
     * Update ScoreDoc scores to match the boosted SearchHit scores.
     * Used to keep ScoreDocs in sync with SearchHits after boosting.
     *
     * @param scoreDocs Array of ScoreDocs to update
     * @param docIdToScore Map of Lucene doc ID to boosted score
     */
    public void updateScoreDocsWithBoostedScores(ScoreDoc[] scoreDocs, Map<Integer, Float> docIdToScore) {
        if (scoreDocs == null || docIdToScore == null) {
            return;
        }

        for (ScoreDoc scoreDoc : scoreDocs) {
            Float boostedScore = docIdToScore.get(scoreDoc.doc);
            if (boostedScore != null) {
                scoreDoc.score = boostedScore;
            }
        }
    }
}
