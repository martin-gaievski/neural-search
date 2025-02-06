/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;

import com.google.common.primitives.Floats;

import lombok.ToString;
import org.opensearch.neuralsearch.processor.NormalizeScoresDTO;
import org.opensearch.neuralsearch.processor.explain.DocIdAtSearchShard;
import org.opensearch.neuralsearch.processor.explain.ExplanationDetails;
import org.opensearch.neuralsearch.processor.explain.ExplainableTechnique;

import static org.opensearch.neuralsearch.processor.explain.ExplanationUtils.getDocIdAtQueryForNormalization;

/**
 * Abstracts normalization of scores based on min-max method
 */
@ToString(onlyExplicitlyIncluded = true)
public class MinMaxScoreNormalizationTechnique implements ScoreNormalizationTechnique, ExplainableTechnique {
    @ToString.Include
    public static final String TECHNIQUE_NAME = "min_max";
    private static final float MIN_SCORE = 0.001f;
    private static final float SINGLE_RESULT_SCORE = 1.0f;
    private final List<Pair<Boolean, Float>> lowerBounds;

    public MinMaxScoreNormalizationTechnique() {
        this(Map.of(), new ScoreNormalizationUtil());
    }

    public MinMaxScoreNormalizationTechnique(final Map<String, Object> params, final ScoreNormalizationUtil scoreNormalizationUtil) {
        lowerBounds = getLowerBounds(params);
    }

    /**
     * Min-max normalization method.
     * nscore = (score - min_score)/(max_score - min_score)
     * Main algorithm steps:
     * - calculate min and max scores for each sub query
     * - iterate over each result and update score as per formula above where "score" is raw score returned by Hybrid query
     */
    @Override
    public void normalize(final NormalizeScoresDTO normalizeScoresDTO) {
        final List<CompoundTopDocs> queryTopDocs = normalizeScoresDTO.getQueryTopDocs();
        MinMaxScores minMaxScores = getMinMaxScoresResult(queryTopDocs);
        float[] decayRates = decayRates(queryTopDocs);
        // do normalization using actual score and min and max scores for corresponding sub query
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (Objects.isNull(compoundQueryTopDocs)) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            if (Objects.nonNull(lowerBounds) && !lowerBounds.isEmpty() && lowerBounds.size() != topDocsPerSubQuery.size()) {
                throw new IllegalArgumentException("lower bounds size should be same as number of sub queries");
            }
            for (int j = 0; j < topDocsPerSubQuery.size(); j++) {
                TopDocs subQueryTopDoc = topDocsPerSubQuery.get(j);
                LowerBound lowerBound = getLowerBound(j, decayRates);
                for (ScoreDoc scoreDoc : subQueryTopDoc.scoreDocs) {
                    scoreDoc.score = normalizeSingleScore(
                        scoreDoc.score,
                        minMaxScores.getMinScoresPerSubquery()[j],
                        minMaxScores.getMaxScoresPerSubquery()[j],
                        lowerBound
                    );
                }
            }
        }
    }

    private LowerBound getLowerBound(int j, float[] decayRates) {
        LowerBound lowerBound;
        if (Objects.isNull(lowerBounds) || lowerBounds.isEmpty()) {
            lowerBound = new LowerBound();
        } else {
            lowerBound = new LowerBound(true, lowerBounds.get(j).getLeft(), lowerBounds.get(j).getRight(), decayRates[j]);
        }
        return lowerBound;
    }

    private MinMaxScores getMinMaxScoresResult(final List<CompoundTopDocs> queryTopDocs) {
        int numOfSubqueries = getNumOfSubqueries(queryTopDocs);
        // get min scores for each sub query
        float[] minScoresPerSubquery = getMinScores(queryTopDocs, numOfSubqueries);
        // get max scores for each sub query
        float[] maxScoresPerSubquery = getMaxScores(queryTopDocs, numOfSubqueries);
        return new MinMaxScores(minScoresPerSubquery, maxScoresPerSubquery);
    }

    @Override
    public String describe() {
        return String.format(Locale.ROOT, "%s", TECHNIQUE_NAME);
    }

    @Override
    public Map<DocIdAtSearchShard, ExplanationDetails> explain(final List<CompoundTopDocs> queryTopDocs) {
        MinMaxScores minMaxScores = getMinMaxScoresResult(queryTopDocs);
        float[] decayRates = decayRates(queryTopDocs);
        Map<DocIdAtSearchShard, List<Float>> normalizedScores = new HashMap<>();
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (Objects.isNull(compoundQueryTopDocs)) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            int numberOfSubQueries = topDocsPerSubQuery.size();
            for (int subQueryIndex = 0; subQueryIndex < numberOfSubQueries; subQueryIndex++) {
                TopDocs subQueryTopDoc = topDocsPerSubQuery.get(subQueryIndex);
                for (ScoreDoc scoreDoc : subQueryTopDoc.scoreDocs) {
                    DocIdAtSearchShard docIdAtSearchShard = new DocIdAtSearchShard(scoreDoc.doc, compoundQueryTopDocs.getSearchShard());
                    LowerBound lowerBound = getLowerBound(subQueryIndex, decayRates);
                    float normalizedScore = normalizeSingleScore(
                        scoreDoc.score,
                        minMaxScores.getMinScoresPerSubquery()[subQueryIndex],
                        minMaxScores.getMaxScoresPerSubquery()[subQueryIndex],
                        lowerBound
                    );
                    ScoreNormalizationUtil.setNormalizedScore(
                        normalizedScores,
                        docIdAtSearchShard,
                        subQueryIndex,
                        numberOfSubQueries,
                        normalizedScore
                    );
                    scoreDoc.score = normalizedScore;
                }
            }
        }
        return getDocIdAtQueryForNormalization(normalizedScores, this);
    }

    private int getNumOfSubqueries(final List<CompoundTopDocs> queryTopDocs) {
        return queryTopDocs.stream()
            .filter(Objects::nonNull)
            .filter(topDocs -> !topDocs.getTopDocs().isEmpty())
            .findAny()
            .get()
            .getTopDocs()
            .size();
    }

    private float[] getMaxScores(final List<CompoundTopDocs> queryTopDocs, final int numOfSubqueries) {
        float[] maxScores = new float[numOfSubqueries];
        Arrays.fill(maxScores, Float.MIN_VALUE);
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (Objects.isNull(compoundQueryTopDocs)) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            for (int j = 0; j < topDocsPerSubQuery.size(); j++) {
                maxScores[j] = Math.max(
                    maxScores[j],
                    Arrays.stream(topDocsPerSubQuery.get(j).scoreDocs)
                        .map(scoreDoc -> scoreDoc.score)
                        .max(Float::compare)
                        .orElse(Float.MIN_VALUE)
                );
            }
        }
        return maxScores;
    }

    private float[] getMinScores(final List<CompoundTopDocs> queryTopDocs, final int numOfScores) {
        // boolean applyLowerBounds = true;
        float[] minScores = new float[numOfScores];
        Arrays.fill(minScores, Float.MAX_VALUE);
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (Objects.isNull(compoundQueryTopDocs)) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            for (int j = 0; j < topDocsPerSubQuery.size(); j++) {
                boolean applyLowerBounds = Objects.nonNull(lowerBounds) && !lowerBounds.isEmpty() ? lowerBounds.get(j).getLeft() : false;
                if (applyLowerBounds) {
                    minScores[j] = lowerBounds.get(j).getRight();
                } else {
                    minScores[j] = Math.min(
                        minScores[j],
                        Arrays.stream(topDocsPerSubQuery.get(j).scoreDocs)
                            .map(scoreDoc -> scoreDoc.score)
                            .min(Float::compare)
                            .orElse(Float.MAX_VALUE)
                    );
                }
            }
        }
        return minScores;
    }

    private float normalizeSingleScore(final float score, final float minScore, final float maxScore, LowerBound lowerBound) {
        // edge case when there is only one score and min and max scores are same
        if (Floats.compare(maxScore, minScore) == 0 && Floats.compare(maxScore, score) == 0) {
            return SINGLE_RESULT_SCORE;
        }
        if (!lowerBound.isEnabled()) {
            float normalizedScore = (score - minScore) / (maxScore - minScore);
            return normalizedScore == 0.0f ? MIN_SCORE : normalizedScore;
        }
        float normalizedScore;
        if (lowerBound.isApplyStrictCutoff() && score < minScore) {
            normalizedScore = applyPenalty(score, minScore, lowerBound.getDecayRate());
        } else if (!lowerBound.isApplyStrictCutoff() && score < minScore) {
            normalizedScore = minScore;
        } else {
            normalizedScore = (score - minScore) / (maxScore - minScore);
        }
        return normalizedScore;
    }

    float applyPenalty(float score, float minScore, float decayRate) {
        return score * (float) Math.exp(-decayRate * (minScore - score));
    }

    protected float[] decayRates(final List<CompoundTopDocs> queryTopDocs) {
        float[] iqrs = iqrs(queryTopDocs);
        float[] decayRates = new float[iqrs.length];
        for (int i = 0; i < iqrs.length; i++) {
            decayRates[i] = 1.0f / (1.0f + iqrs[i]);
        }
        return decayRates;
    }

    private float[] iqrs(final List<CompoundTopDocs> queryTopDocs) {
        int numOfScores = getNumOfSubqueries(queryTopDocs);
        float[] iqrs = new float[numOfScores];

        // Initialize statistics objects for each subquery
        DescriptiveStatistics[] stats = new DescriptiveStatistics[numOfScores];
        for (int i = 0; i < numOfScores; i++) {
            stats[i] = new DescriptiveStatistics();
        }

        // Collect scores
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (compoundQueryTopDocs == null) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();

            for (int j = 0; j < topDocsPerSubQuery.size(); j++) {
                ScoreDoc[] scoreDocs = topDocsPerSubQuery.get(j).scoreDocs;
                for (ScoreDoc scoreDoc : scoreDocs) {
                    stats[j].addValue(scoreDoc.score);
                }
            }
        }

        // Calculate IQR for each subquery
        for (int j = 0; j < numOfScores; j++) {
            if (stats[j].getN() == 0 || stats[j].getN() == 1) {
                iqrs[j] = 0.0f;
            } else if (stats[j].getN() < 4) {
                iqrs[j] = (float) ((stats[j].getMax() - stats[j].getMin()) / 2.0);
            } else {
                // Calculate IQR using percentiles
                double q3 = stats[j].getPercentile(75);
                double q1 = stats[j].getPercentile(25);
                iqrs[j] = (float) (q3 - q1);
            }
        }

        return iqrs;
    }

    /**
     * Result class to hold min and max scores for each sub query
     */
    @AllArgsConstructor
    @Getter
    private class MinMaxScores {
        float[] minScoresPerSubquery;
        float[] maxScoresPerSubquery;
    }

    @Getter
    private class LowerBound {
        boolean enabled;
        boolean applyStrictCutoff;
        float minScore;
        float decayRate;

        LowerBound() {
            this(false, false, Float.MIN_VALUE, Float.MIN_VALUE);
        }

        LowerBound(boolean enabled, boolean applyStrictCutoff, float minScore, float decayRate) {
            this.enabled = enabled;
            this.applyStrictCutoff = applyStrictCutoff;
            this.minScore = minScore;
            this.decayRate = decayRate;
        }
    }

    private List<Pair<Boolean, Float>> getLowerBounds(final Map<String, Object> params) {
        List<Pair<Boolean, Float>> lowerBounds = new ArrayList<>();

        // Early return if params is null or doesn't contain lower_bounds
        if (Objects.isNull(params) || !params.containsKey("lower_bounds")) {
            return lowerBounds;
        }

        Object lowerBoundsObj = params.get("lower_bounds");
        if (!(lowerBoundsObj instanceof List)) {
            throw new IllegalArgumentException("lower_bounds must be a List");
        }

        List<?> lowerBoundsParams = (List<?>) lowerBoundsObj;

        for (Object boundObj : lowerBoundsParams) {
            if (!(boundObj instanceof Map)) {
                throw new IllegalArgumentException("Each lower bound must be a Map");
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> lowerBound = (Map<String, Object>) boundObj;

            // Validate required fields
            if (!lowerBound.containsKey("apply_strict_cutoff") || !lowerBound.containsKey("min_score")) {
                throw new IllegalArgumentException("Lower bound must contain 'apply_strict_cutoff' and 'min_score' fields");
            }

            try {
                boolean applyStrictCutoff = Boolean.parseBoolean(String.valueOf(lowerBound.get("apply_strict_cutoff")));
                float minScore = Float.parseFloat(String.valueOf(lowerBound.get("min_score")));

                // Validate minScore is within reasonable bounds if needed
                if (Float.isNaN(minScore) || Float.isInfinite(minScore)) {
                    throw new IllegalArgumentException("min_score must be a valid finite number");
                }

                lowerBounds.add(ImmutablePair.of(applyStrictCutoff, minScore));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid format for min_score: must be a valid float value", e);
            }
        }

        return lowerBounds;
    }
}
