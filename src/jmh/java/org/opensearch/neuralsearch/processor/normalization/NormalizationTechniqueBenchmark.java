/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.neuralsearch.processor.NormalizeScoresDTO;
import org.opensearch.neuralsearch.processor.SearchShard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@Fork(1)
public class NormalizationTechniqueBenchmark {
    private MinMaxScoreNormalizationTechnique technique;
    private Map<String, Object> parametersWithBounds;
    private Map<String, Object> parametersEmpty;
    private NormalizeScoresDTO testScores;

    @Setup
    public void setup() {
        technique = new MinMaxScoreNormalizationTechnique();
        List<CompoundTopDocs> testDocs = createTestCompoundTopDocs(new float[] { 0.1f, 0.5f, 0.8f });
        testScores = new NormalizeScoresDTO(testDocs, technique);
    }

    private List<CompoundTopDocs> createTestCompoundTopDocs(float[] scores) {
        List<CompoundTopDocs> result = new ArrayList<>();
        for (float score : scores) {
            ScoreDoc[] scoreDocs = new ScoreDoc[] { new ScoreDoc(1, score) };
            TopDocs topDocs = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), scoreDocs);
            TotalHits totalHits = new TotalHits(1, TotalHits.Relation.EQUAL_TO);
            List<TopDocs> topDocsList = Arrays.asList(topDocs);
            List<ScoreDoc> scoreDocList = Arrays.asList(scoreDocs);
            SearchShard searchShard = null;

            result.add(new CompoundTopDocs(totalHits, topDocsList, scoreDocList, searchShard));
        }
        return result;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public String describeWithBounds() {
        return technique.describe();
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void normalizeScores(Blackhole blackhole) {
        technique.normalize(testScores);
        blackhole.consume(testScores);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void normalizeWithDifferentScores(Blackhole blackhole) {
        List<CompoundTopDocs> lowDocs = createTestCompoundTopDocs(new float[] { 0.1f, 0.2f, 0.3f });
        List<CompoundTopDocs> midDocs = createTestCompoundTopDocs(new float[] { 0.4f, 0.5f, 0.6f });
        List<CompoundTopDocs> highDocs = createTestCompoundTopDocs(new float[] { 0.7f, 0.8f, 0.9f });

        NormalizeScoresDTO lowScores = new NormalizeScoresDTO(lowDocs, technique);
        NormalizeScoresDTO midScores = new NormalizeScoresDTO(midDocs, technique);
        NormalizeScoresDTO highScores = new NormalizeScoresDTO(highDocs, technique);

        technique.normalize(lowScores);
        technique.normalize(midScores);
        technique.normalize(highScores);

        blackhole.consume(lowScores);
        blackhole.consume(midScores);
        blackhole.consume(highScores);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public String emptyParameters() {
        MinMaxScoreNormalizationTechnique emptyTechnique = new MinMaxScoreNormalizationTechnique();
        return emptyTechnique.describe();
    }
}
