/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.tests.util.TestUtil;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import lombok.SneakyThrows;

public class HybridQueryScorerTests extends OpenSearchQueryTestCase {

    @SneakyThrows
    public void testWithRandomDocuments_whenOneSubScorer_thenReturnSuccessfully() {
        int maxDocId = TestUtil.nextInt(random(), 10, 10_000);
        Pair<int[], float[]> docsAndScores = generateDocuments(maxDocId);
        int[] docs = docsAndScores.getLeft();
        float[] scores = docsAndScores.getRight();

        Weight weight = mock(Weight.class);

        HybridQueryScorer hybridQueryScorer = new HybridQueryScorer(
            weight,
            Arrays.asList(scorer(docsAndScores.getLeft(), docsAndScores.getRight(), fakeWeight(new MatchAllDocsQuery())))
        );

        testWithQuery(docs, scores, hybridQueryScorer);
    }

    @SneakyThrows
    public void testWithRandomDocumentsAndHybridScores_whenMultipleScorers_thenReturnSuccessfully() {
        int maxDocId1 = TestUtil.nextInt(random(), 10, 10_000);
        Pair<int[], float[]> docsAndScores1 = generateDocuments(maxDocId1);
        int[] docs1 = docsAndScores1.getLeft();
        float[] scores1 = docsAndScores1.getRight();
        int maxDocId2 = TestUtil.nextInt(random(), 10, 10_000);
        Pair<int[], float[]> docsAndScores2 = generateDocuments(maxDocId2);
        int[] docs2 = docsAndScores2.getLeft();
        float[] scores2 = docsAndScores2.getRight();

        Weight weight = mock(Weight.class);

        HybridQueryScorer hybridQueryScorer = new HybridQueryScorer(
            weight,
            Arrays.asList(
                scorer(docs1, scores1, fakeWeight(new MatchAllDocsQuery())),
                scorer(docs2, scores2, fakeWeight(new MatchNoDocsQuery()))
            )
        );
        int doc = -1;
        int numOfActualDocs = 0;
        Set<Integer> uniqueDocs1 = Arrays.stream(docs1).boxed().collect(Collectors.toSet());
        Set<Integer> uniqueDocs2 = Arrays.stream(docs2).boxed().collect(Collectors.toSet());
        while (doc != NO_MORE_DOCS) {
            doc = hybridQueryScorer.iterator().nextDoc();
            if (doc == DocIdSetIterator.NO_MORE_DOCS) {
                continue;
            }
            float[] actualTotalScores = hybridQueryScorer.hybridScores();
            float actualTotalScore = 0.0f;
            for (float score : actualTotalScores) {
                actualTotalScore += score;
            }
            float expectedScore = 0.0f;
            if (uniqueDocs1.contains(doc)) {
                int idx = Arrays.binarySearch(docs1, doc);
                expectedScore += scores1[idx];
            }
            if (uniqueDocs2.contains(doc)) {
                int idx = Arrays.binarySearch(docs2, doc);
                expectedScore += scores2[idx];
            }
            assertEquals(expectedScore, actualTotalScore, 0.001f);
            numOfActualDocs++;
        }

        int totalUniqueCount = uniqueDocs1.size();
        for (int n : uniqueDocs2) {
            if (!uniqueDocs1.contains(n)) {
                totalUniqueCount++;
            }
        }
        assertEquals(totalUniqueCount, numOfActualDocs);
    }

    @SneakyThrows
    public void testWithRandomDocumentsAndCombinedScore_whenMultipleScorers_thenReturnSuccessfully() {
        int maxDocId1 = TestUtil.nextInt(random(), 10, 10_000);
        Pair<int[], float[]> docsAndScores1 = generateDocuments(maxDocId1);
        int[] docs1 = docsAndScores1.getLeft();
        float[] scores1 = docsAndScores1.getRight();
        int maxDocId2 = TestUtil.nextInt(random(), 10, 10_000);
        Pair<int[], float[]> docsAndScores2 = generateDocuments(maxDocId2);
        int[] docs2 = docsAndScores2.getLeft();
        float[] scores2 = docsAndScores2.getRight();

        Weight weight = mock(Weight.class);

        HybridQueryScorer hybridQueryScorer = new HybridQueryScorer(
            weight,
            Arrays.asList(
                scorer(docs1, scores1, fakeWeight(new MatchAllDocsQuery())),
                scorer(docs2, scores2, fakeWeight(new MatchNoDocsQuery()))
            )
        );
        int doc = -1;
        int numOfActualDocs = 0;
        Set<Integer> uniqueDocs1 = Arrays.stream(docs1).boxed().collect(Collectors.toSet());
        Set<Integer> uniqueDocs2 = Arrays.stream(docs2).boxed().collect(Collectors.toSet());
        while (doc != NO_MORE_DOCS) {
            doc = hybridQueryScorer.iterator().nextDoc();
            if (doc == DocIdSetIterator.NO_MORE_DOCS) {
                continue;
            }
            float expectedScore = 0.0f;
            if (uniqueDocs1.contains(doc)) {
                int idx = Arrays.binarySearch(docs1, doc);
                expectedScore += scores1[idx];
            }
            if (uniqueDocs2.contains(doc)) {
                int idx = Arrays.binarySearch(docs2, doc);
                expectedScore += scores2[idx];
            }
            float hybridScore = 0.0f;
            for (float score : hybridQueryScorer.hybridScores()) {
                hybridScore += score;
            }
            assertEquals(expectedScore, hybridScore, 0.001f);
            numOfActualDocs++;
        }

        int totalUniqueCount = uniqueDocs1.size();
        for (int n : uniqueDocs2) {
            if (!uniqueDocs1.contains(n)) {
                totalUniqueCount++;
            }
        }
        assertEquals(totalUniqueCount, numOfActualDocs);
    }

    @SneakyThrows
    public void testWithRandomDocuments_whenMultipleScorersAndSomeScorersEmpty_thenReturnSuccessfully() {
        int maxDocId = TestUtil.nextInt(random(), 10, 10_000);
        Pair<int[], float[]> docsAndScores = generateDocuments(maxDocId);
        int[] docs = docsAndScores.getLeft();
        float[] scores = docsAndScores.getRight();

        Weight weight = mock(Weight.class);

        HybridQueryScorer hybridQueryScorer = new HybridQueryScorer(
            weight,
            Arrays.asList(null, scorer(docs, scores, fakeWeight(new MatchAllDocsQuery())), null)
        );

        testWithQuery(docs, scores, hybridQueryScorer);
    }

    @SneakyThrows
    public void testMaxScore_whenMultipleScorers_thenSuccessful() {
        int maxDocId = TestUtil.nextInt(random(), 10, 10_000);
        Pair<int[], float[]> docsAndScores = generateDocuments(maxDocId);
        int[] docs = docsAndScores.getLeft();
        float[] scores = docsAndScores.getRight();

        Weight weight = mock(Weight.class);

        HybridQueryScorer hybridQueryScorerWithAllNonNullSubScorers = new HybridQueryScorer(
            weight,
            Arrays.asList(
                scorer(docs, scores, fakeWeight(new MatchAllDocsQuery())),
                scorer(docs, scores, fakeWeight(new MatchNoDocsQuery()))
            )
        );

        float maxScore = hybridQueryScorerWithAllNonNullSubScorers.getMaxScore(Integer.MAX_VALUE);
        assertTrue(maxScore > 0.0f);

        HybridQueryScorer hybridQueryScorerWithSomeNullSubScorers = new HybridQueryScorer(
            weight,
            Arrays.asList(null, scorer(docs, scores, fakeWeight(new MatchAllDocsQuery())), null)
        );

        maxScore = hybridQueryScorerWithSomeNullSubScorers.getMaxScore(Integer.MAX_VALUE);
        assertTrue(maxScore > 0.0f);

        HybridQueryScorer hybridQueryScorerWithAllNullSubScorers = new HybridQueryScorer(weight, Arrays.asList(null, null));

        maxScore = hybridQueryScorerWithAllNullSubScorers.getMaxScore(Integer.MAX_VALUE);
        assertEquals(0.0f, maxScore, 0.0f);
    }

    @SneakyThrows
    public void testMaxScoreFailures_whenScorerThrowsException_thenFail() {
        int maxDocId = TestUtil.nextInt(random(), 10, 10_000);
        Pair<int[], float[]> docsAndScores = generateDocuments(maxDocId);
        int[] docs = docsAndScores.getLeft();
        float[] scores = docsAndScores.getRight();

        Weight weight = mock(Weight.class);

        Scorer scorer = mock(Scorer.class);
        when(scorer.getWeight()).thenReturn(fakeWeight(new MatchAllDocsQuery()));
        when(scorer.iterator()).thenReturn(iterator(docs));
        when(scorer.getMaxScore(anyInt())).thenThrow(new IOException("Test exception"));

        IOException runtimeException = expectThrows(IOException.class, () -> new HybridQueryScorer(weight, Arrays.asList(scorer)));
        assertTrue(runtimeException.getMessage().contains("Test exception"));
    }

    @SneakyThrows
    public void testApproximationIterator_whenSubScorerSupportsApproximation_thenSuccessful() {
        final int maxDoc = TestUtil.nextInt(random(), 10, 1_000);
        final int numDocs = TestUtil.nextInt(random(), 1, maxDoc / 2);
        final Set<Integer> uniqueDocs = new HashSet<>();
        while (uniqueDocs.size() < numDocs) {
            uniqueDocs.add(random().nextInt(maxDoc));
        }
        final int[] docs = new int[numDocs];
        int i = 0;
        for (int doc : uniqueDocs) {
            docs[i++] = doc;
        }
        Arrays.sort(docs);
        final float[] scores1 = new float[numDocs];
        for (i = 0; i < numDocs; ++i) {
            scores1[i] = random().nextFloat();
        }
        final float[] scores2 = new float[numDocs];
        for (i = 0; i < numDocs; ++i) {
            scores2[i] = random().nextFloat();
        }

        Weight weight = mock(Weight.class);

        HybridQueryScorer queryScorer = new HybridQueryScorer(
            weight,
            Arrays.asList(
                scorerWithTwoPhaseIterator(docs, scores1, fakeWeight(new MatchAllDocsQuery()), maxDoc),
                scorerWithTwoPhaseIterator(docs, scores2, fakeWeight(new MatchNoDocsQuery()), maxDoc)
            )
        );

        int doc = -1;
        int idx = 0;
        while (doc != DocIdSetIterator.NO_MORE_DOCS) {
            doc = queryScorer.iterator().nextDoc();
            if (idx == docs.length) {
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, doc);
            } else {
                assertEquals(docs[idx], doc);
                assertEquals(scores1[idx] + scores2[idx], queryScorer.score(), 0.001f);
            }
            idx++;
        }
    }

    protected static Scorer scorerWithTwoPhaseIterator(final int[] docs, final float[] scores, Weight weight, int maxDoc) {
        final DocIdSetIterator iterator = DocIdSetIterator.all(maxDoc);
        return new Scorer(weight) {

            int lastScoredDoc = -1;

            public DocIdSetIterator iterator() {
                return TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator());
            }

            @Override
            public int docID() {
                return iterator.docID();
            }

            @Override
            public float score() {
                assertNotEquals("score() called twice on doc " + docID(), lastScoredDoc, docID());
                lastScoredDoc = docID();
                final int idx = Arrays.binarySearch(docs, docID());
                return scores[idx];
            }

            @Override
            public float getMaxScore(int upTo) {
                return Float.MAX_VALUE;
            }

            @Override
            public TwoPhaseIterator twoPhaseIterator() {
                return new TwoPhaseIterator(iterator) {

                    @Override
                    public boolean matches() {
                        return Arrays.binarySearch(docs, iterator.docID()) >= 0;
                    }

                    @Override
                    public float matchCost() {
                        return 10;
                    }
                };
            }
        };
    }

    private Pair<int[], float[]> generateDocuments(int maxDocId) {
        final int numDocs = RandomizedTest.randomIntBetween(1, maxDocId / 2);
        final int[] docs = new int[numDocs];
        final Set<Integer> uniqueDocs = new HashSet<>();
        while (uniqueDocs.size() < numDocs) {
            uniqueDocs.add(random().nextInt(maxDocId));
        }
        int i = 0;
        for (int doc : uniqueDocs) {
            docs[i++] = doc;
        }
        Arrays.sort(docs);
        final float[] scores = new float[numDocs];
        for (int j = 0; j < numDocs; ++j) {
            scores[j] = random().nextFloat();
        }
        return new ImmutablePair<>(docs, scores);
    }

    private void testWithQuery(int[] docs, float[] scores, HybridQueryScorer hybridQueryScorer) throws IOException {
        int doc = -1;
        int numOfActualDocs = 0;
        while (doc != NO_MORE_DOCS) {
            int target = doc + 1;
            doc = hybridQueryScorer.iterator().nextDoc();
            int idx = Arrays.binarySearch(docs, target);
            idx = (idx >= 0) ? idx : (-1 - idx);
            if (idx == docs.length) {
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, doc);
            } else {
                assertEquals(docs[idx], doc);
                assertEquals(scores[idx], hybridQueryScorer.score(), 0f);
                numOfActualDocs++;
            }
        }
        assertEquals(docs.length, numOfActualDocs);
    }
}
