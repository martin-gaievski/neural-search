/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.search.DisiPriorityQueue;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DisjunctionDISIApproximation;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.PriorityQueue;
import org.opensearch.neuralsearch.search.HybridDisiWrapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Class abstracts functionality of Scorer for hybrid query. When iterating over documents in increasing
 * order of doc id, this class fills up array of scores per sub-query for each doc id. Order in array of scores
 * corresponds to order of sub-queries in an input Hybrid query.
 */
@Log4j2
public final class HybridQueryScorer_HighestScoreDocIterator extends Scorer {

    // score for each of sub-query in this hybrid query
    @Getter
    private final List<HybridScorerWrapper> subScorers;

    // private final DisiPriorityQueue subScorersPQ;
    final DocIdSetIterator[] approximations;
    final TwoPhaseIterator[] twoPhases;

    Map<Integer, float[]> scoresByDocId = new HashMap<>();

    // private final float[] subScores;
    float minScore;

    // private final DocIdSetIterator approximation;
    // private final HybridScoreBlockBoundaryPropagator disjunctionBlockPropagator;
    // private final TwoPhase twoPhase;

    public HybridQueryScorer_HighestScoreDocIterator(final Weight weight, final List<Scorer> subScorers) throws IOException {
        this(weight, subScorers, ScoreMode.TOP_SCORES);
    }

    HybridQueryScorer_HighestScoreDocIterator(final Weight weight, final List<Scorer> subScorers, final ScoreMode scoreMode)
        throws IOException {
        super(weight);

        this.subScorers = new ArrayList<>();
        for (int i = 0; i < subScorers.size(); i++) {
            Scorer s = subScorers.get(i);
            HybridScorerWrapper hybridQueryScorer = new HybridScorerWrapper(i, s);
            this.subScorers.add(hybridQueryScorer);
        }
        // sort scorers by cost
        // this.subScorers.sort(Comparator.comparingLong(s -> s.iterator().cost()));
        this.subScorers.sort((s1, s2) -> {
            if (Objects.isNull(s1.getScorer()) && Objects.isNull(s2.getScorer())) {
                return 0;
            }
            if (Objects.isNull(s1.getScorer())) {
                return -1;
            }
            if (Objects.isNull(s2.getScorer())) {
                return 1;
            }
            return Long.compare(s1.getScorer().iterator().cost(), s2.getScorer().iterator().cost());
        });
        this.approximations = new DocIdSetIterator[subScorers.size()];
        List<TwoPhaseIterator> twoPhaseList = new ArrayList<>();
        for (int i = 0; i < subScorers.size(); i++) {
            Scorer scorer = subScorers.get(i);
            if (Objects.isNull(scorer)) {
                continue;
            }
            TwoPhaseIterator twoPhase = scorer.twoPhaseIterator();
            if (twoPhase != null) {
                twoPhaseList.add(twoPhase);
                approximations[i] = twoPhase.approximation();
            } else {
                approximations[i] = scorer.iterator();
            }
            scorer.advanceShallow(0);
        }
        this.twoPhases = twoPhaseList.toArray(new TwoPhaseIterator[twoPhaseList.size()]);
        Arrays.sort(this.twoPhases, Comparator.comparingDouble(TwoPhaseIterator::matchCost));

        // subScores = new float[subScorers.size()];
        // this.subScorersPQ = initializeSubScorersPQ();
        boolean needsScores = scoreMode != ScoreMode.COMPLETE_NO_SCORES;

        // this.approximation = new HybridSubqueriesDISIApproximation(this.subScorersPQ);
        /*if (scoreMode == ScoreMode.TOP_SCORES) {
            this.disjunctionBlockPropagator = new HybridScoreBlockBoundaryPropagator(subScorers);
        } else {
            this.disjunctionBlockPropagator = null;
        }

        boolean hasApproximation = false;
        float sumMatchCost = 0;
        long sumApproxCost = 0;
        // Compute matchCost as the average over the matchCost of the subScorers.
        // This is weighted by the cost, which is an expected number of matching documents.
        for (DisiWrapper w : subScorersPQ) {
            long costWeight = (w.cost <= 1) ? 1 : w.cost;
            sumApproxCost += costWeight;
            if (w.twoPhaseView != null) {
                hasApproximation = true;
                sumMatchCost += w.matchCost * costWeight;
            }
        }
        if (!hasApproximation) { // no sub scorer supports approximations
            twoPhase = null;
        } else {
            final float matchCost = sumMatchCost / sumApproxCost;
            twoPhase = new TwoPhase(approximation, matchCost, subScorersPQ, needsScores);
        }*/
    }

    /**
     * Returns the score of the current document matching the query. Score is a sum of all scores from sub-query scorers.
     * @return combined total score of all sub-scores
     * @throws IOException
     */
    @Override
    public float score() throws IOException {
        int docId = docID();
        float[] scores = new float[subScorers.size()];
        scoresByDocId.put(docId, scores);
        double score = 0;
        for (HybridScorerWrapper scorerWrapper : subScorers) {
            Scorer scorer = scorerWrapper.getScorer();
            if (Objects.nonNull(scorer)) {
                score += scorer.score();
                scores[scorerWrapper.getQueryIndex()] = scorer.score();
            }
        }
        return (float) score;
    }

    private float score(DisiWrapper topList) throws IOException {
        float totalScore = 0.0f;
        for (DisiWrapper disiWrapper = topList; disiWrapper != null; disiWrapper = disiWrapper.next) {
            // check if this doc has match in the subQuery. If not, add score as 0.0 and continue
            if (disiWrapper.scorer.docID() == DocIdSetIterator.NO_MORE_DOCS) {
                continue;
            }
            totalScore += disiWrapper.scorer.score();
        }
        return totalScore;
    }

    /**
     * Return a DocIdSetIterator over matching documents.
     * @return DocIdSetIterator object
     */
    @Override
    public DocIdSetIterator iterator() {
        return twoPhases.length == 0 ? approximation() : TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator());
    }

    @Override
    public TwoPhaseIterator twoPhaseIterator() {
        if (twoPhases.length == 0) {
            return null;
        }
        float matchCost = (float) Arrays.stream(twoPhases).mapToDouble(TwoPhaseIterator::matchCost).sum();
        final DocIdSetIterator approx = approximation();
        return new TwoPhaseIterator(approx) {
            @Override
            public boolean matches() throws IOException {
                for (TwoPhaseIterator twoPhase : twoPhases) {
                    assert twoPhase.approximation().docID() == docID();
                    if (twoPhase.matches() == false) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public float matchCost() {
                return matchCost;
            }
        };
    }

    private DocIdSetIterator approximation() {
        final DocIdSetIterator lead = approximations[0];

        return new DocIdSetIterator() {

            float maxScore;
            int upTo = -1;

            @Override
            public int docID() {
                return lead == null ? NO_MORE_DOCS : lead.docID();
            }

            @Override
            public long cost() {
                return lead.cost();
            }

            private void moveToNextBlock(int target) throws IOException {
                upTo = advanceShallow(target);
                maxScore = getMaxScore(upTo);
            }

            private int advanceTarget(int target) throws IOException {
                if (target > upTo) {
                    moveToNextBlock(target);
                }

                while (true) {
                    assert upTo >= target;

                    if (maxScore >= minScore) {
                        return target;
                    }

                    if (upTo == NO_MORE_DOCS) {
                        return NO_MORE_DOCS;
                    }

                    target = upTo + 1;

                    moveToNextBlock(target);
                }
            }

            @Override
            public int nextDoc() throws IOException {
                return advance(docID() + 1);
            }

            @Override
            public int advance(int target) throws IOException {
                return doNext(lead.advance(advanceTarget(target)));
            }

            private int doNext(int doc) throws IOException {
                advanceHead: for (;;) {
                    assert doc == lead.docID();

                    if (doc == NO_MORE_DOCS) {
                        return NO_MORE_DOCS;
                    }

                    if (doc > upTo) {
                        // This check is useful when scorers return information about blocks
                        // that do not actually have any matches. Otherwise `doc` will always
                        // be in the current block already since it is always the result of
                        // lead.advance(advanceTarget(some_doc_id))
                        final int nextTarget = advanceTarget(doc);
                        if (nextTarget != doc) {
                            doc = lead.advance(nextTarget);
                            continue;
                        }
                    }

                    assert doc <= upTo;

                    // then find agreement with other iterators
                    for (int i = 1; i < approximations.length; ++i) {
                        final DocIdSetIterator other = approximations[i];
                        if (Objects.isNull(other)) {
                            continue;
                        }
                        // other.doc may already be equal to doc if we "continued advanceHead"
                        // on the previous iteration and the advance on the lead scorer exactly matched.
                        if (other.docID() < doc) {
                            final int next = other.advance(doc);

                            if (next > doc) {
                                // iterator beyond the current doc - advance lead and continue to the new highest
                                // doc.
                                doc = lead.advance(advanceTarget(next));
                                continue advanceHead;
                            }
                        }

                        assert other.docID() == doc;
                    }

                    // success - all iterators are on the same doc and the score is competitive
                    return doc;
                }
            }
        };
    }

    public int advanceShallow(int target) throws IOException {
        // We use block boundaries of the lead scorer.
        // It is tempting to fold in other clauses as well to have better bounds of
        // the score, but then there is a risk of not progressing fast enough.
        int result = subScorers.get(0).getScorer().advanceShallow(target);
        // But we still need to shallow-advance other clauses, in order to have
        // better score upper bounds
        for (int i = 1; i < subScorers.size(); ++i) {
            Scorer scorer = subScorers.get(i).getScorer();
            if (Objects.nonNull(scorer)) {
                scorer.advanceShallow(target);
            }
        }
        return result;
    }

    /**
     * Return the maximum score that documents between the last target that this iterator was shallow-advanced to included and upTo included.
     * @param upTo upper limit for document id
     * @return max score
     * @throws IOException
     */
    @Override
    public float getMaxScore(int upTo) throws IOException {
        double sum = 0;
        for (HybridScorerWrapper scorerWrapper : subScorers) {
            Scorer scorer = scorerWrapper.getScorer();
            if (Objects.nonNull(scorer)) {
                sum += scorer.getMaxScore(upTo);
            }
        }
        return (float) sum;
    }

    @Override
    public void setMinCompetitiveScore(float score) throws IOException {
        minScore = score;
    }

    /**
     * Returns the doc ID that is currently being scored.
     * @return document id
     */
    @Override
    public int docID() {
        return subScorers.get(0).getScorer().docID();
    }

    /**
     * Return array of scores per sub-query for doc id that is defined by current iterator position
     * @return
     * @throws IOException
     */
    public float[] hybridScores() throws IOException {
        // float[] scores = new float[subScorers.size()];// new float[subScores.length];
        /*DisiWrapper topList = subScorersPQ.topList();
        if (topList instanceof HybridDisiWrapper == false) {
            log.error(
                String.format(
                    Locale.ROOT,
                    "Unexpected type of DISI wrapper, expected [%s] but found [%s]",
                    HybridDisiWrapper.class.getSimpleName(),
                    subScorersPQ.topList().getClass().getSimpleName()
                )
            );
            throw new IllegalStateException(
                "Unable to collect scores for one of the sub-queries, encountered an unexpected type of score iterator."
            );
        }
        for (HybridDisiWrapper disiWrapper = (HybridDisiWrapper) topList; disiWrapper != null; disiWrapper =
            (HybridDisiWrapper) disiWrapper.next) {
            // check if this doc has match in the subQuery. If not, add score as 0.0 and continue
            Scorer scorer = disiWrapper.scorer;
            if (scorer.docID() == DocIdSetIterator.NO_MORE_DOCS) {
                continue;
            }
            scores[disiWrapper.getSubQueryIndex()] = scorer.score();
        }
        return scores;*/
        /*for (HybridScorerWrapper hybridScorerWrapper : subScorers) {
            int idx = hybridScorerWrapper.getQueryIndex();
            if (Objects.nonNull(hybridScorerWrapper.getScorer())) {
                scores[idx] = hybridScorerWrapper.getScorer().score();
            }
        }*/
        float[] scores = scoresByDocId.get(docID());
        return scores;
    }

    private DisiPriorityQueue initializeSubScorersPQ() {
        Objects.requireNonNull(subScorers, "should not be null");
        // we need to count this way in order to include all identical sub-queries
        int numOfSubQueries = subScorers.size();
        DisiPriorityQueue subScorersPQ = new DisiPriorityQueue(numOfSubQueries);
        for (int idx = 0; idx < subScorers.size(); idx++) {
            Scorer scorer = subScorers.get(idx).getScorer();
            if (scorer == null) {
                continue;
            }
            final HybridDisiWrapper disiWrapper = new HybridDisiWrapper(scorer, idx);
            subScorersPQ.add(disiWrapper);
        }
        return subScorersPQ;
    }

    @Override
    public Collection<ChildScorable> getChildren() {
        ArrayList<ChildScorable> children = new ArrayList<>();
        for (HybridScorerWrapper hybridScorerWrapper : subScorers) {
            Scorer scorer = hybridScorerWrapper.getScorer();
            children.add(new ChildScorable(scorer, "MUST"));
        }
        return children;
    }

    /**
     *  Object returned by {@link Scorer#twoPhaseIterator()} to provide an approximation of a {@link DocIdSetIterator}.
     *  After calling {@link DocIdSetIterator#nextDoc()} or {@link DocIdSetIterator#advance(int)} on the iterator
     *  returned by approximation(), you need to check {@link TwoPhaseIterator#matches()} to confirm if the retrieved
     *  document ID is a match. Implementation inspired by identical class for
     *  <a href="https://github.com/apache/lucene/blob/branch_9_10/lucene/core/src/java/org/apache/lucene/search/DisjunctionScorer.java">DisjunctionScorer</a>
     */
    static class TwoPhase extends TwoPhaseIterator {
        private final float matchCost;
        // list of verified matches on the current doc
        DisiWrapper verifiedMatches;
        // priority queue of approximations on the current doc that have not been verified yet
        final PriorityQueue<DisiWrapper> unverifiedMatches;
        DisiPriorityQueue subScorers;
        boolean needsScores;

        private TwoPhase(DocIdSetIterator approximation, float matchCost, DisiPriorityQueue subScorers, boolean needsScores) {
            super(approximation);
            this.matchCost = matchCost;
            this.subScorers = subScorers;
            unverifiedMatches = new PriorityQueue<>(subScorers.size()) {
                @Override
                protected boolean lessThan(DisiWrapper a, DisiWrapper b) {
                    return a.matchCost < b.matchCost;
                }
            };
            this.needsScores = needsScores;
        }

        DisiWrapper getSubMatches() throws IOException {
            for (DisiWrapper wrapper : unverifiedMatches) {
                if (wrapper.twoPhaseView.matches()) {
                    wrapper.next = verifiedMatches;
                    verifiedMatches = wrapper;
                }
            }
            unverifiedMatches.clear();
            return verifiedMatches;
        }

        @Override
        public boolean matches() throws IOException {
            verifiedMatches = null;
            unverifiedMatches.clear();

            for (DisiWrapper wrapper = subScorers.topList(); wrapper != null;) {
                DisiWrapper next = wrapper.next;

                if (Objects.isNull(wrapper.twoPhaseView)) {
                    // implicitly verified, move it to verifiedMatches
                    wrapper.next = verifiedMatches;
                    verifiedMatches = wrapper;

                    if (!needsScores) {
                        // we can stop here
                        return true;
                    }
                } else {
                    unverifiedMatches.add(wrapper);
                }
                wrapper = next;
            }

            if (Objects.nonNull(verifiedMatches)) {
                return true;
            }

            // verify subs that have an two-phase iterator
            // least-costly ones first
            while (unverifiedMatches.size() > 0) {
                DisiWrapper wrapper = unverifiedMatches.pop();
                if (wrapper.twoPhaseView.matches()) {
                    wrapper.next = null;
                    verifiedMatches = wrapper;
                    return true;
                }
            }
            return false;
        }

        @Override
        public float matchCost() {
            return matchCost;
        }
    }

    /**
     * A DocIdSetIterator which is a disjunction of the approximations of the provided iterators and supports
     * sub iterators that return empty results
     */
    static class HybridSubqueriesDISIApproximation extends DocIdSetIterator {
        final DocIdSetIterator docIdSetIterator;
        final DisiPriorityQueue subIterators;

        public HybridSubqueriesDISIApproximation(final DisiPriorityQueue subIterators) {
            docIdSetIterator = new DisjunctionDISIApproximation(subIterators);
            this.subIterators = subIterators;
        }

        @Override
        public long cost() {
            return docIdSetIterator.cost();
        }

        @Override
        public int docID() {
            if (subIterators.size() == 0) {
                return NO_MORE_DOCS;
            }
            return docIdSetIterator.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            if (subIterators.size() == 0) {
                return NO_MORE_DOCS;
            }
            return docIdSetIterator.nextDoc();
        }

        @Override
        public int advance(final int target) throws IOException {
            if (subIterators.size() == 0) {
                return NO_MORE_DOCS;
            }
            return docIdSetIterator.advance(target);
        }
    }

    @AllArgsConstructor
    static class HybridScorerWrapper {
        @Getter
        final int queryIndex;
        @Getter
        final Scorer scorer;
    }
}
