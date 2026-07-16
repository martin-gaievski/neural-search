/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.opensearch.neuralsearch.query.HybridSubQueryScorer;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;

import lombok.SneakyThrows;

/**
 * POC: verifies the shard-side half of condition-based result boost — that {@link HybridTopScoreDocCollector}
 * evaluates ordered boost conditions per collected document and records the first-matching condition index (tier).
 */
public class HybridConditionalBoostCollectorTests extends HybridCollectorTestCase {

    private static final String TEXT_FIELD = "title";
    private static final String PROMO_FIELD = "promo";
    private static final int TOTAL_HITS_UP_TO = 1000;
    private static final int NUM_HITS = 10;

    /**
     * Index layout (segment-local docId assigned in add order):
     *   doc0 promo="hero"   -> matches condition[0] -> tier 0
     *   doc1 promo="sale"   -> matches condition[1] -> tier 1
     *   doc2 promo="hero"   -> matches condition[0] -> tier 0
     *   doc3 (no promo)     -> organic (absent from map)
     *   doc4 promo="hero"+"sale" via two values -> matches condition[0] first -> tier 0 (highest priority wins)
     */
    @SneakyThrows
    public void testTierAssignment_whenMultipleOrderedConditions_thenFirstMatchWins() {
        final Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());

        w.addDocument(doc("running shoes", "hero"));      // doc0
        w.addDocument(doc("running shoes", "sale"));       // doc1
        w.addDocument(doc("trail shoes", "hero"));         // doc2
        w.addDocument(doc("dress shoes"));                  // doc3 (no promo)
        Document multi = new Document();
        multi.add(new StringField(TEXT_FIELD, "sport shoes", Field.Store.NO));
        multi.add(new StringField(PROMO_FIELD, "sale", Field.Store.NO));
        multi.add(new StringField(PROMO_FIELD, "hero", Field.Store.NO));
        w.addDocument(multi);                               // doc4 (both promos)
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = new IndexSearcher(reader);

        // Ordered conditions: condition[0] = promo:hero (top tier), condition[1] = promo:sale (second tier).
        Weight heroWeight = searcher.createWeight(
            searcher.rewrite(new TermQuery(new Term(PROMO_FIELD, "hero"))),
            ScoreMode.COMPLETE_NO_SCORES,
            1f
        );
        Weight saleWeight = searcher.createWeight(
            searcher.rewrite(new TermQuery(new Term(PROMO_FIELD, "sale"))),
            ScoreMode.COMPLETE_NO_SCORES,
            1f
        );

        HybridTopScoreDocCollector collector = new HybridTopScoreDocCollector(
            NUM_HITS,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO),
            List.of(heroWeight, saleWeight)
        );

        // single-segment index (one commit) -> one leaf; docBase == 0
        LeafReaderContext leaf = reader.getContext().leaves().get(0);
        LeafCollector leafCollector = collector.getLeafCollector(leaf);

        HybridSubQueryScorer scorer = new HybridSubQueryScorer(2);
        leafCollector.setScorer(scorer);

        // Drive collection in a SINGLE ascending pass over segment-local docIds (0..4), setting a positive score on
        // at least one sub-query so the doc is collected. This respects the ascending-order contract of the
        // sequential-access condition bits (unlike a per-sub-query multi-pass driver).
        int numDocs = reader.maxDoc();
        for (int doc = 0; doc < numDocs; doc++) {
            scorer.getSubQueryScores()[0] = 1.0f;
            scorer.getSubQueryScores()[1] = 1.0f;
            leafCollector.collect(doc);
            scorer.resetScores();
        }

        Map<Integer, Integer> docIdToTier = collector.getDocIdToTier();

        // doc0, doc2, doc4 -> tier 0 (hero, first matching condition); doc1 -> tier 1 (sale); doc3 -> organic (absent)
        assertEquals(Integer.valueOf(0), docIdToTier.get(0));
        assertEquals(Integer.valueOf(1), docIdToTier.get(1));
        assertEquals(Integer.valueOf(0), docIdToTier.get(2));
        assertFalse("organic doc must not be tiered", docIdToTier.containsKey(3));
        assertEquals("doc matching both conditions takes the highest (first) tier", Integer.valueOf(0), docIdToTier.get(4));
        // doc0, doc1, doc2, doc4 are tiered; only doc3 (no promo) is organic
        assertEquals(4, docIdToTier.size());

        reader.close();
        w.close();
        directory.close();
    }

    /**
     * A condition that matches no document on the segment yields a MatchNoBits, so no doc is tiered and get() never
     * throws.
     */
    @SneakyThrows
    public void testTierAssignment_whenConditionMatchesNothing_thenAllOrganic() {
        final Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());
        w.addDocument(doc("running shoes", "hero"));       // doc0
        w.addDocument(doc("trail shoes", "hero"));         // doc1
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = new IndexSearcher(reader);

        Weight noMatchWeight = searcher.createWeight(
            searcher.rewrite(new TermQuery(new Term(PROMO_FIELD, "does-not-exist"))),
            ScoreMode.COMPLETE_NO_SCORES,
            1f
        );

        HybridTopScoreDocCollector collector = new HybridTopScoreDocCollector(
            NUM_HITS,
            new HitsThresholdChecker(TOTAL_HITS_UP_TO),
            List.of(noMatchWeight)
        );
        LeafReaderContext leaf = reader.getContext().leaves().get(0);
        LeafCollector leafCollector = collector.getLeafCollector(leaf);
        HybridSubQueryScorer scorer = new HybridSubQueryScorer(2);
        leafCollector.setScorer(scorer);

        for (int doc = 0; doc < reader.maxDoc(); doc++) {
            scorer.getSubQueryScores()[0] = 1.0f;
            leafCollector.collect(doc);
            scorer.resetScores();
        }

        assertTrue("no doc should be tiered when the condition matches nothing", collector.getDocIdToTier().isEmpty());

        reader.close();
        w.close();
        directory.close();
    }

    /**
     * No boost conditions configured (empty weight list) -> tier map stays empty, feature is a no-op.
     */
    @SneakyThrows
    public void testTierAssignment_whenNoConditions_thenNoOp() {
        final Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());
        w.addDocument(doc("running shoes", "hero"));
        w.commit();

        DirectoryReader reader = DirectoryReader.open(w);
        HybridTopScoreDocCollector collector = new HybridTopScoreDocCollector(NUM_HITS, new HitsThresholdChecker(TOTAL_HITS_UP_TO));
        LeafReaderContext leaf = reader.getContext().leaves().get(0);
        LeafCollector leafCollector = collector.getLeafCollector(leaf);
        HybridSubQueryScorer scorer = new HybridSubQueryScorer(2);
        leafCollector.setScorer(scorer);
        scorer.getSubQueryScores()[0] = 1.0f;
        leafCollector.collect(0);

        assertTrue(collector.getDocIdToTier().isEmpty());

        reader.close();
        w.close();
        directory.close();
    }

    private Document doc(String title, String promo) {
        Document d = new Document();
        d.add(new StringField(TEXT_FIELD, title, Field.Store.NO));
        d.add(new StringField(PROMO_FIELD, promo, Field.Store.NO));
        return d;
    }

    private Document doc(String title) {
        Document d = new Document();
        d.add(new StringField(TEXT_FIELD, title, Field.Store.NO));
        return d;
    }
}
