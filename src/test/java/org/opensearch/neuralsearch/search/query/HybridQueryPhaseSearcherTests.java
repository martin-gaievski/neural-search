/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.search.query;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.LinkedList;

import lombok.SneakyThrows;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.opensearch.action.OriginalIndices;
import org.opensearch.index.Index;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.knn.index.mapper.KNNVectorFieldMapper;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.neuralsearch.search.HybridTopScoreDocCollector;
import org.opensearch.neuralsearch.search.query.HybridQueryPhaseSearcher;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QueryCollectorContext;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.TestSearchContext;

import com.carrotsearch.randomizedtesting.RandomizedTest;

public class HybridQueryPhaseSearcherTests extends OpenSearchQueryTestCase {
    private static final String VECTOR_FIELD_NAME = "vectorField";
    private static final String TEXT_FIELD_NAME = "field";
    private static final String QUERY_TEXT = "Hello world!";
    private static final String TERM_QUERY_TEXT = "keyword";
    private static final String TEST_DOC_TEXT1 = "Hello world";
    private static final String TEST_DOC_TEXT2 = "Hi to this place";
    private static final String TEST_DOC_TEXT3 = "We would like to welcome everyone";
    Index dummyIndex = new Index("dummy", "dummy");

    @SneakyThrows
    public void testQueryType_whenQueryIsHybrid_thenCallHybridDocCollector() {
        HybridQueryPhaseSearcher hybridQueryPhaseSearcher = spy(new HybridQueryPhaseSearcher());
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        KNNVectorFieldMapper.KNNVectorFieldType mockKNNVectorField = mock(KNNVectorFieldMapper.KNNVectorFieldType.class);
        when(mockQueryShardContext.index()).thenReturn(dummyIndex);
        when(mockKNNVectorField.getDimension()).thenReturn(4);
        when(mockQueryShardContext.fieldMapper(eq(VECTOR_FIELD_NAME))).thenReturn(mockKNNVectorField);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();

        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), TEST_DOC_TEXT1, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), TEST_DOC_TEXT2, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), TEST_DOC_TEXT3, ft));
        w.commit();

        IndexReader reader = DirectoryReader.open(w);

        ContextIndexSearcher contextIndexSearcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true,
            null
        );

        SearchContext searchContext = mock(SearchContext.class);
        ShardId shardId = new ShardId(dummyIndex, 1);
        SearchShardTarget shardTarget = new SearchShardTarget(
            randomAlphaOfLength(10),
            shardId,
            randomAlphaOfLength(10),
            OriginalIndices.NONE
        );
        when(searchContext.shardTarget()).thenReturn(shardTarget);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);

        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();
        boolean hasFilterCollector = randomBoolean();
        boolean hasTimeout = randomBoolean();

        HybridQueryBuilder queryBuilder = new HybridQueryBuilder();

        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, "hello");
        queryBuilder.add(termSubQuery);

        Query query = queryBuilder.toQuery(mockQueryShardContext);
        when(searchContext.query()).thenReturn(query);

        hybridQueryPhaseSearcher.searchWith(searchContext, contextIndexSearcher, query, collectors, hasFilterCollector, hasTimeout);

        w.close();
        reader.close();
        directory.close();

        verify(hybridQueryPhaseSearcher, atLeastOnce()).searchWithCollector(any(), any(), any(), any(), anyBoolean(), anyBoolean());
    }

    @SneakyThrows
    public void testQueryType_whenQueryIsNotHybrid_thenCallGeneralDocCollector() {
        HybridQueryPhaseSearcher hybridQueryPhaseSearcher = spy(new HybridQueryPhaseSearcher());
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();

        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), TEST_DOC_TEXT1, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), TEST_DOC_TEXT2, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), TEST_DOC_TEXT3, ft));
        w.commit();

        IndexReader reader = DirectoryReader.open(w);

        ContextIndexSearcher contextIndexSearcher = new ContextIndexSearcher(
                reader,
                IndexSearcher.getDefaultSimilarity(),
                IndexSearcher.getDefaultQueryCache(),
                IndexSearcher.getDefaultQueryCachingPolicy(),
                true,
                null
        );

        SearchContext searchContext = mock(SearchContext.class);
        ShardId shardId = new ShardId(dummyIndex, 1);
        SearchShardTarget shardTarget = new SearchShardTarget(
                randomAlphaOfLength(10),
                shardId,
                randomAlphaOfLength(10),
                OriginalIndices.NONE
        );
        when(searchContext.shardTarget()).thenReturn(shardTarget);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        when(searchContext.queryResult()).thenReturn(new QuerySearchResult());

        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();
        boolean hasFilterCollector = randomBoolean();
        boolean hasTimeout = randomBoolean();

        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, "hello");

        Query query = termSubQuery.toQuery(mockQueryShardContext);
        when(searchContext.query()).thenReturn(query);

        hybridQueryPhaseSearcher.searchWith(searchContext, contextIndexSearcher, query, collectors, hasFilterCollector, hasTimeout);

        w.close();
        reader.close();
        directory.close();

        verify(hybridQueryPhaseSearcher, never()).searchWithCollector(any(), any(), any(), any(), anyBoolean(), anyBoolean());
    }
}
