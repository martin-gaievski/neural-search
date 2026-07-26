/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.mockito.Mockito.mock;
import static org.opensearch.neuralsearch.util.NeuralSearchClusterTestUtils.setUpClusterService;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.opensearch.Version;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.neuralsearch.util.TestUtils;

import lombok.SneakyThrows;

public class HybridQueryBuilderFusedModeTests extends OpenSearchQueryTestCase {

    private static final String TEXT_FIELD_NAME = "field";
    private static final String TERM_QUERY_TEXT = "keyword";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        TestUtils.initializeEventStatsManager();
        setUpClusterService(Version.CURRENT);
    }

    private NamedXContentRegistry fusedModeXContentRegistry() {
        return new NamedXContentRegistry(
            List.of(
                new NamedXContentRegistry.Entry(QueryBuilder.class, new ParseField(TermQueryBuilder.NAME), TermQueryBuilder::fromXContent),
                new NamedXContentRegistry.Entry(
                    QueryBuilder.class,
                    new ParseField(HybridQueryBuilder.NAME),
                    HybridQueryBuilder::fromXContent
                )
            )
        );
    }

    private HybridQueryBuilder parse(XContentBuilder xContentBuilder) throws IOException {
        XContentParser parser = createParser(
            fusedModeXContentRegistry(),
            xContentBuilder.contentType().xContent(),
            BytesReference.bytes(xContentBuilder)
        );
        parser.nextToken();
        return HybridQueryBuilder.fromXContent(parser);
    }

    private XContentBuilder twoLegBody() throws IOException {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startArray("queries")
            .startObject()
            .startObject(TermQueryBuilder.NAME)
            .field(TEXT_FIELD_NAME, TERM_QUERY_TEXT)
            .endObject()
            .endObject()
            .startObject()
            .startObject(TermQueryBuilder.NAME)
            .field(TEXT_FIELD_NAME, "other")
            .endObject()
            .endObject()
            .endArray();
    }

    @SneakyThrows
    public void testParse_whenModeAbsent_thenDefaultsToPipeline() {
        HybridQueryBuilder query = parse(twoLegBody().endObject());
        assertEquals(HybridQueryBuilder.Mode.PIPELINE, query.mode());
        assertNull(query.rankWindowSize());
    }

    @SneakyThrows
    public void testParse_whenModeFused_thenParsedWithRankWindow() {
        HybridQueryBuilder query = parse(twoLegBody().field("mode", "fused").field("rank_window_size", 50).endObject());
        assertEquals(HybridQueryBuilder.Mode.FUSED, query.mode());
        assertEquals(Integer.valueOf(50), query.rankWindowSize());
        assertEquals(2, query.queries().size());
    }

    @SneakyThrows
    public void testParse_whenModeInvalid_thenThrows() {
        expectThrows(IllegalArgumentException.class, () -> parse(twoLegBody().field("mode", "bogus").endObject()));
    }

    @SneakyThrows
    public void testParse_whenRankWindowSizeInPipelineMode_thenRejected() {
        ParsingException e = expectThrows(ParsingException.class, () -> parse(twoLegBody().field("rank_window_size", 50).endObject()));
        assertTrue(e.getMessage().contains("rank_window_size"));
    }

    @SneakyThrows
    public void testParse_whenPaginationDepthInFusedMode_thenRejected() {
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> parse(twoLegBody().field("mode", "fused").field("pagination_depth", 10).endObject())
        );
        assertTrue(e.getMessage().contains("pagination_depth"));
    }

    @SneakyThrows
    public void testParse_whenRankWindowSizeNonPositive_thenRejected() {
        expectThrows(ParsingException.class, () -> parse(twoLegBody().field("mode", "fused").field("rank_window_size", 0).endObject()));
    }

    @SneakyThrows
    public void testWireRoundTrip_preservesModeAndWindow() {
        HybridQueryBuilder original = new HybridQueryBuilder().mode(HybridQueryBuilder.Mode.FUSED).rankWindowSize(42);
        original.add(new TermQueryBuilder(TEXT_FIELD_NAME, TERM_QUERY_TEXT));
        original.add(new TermQueryBuilder(TEXT_FIELD_NAME, "other"));

        NamedWriteableRegistry registry = new NamedWriteableRegistry(
            List.of(
                new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new),
                new NamedWriteableRegistry.Entry(QueryBuilder.class, HybridQueryBuilder.NAME, HybridQueryBuilder::new)
            )
        );

        HybridQueryBuilder deserialized;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeNamedWriteable(original);
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                deserialized = (HybridQueryBuilder) in.readNamedWriteable(QueryBuilder.class);
            }
        }
        assertEquals(HybridQueryBuilder.Mode.FUSED, deserialized.mode());
        assertEquals(Integer.valueOf(42), deserialized.rankWindowSize());
        assertEquals(original, deserialized);
    }

    @SneakyThrows
    public void testBuildLegMultiSearch_pinsEachLegToNoopPipeline() {
        SearchRequest request = new SearchRequest("test-index");
        List<QueryBuilder> legs = List.of(
            new TermQueryBuilder(TEXT_FIELD_NAME, TERM_QUERY_TEXT),
            new TermQueryBuilder(TEXT_FIELD_NAME, "other")
        );

        MultiSearchRequest multiSearchRequest = HybridFusionOrchestrator.buildLegMultiSearch(request, legs, 100, false);

        assertEquals(legs.size(), multiSearchRequest.requests().size());
        for (SearchRequest legRequest : multiSearchRequest.requests()) {
            assertEquals(
                "each fused leg must be pinned to the no-op pipeline so it does not inherit the index default pipeline",
                SearchPipelineService.NOOP_PIPELINE_ID,
                legRequest.pipeline()
            );
        }
    }

    public void testDoToQuery_whenFusedModeReachesShard_thenThrows() {
        HybridQueryBuilder query = new HybridQueryBuilder().mode(HybridQueryBuilder.Mode.FUSED);
        query.add(new TermQueryBuilder(TEXT_FIELD_NAME, TERM_QUERY_TEXT));
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> query.doToQuery(mock(org.opensearch.index.query.QueryShardContext.class))
        );
        assertTrue(e.getMessage().contains("coordinator-only"));
    }

    // ---- FusionSpec: reading config from a pipeline-config map ----

    public void testFusionSpec_readsNormalizationProcessor() {
        Map<String, Object> config = Map.of(
            "phase_results_processors",
            List.of(
                Map.of(
                    "normalization-processor",
                    Map.of(
                        "normalization",
                        Map.of("technique", "min_max"),
                        "combination",
                        Map.of("technique", "arithmetic_mean", "parameters", Map.of("weights", List.of(0.3, 0.7)))
                    )
                )
            )
        );
        FusionSpec spec = FusionSpec.fromPipelineConfig(config);
        assertNotNull(spec);
        assertEquals(FusionSpec.TECHNIQUE_ARITHMETIC_MEAN, spec.combinationTechnique());
        assertEquals(FusionSpec.NORMALIZATION_MIN_MAX, spec.normalizationTechnique());
        assertEquals(2, spec.weights().length);
        assertEquals(0.3f, spec.weights()[0], 0.0001f);
        assertEquals(0.7f, spec.weights()[1], 0.0001f);
    }

    public void testFusionSpec_readsScoreRankerProcessorRrf() {
        Map<String, Object> config = Map.of(
            "phase_results_processors",
            List.of(
                Map.of(
                    "score-ranker-processor",
                    Map.of("combination", Map.of("technique", "rrf", "parameters", Map.of("rank_constant", 42)))
                )
            )
        );
        FusionSpec spec = FusionSpec.fromPipelineConfig(config);
        assertNotNull(spec);
        assertEquals(FusionSpec.TECHNIQUE_RRF, spec.combinationTechnique());
        assertEquals(42, spec.rankConstant());
    }

    public void testFusionSpec_returnsNullWhenNoFusionProcessor() {
        Map<String, Object> config = Map.of("phase_results_processors", List.of());
        assertNull(FusionSpec.fromPipelineConfig(config));
        assertNull(FusionSpec.fromPipelineConfig(Map.of()));
        assertNull(FusionSpec.fromPipelineConfig(null));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // buildFusedQuery Tail-precedence: explicit track_total_hits:false must win over the merely-informative Tail uses
    // (explain/profile), while correctness-bearing uses (aggregations/highlight) still retain the Tail.
    // ---------------------------------------------------------------------------------------------------------------

    @SneakyThrows
    private static QueryBuilder buildFusedQueryFor(org.opensearch.search.builder.SearchSourceBuilder source) {
        org.opensearch.search.SearchHit hit = new org.opensearch.search.SearchHit(0, "doc-1", Map.of(), Map.of());
        hit.score(1.0f);
        org.opensearch.search.SearchHits hits = new org.opensearch.search.SearchHits(
            new org.opensearch.search.SearchHit[] { hit },
            new org.apache.lucene.search.TotalHits(1, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO),
            1.0f
        );
        org.opensearch.action.search.SearchResponseSections sections = new org.opensearch.action.search.SearchResponseSections(
            hits,
            null,
            null,
            false,
            false,
            null,
            0,
            new java.util.ArrayList<>()
        );
        org.opensearch.action.search.SearchResponse legResponse = new org.opensearch.action.search.SearchResponse(
            sections,
            null,
            1,
            1,
            0,
            1,
            null,
            new org.opensearch.action.search.ShardSearchFailure[0],
            org.opensearch.action.search.SearchResponse.Clusters.EMPTY,
            null
        );
        org.opensearch.action.search.MultiSearchResponse multiSearchResponse = new org.opensearch.action.search.MultiSearchResponse(
            new org.opensearch.action.search.MultiSearchResponse.Item[] {
                new org.opensearch.action.search.MultiSearchResponse.Item(legResponse, null) },
            1
        );
        List<QueryBuilder> legs = List.of(new TermQueryBuilder("field", "value"));
        FusionSpec spec = FusionSpec.fromInlineFusion(Map.of("normalization", Map.of("technique", "min_max")));
        return HybridFusionOrchestrator.buildFusedQuery(source, multiSearchResponse, legs, spec, 100, true);
    }

    private static boolean hasTail(QueryBuilder fused) {
        assertTrue("expected a HybridFusionQuery", fused instanceof HybridFusionQuery);
        return ((HybridFusionQuery) fused).hasTail();
    }

    public void testBuildFusedQuery_whenTrackTotalHitsFalse_thenTopOnlyEvenWithExplainAndProfile() {
        org.opensearch.search.builder.SearchSourceBuilder source = new org.opensearch.search.builder.SearchSourceBuilder().trackTotalHits(
            false
        ).explain(true).profile(true);
        assertFalse("explain/profile must not override explicit track_total_hits:false", hasTail(buildFusedQueryFor(source)));
    }

    public void testBuildFusedQuery_whenTrackTotalHitsFalseWithAggregations_thenTailRetained() {
        org.opensearch.search.builder.SearchSourceBuilder source = new org.opensearch.search.builder.SearchSourceBuilder().trackTotalHits(
            false
        ).aggregation(org.opensearch.search.aggregations.AggregationBuilders.terms("by_field").field("field"));
        assertTrue("aggregations are silently wrong without the Tail and must retain it", hasTail(buildFusedQueryFor(source)));
    }

    public void testBuildFusedQuery_whenDefaultTotals_thenTailPresent() {
        org.opensearch.search.builder.SearchSourceBuilder source = new org.opensearch.search.builder.SearchSourceBuilder();
        assertTrue("default (totals wanted) keeps the Tail", hasTail(buildFusedQueryFor(source)));
    }
}
