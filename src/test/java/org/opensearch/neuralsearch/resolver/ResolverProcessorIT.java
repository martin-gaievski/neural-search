/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.resolver;

import com.google.common.collect.ImmutableList;
import lombok.SneakyThrows;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.message.BasicHeader;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import java.util.List;
import java.util.Map;

import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_USER_AGENT;

/**
 * End-to-end demonstration of the Resolver framework POC (Phase 1, RRF).
 *
 * <p>Runs a single {@code resolver} query over a 3-shard index through a search pipeline containing
 * the {@code resolver} request processor. The processor fires the two legs (match on {@code title},
 * match on {@code body}) as a parallel MultiSearch, fuses them with coordinator-level RRF, and
 * rewrites the request into a standard scored query. Because fusion happens at the coordinator, the
 * document that matches BOTH legs ranks first regardless of shard placement.
 */
public class ResolverProcessorIT extends BaseNeuralSearchIT {

    private static final String INDEX = "resolver-poc-index";
    private static final String PIPELINE = "resolver-poc-pipeline";
    private static final String TITLE = "title";
    private static final String BODY = "body";

    @SneakyThrows
    public void testResolverRrf_whenDocMatchesBothLegs_thenRanksFirst() {
        initIndexIfNeeded();
        createResolverPipeline(PIPELINE);

        // Two legs: lexical match on title:"apple" and body:"banana".
        // d_both matches both legs; d_title only leg 1; d_body only leg 2; d_none matches neither.
        ResolverQueryBuilder resolver = new ResolverQueryBuilder(
            List.of(new MatchQueryBuilder(TITLE, "apple"), new MatchQueryBuilder(BODY, "banana")),
            ResolverQueryBuilder.TECHNIQUE_RRF,
            60,
            100
        );

        Map<String, Object> response = search(INDEX, resolver, null, 10, Map.of("search_pipeline", PIPELINE), null);

        List<Map<String, Object>> hits = readHits(response);
        List<String> ids = hits.stream().map(hit -> (String) hit.get("_id")).toList();

        // Union of the two legs is exactly {d_both, d_title, d_body}; d_none is in neither leg.
        assertEquals(3, ids.size());
        assertTrue(ids.contains("d_both"));
        assertTrue(ids.contains("d_title"));
        assertTrue(ids.contains("d_body"));
        assertFalse(ids.contains("d_none"));

        // Coordinator-level RRF: the doc in BOTH legs accumulates two contributions -> ranked first.
        assertEquals("d_both", ids.get(0));

        // Final result is a standard scored query, so scores must be in descending order.
        List<Double> scores = hits.stream().map(hit -> ((Number) hit.get("_score")).doubleValue()).toList();
        for (int i = 0; i < scores.size() - 1; i++) {
            assertTrue("resolver scores must be descending", scores.get(i) >= scores.get(i + 1));
        }
    }

    @SneakyThrows
    private void initIndexIfNeeded() {
        if (indexExists(INDEX)) {
            return;
        }
        String mapping = "{"
            + "\"settings\":{\"index\":{\"number_of_shards\":3,\"number_of_replicas\":0}},"
            + "\"mappings\":{\"properties\":{\"title\":{\"type\":\"text\"},\"body\":{\"type\":\"text\"}}}"
            + "}";
        createIndex(INDEX, mapping);
        ingestDocument(INDEX, "{\"title\":\"apple pie recipe\",\"body\":\"banana bread loaf\"}", "d_both");
        ingestDocument(INDEX, "{\"title\":\"apple orchard tour\",\"body\":\"fresh grape juice\"}", "d_title");
        ingestDocument(INDEX, "{\"title\":\"classic cherry tart\",\"body\":\"banana milk smoothie\"}", "d_body");
        ingestDocument(INDEX, "{\"title\":\"cherry chocolate cake\",\"body\":\"grape jam jar\"}", "d_none");
    }

    private void createResolverPipeline(final String pipelineName) throws Exception {
        makeRequest(
            client(),
            "PUT",
            "/_search/pipeline/" + pipelineName,
            null,
            toHttpEntity("{\"request_processors\":[{\"resolver\":{}}]}"),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> readHits(final Map<String, Object> response) {
        Map<String, Object> hitsMap = (Map<String, Object>) response.get("hits");
        return (List<Map<String, Object>>) hitsMap.get("hits");
    }
}
