# Resolver Framework — Phase 1 POC (running code)

Branch: `poc/resolver-phase1`

This is a **runnable** proof-of-concept for the Resolver framework — next-generation hybrid
search for OpenSearch that runs **coordinator-level RRF as pre-search orchestration** and
**self-erases into a standard query** so `explain`, `profile`, and aggregations work natively.

Design background: `steering_documents/hybrid query/simplified_hybrid_search/`
(`resolver_showcase_and_alternatives_comparison.md`, `resolver_framework_poc_design.md`).

---

## What this POC does

A single self-contained query (no normalization search pipeline needed):

```json
POST /my-index/_search?search_pipeline=resolver_pipeline
{
  "query": {
    "resolver": {
      "technique": "rrf",
      "queries": [
        { "match":  { "title": "apple" } },
        { "match":  { "body":  "banana" } }
      ],
      "rank_constant": 60,
      "rank_window_size": 100
    }
  }
}
```

Flow (all on the coordinator, before the query phase):
1. The `resolver` request processor detects the `resolver` query.
2. It fires each sub-query as an **independent, parallel** search via `MultiSearch`
   (each leg fans out to all shards → globally merged results).
3. It fuses the per-leg results with **Reciprocal Rank Fusion** at the coordinator
   (`score(d) = Σ 1 / (k + rank_i(d))`) — this is the coordinator-level RRF that keeps
   multi-shard relevance quality.
4. It **rewrites the request** into a standard query carrying the fused scores
   (`bool { should: constant_score(ids: [id])^rrfScore }`) and removes the resolver.
   The query phase now runs a standard query — explain/profile/aggregations work natively.

## What's implemented

| File | Role |
|---|---|
| `src/main/java/org/opensearch/neuralsearch/resolver/ResolverQueryBuilder.java` | `resolver` marker query — carries sub-queries + RRF params; throws if it reaches a shard unprocessed |
| `src/main/java/org/opensearch/neuralsearch/resolver/ResolverProcessor.java` | async search request processor — MultiSearch + coordinator RRF + self-erasing rewrite |
| `src/main/java/org/opensearch/neuralsearch/plugin/NeuralSearch.java` | registers the query + processor; captures the node `Client` |
| `src/test/java/org/opensearch/neuralsearch/resolver/ResolverQueryBuilderTests.java` | unit tests: parsing, validation, serialization, shard-guard |
| `src/test/java/org/opensearch/neuralsearch/resolver/ResolverProcessorIT.java` | end-to-end IT: 3-shard index, verifies the doc matching both legs ranks first |

## Build & test

```bash
# compile
./gradlew spotlessApply compileJava

# unit tests
./gradlew test --tests "*.ResolverQueryBuilderTests"

# end-to-end integration test (spins up a live cluster)
./gradlew integTest --tests "*.ResolverProcessorIT"
```

## Live demo (curl)

```bash
# 1) start a local cluster with the plugin
./gradlew run    # http://localhost:9200

# 2) create a 3-shard index
curl -s -XPUT "localhost:9200/demo" -H 'Content-Type: application/json' -d '{
  "settings": { "index": { "number_of_shards": 3, "number_of_replicas": 0 } },
  "mappings": { "properties": { "title": {"type":"text"}, "body": {"type":"text"} } }
}'

# 3) index 4 docs (d_both matches both legs; d_none matches neither)
curl -s -XPOST "localhost:9200/demo/_doc/d_both?refresh"  -H 'Content-Type: application/json' -d '{"title":"apple pie recipe","body":"banana bread loaf"}'
curl -s -XPOST "localhost:9200/demo/_doc/d_title?refresh" -H 'Content-Type: application/json' -d '{"title":"apple orchard tour","body":"fresh grape juice"}'
curl -s -XPOST "localhost:9200/demo/_doc/d_body?refresh"  -H 'Content-Type: application/json' -d '{"title":"classic cherry tart","body":"banana milk smoothie"}'
curl -s -XPOST "localhost:9200/demo/_doc/d_none?refresh"  -H 'Content-Type: application/json' -d '{"title":"cherry chocolate cake","body":"grape jam jar"}'

# 4) create the search pipeline with the resolver request processor
curl -s -XPUT "localhost:9200/_search/pipeline/resolver_pipeline" -H 'Content-Type: application/json' -d '{
  "request_processors": [ { "resolver": {} } ]
}'

# 5) run one resolver query
curl -s -XPOST "localhost:9200/demo/_search?search_pipeline=resolver_pipeline" -H 'Content-Type: application/json' -d '{
  "query": { "resolver": {
    "technique": "rrf",
    "queries": [ { "match": { "title": "apple" } }, { "match": { "body": "banana" } } ],
    "rank_constant": 60, "rank_window_size": 100
  } },
  "size": 10
}'
# Expected: d_both first (in both legs), then d_title / d_body; d_none absent.

# 6) explain works natively (the resolver has self-erased into a standard query)
curl -s -XPOST "localhost:9200/demo/_search?search_pipeline=resolver_pipeline" -H 'Content-Type: application/json' -d '{
  "explain": true, "size": 3,
  "query": { "resolver": { "queries": [ { "match": { "title": "apple" } }, { "match": { "body": "banana" } } ] } }
}'
```

Running the `resolver` query **without** the pipeline surfaces a clear error (the marker query
is coordinator-only and must be processed by the `resolver` request processor).

## POC simplifications vs. the production design

- **Entry point:** plugin-only `resolver` **query** + request processor, instead of the core
  top-level `resolver` field + `SearchSourceBuilder.rewrite()` SPI (which needs an OpenSearch
  core change). Same architecture (coordinator orchestration + self-erasing rewrite).
- **Snapshot consistency:** legs are matched back by `_id` (no PIT). Production uses PIT +
  `_shard_doc` so all legs see the same snapshot.
- **Explain depth:** the injected query uses `constant_score` per id, so explain shows the
  fused score, not a per-leg RRF breakdown. Rich explain (per-leg rank/score) is a Phase-2
  follow-up via a custom combine / `RankDocsQuery`.
- **Total hits / aggregations scope:** reflects the fused window; production adds a tail query
  for exact totals.
- **Techniques:** only `rrf`. `linear` (the direct hybrid+NormalizationProcessor replacement),
  `rescorer`, weighted RRF, and rerankers are Phase 2/3.
- **Stats:** no `EventStatsManager` counters wired yet (follow-up).
