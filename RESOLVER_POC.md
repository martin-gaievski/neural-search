# Resolver Framework — Phase 1 POC (running code)

Branch: `poc/resolver-phase1`

This is a **runnable** proof-of-concept for the Resolver framework — next-generation hybrid
search for OpenSearch that runs **coordinator-level RRF as pre-search orchestration** and
**self-erases into a standard query** so `explain`, `profile`, and aggregations work natively.

Design background: `steering_documents/hybrid query/simplified_hybrid_search/`
(`resolver_showcase_and_alternatives_comparison.md`, `resolver_framework_poc_design.md`).

---

## What this POC does

A single self-contained query — **no search pipeline required** (the `ResolverActionFilter` intercepts it on the coordinator):

```json
POST /my-index/_search
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
1. The `ResolverActionFilter` (pipeline-free; or, optionally, the `resolver` request processor) detects the `resolver` query.
2. It fires each sub-query as an **independent, parallel** search via `MultiSearch`
   (each leg fans out to all shards → globally merged results).
3. It fuses the per-leg results with **Reciprocal Rank Fusion** at the coordinator
   (`score(d) = Σ 1 / (k + rank_i(d))`) — this is the coordinator-level RRF that keeps
   multi-shard relevance quality.
4. It **rewrites the request** into a standard `RankDocsQuery` — **Top** (ranked docs with RRF
   scores) + **Tail** (source legs as a non-scoring filter) — and removes the resolver.
   The query phase now runs a standard query, so explain/profile work natively and
   total-hits/aggregations cover the full match set (via the Tail).

## What's implemented

| File | Role |
|---|---|
| `src/main/java/org/opensearch/neuralsearch/resolver/ResolverQueryBuilder.java` | `resolver` marker query — carries sub-queries + RRF params; throws if it reaches a shard unprocessed |
| `src/main/java/org/opensearch/neuralsearch/resolver/ResolverOrchestrator.java` | shared orchestration — build parallel-leg MultiSearch + coordinator RRF + rewrite to `RankDocsQuery` (Top + conditional Tail) |
| `src/main/java/org/opensearch/neuralsearch/resolver/ResolverActionFilter.java` | **pipeline-free** entry — ActionFilter that runs the orchestration for any `resolver` query, no search pipeline needed |
| `src/main/java/org/opensearch/neuralsearch/resolver/ResolverProcessor.java` | search-pipeline entry (now **optional** — the ActionFilter supersedes it) |
| `src/main/java/org/opensearch/neuralsearch/resolver/RankDocsQueryBuilder.java` | the injected standard query — **Top** (ranked docs w/ RRF scores) + **Tail** (source legs as a non-scoring filter for total-hits/aggregations/highlight) |
| `src/main/java/org/opensearch/neuralsearch/plugin/NeuralSearch.java` | registers the `resolver` + `rank_docs` queries, the `ResolverActionFilter` (pipeline-free), and the optional `resolver` processor; captures the node `Client` |
| `src/test/java/org/opensearch/neuralsearch/resolver/ResolverQueryBuilderTests.java` | unit: parsing, validation, serialization, shard-guard |
| `src/test/java/org/opensearch/neuralsearch/resolver/RankDocsQueryBuilderTests.java` | unit: serialization roundtrip, non-parseable guard |
| `src/test/java/org/opensearch/neuralsearch/resolver/ResolverProcessorIT.java` | end-to-end ITs (11): fusion (3-shard), pipeline-free, nested-in-`bool` + multi-marker/multi-level (filter push-down), combined rescore, + RankDocsQuery total-hits / aggregations / highlight / explain / conditional-Tail |

## Pipeline-free (no search pipeline required)

The resolver works **without any search pipeline**. `ResolverActionFilter` (registered via `getActionFilters()`) intercepts every search on the coordinator, detects the `resolver` query, and runs the MultiSearch + RRF + rewrite orchestration — mirroring `HybridQuerySearchRequestFilter` (documented as working "transparently without any pipeline"), but async. So this works with **no `?search_pipeline=`**:

```json
POST /demo/_search
{ "query": { "resolver": { "queries": [ { "match": { "title": "apple" } }, { "match": { "body": "banana" } } ],
                           "technique": "rrf", "rank_constant": 60, "rank_window_size": 100 } } }
```

Verified by `ResolverProcessorIT.testResolver_worksWithoutSearchPipeline`. This removes the "mandatory pipeline" friction (a win for managed/serverless — no pipeline object to create or manage, no pipeline restrictions). The request processor + `?search_pipeline=` path still works but is now optional. Note: dropping the pipeline is an operational/infra win, not a latency win (pipeline execution is cheap).

## Nested placement (resolver inside a bool)

Unlike the hybrid query — which must be the top-level query, because it emits non-standard `CompoundTopDocs` — the resolver can be **nested inside a `bool`**, because it self-erases into a standard `RankDocsQuery`. The `ResolverActionFilter` recursively finds resolver markers in the query tree, resolves each to a Top-only `RankDocsQuery`, and splices it back in place. Enclosing `bool` `filter` clauses are **pushed down into each leg**, so fusion runs over the filtered candidate set:

```json
POST /demo/_search
{ "query": { "bool": {
    "must":   [ { "resolver": { "queries": [ {"match":{"title":"apple"}}, {"match":{"body":"banana"}} ],
                                "technique": "rrf", "rank_constant": 60, "rank_window_size": 100 } } ],
    "filter": [ { "term": { "category": "x" } } ] } } }
```

Verified by `ResolverProcessorIT.testResolver_nestedInBoolMust_noFilter_thenFuses` and `testResolver_nestedInBool_pushesFilterIntoLegs` (a decisive `rank_window_size=1` push-down test: fuse-then-filter would return empty). Scope: `bool` traversal only in this prototype; nested markers use Top-only (no Tail); only `filter` clauses are pushed down (non-scoring), which avoids score-scale mixing with sibling clauses.

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

Steps 4–5 (the pipeline) are **optional**: running the `resolver` query **without** any pipeline
also works — the `ResolverActionFilter` handles it on the coordinator (see the "Pipeline-free"
section above).

## Combined rescore (standard syntax, verified)

Because the resolver self-erases into a standard query, the **standard OpenSearch top-level `rescore`** element works and is applied to the **fused (combined) scores** — the plugin does not parse `rescore`; core does, and the `ResolverProcessor` leaves it untouched. This is the mode hybrid query cannot do (its rescore is per-leg, pre-normalization, capped by leg weight).

```json
POST /demo/_search?search_pipeline=resolver_pipeline
{
  "query": { "resolver": { "queries": [ { "match": { "title": "apple" } }, { "match": { "body": "banana" } } ],
                           "technique": "rrf", "rank_constant": 60, "rank_window_size": 100 } },
  "rescore": {
    "window_size": 50,
    "query": {
      "rescore_query": { "match_phrase": { "content": "open source search" } },
      "query_weight": 0.6, "rescore_query_weight": 1.4, "score_mode": "total"
    }
  }
}
```

Verified by `ResolverProcessorIT.testResolverRrf_withStandardRescore_thenFusedRankingIsRescored`: without rescore the RRF leader ranks first; with the rescore above, the only document containing the phrase is lifted to #1 — i.e. the rescore blends with the combined RRF score. Caveat: RRF scores are small (~0.01–0.05), so `query_weight`/`rescore_query_weight` must be tuned or the rescore query dominates. Per-leg rescore (rescore inside a leg) is **not** supported in this POC — that's the Phase-2 `rescorer` resolver.

## RankDocsQuery — verified improvements (and what still needs work)

The injected query is a `RankDocsQuery` (Top + Tail). ITs verify which improvements the composite actually delivers:

| Claim | Result | Test |
|---|---|---|
| Total hits cover ALL matches (not just the fused window) | ✅ achieved | `testRankDocs_totalHits_coversAllMatchesNotJustWindow` (window=1 → total_hits=3) |
| Aggregations cover ALL matches | ✅ achieved | `testRankDocs_aggregations_coverAllMatchesNotJustWindow` (buckets A=2, B=1) |
| Highlighting on sub-query terms | ✅ achieved | `testRankDocs_highlightOnSubQueryTerms` (title → `<em>apple</em>`) |
| Explain present & score-consistent | ✅ achieved | `testRankDocs_explainIsPresentAndConsistent` |
| Rich per-leg RRF rank breakdown in explain | ❌ not achieved | explain shows `ConstantScore(_id)^rrf` + source query as a 0-contribution filter, not per-leg ranks — needs a custom Top query |
| Inner hits / nested | ❌ not implemented | would need a custom Top query / propagation |
| Collapse completeness, global field sort, deep pagination | ❌ still window-bounded | fundamental to fusion, not fixable here |

Net: the Tail recovers **total-hits, aggregations, and highlighting** (over all matches); explain **works but is not a per-leg breakdown**; inner-hits and the window-bounded items remain.

## POC simplifications vs. the production design

- **Entry point:** plugin-only `resolver` **query** processed by an **`ActionFilter`** (pipeline-free),
  instead of the core top-level `resolver` field + `SearchSourceBuilder.rewrite()` SPI (which needs an
  OpenSearch core change). Same architecture (coordinator orchestration + self-erasing rewrite). The POC
  also supports **nested placement** (resolver inside `bool`, multiple markers at different levels,
  filter push-down) — beyond the original top-level-only design.
- **Snapshot consistency:** legs are matched back by `_id` (no PIT). Production uses PIT +
  `_shard_doc` so all legs see the same snapshot.
- **Explain depth:** explain works and is score-consistent, but shows `ConstantScore(_id)^rrf`
  plus the source query as a 0-contribution filter match — not a per-leg RRF rank breakdown.
  The clean breakdown needs a custom Top query (`RRFRankDoc` with per-leg positions).
- **Inner hits / nested:** not produced by the Top+Tail composite — would need the custom Top query.
- **Tail cost:** the Tail re-runs the source legs on the main search (for all-match total-hits/aggs);
  production makes the Tail conditional (only when aggs/total-hits/explain need it).
- **Techniques:** only `rrf`. `linear` (the direct hybrid+NormalizationProcessor replacement),
  `rescorer`, weighted RRF, and rerankers are Phase 2/3.
- **Stats:** no `EventStatsManager` counters wired yet (follow-up).
