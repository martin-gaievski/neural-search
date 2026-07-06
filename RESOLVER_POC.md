# Resolver Framework — POC (running code)

Branch: `poc/resolver-v2-adaptive-fusion` (off tag `resolver-poc-v1`; tip carries the v2 adaptive-fusion + audit work).

This is a **runnable** proof-of-concept for the Resolver framework — next-generation hybrid
search for OpenSearch that runs **coordinator-level fusion as pre-search orchestration** and
**self-erases into a standard query** so `explain`, `profile`, and aggregations work natively.

Design background: `steering_documents/hybrid query/simplified_hybrid_search/`
(`resolver_showcase_and_alternatives_comparison.md`, `resolver_framework_poc_design.md`).

**Flow & component diagrams (Mermaid, render on GitHub):** [`RESOLVER_POC_DIAGRAMS.md`](RESOLVER_POC_DIAGRAMS.md).

---

## What this POC does

A single self-contained query — **no search pipeline required**:

```json
POST /my-index/_search
{
  "query": {
    "resolver": {
      "queries": [
        { "match": { "title": "apple" } },
        { "knn":   { "body_vector": { "vector": [ ... ], "k": 100 } } }
      ],
      "combination": { "technique": "rrf", "parameters": { "rank_constant": 60 } },
      "rank_window_size": 100
    }
  }
}
```

Flow (all on the coordinator, before the query phase — via `ResolverQueryBuilder.doRewrite()`):
1. At the **coordinator rewrite**, `ResolverQueryBuilder.doRewrite()` detects the coordinator context and
   registers an async action via `QueryRewriteContext.registerAsyncAction` (the same mechanism
   `NeuralQueryBuilder` / `NeuralSparseQueryBuilder` use — and the same one ES's compound retrievers use).
2. The async action fires each sub-query ("leg") as an **independent, parallel** search via `MultiSearch`
   (each leg fans out to all shards → globally merged results).
3. It fuses the per-leg results **on the coordinator** (`ResolverOrchestrator`) — RRF or a normalized
   arithmetic mean (see [Fusion techniques](#fusion-techniques)) — keeping multi-shard relevance quality.
4. On the next rewrite round it **self-erases into a standard `RankDocsQuery`** — **Top** (ranked docs with
   fused scores) + a **conditional Tail** (source legs as a non-scoring filter). The query phase then runs a
   standard query, so explain/profile work natively and total-hits/aggregations cover the full match set.

For plain top-K there is also a **stage-B-free fast path** (below) that fabricates the response directly
from the fused window, skipping the second query phase — the resolver's below-hybrid latency win.

## Fusion techniques

Selected via the `normalization` / `combination` objects (Option-B config, mirrors the hybrid pipeline
and ES retrievers). Per-leg `weights` are optional (validated finite, non-negative, not-all-zero).

| combination | normalization | Notes |
|---|---|---|
| `rrf` | — (rank-based) | **weighted RRF** — `score(d) = Σ weightᵢ / (rank_constant + rankᵢ(d) + 1)`. Exact parity with the hybrid RRF processor and ES `rrf`. Zero-config default. |
| `arithmetic_mean` | `min_max` | range normalization per leg, then weighted mean. Best on dense-dominant data. |
| `arithmetic_mean` | `z_score` | **DBSF-style** per-query distribution normalization (mean/std, Qdrant-style). Best on lexical-signal data; do not use under `per_shard` on dense-dominant data (collapses). |
| `arithmetic_mean` | `l2` | magnitude-preserving `s / √Σs²` per leg (matches the hybrid processor / ES `l2_norm`). The robust middle option. |

(`geometric_mean`/`harmonic_mean` were benchmarked and measured *worse* than arithmetic mean — intentionally not added.)

## What's implemented

| File | Role |
|---|---|
| `.../resolver/ResolverQueryBuilder.java` | `resolver` marker query — sub-queries + fusion spec + `collection`/`candidate_depth` + `weights`. **`doRewrite()` is the entry point**: self-erases at the coordinator rewrite via `registerAsyncAction`. `doToQuery()` throws (must never reach a shard). |
| `.../resolver/ResolverOrchestrator.java` | stateless orchestration — `planCollection`, `buildLegMultiSearch`, `buildFusedQuery` (pure; returns the query), fusion (`rrf` / `min_max` / `z_score` / `l2`), `survivingLegQueries`/`survivingWeight` (graceful per-leg failure), `fabricateFastPathResponse`/`fastPathEligible` (fast path), `legUnionTotalHits`. |
| `.../resolver/ResolverActionFilter.java` | **thin coordinator hook for the stage-B-free fast path ONLY** — fabricates the response and short-circuits (`listener.onResponse`, no `chain.proceed`) when `fastPathEligible`. Everything else falls through to `chain.proceed` and is handled by `doRewrite`. (A rewrite can return a `QueryBuilder` but never a `SearchResponse`, so the fast path needs a request/response-boundary hook.) |
| `.../resolver/RankDocsQueryBuilder.java` | the injected standard query — **Top** (`constant_score(_id)^fusedScore`) + **conditional Tail** (surviving source legs as a non-scoring filter for total-hits/aggregations/highlight). |
| `.../plugin/NeuralSearch.java` | registers the `resolver` + `rank_docs` queries (`getQueries()`) and the thin `ResolverActionFilter` (`getActionFilters()`); captures the node `Client`. |
| `.../resolver/ResolverQueryBuilderTests.java` | unit: parsing/validation/serialization/shard-guard, `doRewrite` gating + self-erase, weight validation, fast-path eligibility, z_score/l2/weighted-RRF. |
| `.../resolver/RankDocsQueryBuilderTests.java` | unit: serialization roundtrip, non-parseable guard. |
| `.../resolver/ResolverProcessorIT.java` | end-to-end ITs (30, all pipeline-free): RRF / min_max+AM / z_score / l2 / weighted RRF, per-shard collection (doc-for-doc == hybrid), fast path (+ min_score, source-filtering/suggest fallback), nested-in-`bool`/`dis_max`/`function_score` (fuse-then-filter), multi-marker, graceful per-leg failure, combined rescore, RankDocsQuery total-hits / aggs / highlight / explain / conditional-Tail. |

## Pipeline-free (no search pipeline required)

The resolver works **without any search pipeline** — `ResolverQueryBuilder.doRewrite()` self-erases at the
coordinator rewrite, no `?search_pipeline=` and no pipeline object to create or manage (a win for
managed/serverless). Verified by `ResolverProcessorIT.testResolver_worksWithoutSearchPipeline` (and every
IT runs pipeline-free). The thin `ResolverActionFilter` exists only to serve the fast path; it is **not** a
general request interceptor and does not run the orchestration for ordinary searches.

## Nested placement (resolver inside bool / dis_max / function_score)

Unlike the hybrid query — which must be top-level, because it emits non-standard `CompoundTopDocs` — the
resolver can be **nested inside any container**, because each marker self-erases into a standard
`RankDocsQuery`. This is **structural**: the rewrite framework recurses `rewrite()` into container query
builders (`bool`, `dis_max`, `function_score`, `constant_score`), so each nested marker's own `doRewrite`
fires independently. No bespoke tree-walk.

```json
POST /demo/_search
{ "query": { "bool": {
    "must":   [ { "resolver": { "queries": [ {"match":{"title":"apple"}}, {"match":{"body":"banana"}} ],
                                "combination": { "technique": "rrf" }, "rank_window_size": 100 } } ],
    "filter": [ { "term": { "category": "x" } } ] } } }
```

**Fuse-then-filter semantics:** a nested marker fuses over the *unfiltered* candidate set and self-erases to
a **Top-only** `RankDocsQuery`; an enclosing `bool` filter then intersects the fused window at the query
phase. (This replaces the earlier "filter push-down into legs" — a deliberate change; the tradeoff is that
under a highly selective filter with a small `rank_window_size`, recall can be lower than push-down.)
Verified by `testResolver_nestedInBool_filterAppliesAfterFusion`,
`testResolver_nestedInDisMaxAndFunctionScore_selfOrchestrates`, and
`testResolver_manyIndependentMarkers_withinRewriteRoundCap`.

## Build & test

```bash
./gradlew spotlessApply compileJava                          # compile
./gradlew test --tests "*.ResolverQueryBuilderTests"         # unit
./gradlew integTest --tests "*.ResolverProcessorIT"          # end-to-end (spins a live cluster)
```

## Live demo (curl)

```bash
./gradlew run    # http://localhost:9200

curl -s -XPUT "localhost:9200/demo" -H 'Content-Type: application/json' -d '{
  "settings": { "index": { "number_of_shards": 3, "number_of_replicas": 0 } },
  "mappings": { "properties": { "title": {"type":"text"}, "body": {"type":"text"} } }
}'
curl -s -XPOST "localhost:9200/demo/_doc/d_both?refresh"  -H 'Content-Type: application/json' -d '{"title":"apple pie recipe","body":"banana bread loaf"}'
curl -s -XPOST "localhost:9200/demo/_doc/d_title?refresh" -H 'Content-Type: application/json' -d '{"title":"apple orchard tour","body":"fresh grape juice"}'
curl -s -XPOST "localhost:9200/demo/_doc/d_body?refresh"  -H 'Content-Type: application/json' -d '{"title":"classic cherry tart","body":"banana milk smoothie"}'
curl -s -XPOST "localhost:9200/demo/_doc/d_none?refresh"  -H 'Content-Type: application/json' -d '{"title":"cherry chocolate cake","body":"grape jam jar"}'

# one resolver query — no search pipeline; the coordinator rewrite handles it
curl -s -XPOST "localhost:9200/demo/_search" -H 'Content-Type: application/json' -d '{
  "query": { "resolver": {
    "queries": [ { "match": { "title": "apple" } }, { "match": { "body": "banana" } } ],
    "combination": { "technique": "rrf", "parameters": { "rank_constant": 60 } },
    "rank_window_size": 100
  } },
  "size": 10
}'
# Expected: d_both first (in both legs), then d_title / d_body; d_none absent.
```

Every query runs **without any search pipeline** — the resolver self-erases at the coordinator rewrite.

## Combined rescore (standard syntax, verified)

Because the resolver self-erases into a standard query, the **standard OpenSearch top-level `rescore`**
element works and is applied to the **fused (combined) scores** — the plugin does not parse `rescore`; core
does, and the resolver orchestration leaves it untouched. This is the mode hybrid query cannot do (its
rescore is per-leg, pre-normalization, capped by leg weight). Verified by
`testResolverRrf_withStandardRescore_thenFusedRankingIsRescored`. Caveat: RRF scores are small (~0.01–0.05),
so `query_weight`/`rescore_query_weight` must be tuned. (Per-leg rescore is a Phase-2 `rescorer` resolver.)

## RankDocsQuery — verified improvements (and what still needs work)

The injected query is a `RankDocsQuery` (Top + Tail). ITs verify which improvements it delivers:

| Claim | Result | Test |
|---|---|---|
| Total hits cover ALL matches (not just the fused window) | ✅ | `testRankDocs_totalHits_coversAllMatchesNotJustWindow` |
| Aggregations cover ALL matches | ✅ | `testRankDocs_aggregations_coverAllMatchesNotJustWindow` |
| Highlighting on sub-query terms | ✅ | `testRankDocs_highlightOnSubQueryTerms` |
| Explain present & score-consistent | ✅ | `testRankDocs_explainIsPresentAndConsistent` |
| `min_score` on the fast path (post-fusion threshold) | ✅ | `testFastPath_minScore_filtersFusedWindow` |
| Graceful per-leg failure (drop failed leg, fuse survivors) | ✅ | `testResolver_gracefulLegFailure_returnsSurvivingLeg` |
| Rich per-leg rank breakdown in explain | ❌ | shows `constant_score(_id)^fusedScore` + source query as a 0-contribution filter — needs a custom Top query |
| Inner hits / nested | ❌ | would need a custom Top query |
| Collapse completeness, global field sort, deep pagination | ❌ | window-bounded; fundamental to fusion |

## POC status vs. the production design

- **Entry point:** the resolver self-erases at the coordinator rewrite via `ResolverQueryBuilder.doRewrite` +
  `registerAsyncAction` (plugin-only; the earlier v1 global-`ActionFilter`-as-sole-mechanism was **re-homed** —
  this was a GA blocker, now done). The production target is the same mechanism as a core
  `SearchSourceBuilder.rewrite()` SPI, but the plugin `QueryBuilder` path needs no core change.
- **Techniques:** `rrf` (weighted), `min_max`+`arithmetic_mean`, `z_score`+`arithmetic_mean` (DBSF),
  `l2`+`arithmetic_mean` are all implemented and benchmarked. `rescorer` and rerankers are Phase 2/3.
- **Snapshot consistency:** legs are matched back by `_id` (no PIT). Production needs **PIT** + `_shard_doc`
  so all legs see the same snapshot — **the one remaining GA blocker** (a correctness prerequisite under
  concurrent indexing, not an optimization).
- **Explain depth / inner_hits:** still need a custom Top query (`RRFRankDoc`-style per-leg positions).
- **Open backlog:** widen the fast path to the default `track_total_hits` (needs a leg-side accurate count
  independent of the retrieved window size — not a gate flip); `min_max` lower/upper bounds; per_shard
  observability signal; CCS gating; a core query-phase shard-collector (the credible per_shard latency fix).

## Production feasibility & benchmarks (summary)

Assessed across 3 BEIR datasets (TREC-COVID / Quora / NQ) on a 3-node / 12-shard cluster, and against a
matched 3-node Elasticsearch 8.19 cluster — adversarially verified. **Feasible now for RRF + coordinator +
fast path; the rest needs defined work (PIT).**

- ✅ **RRF + coordinator + fast path** — RRF ≈ hybrid relevance (exact on Quora; −3…−6% rank residual on
  TREC-COVID). The stage-B-free fast path is **~1.7–2.0× faster than ES's `linear` retriever** on all 3
  datasets and **faster than OpenSearch's own hybrid pipeline on heavy-kNN NQ** (96 vs 144 ms p50), at
  comparable relevance. RRF never fans out. Pipeline-free is a real managed/serverless win.
- **Score-based (min_max / z_score / l2) + `collection: per_shard`** closes the coordinator relevance gap to
  within ~−1…−3% of hybrid (a candidate-pool-width effect), at a fan-out latency cost — ship behind a flag.
  z_score wins on lexical-signal data; l2 is the robust middle; min_max the safe default.
- **One GA blocker remains: PIT** for stage-B snapshot consistency. CCS/remote targets need explicit gating.

Full assessment + roadmap: the design package's showcase doc §13; three-way benchmark tables:
`resolver_v2_threeway_benchmark.md`; ES gap analysis: `resolver_es_normalization_gap_analysis.md`.
