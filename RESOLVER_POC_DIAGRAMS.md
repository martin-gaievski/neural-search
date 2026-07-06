# Resolver — flow & component diagrams (v2)

Mermaid diagrams for the resolver approach, current as of **POC v2** (branch `poc/resolver-v2-adaptive-fusion`). Renders natively on GitHub. Companion to [`RESOLVER_POC.md`](RESOLVER_POC.md). Grounded in the actual code in `src/main/java/org/opensearch/neuralsearch/resolver/` — `ResolverQueryBuilder`, `ResolverActionFilter`, `ResolverOrchestrator`, `RankDocsQueryBuilder` — registered in `NeuralSearch.getQueries()` / `getActionFilters()`.

**One-line model (v2):** the resolver **self-erases at the coordinator rewrite** — `ResolverQueryBuilder.doRewrite()` fires the legs as a parallel MultiSearch via `registerAsyncAction`, fuses globally, and replaces itself with a standard `RankDocsQuery` that the ordinary search phase runs (so explain / aggs / highlight / nesting all work for free). A **thin `ResolverActionFilter`** short-circuits ONLY the stage-B-free **fast path** (fabricate the response directly for plain top-K).

> **Architecture change from v1 (the re-home).** In v1 a *global* `ActionFilter` was the sole entry point and hand-walked `bool` trees. In v2 the entry point is `doRewrite` on the query builder (the same `registerAsyncAction` pattern `NeuralQueryBuilder`/`NeuralSparseQueryBuilder` use); the ActionFilter is retained only for the fast path (a rewrite cannot fabricate a `SearchResponse`). Nesting is now **structural** (the rewrite framework recurses into any container — `bool`, `dis_max`, `function_score`, `constant_score`) and uses **fuse-then-filter** (an enclosing filter intersects the fused window at the query phase), replacing v1's bool-only tree-walk + filter push-down.

---

## 1. Component diagram — the pieces and how they relate

```mermaid
flowchart TB
    subgraph plugin["NeuralSearch plugin (registration)"]
        REG_Q["getQueries() →\nQuerySpec(resolver), QuerySpec(rank_docs)"]
        REG_AF["getActionFilters() →\n[HybridQuerySearchRequestFilter,\n ResolverActionFilter(() → client)]\n(thin: fast path only)"]
    end

    subgraph queries["Query builders"]
        RQB["ResolverQueryBuilder\n(the 'resolver' marker: legs + fusion spec\n+ collection/candidate_depth + weights)\ndoRewrite() = coordinator self-erase\ndoToQuery() THROWS — must never reach a shard"]
        RDQB["RankDocsQueryBuilder\n(the injected standard query:\nTop = constant_score(id)^fusedScore\n+ optional Tail = surviving legs)"]
    end

    subgraph coord["Coordinator node (pre-query-phase)"]
        RW["QueryRewriteContext.registerAsyncAction\n(drained by TransportSearchAction.rewriteAndFetch)"]
        AF["ResolverActionFilter (order=10)\napply(): fast path ONLY"]
        ORCH["ResolverOrchestrator\n(stateless static helpers)"]
    end

    subgraph orchparts["ResolverOrchestrator responsibilities"]
        PLAN["planCollection()\n→ CollectionPlan (coordinator | per_shard)"]
        BUILD["buildLegMultiSearch()\n(1 search/leg, or 1/(leg×shard))"]
        FUSE["computeRankedDocs()\nrrf(weighted) | min_max | z_score | l2\n(over groupLegHits union; drops failed legs)"]
        BFQ["buildFusedQuery()\n→ RankDocsQuery (Top + conditional Tail)\nor match_none"]
        FAB["fabricateFastPathResponse()\n(skip stage B; +min_score filter)"]
    end

    subgraph core["OpenSearch core (unchanged — no core change)"]
        MS["client.multiSearch()\n(fires legs in parallel)"]
        QP["standard query phase\n(runs RankDocsQuery)"]
    end

    REG_Q -.registers.-> RQB
    REG_Q -.registers.-> RDQB
    REG_AF -.registers.-> AF
    RQB -->|"doRewrite() registers async action"| RW
    RW -->|"round 2: self-erase"| RDQB
    AF -->|"fast-path eligible top-level resolver"| FAB
    ORCH --> PLAN & BUILD & FUSE & BFQ & FAB
    RQB -.uses.-> ORCH
    AF -.uses.-> ORCH
    BUILD --> MS
    BFQ -->|"returned from doRewrite"| RDQB
    RDQB --> QP
    FAB -->|"listener.onResponse(fabricated)"| RESP["SearchResponse (no stage B)"]

    classDef novel fill:#e6f2ff,stroke:#0366d6;
    class RQB,RDQB,RW,AF,ORCH,PLAN,BUILD,FUSE,BFQ,FAB novel;
```

**Key relationships**
- **Two entry points, mutually exclusive per request:** `ResolverQueryBuilder.doRewrite` (coordinator rewrite) handles the standard / nested / per-shard paths and self-erases into a `RankDocsQuery`; the thin `ResolverActionFilter` handles ONLY the fast path (it must sit at the request/response boundary because a rewrite can return a `QueryBuilder` but never fabricate a `SearchResponse`).
- `ResolverQueryBuilder` is a **coordinator-only marker** — `doToQuery()` throws, so it must be self-erased at the coordinator and never reach a shard.
- All fusion logic lives in the **stateless `ResolverOrchestrator`** (shared by both entry points).
- The output is always a **standard query** (`RankDocsQuery`) or a **fabricated response** — never a non-standard transport type (contrast hybrid's `CompoundTopDocs`). That's why nesting/features work for free.

---

## 2. Flow — top-level dispatch (fast path vs self-erase)

```mermaid
flowchart TD
    START(["POST /_search with a top-level resolver query"]) --> AF{"ResolverActionFilter.apply():\nscroll==null AND\nfastPathEligible(source, resolver)?\n(plain top-K, no aggs/explain/highlight/sort/\ncollapse/rescore/post_filter/search_after/suggest/\nsource-filtering; totals not beyond window)"}
    AF -->|"yes"| FAST["FAST PATH (diagram 3)\nfabricate response, NO chain.proceed"]
    AF -->|"no"| PROCEED["chain.proceed(...)\n→ ordinary search rewrite"]
    PROCEED --> DR{"ResolverQueryBuilder.doRewrite():\nconvertToCoordinatorContext() != null\nAND request instanceof SearchRequest?"}
    DR -->|"no (shard/base ctx, or _explain/_validate)"| THIS["return this\n(no-op; doToQuery is the shard safety net)"]
    DR -->|"yes (coordinator rewrite)"| SELF["SELF-ERASE PATH (diagram 4)\nregisterAsyncAction → fuse → RankDocsQuery"]

    classDef novel fill:#e6f2ff,stroke:#0366d6;
    class AF,FAST,DR,SELF novel;
    classDef passthru fill:#f0f0f0,stroke:#999;
    class PROCEED,THIS passthru;
```

Non-resolver searches: the ActionFilter does one `instanceof ResolverQueryBuilder` check and falls straight through (no tree-walk — that was v1); `doRewrite` is never entered because there's no resolver marker to rewrite. Overhead on ordinary queries is ~a single `instanceof`.

---

## 3. Flow — FAST PATH (plain top-K, beats hybrid latency)

Skips the second (stage-B) distributed search: the legs fetch `_source`, and the response is fabricated from the fused window. The ONLY path that uses the ActionFilter.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant AF as ResolverActionFilter (coordinator)
    participant O as ResolverOrchestrator
    participant MS as client.multiSearch
    participant SH as Shards (per leg)

    C->>AF: POST /_search {resolver, track_total_hits:false}
    AF->>O: fastPathEligible() → true
    AF->>O: planCollection() → plan
    AF->>O: buildLegMultiSearch(plan, fetchSource=true)
    O-->>AF: MultiSearchRequest (1 search/leg [+ per shard], _source ON)
    AF->>MS: multiSearch(legs)   %% ONE parallel round of shard work
    MS->>SH: leg 1 (BM25), leg 2 (kNN), …
    SH-->>MS: hits (ids + scores + _source)
    MS-->>AF: MultiSearchResponse
    AF->>O: fabricateFastPathResponse(items, plan)
    Note over O: groupLegHits (drop fully-failed legs) → computeRankedDocs<br/>(rrf / min_max / z_score / l2) → reuse hydrated hits,<br/>override _score with fused score, apply min_score threshold,<br/>sort desc, page [from, from+size), leg-union total
    O-->>AF: SearchResponse (fabricated)
    AF-->>C: listener.onResponse(response)   %% NO chain.proceed, NO stage B
```

**Why it's faster than hybrid (and ES):** exactly one round of shard work (the parallel legs), no second query+fetch pass. Measured 0.58–0.88× hybrid p50, ~1.7–2× faster than ES's linear retriever, and faster than OS hybrid on heavy-kNN NQ (96 vs 144 ms). `min_score` (v2/C1) is applied here as a post-fusion threshold — it no longer forces the standard path.

---

## 4. Flow — SELF-ERASE PATH (`doRewrite`; standard / nested / per-shard / accurate-totals)

Two coordinator rewrite rounds, then the ordinary query phase runs a standard `RankDocsQuery`. No ActionFilter involved.

```mermaid
sequenceDiagram
    autonumber
    participant RA as Rewriteable.rewriteAndFetch (coordinator)
    participant RQB as ResolverQueryBuilder.doRewrite
    participant O as ResolverOrchestrator
    participant MS as client.multiSearch (async action)
    participant QP as Query phase (shards)

    Note over RA,RQB: ROUND 1 — register the async action
    RA->>RQB: rewrite() [coordinator context]
    RQB->>O: planCollection(searchRequest, resolver) → plan
    RQB->>RA: registerAsyncAction((client,l) → client.multiSearch(buildLegMultiSearch(plan)))
    RQB-->>RA: return a NEW ResolverQueryBuilder (holds a SetOnce supplier)
    RA->>MS: drain async action  [stage A]
    MS-->>O: per-leg hits (id-only)
    O->>O: buildFusedQuery(source, items, plan, topLevel)
    Note over O: groupLegHits (drop failed legs) → computeRankedDocs →<br/>topLevel ? conditional Tail : Top-only → RankDocsQuery / match_none
    O-->>RQB: SetOnce := fused query
    Note over RA,RQB: ROUND 2 — self-erase
    RA->>RQB: rewrite() again
    RQB-->>RA: return the fused RankDocsQuery (marker is now GONE)
    RA->>QP: run standard RankDocsQuery  [stage B]
    Note over QP: Top: constant_score(id)^fusedScore (scored window)<br/>Tail (filter, conditional): surviving legs → aggs/total_hits/highlight over ALL matches
    QP-->>RA: hits + aggs + explain + …
```

`RankDocsQuery` shape (unchanged from v1, but the Tail now uses only **surviving** legs — B1):

```mermaid
flowchart LR
    RDQ["RankDocsQuery = bool"] --> TOP["should[]: TOP (scoring)\nconstant_score(_id ∈ topK)^fusedScore\n→ returns the fused window in order"]
    RDQ --> TAIL["filter: TAIL (non-scoring, CONDITIONAL)\nbool{should: [surviving leg1, leg2, …]}\n→ matches ALL leg docs so aggs/total_hits/\nhighlight cover the full set"]
    classDef novel fill:#e6f2ff,stroke:#0366d6;
    class RDQ,TOP,TAIL novel;
```

---

## 5. Flow — NESTED path (resolver inside `bool` / `dis_max` / `function_score`)

Hybrid cannot nest; the resolver can, because each marker self-erases into a standard `RankDocsQuery` **in place** during the rewrite. In v2 this is **structural** — the framework recurses `rewrite()` into every container, so each nested marker's own `doRewrite` fires independently. No bespoke tree-walk.

```mermaid
sequenceDiagram
    autonumber
    participant RA as Rewriteable.rewriteAndFetch
    participant BOOL as Container (bool/dis_max/function_score)
    participant RQB as nested ResolverQueryBuilder.doRewrite
    participant O as ResolverOrchestrator
    participant QP as Query phase

    Note over RA,BOOL: query tree contains resolver marker(s) at any depth
    RA->>BOOL: rewrite()  %% container recurses into its children
    BOOL->>RQB: child.rewrite()  %% each nested marker self-erases on its own
    RQB->>O: planCollection + registerAsyncAction(legs) [async]
    O->>O: buildFusedQuery(..., topLevel=false) → Top-only RankDocsQuery
    RQB-->>BOOL: fused RankDocsQuery (spliced in place)
    RA->>QP: run the rebuilt standard tree
    Note over QP: FUSE-THEN-FILTER: the marker fused UNFILTERED;<br/>the enclosing bool filter intersects the fused window here
```

**Fuse-then-filter (v2 semantics):** a nested marker fuses over the *unfiltered* candidate set and self-erases to a **Top-only** query; an enclosing `bool` filter then intersects the fused window at the query phase. This replaces v1's filter push-down (which pre-filtered each leg). Tradeoff: under a highly selective filter with a small `rank_window_size`, recall can be lower than push-down (the fused global leader may be filtered out) — a documented, deliberate behavioral difference. Unlocks nesting in `dis_max`/`function_score` that the v1 bool-only walk could not reach.

---

## 6. Candidate collection — coordinator vs per-shard (the relevance knob)

Where the shard→coordinator reduce happens determines the fusion pool → score-based (min_max / z_score / l2) relevance.

```mermaid
flowchart TB
    subgraph coordC["collection: coordinator (default; fast-path eligible)"]
        direction TB
        c1["each leg = 1 standalone search, size = rank_window_size"]
        c2["core reduce → GLOBAL top-K per leg\n(narrow, compressed score range)"]
        c3["fuse over global top-K"]
        c1 --> c2 --> c3
    end
    subgraph psC["collection: per_shard (opt-in; arithmetic_mean only)"]
        direction TB
        p1["each leg × each shard = 1 search\npreference=_shards:i|_primary, size=candidate_depth"]
        p2["union per-shard slices\n(= num_shards × depth pool, tails included)"]
        p3["fuse over the union\n(= hybrid's exact normalization pool)"]
        p1 --> p2 --> p3
    end
    c3 --> RCOORD["RRF: exact parity ✅\nmin_max: −13% on lexical-signal (window-sensitive)\nz_score: recovers most of it; l2: middle"]
    p3 --> RPS["min_max/l2: within ~−1..−3% of hybrid ✅\nz_score: COLLAPSES on dense-dominant (avoid)\ncost: num_shards×legs searches"]
    classDef novel fill:#e6f2ff,stroke:#0366d6;
    class c1,c2,c3,p1,p2,p3,RCOORD,RPS novel;
```

Guards: `planCollection` falls back to coordinator unless per_shard is requested for `arithmetic_mean` on a single concrete index with ≥2 shards, no custom routing/preference, and fan-out (num_shards × legs) ≤ 128.

---

## Legend / cross-references
- **Blue** = resolver components (new); **grey** = untouched core / pass-through.
- Prose + build/test + demo: [`RESOLVER_POC.md`](RESOLVER_POC.md). Full design + benchmarks live in the design package (`steering_documents/hybrid query/simplified_hybrid_search/`).
- Source: `src/main/java/org/opensearch/neuralsearch/resolver/` on `poc/resolver-v2-adaptive-fusion`.
