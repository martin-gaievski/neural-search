# Resolver POC — flow & component diagrams

Mermaid diagrams for the resolver approach (branch `poc/resolver-phase1`). Renders natively on GitHub. Companion to [`RESOLVER_POC.md`](RESOLVER_POC.md). Grounded in the actual code in `src/main/java/org/opensearch/neuralsearch/resolver/` — `ResolverQueryBuilder`, `ResolverActionFilter`, `ResolverOrchestrator`, `RankDocsQueryBuilder` — registered in `NeuralSearch.getActionFilters()` / `getQueries()`.

One-line model: **fire the legs as parallel searches on the coordinator BEFORE the query phase, fuse globally, then either fabricate the response directly (fast path) or self-erase into a standard `RankDocsQuery` that the ordinary search phase runs — so explain/aggs/highlight/nesting all work for free.**

---

## 1. Component diagram — the pieces and how they relate

```mermaid
flowchart TB
    subgraph plugin["NeuralSearch plugin (registration)"]
        REG_AF["getActionFilters() →\n[HybridQuerySearchRequestFilter,\n ResolverActionFilter(() → client)]"]
        REG_Q["getQueries() →\nQuerySpec(resolver), QuerySpec(rank_docs)"]
    end

    subgraph coord["Coordinator node (pre-query-phase)"]
        AF["ResolverActionFilter\n(global ActionFilter, order=10)\napply() runs on every search"]
        ORCH["ResolverOrchestrator\n(stateless helpers)"]
        AF -->|"delegates all logic"| ORCH
    end

    subgraph queries["Query builders"]
        RQB["ResolverQueryBuilder\n(the 'resolver' marker:\nlegs + fusion spec +\ncollection/candidate_depth)\ndoToQuery() THROWS —\nmust never reach a shard"]
        RDQB["RankDocsQueryBuilder\n(the injected standard query:\nTop = constant_score(id)^score\n+ optional Tail = source legs)"]
    end

    subgraph orchparts["ResolverOrchestrator responsibilities"]
        PLAN["planCollection()\n→ CollectionPlan\n(coordinator | per_shard)"]
        BUILD["buildLegMultiSearch()\n(1 search/leg, or 1/(leg×shard))"]
        FUSE["computeRankedDocs()\nrrf | minMaxArithmeticMean\n(over groupLegHits union)"]
        FAB["fabricateFastPathResponse()\n(skip stage B)"]
        APPLY["applyFusedResults()\n→ inject RankDocsQuery\n(+ conditional Tail)"]
        NEST["collectMarkers /\nresolveMarkers /\nreplaceMarkers\n(nested-in-bool path)"]
    end

    subgraph core["OpenSearch core (unchanged — no core change)"]
        MS["client.multiSearch()\n(fires legs in parallel)"]
        QP["standard query phase\n(runs RankDocsQuery)"]
    end

    REG_AF -.registers.-> AF
    REG_Q -.registers.-> RQB
    REG_Q -.registers.-> RDQB
    ORCH --> PLAN & BUILD & FUSE & FAB & APPLY & NEST
    AF -->|"reads the 'resolver' marker"| RQB
    BUILD --> MS
    APPLY -->|"source.query(rankDocs)"| RDQB
    NEST -->|"splice markers → rankDocs"| RDQB
    RDQB --> QP
    FAB -->|"listener.onResponse(fabricated)"| RESP["SearchResponse (no stage B)"]

    classDef novel fill:#e6f2ff,stroke:#0366d6;
    class AF,ORCH,RQB,RDQB,PLAN,BUILD,FUSE,FAB,APPLY,NEST novel;
```

**Key relationships**
- The **ActionFilter is the sole entry point** (pipeline-free). It's a *global* filter but does ~nothing for non-resolver queries (§6.7 / overhead note).
- `ResolverQueryBuilder` is a **marker only** — its `doToQuery()` throws, so it must be consumed on the coordinator and never reach a shard.
- All real logic lives in the **stateless `ResolverOrchestrator`**; the filter just routes.
- The output is always a **standard query** (`RankDocsQuery`) or a **fabricated response** — never a non-standard transport type (contrast hybrid's `CompoundTopDocs`). That's why nesting/features work.

---

## 2. Flow — `ResolverActionFilter.apply()` decision tree (every search)

```mermaid
flowchart TD
    START(["apply(task, action, request, listener, chain)"]) --> A{"action == SearchAction.NAME\n&& request is SearchRequest\n&& source().query() != null?"}
    A -->|no| PROCEED["chain.proceed(...)\n(untouched)"]
    A -->|yes| B{"query instanceof\nResolverQueryBuilder?"}

    B -->|"no"| N{"query instanceof\nBoolQueryBuilder?"}
    N -->|no| PROCEED
    N -->|yes| NC["collectMarkers(query)"]
    NC --> ND{"markers found?"}
    ND -->|no| PROCEED
    ND -->|yes| NESTED["NESTED PATH\n(diagram 5)"]

    B -->|"yes (top-level resolver)"| PLAN["plan = planCollection(request, resolver)"]
    PLAN --> FP{"fastPathEligible(source, resolver)?\n(plain top-K, no aggs/explain/highlight/\nsort/collapse/rescore/…, totals not\nbeyond window)"}
    FP -->|yes| FASTPATH["FAST PATH\n(diagram 3)"]
    FP -->|no| STD["STANDARD / SELF-ERASE PATH\n(diagram 4)"]

    classDef novel fill:#e6f2ff,stroke:#0366d6;
    class B,N,NC,ND,PLAN,FP,FASTPATH,STD,NESTED novel;
    classDef passthru fill:#f0f0f0,stroke:#999;
    class PROCEED passthru;
```

The **non-resolver fall-through** (grey path) is the common case: a couple of `instanceof`/`equals` checks, and for a bool query one `collectMarkers` walk that finds nothing → `chain.proceed`. Measured ~5 ns for a leaf query (coordinator-only, once/request).

---

## 3. Flow — FAST PATH (plain top-K, beats hybrid latency)

Skips the second (stage-B) distributed search: the legs fetch `_source`, and the response is fabricated from the fused window.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant AF as ResolverActionFilter (coordinator)
    participant O as ResolverOrchestrator
    participant MS as client.multiSearch
    participant SH as Shards (per leg)

    C->>AF: POST /_search {resolver, track_total_hits:false}
    AF->>O: planCollection() → plan
    AF->>O: fastPathEligible() → true
    AF->>O: buildLegMultiSearch(plan, fetchSource=true)
    O-->>AF: MultiSearchRequest (1 search/leg [+ per shard], _source ON)
    AF->>MS: multiSearch(legs)   %% ONE parallel round of shard work
    MS->>SH: leg 1 (BM25), leg 2 (kNN), …
    SH-->>MS: hits (ids + scores + _source)
    MS-->>AF: MultiSearchResponse
    AF->>O: fabricateFastPathResponse(items, plan)
    Note over O: groupLegHits → computeRankedDocs (RRF / min_max+AM)<br/>reuse hydrated hits, override _score with fused score,<br/>sort desc, page [from, from+size), leg-union total
    O-->>AF: SearchResponse (fabricated)
    AF-->>C: listener.onResponse(response)   %% NO chain.proceed, NO stage B
```

**Why it's faster than hybrid:** exactly one round of shard work (the parallel legs), no second query+fetch pass. Measured 0.58–0.88× hybrid p50.

---

## 4. Flow — STANDARD / SELF-ERASE PATH (aggs / explain / accurate totals / …)

Two phases: coordinator orchestration rewrites the request into a standard `RankDocsQuery`, then the ordinary query phase runs it.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant AF as ResolverActionFilter (coordinator)
    participant O as ResolverOrchestrator
    participant MS as client.multiSearch
    participant CH as chain.proceed (normal search)
    participant QP as Query phase (shards)

    C->>AF: POST /_search {resolver}  (aggs/explain/… or accurate totals)
    AF->>O: planCollection() → plan
    AF->>O: buildLegMultiSearch(plan)   %% id-only legs
    AF->>MS: multiSearch(legs)  [stage A]
    MS-->>AF: per-leg hits (ids + scores)
    AF->>O: applyFusedResults(source, items, plan)
    Note over O: computeRankedDocs → ids/scores<br/>needsExecutionTail? → Tail on/off<br/>source.query(RankDocsQuery(ids, scores, [Tail legs]))<br/>resolver marker is now GONE
    O-->>AF: patchTotal (leg-union) or null
    AF->>CH: chain.proceed(request)  [stage B]
    CH->>QP: run standard RankDocsQuery
    Note over QP: Top: constant_score(id)^fusedScore (scored window)<br/>Tail (filter): source legs → aggs/total_hits/highlight over ALL matches
    QP-->>CH: hits + aggs + explain + …
    CH-->>C: SearchResponse (total_hits patched if derived from legs)
```

`RankDocsQuery` shape:

```mermaid
flowchart LR
    RDQ["RankDocsQuery = bool"] --> TOP["should[]: TOP (scoring)\nconstant_score(_id ∈ topK)^fusedScore\n→ returns the fused window in order"]
    RDQ --> TAIL["filter: TAIL (non-scoring, CONDITIONAL)\nbool{should: [leg1, leg2, …]}\n→ matches ALL leg docs so aggs/total_hits/\nhighlight cover the full set"]
    classDef novel fill:#e6f2ff,stroke:#0366d6;
    class RDQ,TOP,TAIL novel;
```

---

## 5. Flow — NESTED path (resolver inside a `bool`, ≥1 markers)

Hybrid cannot nest; the resolver can, because each marker self-erases into a standard `RankDocsQuery` spliced back into the tree.

```mermaid
sequenceDiagram
    autonumber
    participant AF as ResolverActionFilter
    participant O as ResolverOrchestrator
    participant MS as client.multiSearch
    participant CH as chain.proceed

    Note over AF: query is a bool tree containing resolver marker(s)
    AF->>O: collectMarkers(query)
    Note over O: recurse bool must/should/filter;<br/>accumulate enclosing filter clauses as push-down
    O-->>AF: List<MarkerContext> (marker + pushDownFilters)
    AF->>O: buildMarkerMultiSearch(markers)   %% all legs of all markers, filters pushed into each leg
    AF->>MS: multiSearch(all legs)
    MS-->>AF: per-marker leg hits
    AF->>O: resolveMarkers(markers, responses) → IdentityHashMap<marker, RankDocsQuery (Top-only)>
    AF->>O: replaceMarkers(query, resolved)  %% rebuild bool with each marker swapped
    O-->>AF: rewritten standard query tree
    AF->>CH: source.query(rewritten); chain.proceed
    Note over CH: ordinary search runs the standard bool tree
```

Scope: traversal is `bool`-only; **filter push-down** (enclosing `filter` clauses injected into each leg) is what makes nesting semantically correct (fuse over the filtered candidate set). Nested markers are Top-only (per-shard collection is top-level-only).

---

## 6. Candidate collection — coordinator vs per-shard (the relevance knob)

Where the shard→coordinator reduce happens determines the fusion pool → min_max+AM relevance.

```mermaid
flowchart TB
    subgraph coordC["collection: coordinator (default)"]
        direction TB
        c1["each leg = 1 standalone search, size = rank_window_size"]
        c2["core reduce → GLOBAL top-K per leg\n(narrow, compressed score range)"]
        c3["fuse over global top-K"]
        c1 --> c2 --> c3
    end
    subgraph psC["collection: per_shard (opt-in, min_max+AM)"]
        direction TB
        p1["each leg × each shard = 1 search\npreference=_shards:i|_primary, size=candidate_depth"]
        p2["union per-shard slices\n(= num_shards × depth pool, tails included)"]
        p3["fuse over the union\n(= hybrid's exact normalization pool)"]
        p1 --> p2 --> p3
    end
    c3 --> RCOORD["RRF: parity ✅\nmin_max+AM: −11%…−13% (window-sensitive)"]
    p3 --> RPS["min_max+AM: within ~−1% of hybrid ✅\n(cost: num_shards×legs id-only searches)"]
    classDef novel fill:#e6f2ff,stroke:#0366d6;
    class c1,c2,c3,p1,p2,p3,RCOORD,RPS novel;
```

---

## Legend / cross-references
- **Blue** = resolver POC components (new); **grey** = untouched core / pass-through.
- **Source:** [`src/main/java/org/opensearch/neuralsearch/resolver/`](src/main/java/org/opensearch/neuralsearch/resolver/) — `ResolverQueryBuilder`, `ResolverActionFilter`, `ResolverOrchestrator`, `RankDocsQueryBuilder`. Registered in [`NeuralSearch.java`](src/main/java/org/opensearch/neuralsearch/plugin/NeuralSearch.java) (`getActionFilters()` / `getQueries()`).
- **POC overview & curl demos:** [`RESOLVER_POC.md`](RESOLVER_POC.md).
- **Full design prose, feature-impact matrix, consolidated outcome & BEIR benchmarks** live in the design package (`resolver_showcase_and_alternatives_comparison.md` §6/§6.7/§12, `RESULTS.md`) referenced from `RESOLVER_POC.md`.
