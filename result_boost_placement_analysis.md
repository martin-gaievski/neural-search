# Result Boost Processor: OpenSearch Core vs Neural-Search Plugin Evaluation

## Validation of Assumption

**Confirmed: No existing response processor in OpenSearch core or neural-search performs document-level score boosting based on document ID.**

Existing SearchResponseProcessors in neural-search:
1. **RerankProcessor** - Uses ML models to reorder results (different purpose)
2. **ExplanationResponseProcessor** - Adds score explanation metadata
3. **AgenticContextResponseProcessor** - Agentic search-specific context
4. **SemanticHighlightingProcessor** - ML-based highlighting

None of these modify scores based on explicit document ID boost configuration.

---

## Option 1: OpenSearch Core

### Arguments FOR Core Placement

| Factor | Analysis |
|--------|----------|
| **Universality** | Result boosting is a generic search concept applicable to ANY search use case, not just hybrid/neural search. E-commerce, content management, and traditional BM25 searches all benefit from pinning/boosting specific documents. |
| **No ML Dependencies** | Unlike reranking, the boost logic is pure arithmetic (multiply/add). No ML models, no external services, no neural networks. |
| **Standard Search Feature** | Elasticsearch has similar functionality via `function_score` with `script_score` and `pinned` query. This would provide OpenSearch parity. |
| **Broader Adoption** | In core, the feature is available to ALL OpenSearch users without requiring the neural-search plugin. |
| **Simpler Pipeline** | Users doing traditional keyword search don't need to install neural-search just to boost documents. |
| **Architectural Fit** | Search pipelines are a core framework; a generic score manipulation processor fits naturally. |
| **Maintenance** | Core features have broader ownership and testing across the community. |

### Arguments AGAINST Core Placement

| Factor | Analysis |
|--------|----------|
| **Release Cycle** | Core releases are less frequent; harder to iterate on feedback. |
| **Review Overhead** | Core PRs have higher bar; more reviewers, more scrutiny. |
| **Scope Creep Risk** | Core tends toward minimal features; reviewers may push back on "niche" use cases. |
| **Breaking Change Risk** | Once in core, changing the API is much harder. |

---

## Option 2: Neural-Search Plugin

### Arguments FOR Plugin Placement

| Factor | Analysis |
|--------|----------|
| **Primary Use Case** | The RFC explicitly mentions hybrid search as the motivation - boosting after `normalization-processor` runs. The feature is designed to solve a neural-search workflow problem. |
| **Ecosystem Coherence** | Users of hybrid search already have neural-search installed. Keeping all hybrid-related processors together is cleaner. |
| **Faster Iteration** | Plugin releases can iterate faster. If users request `max_boost` limits or field-value boosts, these can be added quickly. |
| **Lower Contribution Barrier** | Neural-search maintainers can accept the feature faster than core maintainers. |
| **Feature Bundling** | The `ext.result_boost` search extension ties into the neural-search pipeline model. |
| **Experimental Safety** | If the feature doesn't gain adoption, deprecating a plugin feature is easier than core. |

### Arguments AGAINST Plugin Placement

| Factor | Analysis |
|--------|----------|
| **Artificial Limitation** | Non-neural-search users would need to install the plugin just for generic boosting. |
| **Duplication Risk** | Someone might propose a similar feature in core, leading to fragmentation. |
| **Discoverability** | Users may not find this feature if they're not already using neural-search. |

---

## Comparative Analysis

| Criterion | OpenSearch Core | Neural-Search Plugin |
|-----------|----------------|---------------------|
| **Use Case Scope** | Universal (keyword, vector, hybrid) | Primarily hybrid search |
| **Dependencies** | None | Requires neural-search plugin |
| **Time to Release** | Slower (major releases) | Faster (plugin releases) |
| **API Stability** | Must be stable from day 1 | Can evolve with feedback |
| **Community Ownership** | Broader | Neural-search team |
| **Testing Coverage** | More extensive CI | Plugin-level CI |
| **Similar Features Exist** | Would complement `pinned` query | Complements `normalization-processor` |

---

## Recommendation

### Short-Term: Neural-Search Plugin (Current POC Location) ✅

**Rationale:**
1. The RFC specifically targets hybrid search workflow
2. The feature was designed to work with `normalization-processor`
3. Faster iteration allows refinement based on real user feedback
4. The `ext.result_boost` mechanism is already tied to neural-search's search extensions
5. Lower risk if design needs changes

### Long-Term: Consider Core Migration 

**If the feature gains adoption:**
1. The logic is genuinely generic and useful beyond neural search
2. Core placement would enable wider adoption
3. Could be proposed as a separate "score manipulation processor" in core
4. Would need API stabilization first

---

## Technical Considerations for Each Location

### If Keeping in Neural-Search

```java
// Current approach - works well
// Processor registered in NeuralSearch.getResponseProcessors()
// SearchExt registered in NeuralSearch.getSearchExts()
```

**Dependencies:** Only on core SearchResponseProcessor interface (already stable)

### If Moving to Core

Would require:
1. New package: `org.opensearch.search.pipeline.processors`
2. Register in `SearchPipelineService`
3. Define in `SearchModule` 
4. SearchExt would move to `org.opensearch.search.ext`
5. More extensive backward compatibility testing

---

## Elasticsearch Comparison

Elasticsearch handles similar use cases via:
- **`pinned` query** - Pins specific documents to top (query-time)
- **`function_score`** - Script-based score manipulation (query-time)
- **Rescoring** - Second-pass rescoring with custom functions

OpenSearch's search pipeline approach (response processor) is actually **more flexible** because:
- It runs AFTER normalization/combination
- It works with any query type
- It's composable with other processors

---

## Final Assessment

| Aspect | Recommendation |
|--------|---------------|
| **Current Implementation** | Keep in neural-search |
| **Future Consideration** | Propose to core if >20% of neural-search users adopt |
| **API Design** | Stabilize in plugin before any core proposal |
| **Documentation** | Document as "neural-search feature for hybrid search" |

**The neural-search plugin is the right home for now because:**
1. The feature's genesis is hybrid search optimization
2. Rapid iteration matters for a new feature
3. The plugin's user base is the primary target audience
4. Moving to core later (if warranted) is easier than premature core placement
