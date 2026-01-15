# Result Boost: Zero-Configuration Auto-Attachment Analysis

## The Problem

Current approach requires customers to:
1. Create a search pipeline
2. Add the `result_boost` response processor to the pipeline
3. Configure the pipeline on their index or search request

This adds **configuration complexity** that many customers find overwhelming.

---

## Solution: SystemGeneratedProcessor Pattern

Neural-search already has a pattern for **auto-attaching processors** without customer configuration: `SystemGeneratedProcessor`.

### How It Works

```
Search Request
     │
     ▼
┌────────────────────────────────────────────────────┐
│  Search Pipeline Framework                          │
│                                                     │
│  1. Execute user-configured processors              │
│                                                     │
│  2. Check SystemGeneratedFactory.shouldGenerate()   │
│     ├─ Inspect SearchRequest                        │
│     └─ Return true if trigger condition met         │
│                                                     │
│  3. If true → Auto-create and execute processor     │
│                                                     │
└────────────────────────────────────────────────────┘
```

### Existing Example: SemanticHighlightingFactory

```java
public class SemanticHighlightingFactory 
    implements SystemGeneratedProcessor.SystemGeneratedFactory<SearchResponseProcessor> {
    
    @Override
    public boolean shouldGenerate(ProcessorGenerationContext context) {
        SearchRequest request = context.searchRequest();
        // Auto-attach if request has semantic highlighting field
        HighlightBuilder highlighter = request.source().highlighter();
        String semanticField = extractSemanticField(highlighter);
        return semanticField != null;  // Trigger condition!
    }
    
    @Override
    public SearchResponseProcessor create(...) {
        return new SemanticHighlightingProcessor(ignoreFailure, mlClientAccessor);
    }
}
```

---

## Applying This to ResultBoost

### Option 1: Auto-Attach When `ext.result_boost` Present

```java
public class ResultBoostSystemFactory 
    implements SystemGeneratedProcessor.SystemGeneratedFactory<SearchResponseProcessor> {
    
    @Override
    public boolean shouldGenerate(ProcessorGenerationContext context) {
        SearchRequest request = context.searchRequest();
        if (request == null || request.source() == null) {
            return false;
        }
        
        // Check if ext.result_boost is in the request
        SearchSourceBuilder source = request.source();
        if (source.ext() == null) {
            return false;
        }
        
        return source.ext().stream()
            .anyMatch(ext -> ext instanceof ResultBoostSearchExtBuilder);
    }
    
    @Override
    public SearchResponseProcessor create(...) {
        return new ResultBoostResponseProcessor(...);
    }
}
```

**Customer Experience:**
```json
// Just add ext.result_boost - NO PIPELINE NEEDED!
POST /products/_search
{
    "query": { "match": { "title": "laptop" } },
    "ext": {
        "result_boost": {
            "boosts": [{ "document_id": "sponsored-1", "factor": 5.0 }]
        }
    }
}
```

**What Customer No Longer Needs:**
- ❌ `PUT /_search/pipeline/my-pipeline`
- ❌ `response_processors: [{ "result_boost": {} }]`
- ❌ `?search_pipeline=my-pipeline` or index settings

### Option 2: Auto-Attach When Hybrid Query + `ext.result_boost`

More targeted - only auto-attach for hybrid search workflows:

```java
@Override
public boolean shouldGenerate(ProcessorGenerationContext context) {
    SearchRequest request = context.searchRequest();
    
    // Check 1: Has ext.result_boost
    boolean hasBoostExt = hasResultBoostExtension(request);
    
    // Check 2: Is hybrid query (optional, for targeted activation)
    boolean isHybridQuery = isHybridSearchQuery(request);
    
    return hasBoostExt && isHybridQuery;
}
```

---

## Comparison: Configuration Approaches

| Approach | Customer Configuration | Complexity |
|----------|----------------------|------------|
| **Current POC** | Create pipeline + add processor + attach to index/request | High |
| **SystemGenerated + ext** | Just add `ext.result_boost` to query | **Very Low** |
| **Pure Auto (no ext)** | Would need another trigger mechanism | N/A |

---

## Implementation Steps

### Step 1: Create SystemGeneratedFactory

```java
// New file: ResultBoostSystemFactory.java
public class ResultBoostSystemFactory 
    implements SystemGeneratedProcessor.SystemGeneratedFactory<SearchResponseProcessor> {
    
    public static final String SYSTEM_FACTORY_TYPE = "result_boost_auto";
    
    @Override
    public boolean shouldGenerate(ProcessorGenerationContext context) {
        // Check for ext.result_boost in request
        return hasResultBoostExtension(context.searchRequest());
    }
    
    @Override
    public SearchResponseProcessor create(...) {
        return new ResultBoostResponseProcessor(...);
    }
}
```

### Step 2: Register in Plugin

```java
// In NeuralSearch.java
@Override
public Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<SearchResponseProcessor>> 
    getSystemGeneratedResponseProcessors(Parameters parameters) {
    return Map.of(
        SemanticHighlightingConstants.SYSTEM_FACTORY_TYPE, new SemanticHighlightingFactory(clientAccessor),
        ResultBoostSystemFactory.SYSTEM_FACTORY_TYPE, new ResultBoostSystemFactory()  // NEW
    );
}
```

### Step 3: (Optional) Enable via Settings

For additional control, add a cluster setting:

```yaml
# opensearch.yml - only if explicit opt-in desired
search.pipeline.enabled_system_generated_factories:
  - "org.opensearch.neuralsearch.highlight.SemanticHighlightingProcessorFactory"
  - "org.opensearch.neuralsearch.processor.ResultBoostSystemFactory"  # NEW
```

---

## Benefits of SystemGeneratedProcessor Approach

| Benefit | Explanation |
|---------|-------------|
| **Zero Pipeline Config** | Customers never create a pipeline for basic boosting |
| **Query-Time Activation** | Processor only runs when `ext.result_boost` present |
| **No Index Settings** | No need to set `index.search.default_pipeline` |
| **Backward Compatible** | Explicit pipeline config still works |
| **Composable** | Works alongside user-configured pipelines |
| **Performance** | Only activated when needed (conditional check) |

---

## Considerations

### Pros
1. **Dramatically simplified UX** - Customers just add `ext.result_boost` to query
2. **Follows existing pattern** - SemanticHighlighting already uses this approach
3. **No breaking changes** - Existing pipeline-based usage still works
4. **Opt-in behavior** - Only activates when `ext.result_boost` is present

### Cons
1. **Discovery** - Users may not know the feature exists without documentation
2. **Interaction with pipelines** - Need to handle case where user HAS a pipeline
3. **Settings management** - May need cluster setting to enable/disable

---

## Execution Order Consideration

```java
// In ResultBoostResponseProcessor
@Override
public SystemGeneratedProcessor.ExecutionStage getExecutionStage() {
    // Execute AFTER user-defined processors
    // This ensures result_boost applies to final normalized/combined scores
    return ExecutionStage.POST_USER_DEFINED;
}
```

This ensures the boost happens AFTER:
- Normalization processor
- Any custom user processors
- But BEFORE the response is returned to client

---

## Recommendation

**Implement SystemGeneratedProcessor pattern for ResultBoost:**

1. The `ext.result_boost` in the query acts as the **trigger**
2. No pipeline configuration needed
3. Customers get boosting by just adding `ext` to their query
4. This matches the SemanticHighlighting UX pattern already in neural-search

**Final Customer Experience:**
```json
POST /my-index/_search
{
    "query": { "hybrid": { "queries": [...] } },
    "ext": {
        "result_boost": {
            "boosts": [{ "document_id": "featured-item", "factor": 10.0 }]
        }
    }
}
// That's it! No pipeline setup required.
