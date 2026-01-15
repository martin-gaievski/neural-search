# Result Boost POC - Hybrid Query Score Boosting

This POC implements the Result Boost feature for hybrid search queries in OpenSearch Neural Search plugin, as specified in [RFC #1689](https://github.com/opensearch-project/neural-search/issues/1689).

## Feature Overview

**Result Boost** allows users to boost scores of specific documents at query time by specifying document IDs and boost factors. Boosting is applied **AFTER** score normalization and combination in the hybrid search pipeline.

## Query Syntax

```json
{
  "query": {
    "hybrid": {
      "queries": [
        { "term": { "text": "hello" } },
        { "term": { "text": "world" } }
      ]
    }
  },
  "ext": {
    "result_boost": {
      "boosts": [
        { "document_id": "PROMO-123", "factor": 3.0 },
        { "document_id": "FEATURED-456", "factor": 2.5, "type": "additive" }
      ]
    }
  }
}
```

## Boost Types

| Type | Formula | Description |
|------|---------|-------------|
| `multiplicative` (default) | `new_score = original_score * factor` | Multiplies the combined score |
| `additive` | `new_score = original_score + factor` | Adds to the combined score |

## Implementation Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Search Request Flow                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. Query with ext.result_boost                                             │
│         ↓                                                                   │
│  2. ResultBoostSearchExtBuilder parses "ext.result_boost"                   │
│         ↓                                                                   │
│  3. NormalizationProcessor extracts ResultBoostConfig from ext              │
│         ↓                                                                   │
│  4. Score Normalization (min_max, l2, z_score)                              │
│         ↓                                                                   │
│  5. Score Combination (arithmetic_mean, harmonic_mean, geometric_mean)      │
│         ↓                                                                   │
│  6. ResultBooster.applyBoosts() - Modifies combined scores                  │
│         ↓                                                                   │
│  7. Final ranked results returned                                           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Files Created/Modified

### New Files

| File | Purpose |
|------|---------|
| `src/main/java/org/opensearch/neuralsearch/processor/resultboost/DocumentBoost.java` | Single document boost definition |
| `src/main/java/org/opensearch/neuralsearch/processor/resultboost/ResultBoostConfig.java` | Configuration holder with parsing logic |
| `src/main/java/org/opensearch/neuralsearch/processor/resultboost/ResultBooster.java` | Main logic to apply boosts to scores |
| `src/main/java/org/opensearch/neuralsearch/query/ext/ResultBoostSearchExtBuilder.java` | SearchExtBuilder for parsing ext.result_boost |
| `src/test/java/org/opensearch/neuralsearch/processor/resultboost/ResultBoosterTests.java` | Unit tests for ResultBooster |
| `src/test/java/org/opensearch/neuralsearch/processor/resultboost/ResultBoostConfigTests.java` | Unit tests for ResultBoostConfig |
| `src/test/java/org/opensearch/neuralsearch/processor/ResultBoostIT.java` | Integration tests |

### Modified Files

| File | Changes |
|------|---------|
| `NormalizationProcessor.java` | Extract ResultBoostConfig from SearchExtBuilder |
| `NormalizationProcessorWorkflow.java` | Apply boosts after score combination |
| `NormalizationProcessorWorkflowExecuteRequest.java` | Add resultBoostConfig field |
| `NeuralSearch.java` | Register ResultBoostSearchExtBuilder |

## Test Results

### Unit Tests (10 tests)
```
✓ testApplyBoosts_whenDocumentNotInResults_thenScoreUnchanged
✓ testApplyBoosts_whenMultipleBoosts_thenAllApplied
✓ testApplyBoosts_whenAdditiveBoost_thenScoreAddedByFactor
✓ testApplyBoosts_whenMultiplicativeBoost_thenScoreMultiplied
✓ testApplyBoosts_whenNullConfig_thenNoChanges
✓ testApplyBoosts_whenEmptyBoosts_thenNoChanges
✓ testFromExtContent_whenValidContent_thenParseCorrectly
✓ testFromExtContent_whenInvalidBoost_thenSkipIt
✓ testFromExtContent_whenNoResultBoost_thenReturnNull
✓ testFromExtContent_whenEmptyBoosts_thenReturnEmptyConfig
```

### Integration Tests (2 tests)
```
✓ testResultBoost_whenBoostApplied_thenDocumentScoreIncreases (2.067s)
  - Without boost: Doc 5 score = 0.0005
  - With 10x boost: Doc 5 score = 0.005 ✓

✓ testResultBoost_whenAdditiveBoost_thenScoreIncreasedByFactor (0.865s)
  - Without boost: Doc 3 score = 1.0
  - With +2.5 additive: Doc 3 score = 3.5 ✓
```

## How to Run

### Unit Tests
```bash
./gradlew test --tests "org.opensearch.neuralsearch.processor.resultboost.*"
```

### Integration Tests
```bash
./gradlew integTest --tests "org.opensearch.neuralsearch.processor.ResultBoostIT"
```

## Design Decisions

1. **SearchExtBuilder approach**: Uses the standard OpenSearch `ext` section pattern, consistent with existing features like `rerank`

2. **Post-combination boosting**: Boosts are applied AFTER normalization and combination to give users maximum control over final rankings

3. **Document ID matching**: Uses the exact `_id` field from Lucene documents for reliable matching

4. **Default multiplicative**: The multiplicative boost type is default as it's more intuitive for relevance adjustments

## Limitations (POC)

- Single shard only tested (for POC simplicity)
- No validation against maximum boost factors
- No index-level configuration support (query-time only)

## Future Enhancements

1. Field-based boosting (not just document ID)
2. Conditional boosting based on field values
3. Time-decay boosting
4. Index-level default boost configurations
5. Explain API integration

## Usage Example

```bash
# Create pipeline
PUT _search/pipeline/result-boost-pipeline
{
  "phase_results_processors": [{
    "normalization-processor": {
      "normalization": { "technique": "min_max" },
      "combination": { "technique": "arithmetic_mean" }
    }
  }]
}

# Search with boost
POST /my-index/_search?search_pipeline=result-boost-pipeline
{
  "query": {
    "hybrid": {
      "queries": [
        { "match": { "title": "machine learning" } },
        { "neural": { "embedding": { "query_text": "AI algorithms" } } }
      ]
    }
  },
  "ext": {
    "result_boost": {
      "boosts": [
        { "document_id": "featured-article-001", "factor": 3.0 },
        { "document_id": "sponsored-content-002", "factor": 2.0, "type": "additive" }
      ]
    }
  }
}
