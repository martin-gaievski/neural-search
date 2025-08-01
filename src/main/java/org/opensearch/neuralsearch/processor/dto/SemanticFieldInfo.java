/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.dto;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.neuralsearch.mapper.dto.ChunkingConfig;
import org.opensearch.neuralsearch.mapper.dto.SparseEncodingConfig;
import org.opensearch.neuralsearch.processor.chunker.Chunker;
import org.opensearch.neuralsearch.processor.chunker.ChunkerFactory;
import org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.neuralsearch.constants.MappingConstants.PATH_SEPARATOR;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.EMBEDDING_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.CHUNKS_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.MODEL_FIELD_NAME;
import static org.opensearch.neuralsearch.mapper.dto.ChunkingConfig.ALGORITHM_FIELD;
import static org.opensearch.neuralsearch.mapper.dto.ChunkingConfig.PARAMETERS_FIELD;

/**
 * SemanticFieldInfo is a data transfer object to help hold semantic field info
 */
@Data
@Builder
public class SemanticFieldInfo {
    /**
     * The raw string value of the semantic field
     */
    private String value;
    /**
     * The model id of the semantic field which will be used to generate the embedding
     */
    private String modelId;
    /**
     * The full path to the semantic field in the index mapping
     */
    private String semanticFieldFullPathInMapping;
    /**
     * The full path to the semantic field in the doc
     */
    private String semanticFieldFullPathInDoc;
    /**
     * The full path to the semantic info fields in the doc. The path in the doc will contain the index of the inter
     * nested object.
     */
    private String semanticInfoFullPathInDoc;
    /**
     * If the chunking is enabled for the field
     */
    private Boolean chunkingEnabled;
    /**
     * A list of chunkers that will be used to chunk the semantic field. e.g. If we have chunker1 and chunker2 then
     * we will use chunker1 to chunk the original text "test text" as ["test", "text"]. Then we will use the chunker2
     * to further chunk the chunked text as ["te", "st", "te", "xt"].
     */
    private List<Chunker> chunkers;
    /**
     * The chunked strings of the original string value of the semantic field
     */
    private List<String> chunks;

    private SparseEncodingConfig sparseEncodingConfig;

    /**
     * If we should skip the embedding generation for the semantic field with exist reusable embedding. We think the
     * embedding is reusable if it exists and semantic field value and model are not changed.
     */
    private Boolean skipExistingEmbedding;

    /**
     * The id of the doc of the semantic field
     */
    private String docId;

    /**
     * @return full path to the chunks field of the semantic field in a doc
     */
    public String getFullPathForChunksInDoc() {
        if (Boolean.TRUE.equals(chunkingEnabled)) {
            return new StringBuilder().append(semanticInfoFullPathInDoc).append(PATH_SEPARATOR).append(CHUNKS_FIELD_NAME).toString();
        }
        throw new IllegalStateException(
            String.format(
                Locale.ROOT,
                "Should not try to get full path to chunks for the semantic field at %s when the chunking is not enabled.",
                semanticFieldFullPathInMapping
            )
        );
    }

    /**
     * Return the full path to the embedding field in the doc.
     * @param index index of the chunk the embedding is in. If the chunking is not enabled then the index must be 0.
     * @return full path to the embedding field of the semantic field
     */
    public String getFullPathForEmbeddingInDoc(int index) {
        if (Boolean.TRUE.equals(chunkingEnabled)) {
            return new StringBuilder().append(semanticInfoFullPathInDoc)
                .append(PATH_SEPARATOR)
                .append(CHUNKS_FIELD_NAME)
                .append(PATH_SEPARATOR)
                .append(index)
                .append(PATH_SEPARATOR)
                .append(EMBEDDING_FIELD_NAME)
                .toString();
        } else {
            if (index != 0) {
                throw new IllegalStateException(
                    String.format(
                        Locale.ROOT,
                        "Should not try to get the full path for the embedding with index %d when the chunking is not enabled for the semantic field at %s.",
                        index,
                        semanticFieldFullPathInMapping
                    )
                );
            }
            return new StringBuilder().append(semanticInfoFullPathInDoc).append(PATH_SEPARATOR).append(EMBEDDING_FIELD_NAME).toString();
        }
    }

    /**
     * @return full path to the model info fields in the doc
     */
    public String getFullPathForModelInfoInDoc() {
        return new StringBuilder().append(semanticInfoFullPathInDoc).append(PATH_SEPARATOR).append(MODEL_FIELD_NAME).toString();
    }

    public void setChunkingConfig(final ChunkingConfig chunkingConfig, @NonNull final AnalysisRegistry analysisRegistry) {
        if (chunkingConfig == null) {
            chunkingEnabled = false;
            return;
        }
        chunkingEnabled = chunkingConfig.isEnabled();
        if (chunkingConfig.getConfigs() != null && chunkingConfig.getConfigs().isEmpty() == false) {
            chunkers = new ArrayList<>();
            for (Map<String, Object> config : chunkingConfig.getConfigs()) {
                final String algorithm = (String) config.get(ALGORITHM_FIELD);
                final Object paramObject = config.get(PARAMETERS_FIELD);
                final Map<String, Object> parameters = paramObject == null
                    ? new HashMap<>()
                    : new HashMap<>((Map<String, Object>) paramObject);
                if (FixedTokenLengthChunker.ALGORITHM_NAME.equals(algorithm)) {
                    parameters.put(FixedTokenLengthChunker.ANALYSIS_REGISTRY_FIELD, analysisRegistry);
                }
                chunkers.add(ChunkerFactory.create(algorithm, parameters));
            }
        }
    }
}
