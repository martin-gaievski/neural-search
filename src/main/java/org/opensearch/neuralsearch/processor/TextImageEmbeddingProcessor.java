/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import lombok.extern.log4j.Log4j2;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;

import com.google.common.annotations.VisibleForTesting;

/**
 * This processor is used for user input data text and image embedding processing, model_id can be used to indicate which model user use,
 * and field_map can be used to indicate which fields needs embedding and the corresponding keys for the embedding results.
 */
@Log4j2
public class TextImageEmbeddingProcessor extends AbstractProcessor {

    public static final String TYPE = "text_image_embedding";
    public static final String MODEL_ID_FIELD = "model_id";
    public static final String EMBEDDING_FIELD = "embedding";
    public static final String FIELD_MAP_FIELD = "field_map";
    public static final String TEXT_FIELD_NAME = "text";
    public static final String IMAGE_FIELD_NAME = "image";
    public static final String INPUT_TEXT = "inputText";
    public static final String INPUT_IMAGE = "inputImage";

    @VisibleForTesting
    private final String modelId;
    private final String embedding;
    private final Map<String, String> fieldMap;

    private final MLCommonsClientAccessor mlCommonsClientAccessor;
    private final Environment environment;
    // limit of 16Mb per field value. This is from current bedrock model, calculated as 2048*2048 pixels (24 bit),
    // image to base64 encoding assumed to have 4/3 ratio, assuming UTF-8 encoding average of 1 byte per character
    private static final int MAX_CONTENT_LENGTH_IN_BYTES = 16 * 1024 * 1024;

    public TextImageEmbeddingProcessor(
        String tag,
        String description,
        String modelId,
        String embedding,
        Map<String, String> fieldMap,
        MLCommonsClientAccessor clientAccessor,
        Environment environment
    ) {
        super(tag, description);
        if (StringUtils.isBlank(modelId)) throw new IllegalArgumentException("model_id is null or empty, can not process it");
        validateEmbeddingConfiguration(fieldMap);

        this.modelId = modelId;
        this.embedding = embedding;
        this.fieldMap = fieldMap;
        this.mlCommonsClientAccessor = clientAccessor;
        this.environment = environment;
    }

    private void validateEmbeddingConfiguration(Map<String, String> fieldMap) {
        if (fieldMap == null
            || fieldMap.isEmpty()
            || fieldMap.entrySet()
                .stream()
                .anyMatch(x -> StringUtils.isBlank(x.getKey()) || Objects.isNull(x.getValue()) || StringUtils.isBlank(x.getValue()))) {
            throw new IllegalArgumentException("Unable to create the TextImageEmbedding processor as field_map has invalid key or value");
        }

        if (fieldMap.entrySet().stream().anyMatch(entry -> !Set.of(TEXT_FIELD_NAME, IMAGE_FIELD_NAME).contains(entry.getKey()))) {
            throw new IllegalArgumentException("Unable to create the TextImageEmbedding processor as field_map has unsupported field name");
        }
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        return ingestDocument;
    }

    /**
     * This method will be invoked by PipelineService to make async inference and then delegate the handler to
     * process the inference response or failure.
     * @param ingestDocument {@link IngestDocument} which is the document passed to processor.
     * @param handler {@link BiConsumer} which is the handler which can be used after the inference task is done.
     */
    @Override
    public void execute(final IngestDocument ingestDocument, final BiConsumer<IngestDocument, Exception> handler) {
        try {
            validateEmbeddingFieldsValue(ingestDocument);
            Map<String, String> knnMap = buildMapWithKnnKeyAndOriginalValue(ingestDocument);
            Map<String, String> inferenceMap = createInferences(knnMap);
            if (inferenceMap.isEmpty()) {
                handler.accept(ingestDocument, null);
            } else {
                mlCommonsClientAccessor.inferenceSentences(this.modelId, inferenceMap, ActionListener.wrap(vectors -> {
                    setVectorFieldsToDocument(ingestDocument, vectors);
                    handler.accept(ingestDocument, null);
                }, e -> { handler.accept(null, e); }));
            }
        } catch (Exception e) {
            handler.accept(null, e);
        }

    }

    private void setVectorFieldsToDocument(IngestDocument ingestDocument, List<Float> vectors) {
        Objects.requireNonNull(vectors, "embedding failed, inference returns null result!");
        log.debug("Text embedding result fetched, starting build vector output!");
        Map<String, Object> textEmbeddingResult = buildTextEmbeddingResult(this.embedding, vectors);
        textEmbeddingResult.forEach(ingestDocument::setFieldValue);
    }

    @SuppressWarnings({ "unchecked" })
    private Map<String, String> createInferences(Map<String, String> knnKeyMap) {
        Map<String, String> texts = new HashMap<>();
        if (fieldMap.containsKey(TEXT_FIELD_NAME) && knnKeyMap.containsKey(fieldMap.get(TEXT_FIELD_NAME))) {
            texts.put(INPUT_TEXT, knnKeyMap.get(fieldMap.get(TEXT_FIELD_NAME)));
        }
        if (fieldMap.containsKey(IMAGE_FIELD_NAME) && knnKeyMap.containsKey(fieldMap.get(IMAGE_FIELD_NAME))) {
            texts.put(INPUT_IMAGE, knnKeyMap.get(fieldMap.get(IMAGE_FIELD_NAME)));
        }
        return texts;
    }

    @VisibleForTesting
    Map<String, String> buildMapWithKnnKeyAndOriginalValue(IngestDocument ingestDocument) {
        Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
        Map<String, String> mapWithKnnKeys = new LinkedHashMap<>();
        for (Map.Entry<String, String> fieldMapEntry : fieldMap.entrySet()) {
            String originalKey = fieldMapEntry.getValue(); // field from ingest document that we need to sent as model input, part of
                                                           // processor definition

            if (!sourceAndMetadataMap.containsKey(originalKey)) {
                continue;
            }
            if (!(sourceAndMetadataMap.get(originalKey) instanceof String)) {
                throw new IllegalArgumentException("Unsupported format of the field in the document, value must be a string");
            }
            if (((String) sourceAndMetadataMap.get(originalKey)).length() > MAX_CONTENT_LENGTH_IN_BYTES) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "content cannot be longer than a %d bytes", MAX_CONTENT_LENGTH_IN_BYTES)
                );
            }
            mapWithKnnKeys.put(originalKey, (String) sourceAndMetadataMap.get(originalKey));
        }
        return mapWithKnnKeys;
    }

    @SuppressWarnings({ "unchecked" })
    @VisibleForTesting
    Map<String, Object> buildTextEmbeddingResult(String knnKey, List<Float> modelTensorList) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put(knnKey, modelTensorList);
        return result;
    }

    private void validateEmbeddingFieldsValue(IngestDocument ingestDocument) {
        Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
        for (Map.Entry<String, String> embeddingFieldsEntry : fieldMap.entrySet()) {
            Object sourceValue = sourceAndMetadataMap.get(embeddingFieldsEntry.getKey());
            if (sourceValue != null) {
                String sourceKey = embeddingFieldsEntry.getKey();
                Class<?> sourceValueClass = sourceValue.getClass();
                if (List.class.isAssignableFrom(sourceValueClass) || Map.class.isAssignableFrom(sourceValueClass)) {
                    validateNestedTypeValue(sourceKey, sourceValue, () -> 1);
                } else if (!String.class.isAssignableFrom(sourceValueClass)) {
                    throw new IllegalArgumentException("field [" + sourceKey + "] is neither string nor nested type, can not process it");
                } else if (StringUtils.isBlank(sourceValue.toString())) {
                    throw new IllegalArgumentException("field [" + sourceKey + "] has empty string value, can not process it");
                }
            }
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void validateNestedTypeValue(String sourceKey, Object sourceValue, Supplier<Integer> maxDepthSupplier) {
        int maxDepth = maxDepthSupplier.get();
        if (maxDepth > MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.get(environment.settings())) {
            throw new IllegalArgumentException("map type field [" + sourceKey + "] reached max depth limit, can not process it");
        } else if ((List.class.isAssignableFrom(sourceValue.getClass()))) {
            validateListTypeValue(sourceKey, sourceValue);
        } else if (Map.class.isAssignableFrom(sourceValue.getClass())) {
            ((Map) sourceValue).values()
                .stream()
                .filter(Objects::nonNull)
                .forEach(x -> validateNestedTypeValue(sourceKey, x, () -> maxDepth + 1));
        } else if (!String.class.isAssignableFrom(sourceValue.getClass())) {
            throw new IllegalArgumentException("map type field [" + sourceKey + "] has non-string type, can not process it");
        } else if (StringUtils.isBlank(sourceValue.toString())) {
            throw new IllegalArgumentException("map type field [" + sourceKey + "] has empty string, can not process it");
        }
    }

    @SuppressWarnings({ "rawtypes" })
    private static void validateListTypeValue(String sourceKey, Object sourceValue) {
        for (Object value : (List) sourceValue) {
            if (value == null) {
                throw new IllegalArgumentException("list type field [" + sourceKey + "] has null, can not process it");
            } else if (!(value instanceof String)) {
                throw new IllegalArgumentException("list type field [" + sourceKey + "] has non string value, can not process it");
            } else if (StringUtils.isBlank(value.toString())) {
                throw new IllegalArgumentException("list type field [" + sourceKey + "] has empty string, can not process it");
            }
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
