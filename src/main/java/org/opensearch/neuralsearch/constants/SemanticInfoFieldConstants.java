/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.constants;

import org.opensearch.index.mapper.TextFieldMapper;

import java.util.Map;

import static org.opensearch.neuralsearch.constants.MappingConstants.PROPERTIES;
import static org.opensearch.neuralsearch.constants.MappingConstants.TYPE;

/**
 * Constants for semantic info
 */
public class SemanticInfoFieldConstants {
    public static final String KNN_VECTOR_DIMENSION_FIELD_NAME = "dimension";
    public static final String KNN_VECTOR_DATA_TYPE_FIELD_NAME = "data_type";
    public static final String KNN_VECTOR_METHOD_FIELD_NAME = "method";
    public static final String KNN_VECTOR_METHOD_NAME_FIELD_NAME = "name";
    public static final String KNN_VECTOR_METHOD_DEFAULT_NAME = "hnsw";
    public static final String KNN_VECTOR_METHOD_SPACE_TYPE_FIELD_NAME = "space_type";

    public static final String CHUNKS_FIELD_NAME = "chunks";
    public static final String CHUNKS_TEXT_FIELD_NAME = "text";
    public static final String EMBEDDING_FIELD_NAME = "embedding";

    public static final String MODEL_FIELD_NAME = "model";
    public static final String MODEL_ID_FIELD_NAME = "id";
    public static final String MODEL_NAME_FIELD_NAME = "name";
    public static final String MODEL_TYPE_FIELD_NAME = "type";

    // A parameter to control if we want to make the field searchable.
    public static final String INDEX_FIELD_NAME = "index";

    // Default model info field config will be text, and we will not index it to save some effort since we don't
    // expect users want to search against them.
    private static final Map<String, Object> DEFAULT_MODEL_INFO_FIELD_CONFIG = Map.of(
        TYPE,
        TextFieldMapper.CONTENT_TYPE,
        INDEX_FIELD_NAME,
        Boolean.FALSE
    );
    // Default model info config which will have id, name and type. We will use it as the metadata to help us identify
    // the embedding is generated by what model. This info can be useful when we want to reuse the embedding in the
    // existing doc to help us ensure the existing embedding was generated by the same model as the current one.
    public static final Map<String, Object> DEFAULT_MODEL_CONFIG = Map.of(
        PROPERTIES,
        Map.of(
            MODEL_ID_FIELD_NAME,
            DEFAULT_MODEL_INFO_FIELD_CONFIG,
            MODEL_NAME_FIELD_NAME,
            DEFAULT_MODEL_INFO_FIELD_CONFIG,
            MODEL_TYPE_FIELD_NAME,
            DEFAULT_MODEL_INFO_FIELD_CONFIG
        )
    );
}
