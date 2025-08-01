/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import static org.opensearch.action.ValidateActions.addValidationError;

import java.io.IOException;
import java.util.Map;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import lombok.Getter;

/**
 * Request for dynamic weights model coefficients upload
 */
@Getter
public class DynamicWeightsRequest extends ActionRequest implements ToXContent {

    private final String modelType;
    private final double intercept;
    private final Map<String, Object> coefficients;
    private final String version;

    public DynamicWeightsRequest(String modelType, double intercept, Map<String, Object> coefficients, String version) {
        super();
        this.modelType = modelType;
        this.intercept = intercept;
        this.coefficients = coefficients;
        this.version = version;
    }

    public DynamicWeightsRequest(StreamInput in) throws IOException {
        super(in);
        this.modelType = in.readString();
        this.intercept = in.readDouble();
        this.coefficients = in.readMap();
        this.version = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(modelType);
        out.writeDouble(intercept);
        out.writeMap(coefficients);
        out.writeString(version);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;

        if (modelType == null || modelType.trim().isEmpty()) {
            validationException = addValidationError("model_type is required", validationException);
        }
        if (!"linear_regression".equals(modelType)) {
            validationException = addValidationError("Only linear_regression model type is supported", validationException);
        }
        if (coefficients == null || coefficients.isEmpty()) {
            validationException = addValidationError("coefficients are required", validationException);
        }
        if (version == null || version.trim().isEmpty()) {
            validationException = addValidationError("version is required", validationException);
        }

        return validationException;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("model_type", modelType);
        builder.field("intercept", intercept);
        builder.field("coefficients", coefficients);
        builder.field("version", version);
        builder.field("status", "uploaded");
        builder.endObject();
        return builder;
    }
}
