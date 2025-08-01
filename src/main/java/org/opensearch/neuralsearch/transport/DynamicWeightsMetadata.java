/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.opensearch.Version;
import org.opensearch.cluster.AbstractNamedDiffable;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Custom metadata for storing dynamic weights model in cluster state
 */
public class DynamicWeightsMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {

    public static final String TYPE = "dynamic_weights";

    private final String modelType;
    private final double intercept;
    private final Map<String, Double> coefficients;
    private final long version;

    public DynamicWeightsMetadata(String modelType, double intercept, Map<String, Double> coefficients) {
        this.modelType = modelType;
        this.intercept = intercept;
        this.coefficients = new HashMap<>(coefficients);
        this.version = System.currentTimeMillis();
    }

    public DynamicWeightsMetadata(StreamInput in) throws IOException {
        this.modelType = in.readString();
        this.intercept = in.readDouble();
        int size = in.readVInt();
        this.coefficients = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            coefficients.put(in.readString(), in.readDouble());
        }
        this.version = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelType);
        out.writeDouble(intercept);
        out.writeVInt(coefficients.size());
        for (Map.Entry<String, Double> entry : coefficients.entrySet()) {
            out.writeString(entry.getKey());
            out.writeDouble(entry.getValue());
        }
        out.writeLong(version);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("model_type", modelType);
        builder.field("intercept", intercept);
        builder.startObject("coefficients");
        for (Map.Entry<String, Double> entry : coefficients.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();
        builder.field("version", version);
        return builder;
    }

    public static DynamicWeightsMetadata fromXContent(XContentParser parser) throws IOException {
        String modelType = null;
        Double intercept = null;
        Map<String, Double> coefficients = new HashMap<>();

        XContentParser.Token token;
        String currentFieldName = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("model_type".equals(currentFieldName)) {
                    modelType = parser.text();
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("intercept".equals(currentFieldName)) {
                    intercept = parser.doubleValue();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("coefficients".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            String featureName = parser.currentName();
                            parser.nextToken();
                            coefficients.put(featureName, parser.doubleValue());
                        }
                    }
                }
            }
        }

        if (modelType == null || intercept == null) {
            throw new IllegalArgumentException("Missing required fields in dynamic weights metadata");
        }

        return new DynamicWeightsMetadata(modelType, intercept, coefficients);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DynamicWeightsMetadata that = (DynamicWeightsMetadata) o;
        return Double.compare(that.intercept, intercept) == 0
            && version == that.version
            && Objects.equals(modelType, that.modelType)
            && Objects.equals(coefficients, that.coefficients);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelType, intercept, coefficients, version);
    }

    // Getters
    public String getModelType() {
        return modelType;
    }

    public double getIntercept() {
        return intercept;
    }

    public Map<String, Double> getCoefficients() {
        return new HashMap<>(coefficients);
    }

    public long getVersion() {
        return version;
    }

    /**
     * Get the metadata from cluster state
     */
    public static DynamicWeightsMetadata fromClusterState(ClusterState clusterState) {
        return clusterState.metadata().custom(TYPE);
    }
}
