/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import java.io.IOException;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Response for dynamic weights upload action
 */
@AllArgsConstructor
@Getter
public class DynamicWeightsResponse extends ActionResponse implements ToXContentObject {

    private final boolean acknowledged;
    private final String message;

    public DynamicWeightsResponse(StreamInput in) throws IOException {
        super(in);
        this.acknowledged = in.readBoolean();
        this.message = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(acknowledged);
        out.writeString(message);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("acknowledged", acknowledged);
        builder.field("message", message);
        builder.endObject();
        return builder;
    }
}
