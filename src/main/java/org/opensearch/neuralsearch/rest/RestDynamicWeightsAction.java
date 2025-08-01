/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.rest;

import static org.opensearch.rest.RestRequest.Method.PUT;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.neuralsearch.transport.DynamicWeightsAction;
import org.opensearch.neuralsearch.transport.DynamicWeightsRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import com.google.common.collect.ImmutableList;

import lombok.extern.log4j.Log4j2;

/**
 * Rest Action to upload dynamic weight prediction model coefficients
 */
@Log4j2
public class RestDynamicWeightsAction extends BaseRestHandler {

    private static final String DYNAMIC_WEIGHTS_ACTION = "dynamic_weights_action";
    private final ClusterService clusterService;

    public RestDynamicWeightsAction(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String getName() {
        return DYNAMIC_WEIGHTS_ACTION;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(PUT, "/_plugins/neural/_dynamic_weights"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        DynamicWeightsRequest dynamicWeightsRequest = getRequest(request);
        return channel -> client.execute(DynamicWeightsAction.INSTANCE, dynamicWeightsRequest, new RestToXContentListener<>(channel));
    }

    /**
     * Creates DynamicWeightsRequest from RestRequest
     */
    private DynamicWeightsRequest getRequest(RestRequest request) throws IOException {
        if (Strings.isNullOrEmpty(request.content().utf8ToString())) {
            throw new IllegalArgumentException("Request body is required");
        }

        XContentParser parser = request.contentParser();
        parser.nextToken(); // START_OBJECT

        String modelType = null;
        Double intercept = null;
        Map<String, Object> coefficients = null;
        String version = null;

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case "model_type":
                    modelType = parser.text();
                    break;
                case "intercept":
                    intercept = parser.doubleValue();
                    break;
                case "coefficients":
                    coefficients = parser.map();
                    break;
                case "version":
                    version = parser.text();
                    break;
                default:
                    parser.skipChildren();
            }
        }

        // Validate required fields
        if (modelType == null) {
            throw new IllegalArgumentException("model_type is required");
        }
        if (!"linear_regression".equals(modelType)) {
            throw new IllegalArgumentException("Only linear_regression model type is supported");
        }
        if (intercept == null) {
            throw new IllegalArgumentException("intercept is required");
        }
        if (coefficients == null || coefficients.isEmpty()) {
            throw new IllegalArgumentException("coefficients are required");
        }

        return new DynamicWeightsRequest(modelType, intercept, coefficients, version != null ? version : "1.0");
    }
}
