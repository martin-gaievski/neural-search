/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import java.util.HashMap;
import java.util.Map;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import lombok.extern.log4j.Log4j2;

/**
 * Transport action for dynamic weights upload
 */
@Log4j2
public class DynamicWeightsTransportAction extends HandledTransportAction<DynamicWeightsRequest, DynamicWeightsResponse> {

    private final ClusterService clusterService;

    @Inject
    public DynamicWeightsTransportAction(TransportService transportService, ActionFilters actionFilters, ClusterService clusterService) {
        super(DynamicWeightsAction.NAME, transportService, actionFilters, DynamicWeightsRequest::new);
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, DynamicWeightsRequest request, ActionListener<DynamicWeightsResponse> listener) {
        try {
            // Convert coefficients from Map<String, Object> to Map<String, Double>
            Map<String, Double> coefficients = new HashMap<>();
            for (Map.Entry<String, Object> entry : request.getCoefficients().entrySet()) {
                if (entry.getValue() instanceof Number) {
                    coefficients.put(entry.getKey(), ((Number) entry.getValue()).doubleValue());
                } else {
                    throw new IllegalArgumentException("Coefficient value must be a number: " + entry.getKey());
                }
            }

            // Create metadata object
            DynamicWeightsMetadata metadata = new DynamicWeightsMetadata(request.getModelType(), request.getIntercept(), coefficients);

            // Update cluster state with the new metadata
            clusterService.submitStateUpdateTask("update-dynamic-weights", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
                    metadataBuilder.putCustom(DynamicWeightsMetadata.TYPE, metadata);

                    return ClusterState.builder(currentState).metadata(metadataBuilder).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    log.error("Failed to update cluster state with dynamic weights", e);
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    log.info(
                        "Successfully stored dynamic weights model in cluster state: modelType={}, intercept={}, coefficients={}",
                        request.getModelType(),
                        request.getIntercept(),
                        request.getCoefficients()
                    );

                    DynamicWeightsResponse response = new DynamicWeightsResponse(
                        true,
                        "Model coefficients uploaded and persisted successfully"
                    );
                    listener.onResponse(response);
                }
            });

        } catch (Exception e) {
            log.error("Failed to upload dynamic weights", e);
            listener.onFailure(e);
        }
    }
}
