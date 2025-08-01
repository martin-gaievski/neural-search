/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import org.opensearch.action.ActionType;

/**
 * Action type for dynamic weights upload
 */
public class DynamicWeightsAction extends ActionType<DynamicWeightsResponse> {

    public static final String NAME = "cluster:admin/neural_search/dynamic_weights";
    public static final DynamicWeightsAction INSTANCE = new DynamicWeightsAction();

    private DynamicWeightsAction() {
        super(NAME, DynamicWeightsResponse::new);
    }
}
