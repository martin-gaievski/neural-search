/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.Objects;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.index.query.QueryBuilder;

import lombok.Getter;

/**
 * A single result-boost condition of a hybrid query. Holds the condition {@code filter} (arbitrary query DSL used
 * for per-document membership) and an optional numeric {@code factor}.
 *
 * <p>In the initial (order/tier) mode {@code factor} is always {@code null}: conditions are ranked by their
 * position in the {@code boost_conditions} list (first match wins the top tier). The {@code factor} slot is
 * reserved so a later multiplicative-boost mode can be added purely additively — the wire layout and model type
 * already carry it, so introducing {@code factor} later needs no serialization migration, only a new
 * cluster-min-version gate on the semantic and the coordinator-side apply. While {@code null} it is inert: never
 * serialized as a value beyond the one-byte optional marker, never emitted in XContent, and never applied.
 */
@Getter
public class BoostCondition implements Writeable {

    private final QueryBuilder filter;
    private final Float factor;

    public BoostCondition(final QueryBuilder filter, final Float factor) {
        this.filter = filter;
        this.factor = factor;
    }

    public BoostCondition(final StreamInput in) throws IOException {
        this.filter = in.readNamedWriteable(QueryBuilder.class);
        // reserved slot: null in the current (order) mode; a later factor mode populates it under a new gate,
        // with no change to this byte layout (optional-float is self-describing: 1 byte when null)
        this.factor = in.readOptionalFloat();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeNamedWriteable(filter);
        out.writeOptionalFloat(factor);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BoostCondition that = (BoostCondition) o;
        return Objects.equals(filter, that.filter) && Objects.equals(factor, that.factor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filter, factor);
    }
}
