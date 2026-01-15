/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query.ext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchExtBuilder;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * SearchExtBuilder for result boost in hybrid search.
 *
 * Allows users to boost specific documents at query time by specifying
 * document IDs and boost factors. Applied AFTER score normalization
 * and combination.
 *
 * Example ext syntax:
 * {
 *   "query": { "hybrid": { ... } },
 *   "ext": {
 *     "result_boost": {
 *       "boosts": [
 *         { "document_id": "PROMO-123", "factor": 3.0 },
 *         { "document_id": "FEATURED-456", "factor": 2.5, "type": "additive" }
 *       ]
 *     }
 *   }
 * }
 */
@AllArgsConstructor
public class ResultBoostSearchExtBuilder extends SearchExtBuilder {

    public static final String PARAM_FIELD_NAME = "result_boost";
    public static final String BOOSTS_FIELD = "boosts";
    public static final String DOCUMENT_ID_FIELD = "document_id";
    public static final String FACTOR_FIELD = "factor";
    public static final String TYPE_FIELD = "type";

    @Getter
    protected Map<String, Object> params;

    public ResultBoostSearchExtBuilder(StreamInput in) throws IOException {
        params = in.readMap();
    }

    @Override
    public String getWriteableName() {
        return PARAM_FIELD_NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(params);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (String key : this.params.keySet()) {
            builder.field(key, this.params.get(key));
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getClass(), this.params);
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof ResultBoostSearchExtBuilder) && params.equals(((ResultBoostSearchExtBuilder) obj).params);
    }

    /**
     * Pick out the first ResultBoostSearchExtBuilder from a list of SearchExtBuilders
     * @param builders list of SearchExtBuilders
     * @return the ResultBoostSearchExtBuilder, or null if not found
     */
    public static ResultBoostSearchExtBuilder fromExtBuilderList(List<SearchExtBuilder> builders) {
        if (builders == null) {
            return null;
        }
        Optional<SearchExtBuilder> b = builders.stream().filter(bldr -> bldr instanceof ResultBoostSearchExtBuilder).findFirst();
        return b.map(searchExtBuilder -> (ResultBoostSearchExtBuilder) searchExtBuilder).orElse(null);
    }

    /**
     * Parse XContent to ResultBoostSearchExtBuilder
     * @param parser parser parsing this searchExt
     * @return ResultBoostSearchExtBuilder represented by this searchExt
     * @throws IOException if problems parsing
     */
    public static ResultBoostSearchExtBuilder parse(XContentParser parser) throws IOException {
        return new ResultBoostSearchExtBuilder(parser.map());
    }
}
