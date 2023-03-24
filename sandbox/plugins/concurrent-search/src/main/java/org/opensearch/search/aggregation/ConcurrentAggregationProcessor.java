/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregation;

import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.aggregations.DefaultAggregationProcessor;
import org.opensearch.search.internal.SearchContext;

public class ConcurrentAggregationProcessor extends DefaultAggregationProcessor {
    @Override public void preProcess(SearchContext context) {

    }

    @Override public void postProcess(SearchContext context) {

    }
}
