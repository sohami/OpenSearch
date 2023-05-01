/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.opensearch.search.internal.SearchContext;

/**
 * Interface to define different stages of aggregation processing
 */
public interface AggregationProcessor {
    void preProcess(SearchContext context);

    void postProcess(SearchContext context);

    void processGlobalAggregators(SearchContext context);
}
