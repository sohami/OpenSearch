/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * {@link AggregationProcessor} implementation to be used with {@link org.opensearch.search.query.ConcurrentQueryPhaseSearcher}
 */
public class ConcurrentAggregationProcessor extends DefaultAggregationProcessor {
    @Override
    public void populateResult(SearchContext context, List<Aggregator> allAggregator) {
        List<InternalAggregation> aggregations = new ArrayList<>(allAggregator.size());
        context.aggregations().resetBucketMultiConsumer();
        for (Aggregator aggregator : allAggregator) {
            try {
                aggregator.postCollection();
                aggregations.add(aggregator.buildTopLevel());
            } catch (IOException e) {
                throw new AggregationExecutionException("Failed to build aggregation [" + aggregator.name() + "]", e);
            }
        }
        // Reduce the aggregations across slices before sending to the shards
        InternalAggregations internalAggregations = InternalAggregations.from(aggregations);
        internalAggregations = InternalAggregations.topLevelReduce(
            Collections.singletonList(internalAggregations),
            context.aggregationReduceContext()
        );
        context.queryResult().aggregations(internalAggregations);
    }
}
