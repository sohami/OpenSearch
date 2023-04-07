/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.profile.query.CollectorResult;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.ReduceableSearchResult;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.opensearch.search.aggregations.DefaultAggregationProcessor.createCollector;

/**
 * CollectorManager for Aggregation operators
 */
public class AggregationCollectorManager implements CollectorManager<Collector, ReduceableSearchResult> {
    private final SearchContext context;
    private final String aggregationReason;
    private Collector collector;

    public AggregationCollectorManager(SearchContext context, String aggregationReason, Collector collector) {
        this.context = context;
        this.aggregationReason = aggregationReason;
        this.collector = collector;
    }

    @Override
    public Collector newCollector() throws IOException {
        if (collector != null) {
            final Collector toReturn = collector;
            collector = null;
            return toReturn;
        }
        final AggregatorFactories factories = context.aggregations().factories();
        final List<Aggregator> aggregators;
        if (aggregationReason.equals(CollectorResult.REASON_AGGREGATION)) {
            aggregators = factories.createNonGlobalTopLevelAggregators(context);
            context.aggregations().addAggregators(aggregators);
        } else {
            // executing for global aggregation
            aggregators = factories.createGlobalTopLevelAggregators(context);
            context.aggregations().addGlobalAggregators(aggregators);
        }
        return createCollector(context, aggregators, aggregationReason);
    }

    @Override
    public ReduceableSearchResult reduce(Collection<Collector> collectors) throws IOException {
        return (QuerySearchResult result) -> {};
    }
}
