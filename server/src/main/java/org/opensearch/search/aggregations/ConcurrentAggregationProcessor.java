/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Query;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.profile.query.CollectorResult;
import org.opensearch.search.profile.query.InternalProfileCollector;
import org.opensearch.search.query.QueryPhaseExecutionException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.opensearch.search.profile.query.CollectorResult.REASON_AGGREGATION_GLOBAL;

/**
 * Used when concurrent search is enabled to process aggregation operators
 */
public class ConcurrentAggregationProcessor extends DefaultAggregationProcessor {
    @Override
    public void preProcess(SearchContext context) {
        if (context.aggregations() != null) {
            try {
                AggregatorFactories factories = context.aggregations().factories();
                List<Aggregator> nonGlobals = factories.createNonGlobalTopLevelAggregators(context);
                List<Aggregator> globals = factories.createGlobalTopLevelAggregators(context);
                context.aggregations().aggregators(nonGlobals);
                context.aggregations().globalAggregators(globals);
                if (!nonGlobals.isEmpty()) {
                    final Collector collector = createCollector(context, nonGlobals, CollectorResult.REASON_AGGREGATION);
                    context.queryCollectorManagers().put(AggregationProcessor.class,
                        new AggregationCollectorManager(context, CollectorResult.REASON_AGGREGATION, collector));
                }
            } catch (IOException e) {
                throw new AggregationInitializationException("Could not initialize aggregators", e);
            }
        }
    }

    @Override
    public void postProcess(SearchContext context) {
        if (context.aggregations() == null) {
            context.queryResult().aggregations(null);
            return;
        }

        if (context.queryResult().hasAggs()) {
            // no need to compute the aggs twice, they should be computed on a per context basis
            return;
        }

        // TODO: Global Aggregations is still performed on single search thread
        final List<Aggregator> globals = context.aggregations().globalAggregators();
        // optimize the global collector based execution
        if (!globals.isEmpty()) {
            Query query = context.buildFilteredQuery(Queries.newMatchAllQuery());

            try {
                final Collector collector = createCollector(context, globals, REASON_AGGREGATION_GLOBAL);
                if (collector instanceof InternalProfileCollector) {
                    // start a new profile with this collector
                    context.getProfilers().addQueryProfiler().setCollector((InternalProfileCollector) collector);
                }
                context.searcher().search(query, collector);
            } catch (Exception e) {
                throw new QueryPhaseExecutionException(context.shardTarget(), "Failed to execute global aggregators", e);
            }
        }

        List<Aggregator> allAggregators = context.aggregations().getAllAggregators();
        List<InternalAggregation> aggregations = new ArrayList<>(allAggregators.size());
        context.aggregations().resetBucketMultiConsumer();
        for (Aggregator aggregator : allAggregators) {
            try {
                aggregator.postCollection();
                aggregations.add(aggregator.buildTopLevel());
            } catch (IOException e) {
                throw new AggregationExecutionException("Failed to build aggregation [" + aggregator.name() + "]", e);
            }
        }
        InternalAggregations internalAggregations = new InternalAggregations(aggregations);
        // check if concurrent or non-concurrent segment level aggregations is performed
        if (context.searcher().getExecutor() != null) {
            // perform shard level reduce
            final List<InternalAggregations> internalAggregationsList = Collections.singletonList(internalAggregations);
            internalAggregations = InternalAggregations.topLevelReduce(internalAggregationsList,
                context.getReduceContext(context.request().source()).forPartialReduction());
        }
        context.queryResult().aggregations(internalAggregations);

        // disable aggregations so that they don't run on next pages in case of scrolling
        context.aggregations(null);
        context.queryCollectorManagers().remove(AggregationProcessor.class);
    }
}
