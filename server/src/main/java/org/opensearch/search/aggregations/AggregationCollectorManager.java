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
import org.opensearch.common.CheckedFunction;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.profile.query.InternalProfileCollector;
import org.opensearch.search.query.ReduceableSearchResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

/**
 * Common {@link CollectorManager} used by both concurrent and non-concurrent aggregation path and also for global and non-global
 * aggregation operators
 */
class AggregationCollectorManager implements CollectorManager<Collector, ReduceableSearchResult> {
    private final SearchContext context;
    private final CheckedFunction<SearchContext, List<Aggregator>, IOException> aggProvider;
    private final String collectorReason;

    AggregationCollectorManager(
        SearchContext context,
        CheckedFunction<SearchContext, List<Aggregator>, IOException> aggProvider,
        String collectorReason
    ) {
        this.context = context;
        this.aggProvider = aggProvider;
        this.collectorReason = collectorReason;
    }

    @Override
    public Collector newCollector() throws IOException {
        final Collector collector = createCollector(context, aggProvider.apply(context), collectorReason);
        // For Aggregations we should not have a NO_OP_Collector
        assert collector != BucketCollector.NO_OP_COLLECTOR;
        return collector;
    }

    @Override
    public ReduceableSearchResult reduce(Collection<Collector> collectors) throws IOException {
        List<Aggregator> aggregators = new ArrayList<>();

        final Deque<Collector> allCollectors = new LinkedList<>(collectors);
        while (!allCollectors.isEmpty()) {
            final Collector currentCollector = allCollectors.pop();
            if (currentCollector instanceof Aggregator) {
                aggregators.add((Aggregator) currentCollector);
            } else if (currentCollector instanceof InternalProfileCollector) {
                if (((InternalProfileCollector) currentCollector).getCollector() instanceof Aggregator) {
                    aggregators.add((Aggregator) ((InternalProfileCollector) currentCollector).getCollector());
                } else if (((InternalProfileCollector) currentCollector).getCollector() instanceof MultiBucketCollector) {
                    allCollectors.addAll(
                        Arrays.asList(((MultiBucketCollector) ((InternalProfileCollector) currentCollector).getCollector()).getCollectors())
                    );
                }
            } else if (currentCollector instanceof MultiBucketCollector) {
                allCollectors.addAll(Arrays.asList(((MultiBucketCollector) currentCollector).getCollectors()));
            }
        }

        final List<InternalAggregation> internals = new ArrayList<>(aggregators.size());
        context.aggregations().resetBucketMultiConsumer();
        for (Aggregator aggregator : aggregators) {
            try {
                aggregator.postCollection();
                internals.add(aggregator.buildTopLevel());
            } catch (IOException e) {
                throw new AggregationExecutionException("Failed to build aggregation [" + aggregator.name() + "]", e);
            }
        }

        // PipelineTreeSource is serialized to the coordinators on older OpenSearch versions for bwc but is deprecated in latest release
        // To handle that we need to add it in the InternalAggregations object sent in QuerySearchResult.
        final InternalAggregations internalAggregations = new InternalAggregations(
            internals,
            context.request().source().aggregations()::buildPipelineTree
        );
        // Reduce the aggregations across slices before sending to the coordinator. We will perform shard level reduce iff multiple slices
        // were created to execute this request and it used concurrent segment search path
        // TODO: Add the check for flag that the request was executed using concurrent search
        if (collectors.size() > 1) {
            // using topLevelReduce here as PipelineTreeSource needs to be sent to coordinator in older release of OpenSearch. The actual
            // evaluation of pipeline aggregation though happens on the coordinator during final reduction phase
            return new AggregationReduceableSearchResult(
                InternalAggregations.topLevelReduce(Collections.singletonList(internalAggregations), context.partial())
            );
        } else {
            return new AggregationReduceableSearchResult(internalAggregations);
        }
    }

    static Collector createCollector(SearchContext context, List<Aggregator> collectors, String reason) throws IOException {
        Collector collector = MultiBucketCollector.wrap(collectors);
        ((BucketCollector) collector).preCollection();
        if (context.getProfilers() != null) {
            collector = new InternalProfileCollector(
                collector,
                reason,
                // TODO: report on child aggs as well
                Collections.emptyList()
            );
        }
        return collector;
    }
}
