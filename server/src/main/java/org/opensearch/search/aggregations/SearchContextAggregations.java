/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations;

import java.util.List;

import static org.opensearch.search.aggregations.MultiBucketConsumerService.MultiBucketConsumer;

/**
 * The aggregation context that is part of the search context.
 *
 * @opensearch.internal
 */
public class SearchContextAggregations {

    private final AggregatorFactories factories;
    private final MultiBucketConsumer multiBucketConsumer;
    private List<Aggregator> aggregators;
    private List<Aggregator> globalAggregators;

    /**
     * Creates a new aggregation context with the parsed aggregator factories
     */
    public SearchContextAggregations(AggregatorFactories factories, MultiBucketConsumer multiBucketConsumer) {
        this.factories = factories;
        this.multiBucketConsumer = multiBucketConsumer;
    }

    public AggregatorFactories factories() {
        return factories;
    }

    /**
     * @return all the registered aggregators (including global ones)
     */
    public List<Aggregator> aggregators() {
        return aggregators;
    }

    /**
     * Registers all the created aggregators (top level aggregators) for the search execution context.
     *
     * @param aggregators The top level aggregators of the search execution.
     */
    public void aggregators(List<Aggregator> aggregators) {
        this.aggregators = aggregators;
    }

    /**
     * @return all the registered global aggregators
     */
    public List<Aggregator> globalAggregators() {
        return globalAggregators;
    }

    /**
     * Registers all the created global aggregators for the search context
     * @param globalAggregators The global aggregators of the search context
     */
    public void globalAggregators(List<Aggregator> globalAggregators) {
        this.globalAggregators = globalAggregators;
    }

    /**
     * Returns a consumer for multi bucket aggregation that checks the total number of buckets
     * created in the response
     */
    public MultiBucketConsumer multiBucketConsumer() {
        return multiBucketConsumer;
    }

    void resetBucketMultiConsumer() {
        multiBucketConsumer.reset();
    }
}
