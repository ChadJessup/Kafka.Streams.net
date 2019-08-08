/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for.Additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using Kafka.Streams.State.Interfaces;

namespace Kafka.Streams.KStream.Internals
{
    public class SessionWindowedKStreamImpl<K, V> : AbstractStream<K, V>, SessionWindowedKStream<K, V>
    {
        private SessionWindows windows;
        private GroupedStreamAggregateBuilder<K, V> aggregateBuilder;
        private IMerger<K, long> countMerger = (aggKey, aggOne, aggTwo)->aggOne + aggTwo;

        SessionWindowedKStreamImpl(SessionWindows windows,
                                    InternalStreamsBuilder builder,
                                    HashSet<string> sourceNodes,
                                    string name,
                                    ISerde<K> keySerde,
                                    ISerde<V> valSerde,
                                    GroupedStreamAggregateBuilder<K, V> aggregateBuilder,
                                    StreamsGraphNode streamsGraphNode)
            : base(name, keySerde, valSerde, sourceNodes, streamsGraphNode, builder)
        {
            windows = windows ?? throw new System.ArgumentNullException("windows can't be null", nameof(windows));
            this.windows = windows;
            this.aggregateBuilder = aggregateBuilder;
        }


        public IKTable<Windowed<K>, long> count()
        {
            return doCount(Materialized.with(keySerde, Serdes.long()));
        }


        public IKTable<Windowed<K>, long> count(Materialized<K, long, ISessionStore<Bytes, byte[]>> materialized)
        {
            materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));

            // TODO: Remove this when we do a topology-incompatible release
            // we used to burn a topology name here, so we have to keep doing it for compatibility
            if (new MaterializedInternal<>(materialized).storeName() == null)
            {
                builder.newStoreName(AGGREGATE_NAME);
            }

            return doCount(materialized);
        }

        private IKTable<Windowed<K>, long> doCount(Materialized<K, long, ISessionStore<Bytes, byte[]>> materialized)
        {
            MaterializedInternal<K, long, ISessionStore<Bytes, byte[]>> materializedInternal =
               new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME);
            if (materializedInternal.keySerde() == null)
            {
                materializedInternal.withKeySerde(keySerde);
            }
            if (materializedInternal.valueSerde() == null)
            {
                materializedInternal.withValueSerde(Serdes.long());
            }

            return aggregateBuilder.build(
                AGGREGATE_NAME,
                materialize(materializedInternal),
                new KStreamSessionWindowAggregate<>(
                    windows,
                    materializedInternal.storeName(),
                    aggregateBuilder.countInitializer,
                    aggregateBuilder.countAggregator,
                    countMerger),
                materializedInternal.queryableStoreName(),
                materializedInternal.keySerde() != null ? new WindowedSerdes.SessionWindowedSerde<>(materializedInternal.keySerde()) : null,
                materializedInternal.valueSerde());
        }


        public IKTable<Windowed<K>, V> reduce(IReducer<V> reducer)
        {
            return reduce(reducer, Materialized.with(keySerde, valSerde));
        }


        public IKTable<Windowed<K>, V> reduce(IReducer<V> reducer,
                                              Materialized<K, V, ISessionStore<Bytes, byte[]>> materialized)
        {
            reducer = reducer ?? throw new System.ArgumentNullException("reducer can't be null", nameof(reducer));
            materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));
            Aggregator<K, V, V> reduceAggregator = aggregatorForReducer(reducer);
            MaterializedInternal<K, V, ISessionStore<Bytes, byte[]>> materializedInternal =
               new MaterializedInternal<>(materialized, builder, REDUCE_NAME);
            if (materializedInternal.keySerde() == null)
            {
                materializedInternal.withKeySerde(keySerde);
            }
            if (materializedInternal.valueSerde() == null)
            {
                materializedInternal.withValueSerde(valSerde);
            }

            return aggregateBuilder.build(
                REDUCE_NAME,
                materialize(materializedInternal),
                new KStreamSessionWindowAggregate<>(
                    windows,
                    materializedInternal.storeName(),
                    aggregateBuilder.reduceInitializer,
                    reduceAggregator,
                    mergerForAggregator(reduceAggregator)
                ),
                materializedInternal.queryableStoreName(),
                materializedInternal.keySerde() != null ? new WindowedSerdes.SessionWindowedSerde<>(materializedInternal.keySerde()) : null,
                materializedInternal.valueSerde());
        }


        public IKTable<Windowed<K>, T> aggregate(
            Initializer<T> initializer,
            Aggregator<K, V, T> aggregator,
            IMerger<K, T> sessionMerger)
        {
            return aggregate(initializer, aggregator, sessionMerger, Materialized.with(keySerde, null));
        }


        public IKTable<Windowed<K>, VR> aggregate(
            Initializer<VR> initializer,
            Aggregator<K, V, VR> aggregator,
            IMerger<K, VR> sessionMerger,
            Materialized<K, VR, ISessionStore<Bytes, byte[]>> materialized)
        {
            initializer = initializer ?? throw new System.ArgumentNullException("initializer can't be null", nameof(initializer));
            aggregator = aggregator ?? throw new System.ArgumentNullException("aggregator can't be null", nameof(aggregator));
            sessionMerger = sessionMerger ?? throw new System.ArgumentNullException("sessionMerger can't be null", nameof(sessionMerger));
            materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));
            MaterializedInternal<K, VR, ISessionStore<Bytes, byte[]>> materializedInternal =
               new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME);

            if (materializedInternal.keySerde() == null)
            {
                materializedInternal.withKeySerde(keySerde);
            }

            return aggregateBuilder.build(
                AGGREGATE_NAME,
                materialize(materializedInternal),
                new KStreamSessionWindowAggregate<>(
                    windows,
                    materializedInternal.storeName(),
                    initializer,
                    aggregator,
                    sessionMerger),
                materializedInternal.queryableStoreName(),
                materializedInternal.keySerde() != null ? new WindowedSerdes.SessionWindowedSerde<>(materializedInternal.keySerde()) : null,
                materializedInternal.valueSerde());
        }


        private StoreBuilder<ISessionStore<K, VR>> materialize(MaterializedInternal<K, VR, ISessionStore<Bytes, byte[]>> materialized)
        {
            SessionBytesStoreSupplier supplier = (SessionBytesStoreSupplier)materialized.storeSupplier();
            if (supplier == null)
            {
                // NOTE: in the future, when we Remove Windows#maintainMs(), we should set the default retention
                // to be (windows.inactivityGap() + windows.grace()). This will yield the same default behavior.
                long retentionPeriod = materialized.retention() != null ? materialized.retention().toMillis() : windows.maintainMs();

                if ((windows.inactivityGap() + windows.gracePeriodMs()) > retentionPeriod)
                {
                    throw new System.ArgumentException("The retention period of the session store "
                                                           + materialized.storeName()
                                                           + " must be no smaller than the session inactivity gap plus the"
                                                           + " grace period."
                                                           + " Got gap=[" + windows.inactivityGap() + "],"
                                                           + " grace=[" + windows.gracePeriodMs() + "],"
                                                           + " retention=[" + retentionPeriod + "]"];
                }
                supplier = Stores.persistentSessionStore(
                    materialized.storeName(),
                    Duration.ofMillis(retentionPeriod)
                );
            }
            StoreBuilder<ISessionStore<K, VR>> builder = Stores.sessionStoreBuilder(
               supplier,
               materialized.keySerde(),
               materialized.valueSerde()
           );

            if (materialized.loggingEnabled())
            {
                builder.withLoggingEnabled(materialized.logConfig());
            }
            else
            {

                builder.withLoggingDisabled();
            }

            if (materialized.cachingEnabled())
            {
                builder.withCachingEnabled();
            }
            return builder;
        }

        private IMerger<K, V> mergerForAggregator(Aggregator<K, V, V> aggregator)
        {
            return (aggKey, aggOne, aggTwo)->aggregator.apply(aggKey, aggTwo, aggOne);
        }

        private Aggregator<K, V, V> aggregatorForReducer(IReducer<V> reducer)
        {
            return (aggKey, value, aggregate)->aggregate == null ? value : reducer.apply(aggregate, value);
        }
    }
}