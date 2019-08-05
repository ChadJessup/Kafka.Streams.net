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
namespace Kafka.Streams.KStream.Internals
{





























    public class TimeWindowedKStreamImpl<K, V, W> : AbstractStream<K, V>, TimeWindowedKStream<K, V>
            where W : Window
    {

        private Windows<W> windows;
        private GroupedStreamAggregateBuilder<K, V> aggregateBuilder;

        TimeWindowedKStreamImpl(Windows<W> windows,
                                 InternalStreamsBuilder builder,
                                 HashSet<string> sourceNodes,
                                 string name,
                                 ISerde<K> keySerde,
                                 ISerde<V> valSerde,
                                 GroupedStreamAggregateBuilder<K, V> aggregateBuilder,
                                 StreamsGraphNode streamsGraphNode)
            : base(name, keySerde, valSerde, sourceNodes, streamsGraphNode, builder)
        {
            this.windows = windows = windows ?? throw new System.ArgumentNullException("windows can't be null", nameof(windows));
            this.aggregateBuilder = aggregateBuilder;
        }


        public KTable<Windowed<K>, long> count()
        {
            return doCount(Materialized.with(keySerde, Serdes.long()));
        }


        public KTable<Windowed<K>, long> count(Materialized<K, long, WindowStore<Bytes, byte[]>> materialized)
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

        private KTable<Windowed<K>, long> doCount(Materialized<K, long, WindowStore<Bytes, byte[]>> materialized)
        {
            MaterializedInternal<K, long, WindowStore<Bytes, byte[]>> materializedInternal =
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
                new KStreamWindowAggregate<>(windows, materializedInternal.storeName(), aggregateBuilder.countInitializer, aggregateBuilder.countAggregator),
                materializedInternal.queryableStoreName(),
                materializedInternal.keySerde() != null ? new FullTimeWindowedSerde<>(materializedInternal.keySerde(), windows.size()) : null,
                materializedInternal.valueSerde());
        }


        public KTable<Windowed<K>, VR> aggregate(Initializer<VR> initializer,
                                                       Aggregator<K, V, VR> aggregator)
        {
            return aggregate(initializer, aggregator, Materialized.with(keySerde, null));
        }


        public KTable<Windowed<K>, VR> aggregate(Initializer<VR> initializer,
                                                       Aggregator<K, V, VR> aggregator,
                                                       Materialized<K, VR, WindowStore<Bytes, byte[]>> materialized)
        {
            initializer = initializer ?? throw new System.ArgumentNullException("initializer can't be null", nameof(initializer));
            aggregator = aggregator ?? throw new System.ArgumentNullException("aggregator can't be null", nameof(aggregator));
            materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));
            MaterializedInternal<K, VR, WindowStore<Bytes, byte[]>> materializedInternal =
               new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME);
            if (materializedInternal.keySerde() == null)
            {
                materializedInternal.withKeySerde(keySerde);
            }
            return aggregateBuilder.build(
                AGGREGATE_NAME,
                materialize(materializedInternal),
                new KStreamWindowAggregate<>(windows, materializedInternal.storeName(), initializer, aggregator),
                materializedInternal.queryableStoreName(),
                materializedInternal.keySerde() != null ? new FullTimeWindowedSerde<>(materializedInternal.keySerde(), windows.size()) : null,
                materializedInternal.valueSerde());
        }


        public KTable<Windowed<K>, V> reduce(Reducer<V> reducer)
        {
            return reduce(reducer, Materialized.with(keySerde, valSerde));
        }


        public KTable<Windowed<K>, V> reduce(Reducer<V> reducer, Materialized<K, V, WindowStore<Bytes, byte[]>> materialized)
        {
            reducer = reducer ?? throw new System.ArgumentNullException("reducer can't be null", nameof(reducer));
            materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));

            MaterializedInternal<K, V, WindowStore<Bytes, byte[]>> materializedInternal =
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
                new KStreamWindowAggregate<>(windows, materializedInternal.storeName(), aggregateBuilder.reduceInitializer, aggregatorForReducer(reducer)),
                materializedInternal.queryableStoreName(),
                materializedInternal.keySerde() != null ? new FullTimeWindowedSerde<>(materializedInternal.keySerde(), windows.size()) : null,
                materializedInternal.valueSerde());
        }


        private StoreBuilder<TimestampedWindowStore<K, VR>> materialize(MaterializedInternal<K, VR, WindowStore<Bytes, byte[]>> materialized)
        {
            WindowBytesStoreSupplier supplier = (WindowBytesStoreSupplier)materialized.storeSupplier();
            if (supplier == null)
            {
                if (materialized.retention() != null)
                {
                    // new style retention: use Materialized retention and default segmentInterval
                    long retentionPeriod = materialized.retention().toMillis();

                    if ((windows.size() + windows.gracePeriodMs()) > retentionPeriod)
                    {
                        throw new System.ArgumentException("The retention period of the window store "
                                                               + name + " must be no smaller than its window size plus the grace period."
                                                               + " Got size=[" + windows.size() + "],"
                                                               + " grace=[" + windows.gracePeriodMs() + "],"
                                                               + " retention=[" + retentionPeriod + "]"];
                    }

                    supplier = Stores.persistentTimestampedWindowStore(
                        materialized.storeName(),
                        Duration.ofMillis(retentionPeriod),
                        Duration.ofMillis(windows.size()),
                        false
                    );

                }
                else
                {

                    // old style retention: use deprecated Windows retention/segmentInterval.

                    // NOTE: in the future, when we Remove Windows#maintainMs(), we should set the default retention
                    // to be (windows.size() + windows.grace()). This will yield the same default behavior.

                    if ((windows.size() + windows.gracePeriodMs()) > windows.maintainMs())
                    {
                        throw new System.ArgumentException("The retention period of the window store "
                                                               + name + " must be no smaller than its window size plus the grace period."
                                                               + " Got size=[" + windows.size() + "],"
                                                               + " grace=[" + windows.gracePeriodMs() + "],"
                                                               + " retention=[" + windows.maintainMs() + "]");
                    }

                    supplier = new RocksDbWindowBytesStoreSupplier(
                        materialized.storeName(),
                        windows.maintainMs(),
                        Math.Max(windows.maintainMs() / (windows.segments - 1), 60_000L),
                        windows.size(),
                        false,
                        true);
                }
            }
            StoreBuilder<TimestampedWindowStore<K, VR>> builder = Stores.timestampedWindowStoreBuilder(
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

        private Aggregator<K, V, V> aggregatorForReducer(Reducer<V> reducer)
        {
            return (aggKey, value, aggregate)->aggregate == null ? value : reducer.apply(aggregate, value);
        }
    }
