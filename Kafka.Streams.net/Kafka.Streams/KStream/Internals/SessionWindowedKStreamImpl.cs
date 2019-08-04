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
namespace Kafka.streams.kstream.internals;



























public class SessionWindowedKStreamImpl<K, V> : AbstractStream<K, V> : SessionWindowedKStream<K, V> {
    private  SessionWindows windows;
    private  GroupedStreamAggregateBuilder<K, V> aggregateBuilder;
    private  Merger<K, long> countMerger = (aggKey, aggOne, aggTwo) -> aggOne + aggTwo;

    SessionWindowedKStreamImpl( SessionWindows windows,
                                InternalStreamsBuilder builder,
                                HashSet<string> sourceNodes,
                                string name,
                                ISerde<K> keySerde,
                                ISerde<V> valSerde,
                                GroupedStreamAggregateBuilder<K, V> aggregateBuilder,
                                StreamsGraphNode streamsGraphNode)
{
        super(name, keySerde, valSerde, sourceNodes, streamsGraphNode, builder);
        Objects.requireNonNull(windows, "windows can't be null");
        this.windows = windows;
        this.aggregateBuilder = aggregateBuilder;
    }

    
    public KTable<Windowed<K>, long> count()
{
        return doCount(Materialized.with(keySerde, Serdes.long()));
    }

    
    public KTable<Windowed<K>, long> count( Materialized<K, long, SessionStore<Bytes, byte[]>> materialized)
{
        Objects.requireNonNull(materialized, "materialized can't be null");

        // TODO: Remove this when we do a topology-incompatible release
        // we used to burn a topology name here, so we have to keep doing it for compatibility
        if (new MaterializedInternal<>(materialized).storeName() == null)
{
            builder.newStoreName(AGGREGATE_NAME);
        }

        return doCount(materialized);
    }

    private KTable<Windowed<K>, long> doCount( Materialized<K, long, SessionStore<Bytes, byte[]>> materialized)
{
         MaterializedInternal<K, long, SessionStore<Bytes, byte[]>> materializedInternal =
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

    
    public KTable<Windowed<K>, V> reduce( Reducer<V> reducer)
{
        return reduce(reducer, Materialized.with(keySerde, valSerde));
    }

    
    public KTable<Windowed<K>, V> reduce( Reducer<V> reducer,
                                          Materialized<K, V, SessionStore<Bytes, byte[]>> materialized)
{
        Objects.requireNonNull(reducer, "reducer can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
         Aggregator<K, V, V> reduceAggregator = aggregatorForReducer(reducer);
         MaterializedInternal<K, V, SessionStore<Bytes, byte[]>> materializedInternal =
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

    
    public <T> KTable<Windowed<K>, T> aggregate( Initializer<T> initializer,
                                                 Aggregator<K, V, T> aggregator,
                                                 Merger<K, T> sessionMerger)
{
        return aggregate(initializer, aggregator, sessionMerger, Materialized.with(keySerde, null));
    }

    
    public KTable<Windowed<K>, VR> aggregate( Initializer<VR> initializer,
                                                   Aggregator<K, V, VR> aggregator,
                                                   Merger<K, VR> sessionMerger,
                                                   Materialized<K, VR, SessionStore<Bytes, byte[]>> materialized)
{
        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(aggregator, "aggregator can't be null");
        Objects.requireNonNull(sessionMerger, "sessionMerger can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
         MaterializedInternal<K, VR, SessionStore<Bytes, byte[]>> materializedInternal =
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

    @SuppressWarnings("deprecation") // continuing to support SessionWindows#maintainMs in fallback mode
    private StoreBuilder<SessionStore<K, VR>> materialize( MaterializedInternal<K, VR, SessionStore<Bytes, byte[]>> materialized)
{
        SessionBytesStoreSupplier supplier = (SessionBytesStoreSupplier) materialized.storeSupplier();
        if (supplier == null)
{
            // NOTE: in the future, when we Remove Windows#maintainMs(), we should set the default retention
            // to be (windows.inactivityGap() + windows.grace()). This will yield the same default behavior.
             long retentionPeriod = materialized.retention() != null ? materialized.retention().toMillis() : windows.maintainMs();

            if ((windows.inactivityGap() + windows.gracePeriodMs()) > retentionPeriod)
{
                throw new ArgumentException("The retention period of the session store "
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
         StoreBuilder<SessionStore<K, VR>> builder = Stores.sessionStoreBuilder(
            supplier,
            materialized.keySerde(),
            materialized.valueSerde()
        );

        if (materialized.loggingEnabled())
{
            builder.withLoggingEnabled(materialized.logConfig());
        } else {
            builder.withLoggingDisabled();
        }

        if (materialized.cachingEnabled())
{
            builder.withCachingEnabled();
        }
        return builder;
    }

    private Merger<K, V> mergerForAggregator( Aggregator<K, V, V> aggregator)
{
        return (aggKey, aggOne, aggTwo) -> aggregator.apply(aggKey, aggTwo, aggOne);
    }

    private Aggregator<K, V, V> aggregatorForReducer( Reducer<V> reducer)
{
        return (aggKey, value, aggregate) -> aggregate == null ? value : reducer.apply(aggregate, value);
    }
}
