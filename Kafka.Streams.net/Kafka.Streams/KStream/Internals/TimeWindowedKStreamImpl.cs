/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.internals.RocksDbWindowBytesStoreSupplier;

import java.time.Duration;
import java.util.Objects;
import java.util.Set;

import static org.apache.kafka.streams.kstream.internals.KGroupedStreamImpl.AGGREGATE_NAME;
import static org.apache.kafka.streams.kstream.internals.KGroupedStreamImpl.REDUCE_NAME;

public class TimeWindowedKStreamImpl<K, V, W : Window> : AbstractStream<K, V> : TimeWindowedKStream<K, V> {

    private  Windows<W> windows;
    private  GroupedStreamAggregateBuilder<K, V> aggregateBuilder;

    TimeWindowedKStreamImpl( Windows<W> windows,
                             InternalStreamsBuilder builder,
                             Set<string> sourceNodes,
                             string name,
                             ISerde<K> keySerde,
                             ISerde<V> valSerde,
                             GroupedStreamAggregateBuilder<K, V> aggregateBuilder,
                             StreamsGraphNode streamsGraphNode)
{
        super(name, keySerde, valSerde, sourceNodes, streamsGraphNode, builder);
        this.windows = Objects.requireNonNull(windows, "windows can't be null");
        this.aggregateBuilder = aggregateBuilder;
    }

    
    public KTable<Windowed<K>, Long> count()
{
        return doCount(Materialized.with(keySerde, Serdes.Long()));
    }

    
    public KTable<Windowed<K>, Long> count( Materialized<K, Long, WindowStore<Bytes, byte[]>> materialized)
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

    private KTable<Windowed<K>, Long> doCount( Materialized<K, Long, WindowStore<Bytes, byte[]>> materialized)
{
         MaterializedInternal<K, Long, WindowStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME);

        if (materializedInternal.keySerde() == null)
{
            materializedInternal.withKeySerde(keySerde);
        }
        if (materializedInternal.valueSerde() == null)
{
            materializedInternal.withValueSerde(Serdes.Long());
        }

        return aggregateBuilder.build(
            AGGREGATE_NAME,
            materialize(materializedInternal),
            new KStreamWindowAggregate<>(windows, materializedInternal.storeName(), aggregateBuilder.countInitializer, aggregateBuilder.countAggregator),
            materializedInternal.queryableStoreName(),
            materializedInternal.keySerde() != null ? new FullTimeWindowedSerde<>(materializedInternal.keySerde(), windows.size()) : null,
            materializedInternal.valueSerde());
    }

    
    public <VR> KTable<Windowed<K>, VR> aggregate( Initializer<VR> initializer,
                                                   Aggregator<? super K, ? super V, VR> aggregator)
{
        return aggregate(initializer, aggregator, Materialized.with(keySerde, null));
    }

    
    public <VR> KTable<Windowed<K>, VR> aggregate( Initializer<VR> initializer,
                                                   Aggregator<? super K, ? super V, VR> aggregator,
                                                   Materialized<K, VR, WindowStore<Bytes, byte[]>> materialized)
{
        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(aggregator, "aggregator can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
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

    
    public KTable<Windowed<K>, V> reduce( Reducer<V> reducer)
{
        return reduce(reducer, Materialized.with(keySerde, valSerde));
    }

    
    public KTable<Windowed<K>, V> reduce( Reducer<V> reducer,  Materialized<K, V, WindowStore<Bytes, byte[]>> materialized)
{
        Objects.requireNonNull(reducer, "reducer can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");

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

    @SuppressWarnings("deprecation") // continuing to support Windows#maintainMs/segmentInterval in fallback mode
    private <VR> StoreBuilder<TimestampedWindowStore<K, VR>> materialize( MaterializedInternal<K, VR, WindowStore<Bytes, byte[]>> materialized)
{
        WindowBytesStoreSupplier supplier = (WindowBytesStoreSupplier) materialized.storeSupplier();
        if (supplier == null)
{
            if (materialized.retention() != null)
{
                // new style retention: use Materialized retention and default segmentInterval
                 long retentionPeriod = materialized.retention().toMillis();

                if ((windows.size() + windows.gracePeriodMs()) > retentionPeriod)
{
                    throw new ArgumentException("The retention period of the window store "
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

            } else {
                // old style retention: use deprecated Windows retention/segmentInterval.

                // NOTE: in the future, when we Remove Windows#maintainMs(), we should set the default retention
                // to be (windows.size() + windows.grace()). This will yield the same default behavior.

                if ((windows.size() + windows.gracePeriodMs()) > windows.maintainMs())
{
                    throw new ArgumentException("The retention period of the window store "
                                                           + name + " must be no smaller than its window size plus the grace period."
                                                           + " Got size=[" + windows.size() + "],"
                                                           + " grace=[" + windows.gracePeriodMs() + "],"
                                                           + " retention=[" + windows.maintainMs() + "]"];
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
        } else {
            builder.withLoggingDisabled();
        }

        if (materialized.cachingEnabled())
{
            builder.withCachingEnabled();
        }
        return builder;
    }

    private Aggregator<K, V, V> aggregatorForReducer( Reducer<V> reducer)
{
        return (aggKey, value, aggregate) -> aggregate == null ? value : reducer.apply(aggregate, value);
    }
}
