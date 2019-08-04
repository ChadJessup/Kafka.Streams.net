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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Objects;
import java.util.Set;

class KGroupedStreamImpl<K, V> : AbstractStream<K, V> : KGroupedStream<K, V> {

    static  string REDUCE_NAME = "KSTREAM-REDUCE-";
    static  string AGGREGATE_NAME = "KSTREAM-AGGREGATE-";

    private  GroupedStreamAggregateBuilder<K, V> aggregateBuilder;

    KGroupedStreamImpl( string name,
                        Set<string> sourceNodes,
                        GroupedInternal<K, V> groupedInternal,
                        bool repartitionRequired,
                        StreamsGraphNode streamsGraphNode,
                        InternalStreamsBuilder builder)
{
        super(name, groupedInternal.keySerde(), groupedInternal.valueSerde(), sourceNodes, streamsGraphNode, builder);
        this.aggregateBuilder = new GroupedStreamAggregateBuilder<>(
            builder,
            groupedInternal,
            repartitionRequired,
            sourceNodes,
            name,
            streamsGraphNode
        );
    }

    
    public KTable<K, V> reduce( Reducer<V> reducer)
{
        return reduce(reducer, Materialized.with(keySerde, valSerde));
    }

    
    public KTable<K, V> reduce( Reducer<V> reducer,
                                Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
{
        Objects.requireNonNull(reducer, "reducer can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");

         MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, REDUCE_NAME);

        if (materializedInternal.keySerde() == null)
{
            materializedInternal.withKeySerde(keySerde);
        }
        if (materializedInternal.valueSerde() == null)
{
            materializedInternal.withValueSerde(valSerde);
        }

        return doAggregate(
            new KStreamReduce<>(materializedInternal.storeName(), reducer),
            REDUCE_NAME,
            materializedInternal
        );
    }

    
    public <VR> KTable<K, VR> aggregate( Initializer<VR> initializer,
                                         Aggregator<? super K, ? super V, VR> aggregator,
                                         Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
{
        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(aggregator, "aggregator can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");

         MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME);

        if (materializedInternal.keySerde() == null)
{
            materializedInternal.withKeySerde(keySerde);
        }

        return doAggregate(
            new KStreamAggregate<>(materializedInternal.storeName(), initializer, aggregator),
            AGGREGATE_NAME,
            materializedInternal
        );
    }

    
    public <VR> KTable<K, VR> aggregate( Initializer<VR> initializer,
                                         Aggregator<? super K, ? super V, VR> aggregator)
{
        return aggregate(initializer, aggregator, Materialized.with(keySerde, null));
    }

    
    public KTable<K, Long> count()
{
        return doCount(Materialized.with(keySerde, Serdes.Long()));
    }

    
    public KTable<K, Long> count( Materialized<K, Long, IKeyValueStore<Bytes, byte[]>> materialized)
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

    private KTable<K, Long> doCount( Materialized<K, Long, IKeyValueStore<Bytes, byte[]>> materialized)
{
         MaterializedInternal<K, Long, IKeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME);

        if (materializedInternal.keySerde() == null)
{
            materializedInternal.withKeySerde(keySerde);
        }
        if (materializedInternal.valueSerde() == null)
{
            materializedInternal.withValueSerde(Serdes.Long());
        }

        return doAggregate(
            new KStreamAggregate<>(materializedInternal.storeName(), aggregateBuilder.countInitializer, aggregateBuilder.countAggregator),
            AGGREGATE_NAME,
            materializedInternal);
    }

    
    public <W : Window> TimeWindowedKStream<K, V> windowedBy( Windows<W> windows)
{

        return new TimeWindowedKStreamImpl<>(
            windows,
            builder,
            sourceNodes,
            name,
            keySerde,
            valSerde,
            aggregateBuilder,
            streamsGraphNode
        );
    }

    
    public SessionWindowedKStream<K, V> windowedBy( SessionWindows windows)
{

        return new SessionWindowedKStreamImpl<>(
            windows,
            builder,
            sourceNodes,
            name,
            keySerde,
            valSerde,
            aggregateBuilder,
            streamsGraphNode
        );
    }

    private <T> KTable<K, T> doAggregate( KStreamAggProcessorSupplier<K, K, V, T> aggregateSupplier,
                                          string functionName,
                                          MaterializedInternal<K, T, IKeyValueStore<Bytes, byte[]>> materializedInternal)
{
        return aggregateBuilder.build(
            functionName,
            new TimestampedKeyValueStoreMaterializer<>(materializedInternal).materialize(),
            aggregateSupplier,
            materializedInternal.queryableStoreName(),
            materializedInternal.keySerde(),
            materializedInternal.valueSerde());
    }
}
