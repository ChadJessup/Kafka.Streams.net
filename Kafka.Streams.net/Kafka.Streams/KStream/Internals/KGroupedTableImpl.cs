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
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.internals.graph.GroupedTableOperationRepartitionNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.kstream.internals.graph.StatefulProcessorNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * The implementation class of {@link KGroupedTable}.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class KGroupedTableImpl<K, V> : AbstractStream<K, V> : KGroupedTable<K, V> {

    private static  string AGGREGATE_NAME = "KTABLE-AGGREGATE-";

    private static  string REDUCE_NAME = "KTABLE-REDUCE-";

    private  string userProvidedRepartitionTopicName;

    private  Initializer<Long> countInitializer = () -> 0L;

    private  Aggregator<K, V, Long> countAdder = (aggKey, value, aggregate) -> aggregate + 1L;

    private  Aggregator<K, V, Long> countSubtractor = (aggKey, value, aggregate) -> aggregate - 1L;

    private StreamsGraphNode repartitionGraphNode;

    KGroupedTableImpl( InternalStreamsBuilder builder,
                       string name,
                       Set<string> sourceNodes,
                       GroupedInternal<K, V> groupedInternal,
                       StreamsGraphNode streamsGraphNode)
{
        super(name, groupedInternal.keySerde(), groupedInternal.valueSerde(), sourceNodes, streamsGraphNode, builder);

        this.userProvidedRepartitionTopicName = groupedInternal.name();
    }

    private <T> KTable<K, T> doAggregate( ProcessorSupplier<K, Change<V>> aggregateSupplier,
                                          string functionName,
                                          MaterializedInternal<K, T, IKeyValueStore<Bytes, byte[]>> materialized)
{

         string sinkName = builder.newProcessorName(KStreamImpl.SINK_NAME);
         string sourceName = builder.newProcessorName(KStreamImpl.SOURCE_NAME);
         string funcName = builder.newProcessorName(functionName);
         string repartitionTopic = (userProvidedRepartitionTopicName != null ? userProvidedRepartitionTopicName : materialized.storeName())
            + KStreamImpl.REPARTITION_TOPIC_SUFFIX;

        if (repartitionGraphNode == null || userProvidedRepartitionTopicName == null)
{
            repartitionGraphNode = createRepartitionNode(sinkName, sourceName, repartitionTopic);
        }


        // the passed in StreamsGraphNode must be the parent of the repartition node
        builder.addGraphNode(this.streamsGraphNode, repartitionGraphNode);

         StatefulProcessorNode statefulProcessorNode = new StatefulProcessorNode<>(
            funcName,
            new ProcessorParameters<>(aggregateSupplier, funcName),
            new TimestampedKeyValueStoreMaterializer<>(materialized).materialize()
        );

        // now the repartition node must be the parent of the StateProcessorNode
        builder.addGraphNode(repartitionGraphNode, statefulProcessorNode);

        // return the KTable representation with the intermediate topic as the sources
        return new KTableImpl<>(funcName,
                                materialized.keySerde(),
                                materialized.valueSerde(),
                                Collections.singleton(sourceName),
                                materialized.queryableStoreName(),
                                aggregateSupplier,
                                statefulProcessorNode,
                                builder);
    }

    private GroupedTableOperationRepartitionNode<K, V> createRepartitionNode( string sinkName,
                                                                              string sourceName,
                                                                              string topic)
{

        return GroupedTableOperationRepartitionNode.<K, V>groupedTableOperationNodeBuilder()
            .withRepartitionTopic(topic)
            .withSinkName(sinkName)
            .withSourceName(sourceName)
            .withKeySerde(keySerde)
            .withValueSerde(valSerde)
            .withNodeName(sourceName).build();
    }

    
    public KTable<K, V> reduce( Reducer<V> adder,
                                Reducer<V> subtractor,
                                Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
{
        Objects.requireNonNull(adder, "adder can't be null");
        Objects.requireNonNull(subtractor, "subtractor can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
         MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME);

        if (materializedInternal.keySerde() == null)
{
            materializedInternal.withKeySerde(keySerde);
        }
        if (materializedInternal.valueSerde() == null)
{
            materializedInternal.withValueSerde(valSerde);
        }
         ProcessorSupplier<K, Change<V>> aggregateSupplier = new KTableReduce<>(materializedInternal.storeName(),
                                                                                     adder,
                                                                                     subtractor);
        return doAggregate(aggregateSupplier, REDUCE_NAME, materializedInternal);
    }

    
    public KTable<K, V> reduce( Reducer<V> adder,
                                Reducer<V> subtractor)
{
        return reduce(adder, subtractor, Materialized.with(keySerde, valSerde));
    }

    
    public KTable<K, Long> count( Materialized<K, Long, IKeyValueStore<Bytes, byte[]>> materialized)
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

         ProcessorSupplier<K, Change<V>> aggregateSupplier = new KTableAggregate<>(materializedInternal.storeName(),
                                                                                        countInitializer,
                                                                                        countAdder,
                                                                                        countSubtractor);

        return doAggregate(aggregateSupplier, AGGREGATE_NAME, materializedInternal);
    }

    
    public KTable<K, Long> count()
{
        return count(Materialized.with(keySerde, Serdes.Long()));
    }

    
    public <VR> KTable<K, VR> aggregate( Initializer<VR> initializer,
                                         Aggregator<? super K, ? super V, VR> adder,
                                         Aggregator<? super K, ? super V, VR> subtractor,
                                         Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
{
        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(adder, "adder can't be null");
        Objects.requireNonNull(subtractor, "subtractor can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");

         MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME);

        if (materializedInternal.keySerde() == null)
{
            materializedInternal.withKeySerde(keySerde);
        }
         ProcessorSupplier<K, Change<V>> aggregateSupplier = new KTableAggregate<>(materializedInternal.storeName(),
                                                                                        initializer,
                                                                                        adder,
                                                                                        subtractor);
        return doAggregate(aggregateSupplier, AGGREGATE_NAME, materializedInternal);
    }

    
    public <T> KTable<K, T> aggregate( Initializer<T> initializer,
                                       Aggregator<? super K, ? super V, T> adder,
                                       Aggregator<? super K, ? super V, T> subtractor)
{
        return aggregate(initializer, adder, subtractor, Materialized.with(keySerde, null));
    }

}
