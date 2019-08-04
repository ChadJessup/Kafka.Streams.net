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
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;
import org.apache.kafka.streams.processor.IProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/*
 * Any classes (KTable, KStream, etc) extending this class should follow the serde specification precedence ordering as:
 *
 * 1) Overridden values via control objects (e.g. Materialized, Serialized, Consumed, etc)
 * 2) Serdes that can be inferred from the operator itself (e.g. groupBy().count(), where value serde can default to `LongSerde`).
 * 3) Serde inherited from parent operator if possible (note if the key / value types have been changed, then the corresponding serde cannot be inherited).
 * 4) Default serde specified in the config.
 */
public abstract class AbstractStream<K, V> {

    protected  string name;
    protected  ISerde<K> keySerde;
    protected  ISerde<V> valSerde;
    protected  Set<string> sourceNodes;
    protected  StreamsGraphNode streamsGraphNode;
    protected  InternalStreamsBuilder builder;

    // This copy-constructor will allow to extend KStream
    // and KTable APIs with new methods without impacting the public interface.
    public AbstractStream( AbstractStream<K, V> stream)
{
        this.name = stream.name;
        this.builder = stream.builder;
        this.keySerde = stream.keySerde;
        this.valSerde = stream.valSerde;
        this.sourceNodes = stream.sourceNodes;
        this.streamsGraphNode = stream.streamsGraphNode;
    }

    AbstractStream( string name,
                    ISerde<K> keySerde,
                    ISerde<V> valSerde,
                    Set<string> sourceNodes,
                    StreamsGraphNode streamsGraphNode,
                    InternalStreamsBuilder builder)
{
        if (sourceNodes == null || sourceNodes.isEmpty())
{
            throw new ArgumentException("parameter <sourceNodes> must not be null or empty");
        }

        this.name = name;
        this.builder = builder;
        this.keySerde = keySerde;
        this.valSerde = valSerde;
        this.sourceNodes = sourceNodes;
        this.streamsGraphNode = streamsGraphNode;
    }

    // This method allows to expose the InternalTopologyBuilder instance
    // to subclasses that extend AbstractStream class.
    protected InternalTopologyBuilder internalTopologyBuilder()
{
        return builder.internalTopologyBuilder;
    }

    Set<string> ensureJoinableWith( AbstractStream<K, ?> other)
{
         Set<string> allSourceNodes = new HashSet<>();
        allSourceNodes.addAll(sourceNodes);
        allSourceNodes.addAll(other.sourceNodes);

        builder.internalTopologyBuilder.copartitionSources(allSourceNodes);

        return allSourceNodes;
    }

    static <T2, T1, R> ValueJoiner<T2, T1, R> reverseJoiner( ValueJoiner<T1, T2, R> joiner)
{
        return (value2, value1) -> joiner.apply(value1, value2);
    }

    static <K, V, VR> ValueMapperWithKey<K, V, VR> withKey( ValueMapper<V, VR> valueMapper)
{
        Objects.requireNonNull(valueMapper, "valueMapper can't be null");
        return (readOnlyKey, value) -> valueMapper.apply(value);
    }

    static <K, V, VR> ValueTransformerWithKeySupplier<K, V, VR> toValueTransformerWithKeySupplier(
         ValueTransformerSupplier<V, VR> valueTransformerSupplier)
{
        Objects.requireNonNull(valueTransformerSupplier, "valueTransformerSupplier can't be null");
        return () -> {
             ValueTransformer<V, VR> valueTransformer = valueTransformerSupplier[];
            return new ValueTransformerWithKey<K, V, VR>()
{
                
                public void init( IProcessorContext context)
{
                    valueTransformer.init(context);
                }

                
                public VR transform( K readOnlyKey,  V value)
{
                    return valueTransformer.transform(value);
                }

                
                public void close()
{
                    valueTransformer.close();
                }
            };
        };
    }

    // for testing only
    public ISerde<K> keySerde()
{
        return keySerde;
    }

    public ISerde<V> valueSerde()
{
        return valSerde;
    }
}
