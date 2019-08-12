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
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.Processor.Internals;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.KStream.Internals
{
    /*
     * Anyes (KTable, KStream, etc) extending this should follow the serde specification precedence ordering as:
     *
     * 1) Overridden values via control objects (e.g. Materialized, Serialized, Consumed, etc)
     * 2) Serdes that can be inferred from the operator itself (e.g. groupBy().count(), where value serde can default to `LongSerde`).
     * 3) Serde inherited from parent operator if possible (note if the key / value types have been changed, then the corresponding serde cannot be inherited).
     * 4) Default serde specified in the config.
     */
    public abstract class AbstractStream<K, V>
    {

        protected string name;
        protected ISerde<K> keySerde;
        protected ISerde<V> valSerde;
        protected HashSet<string> sourceNodes;
        protected StreamsGraphNode streamsGraphNode;
        protected InternalStreamsBuilder builder;

        // This copy-constructor will allow to extend KStream
        // and KTable APIs with new methods without impacting the public interface.
        public AbstractStream(AbstractStream<K, V> stream)
        {
            this.name = stream.name;
            this.builder = stream.builder;
            this.keySerde = stream.keySerde;
            this.valSerde = stream.valSerde;
            this.sourceNodes = stream.sourceNodes;
            this.streamsGraphNode = stream.streamsGraphNode;
        }

        public AbstractStream(
            string name,
            ISerde<K> keySerde,
            ISerde<V> valSerde,
            HashSet<string> sourceNodes,
            StreamsGraphNode streamsGraphNode,
            InternalStreamsBuilder builder)
        {
            if (sourceNodes == null || !sourceNodes.Any())
            {
                throw new System.ArgumentException("parameter <sourceNodes> must not be null or empty");
            }

            this.name = name;
            this.builder = builder;
            this.keySerde = keySerde;
            this.valSerde = valSerde;
            this.sourceNodes = sourceNodes;
            this.streamsGraphNode = streamsGraphNode;
        }

        // This method allows to expose the InternalTopologyBuilder instance
        // to sues that extend AbstractStream.
        protected InternalTopologyBuilder internalTopologyBuilder()
        {
            return builder.internalTopologyBuilder;
        }

        public HashSet<string> ensureJoinableWith(AbstractStream<K, object> other)
        {
            HashSet<string> allSourceNodes = new HashSet<string>();
            allSourceNodes.UnionWith(sourceNodes);
            allSourceNodes.UnionWith(other.sourceNodes);

            builder.internalTopologyBuilder.copartitionSources(allSourceNodes);

            return allSourceNodes;
        }

        static IValueJoiner<T2, T1, R> reverseJoiner<T1, T2, R>(IValueJoiner<T1, T2, R> joiner)
        {
            return null;// (value2, value1)=>joiner.apply(value1, value2);
        }

        protected static IValueMapperWithKey<K, V, VR> withKey<VR>(IValueMapper<V, VR> valueMapper)
        {
            valueMapper = valueMapper ?? throw new System.ArgumentNullException("valueMapper can't be null", nameof(valueMapper));
            return default; // (readOnlyKey, value)=>valueMapper.apply(value);
        }

        static IValueTransformerWithKeySupplier<K, V, VR> toValueTransformerWithKeySupplier<VR>(
             IValueTransformerSupplier<V, VR> valueTransformerSupplier)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new System.ArgumentNullException("valueTransformerSupplier can't be null", nameof(valueTransformerSupplier));
            return null;
            //    return ()=> {
            //        ValueTransformer<V, VR> valueTransformer = valueTransformerSupplier[];
            //        return new ValueTransformerWithKey<K, V, VR>()
            //        {

            //        public void init(IProcessorContext<K, V> context)
            //        {
            //            valueTransformer.init(context);
            //        }


            //        public VR transform(K readOnlyKey, V value)
            //        {
            //            return valueTransformer.transform(value);
            //        }


            //        public void close()
            //        {
            //            valueTransformer.close();
            //        }
            //    };
            //};
        }
    }
}