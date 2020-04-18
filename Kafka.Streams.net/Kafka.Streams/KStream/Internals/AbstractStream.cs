using System;
using System.Collections.Generic;
using System.Linq;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.Topologies;

namespace Kafka.Streams.KStream.Internals
{
    /*
     * Anyone (KTable, KStream, etc) extending this should follow the serde specification precedence ordering as:
     *
     * 1) Overridden values via control objects (e.g. Materialized, Serialized, Consumed, etc)
     * 2) Serdes that can be inferred from the operator itself (e.g. groupBy().count(), where value serde can default to `LongSerde`).
     * 3) Serde inherited from parent operator if possible (note if the key / value types have been changed, then the corresponding serde cannot be inherited).
     * 4) Default serde specified in the config.
     */
    public abstract class AbstractStream<K, V>
    {
        public string Name { get; }
        public ISerde<K>? KeySerde { get; }
        public ISerde<V>? ValueSerde { get; }
        public HashSet<string> SourceNodes { get; }
        public StreamsGraphNode StreamsGraphNode { get; set; }
        protected InternalStreamsBuilder Builder { get; private set; }

        // This copy-constructor will allow to extend KStream
        // and KTable APIs with new methods without impacting the public interface.
        public AbstractStream(AbstractStream<K, V> stream)
        {
            if (stream is null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

            this.Name = stream.Name;
            this.Builder = stream.Builder;
            this.KeySerde = stream.KeySerde;
            this.ValueSerde = stream.ValueSerde;
            this.SourceNodes = stream.SourceNodes;
            this.StreamsGraphNode = stream.StreamsGraphNode;
        }

        public AbstractStream(
            string Name,
            ISerde<K>? keySerde,
            ISerde<V>? valSerde,
            HashSet<string> sourceNodes,
            StreamsGraphNode streamsGraphNode,
            InternalStreamsBuilder builder)
        {
            if (sourceNodes == null || !sourceNodes.Any())
            {
                throw new ArgumentException("parameter <sourceNodes> must not be null or empty");
            }

            this.Name = Name;
            this.Builder = builder;
            this.KeySerde = keySerde;
            this.ValueSerde = valSerde;
            this.SourceNodes = sourceNodes;
            this.StreamsGraphNode = streamsGraphNode;
        }

        // This method allows to expose the InternalTopologyBuilder instance
        // to uses that extend AbstractStream.
        protected InternalTopologyBuilder InternalTopologyBuilder()
            => this.Builder.InternalTopologyBuilder;

        public HashSet<string> EnsureJoinableWith<VO>(AbstractStream<K, VO> other)
        {
            if (other is null)
            {
                throw new ArgumentNullException(nameof(other));
            }

            var allSourceNodes = new HashSet<string>();
            allSourceNodes.UnionWith(this.SourceNodes);
            allSourceNodes.UnionWith(other.SourceNodes);

            this.Builder.InternalTopologyBuilder.CopartitionSources(allSourceNodes);

            return allSourceNodes;
        }

        public static ValueJoiner<T2, T1, R> ReverseJoiner<T1, T2, R>(ValueJoiner<T1, T2, R> joiner)
        {
            return (value2, value1) => joiner(value1, value2);
        }

        protected static ValueMapperWithKey<K, V, VR> WithKey<VR>(ValueMapper<V, VR> valueMapper)
        {
            valueMapper = valueMapper ?? throw new ArgumentNullException(nameof(valueMapper));

            return new ValueMapperWithKey<K, V, VR>((readOnlyKey, value) => valueMapper(value));
        }

        public static IValueTransformerWithKeySupplier<K, V, VR> ToValueTransformerWithKeySupplier<VR>(
             IValueTransformerSupplier<V, VR> valueTransformerSupplier)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new ArgumentNullException(nameof(valueTransformerSupplier));

            return null;
            //    return ()=> {
            //        ValueTransformer<V, VR> valueTransformer = valueTransformerSupplier[];
            //        return new ValueTransformerWithKey<K, V, VR>()
            //        {

            //        public void Init(IProcessorContext context)
            //        {
            //            valueTransformer.Init(context);
            //        }


            //        public VR transform(K readOnlyKey, V value)
            //        {
            //            return valueTransformer.transform(value);
            //        }


            //        public void Close()
            //        {
            //            valueTransformer.Close();
            //        }
            //    };
            //};
        }
    }
}
