using System;
using System.Collections.Generic;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.State;
using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.KStream.Internals
{
    public class GroupedStreamAggregateBuilder<K, V>
    {
        private readonly KafkaStreamsContext context;
        private readonly ISerde<K>? keySerde;
        private readonly ISerde<V>? valueSerde;
        private readonly bool repartitionRequired;
        private readonly string? userProvidedRepartitionTopicName;
        private readonly HashSet<string> sourceNodes;
        private readonly string Name;
        private readonly StreamsGraphNode streamsGraphNode;
        private StreamsGraphNode repartitionNode;

        public Initializer<long> countInitializer { get; } = () => 0L;
        public Aggregator<K, V, long> countAggregator { get; } = (aggKey, value, aggregate) => aggregate + 1;
        public Initializer<V> reduceInitializer { get; } = () => default;

        public GroupedStreamAggregateBuilder(
            KafkaStreamsContext context,
            GroupedInternal<K, V>? groupedInternal,
            bool repartitionRequired,
            HashSet<string> sourceNodes,
            string Name,
            StreamsGraphNode streamsGraphNode)
        {
            this.context = context;
            this.keySerde = groupedInternal?.KeySerde;
            this.valueSerde = groupedInternal?.ValueSerde;
            this.repartitionRequired = repartitionRequired;
            this.sourceNodes = sourceNodes;
            this.Name = Name;
            this.streamsGraphNode = streamsGraphNode;
            this.userProvidedRepartitionTopicName = groupedInternal?.Name;
        }

        public IKTable<KR, VR> Build<KR, VR, TStoreType>(
            string functionName,
            IStoreBuilder<TStoreType> storeBuilder,
            IKStreamAggProcessorSupplier<K, KR, V, VR> aggregateSupplier,
            string queryableStoreName,
            ISerde<KR> keySerde,
            ISerde<VR> valSerde)
            where TStoreType : IStateStore
        {
            if (storeBuilder is null)
            {
                throw new ArgumentNullException(nameof(storeBuilder));
            }

            if (queryableStoreName != null && !queryableStoreName.Equals(storeBuilder.Name))
            {
                throw new ArgumentException($"{nameof(queryableStoreName)} must match {nameof(storeBuilder.Name)}");
            }

            var aggFunctionName = this.context.InternalStreamsBuilder.NewProcessorName(functionName);

            var sourceName = this.Name;
            StreamsGraphNode parentNode = this.streamsGraphNode;

            if (this.repartitionRequired)
            {
                OptimizableRepartitionNodeBuilder<K, V> repartitionNodeBuilder
                    = OptimizableRepartitionNode.GetOptimizableRepartitionNodeBuilder<K, V>();

                var repartitionTopicPrefix = this.userProvidedRepartitionTopicName ?? storeBuilder.Name;

                sourceName = this.CreateRepartitionSource(repartitionTopicPrefix, repartitionNodeBuilder);

                // First time through we need to create a repartition node.
                // Any subsequent calls to GroupedStreamAggregateBuilder#build we check if
                // the user has provided a Name for the repartition topic, is so we re-use
                // the existing repartition node, otherwise we create a new one.
                if (this.repartitionNode == null || this.userProvidedRepartitionTopicName == null)
                {
                    this.repartitionNode = repartitionNodeBuilder.Build();
                }

                this.context.InternalStreamsBuilder.AddGraphNode<K, V>(parentNode, this.repartitionNode);
                parentNode = this.repartitionNode;
            }

            var statefulProcessorNode =
               new StatefulProcessorNode<K, V, TStoreType>(
                   aggFunctionName,
                   new ProcessorParameters<K, V>(aggregateSupplier, aggFunctionName),
                   storeBuilder);

            this.context.InternalStreamsBuilder.AddGraphNode<KR, VR>(parentNode, statefulProcessorNode);

            return new KTable<KR, IKeyValueStore<Bytes, byte[]>, VR>(
                this.context,
                aggFunctionName,
                keySerde,
                valSerde,
                sourceName.Equals(this.Name) ? this.sourceNodes : new HashSet<string> { sourceName },
                queryableStoreName,
                aggregateSupplier,
                statefulProcessorNode);
        }

        /**
         * @return the new sourceName of the repartitioned source
         */
        private string CreateRepartitionSource(
            string repartitionTopicNamePrefix,
            OptimizableRepartitionNodeBuilder<K, V> optimizableRepartitionNodeBuilder)
            => KStream.CreateRepartitionedSource(
                this.context.InternalStreamsBuilder,
                this.keySerde,
                this.valueSerde,
                repartitionTopicNamePrefix,
                optimizableRepartitionNodeBuilder);
    }
}
