using Kafka.Common;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.State;
using Kafka.Streams.State.KeyValues;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class GroupedStreamAggregateBuilder<K, V>
    {
        private readonly KafkaStreamsContext context;
        private readonly InternalStreamsBuilder builder;
        private readonly ISerde<K> keySerde;
        private readonly ISerde<V> valueSerde;
        private readonly bool repartitionRequired;
        private readonly string userProvidedRepartitionTopicName;
        private readonly HashSet<string> sourceNodes;
        private readonly string name;
        private readonly StreamsGraphNode streamsGraphNode;
        private StreamsGraphNode repartitionNode;

        public IInitializer<V> countInitializer;// = () => 0L;
        public IAggregator<K, long, V> countAggregator;// = (aggKey, value, aggregate) => aggregate + 1;
        public IInitializer<V> reduceInitializer;// = () => null;

        public GroupedStreamAggregateBuilder(
            KafkaStreamsContext context,
            InternalStreamsBuilder builder,
            GroupedInternal<K, V> groupedInternal,
            bool repartitionRequired,
            HashSet<string> sourceNodes,
            string name,
            StreamsGraphNode streamsGraphNode)
        {
            this.context = context;
            this.builder = builder;
            this.keySerde = groupedInternal.KeySerde;
            this.valueSerde = groupedInternal.ValueSerde;
            this.repartitionRequired = repartitionRequired;
            this.sourceNodes = sourceNodes;
            this.name = name;
            this.streamsGraphNode = streamsGraphNode;
            this.userProvidedRepartitionTopicName = groupedInternal.Name;
        }

        public IKTable<KR, VR> Build<KR, VR>(
            string functionName,
            IStoreBuilder<IStateStore> storeBuilder,
            IKStreamAggProcessorSupplier<KR, K, VR, V> aggregateSupplier,
            string queryableStoreName,
            ISerde<KR> keySerde,
            ISerde<VR> valSerde)
        {
            if (queryableStoreName != null && !queryableStoreName.Equals(storeBuilder.name))
            {
                throw new ArgumentException($"{nameof(queryableStoreName)} must match {nameof(storeBuilder.name)}");
            }

            var aggFunctionName = builder.NewProcessorName(functionName);

            var sourceName = this.name;
            StreamsGraphNode parentNode = streamsGraphNode;

            if (repartitionRequired)
            {
                OptimizableRepartitionNodeBuilder<K, V> repartitionNodeBuilder
                    = OptimizableRepartitionNode.GetOptimizableRepartitionNodeBuilder<K, V>();

                var repartitionTopicPrefix = userProvidedRepartitionTopicName ?? storeBuilder.name;

                sourceName = CreateRepartitionSource(repartitionTopicPrefix, repartitionNodeBuilder);

                // First time through we need to create a repartition node.
                // Any subsequent calls to GroupedStreamAggregateBuilder#build we check if
                // the user has provided a name for the repartition topic, is so we re-use
                // the existing repartition node, otherwise we create a new one.
                if (repartitionNode == null || userProvidedRepartitionTopicName == null)
                {
                    repartitionNode = repartitionNodeBuilder.Build();
                }

                builder.AddGraphNode<K, V>(parentNode, repartitionNode);
                parentNode = repartitionNode;
            }

            var statefulProcessorNode =
               new StatefulProcessorNode<KR, VR>(
                   aggFunctionName,
                   new ProcessorParameters<KR, VR>(aggregateSupplier, aggFunctionName),
                   storeBuilder);

            builder.AddGraphNode<KR, VR>(parentNode, statefulProcessorNode);

            return new KTable<KR, IKeyValueStore<Bytes, byte[]>, VR>(
                this.context,
                aggFunctionName,
                keySerde,
                valSerde,
                sourceName.Equals(this.name) ? sourceNodes : new HashSet<string> { sourceName },
                queryableStoreName,
                aggregateSupplier,
                statefulProcessorNode,
                builder);
        }

        /**
         * @return the new sourceName of the repartitioned source
         */
        private string CreateRepartitionSource(
            string repartitionTopicNamePrefix,
            OptimizableRepartitionNodeBuilder<K, V> optimizableRepartitionNodeBuilder)
            => KStream<K, V>.CreateRepartitionedSource(
                builder,
                keySerde,
                valueSerde,
                repartitionTopicNamePrefix,
                optimizableRepartitionNodeBuilder);
    }
}
