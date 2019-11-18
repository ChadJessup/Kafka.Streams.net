using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Kafka.Streams.KStream.Internals
{
    public class GroupedStreamAggregateBuilder<K, V>
    {
        private readonly InternalStreamsBuilder builder;
        private readonly ISerde<K> keySerde;
        private readonly ISerde<V> valueSerde;
        private readonly bool repartitionRequired;
        private readonly string userProvidedRepartitionTopicName;
        private readonly HashSet<string> sourceNodes;
        private readonly string name;
        private readonly StreamsGraphNode streamsGraphNode;
        private StreamsGraphNode repartitionNode;

        public IInitializer<long> countInitializer;// = () => 0L;
        public IAggregator<K, V, long> countAggregator;// = (aggKey, value, aggregate) => aggregate + 1;
        public IInitializer<V> reduceInitializer;// = () => null;

        public GroupedStreamAggregateBuilder(
            InternalStreamsBuilder builder,
            GroupedInternal<K, V> groupedInternal,
            bool repartitionRequired,
            HashSet<string> sourceNodes,
            string name,
            StreamsGraphNode streamsGraphNode)
        {
            this.builder = builder;
            this.keySerde = groupedInternal.keySerde;
            this.valueSerde = groupedInternal.valueSerde;
            this.repartitionRequired = repartitionRequired;
            this.sourceNodes = sourceNodes;
            this.name = name;
            this.streamsGraphNode = streamsGraphNode;
            this.userProvidedRepartitionTopicName = groupedInternal.name;
        }

        public IKTable<KR, VR> build<KR, VR>(
            string functionName,
            IStoreBuilder<IStateStore> storeBuilder,
            IKStreamAggProcessorSupplier<K, KR, V, VR> aggregateSupplier,
            string queryableStoreName,
            ISerde<KR> keySerde,
            ISerde<VR> valSerde)
        {
            if (queryableStoreName != null && !queryableStoreName.Equals(storeBuilder.name))
            {
                throw new ArgumentException($"{nameof(queryableStoreName)} must match {nameof(storeBuilder.name)}");
            }

            string aggFunctionName = builder.NewProcessorName(functionName);

            string sourceName = this.name;
            StreamsGraphNode parentNode = streamsGraphNode;

            if (repartitionRequired)
            {
                OptimizableRepartitionNodeBuilder<K, V> repartitionNodeBuilder
                    = OptimizableRepartitionNode.GetOptimizableRepartitionNodeBuilder<K, V>();

                string repartitionTopicPrefix = userProvidedRepartitionTopicName != null
                    ? userProvidedRepartitionTopicName
                    : storeBuilder.name;

                sourceName = createRepartitionSource(repartitionTopicPrefix, repartitionNodeBuilder);

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

            StatefulProcessorNode<K, V> statefulProcessorNode =
               new StatefulProcessorNode<K, V>(
                   aggFunctionName,
                   new ProcessorParameters<K, V>(aggregateSupplier, aggFunctionName),
                   storeBuilder);

            builder.AddGraphNode<K, V>(parentNode, statefulProcessorNode);

            return new KTable<KR, object, VR>(
                aggFunctionName,
                keySerde,
                valSerde,
                sourceName.Equals(this.name) ? sourceNodes : new HashSet<string> { sourceName },
                queryableStoreName,
                aggregateSupplier.GetSwappedProcessorSupplier(),
                statefulProcessorNode,
                builder);
        }

        /**
         * @return the new sourceName of the repartitioned source
         */
        private string createRepartitionSource(
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
