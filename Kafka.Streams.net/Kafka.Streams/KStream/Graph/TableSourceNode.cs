using Kafka.Common.Utils;
using Kafka.Streams.KStream.Graph;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using Kafka.Streams.Topologies;
using System.Collections.Generic;

namespace Kafka.Streams.Nodes
{
    /**
     * Used to represent either a KTable source or a GlobalKTable source. A bool flag is used to indicate if this represents a GlobalKTable a {@link
     * org.apache.kafka.streams.kstream.GlobalKTable}
     */
    public class TableSourceNode<K, V> : StreamSourceNode<K, V>, ITableSourceNode
    {
        private readonly MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal;
        private readonly ProcessorParameters<K, V> processorParameters;
        private readonly string sourceName;
        private readonly bool isGlobalKTable;

        public TableSourceNode(
            string nodeName,
            string sourceName,
            string topic,
            ConsumedInternal<K, V> consumedInternal,
            MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal,
            ProcessorParameters<K, V> processorParameters,
            bool isGlobalKTable)
            : base(nodeName,
                  new List<string> { topic },
                  consumedInternal)
        {
            this.sourceName = sourceName;
            this.isGlobalKTable = isGlobalKTable;
            this.processorParameters = processorParameters;
            this.materializedInternal = materializedInternal;
        }

        public bool ShouldReuseSourceTopicForChangelog { get; set; } = false;

        public override string ToString()
        {
            return "TableSourceNode{" +
                   $"materializedInternal={materializedInternal}" +
                   $", processorParameters={processorParameters}" +
                   $", sourceName='{sourceName}'" +
                   $", isGlobalKTable={isGlobalKTable}" +
                   "} " + base.ToString();
        }

        public static TableSourceNodeBuilder<K, V> tableSourceNodeBuilder<T>()
            where T : IStateStore
        {
            return new TableSourceNodeBuilder<K, V>();
        }

        public override void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        {
            string topicName = GetTopicNames().GetEnumerator().Current;

            // TODO: we assume source KTables can only be timestamped-key-value stores for now.
            // should be expanded for other types of stores as well.
            IStoreBuilder<ITimestampedKeyValueStore<K, V>> storeBuilder =
               new TimestampedKeyValueStoreMaterializer<K, V>(
                   (MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>)materializedInternal).materialize();

            if (isGlobalKTable)
            {
                topologyBuilder.addGlobalStore(
                    storeBuilder,
                    sourceName,
                    consumedInternal.timestampExtractor,
                    consumedInternal.keyDeserializer(),
                    consumedInternal.valueDeserializer(),
                    topicName,
                    processorParameters.processorName,
                    processorParameters.ProcessorSupplier);
            }
            else
            {
                topologyBuilder.AddSource(
                    consumedInternal.OffsetResetPolicy(),
                    sourceName,
                    consumedInternal.timestampExtractor,
                    consumedInternal.keyDeserializer(),
                    consumedInternal.valueDeserializer(),
                    new[] { topicName });

                topologyBuilder.AddProcessor(processorParameters.processorName, processorParameters.ProcessorSupplier, sourceName);

                // only add state store if the source KTable should be materialized
                KTableSource<K, V> ktableSource = (KTableSource<K, V>)processorParameters.ProcessorSupplier;
                if (ktableSource.queryableName != null)
                {
                    topologyBuilder.addStateStore<K, V, ITimestampedKeyValueStore<K, V>>(storeBuilder, new[] { this.NodeName });

                    if (ShouldReuseSourceTopicForChangelog)
                    {
                        storeBuilder.WithLoggingDisabled();
                        topologyBuilder.connectSourceStoreAndTopic(storeBuilder.name, topicName);
                    }
                }
            }
        }
    }
}