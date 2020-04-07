using Kafka.Common;
using Kafka.Streams.KStream.Graph;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.State;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.TimeStamped;
using Kafka.Streams.Topologies;

using System;
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
        private readonly KafkaStreamsContext context;
        private readonly string sourceName;
        private readonly bool isGlobalKTable;

        public TableSourceNode(
            KafkaStreamsContext context,
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
            this.context = context;
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

        public static TableSourceNodeBuilder<K, V> TableSourceNodeBuilder<T>(KafkaStreamsContext context)
            where T : IStateStore
        {
            return new TableSourceNodeBuilder<K, V>(context);
        }

        public override void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        {
            var topicEnumerator = GetTopicNames().GetEnumerator();
            var topicName = string.Empty;

            if (topicEnumerator.MoveNext())
            {
                topicName = topicEnumerator.Current;
            }
            else
            {
                // TODO: chad - see if no topic name should be allowed in the end.
                throw new InvalidOperationException("TableSourceNode: Unable to WriteToTopology, no Topic names set.");
            }

            // TODO: we assume source KTables can only be timestamped-key-value stores for now.
            // should be expanded for other types of stores as well.
            IStoreBuilder<ITimestampedKeyValueStore<K, V>> storeBuilder =
               new TimestampedKeyValueStoreMaterializer<K, V>(
                   this.context,
                   materializedInternal).Materialize();

            if (isGlobalKTable)
            {
                topologyBuilder.AddGlobalStore(
                    storeBuilder,
                    sourceName,
                    consumedInternal.timestampExtractor,
                    consumedInternal.KeyDeserializer(),
                    consumedInternal.ValueDeserializer(),
                    topicName,
                    processorParameters.ProcessorName,
                    processorParameters.ProcessorSupplier);
            }
            else
            {
                topologyBuilder.AddSource(
                    consumedInternal.OffsetResetPolicy(),
                    sourceName,
                    consumedInternal.timestampExtractor,
                    consumedInternal.KeyDeserializer(),
                    consumedInternal.ValueDeserializer(),
                    new[] { topicName });

                topologyBuilder.AddProcessor<K, V>(
                    processorParameters.ProcessorName,
                    processorParameters.ProcessorSupplier,
                    sourceName);

                // only add state store if the source KTable should be materialized
                var ktableSource = (KTableSource<K, V>)processorParameters.ProcessorSupplier;
                if (ktableSource.queryableName != null)
                {
                    topologyBuilder.AddStateStore<K, V, ITimestampedKeyValueStore<K, V>>(storeBuilder, new[] { this.NodeName });

                    if (ShouldReuseSourceTopicForChangelog)
                    {
                        storeBuilder.WithLoggingDisabled();
                        topologyBuilder.ConnectSourceStoreAndTopic(storeBuilder.name, topicName);
                    }
                }
            }
        }
    }
}
