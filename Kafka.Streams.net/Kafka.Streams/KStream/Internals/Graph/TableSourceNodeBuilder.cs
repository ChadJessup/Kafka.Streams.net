using Kafka.Common.Utils;
using Kafka.Streams.State.Internals;

namespace Kafka.Streams.KStream.Internals.Graph
{
    public class TableSourceNodeBuilder<K, V>
    {
        private string nodeName;
        private string sourceName;
        private string topic;
        private ConsumedInternal<K, V> consumedInternal;
        private MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal;
        private ProcessorParameters<K, V> processorParameters;
        private bool _isGlobalKTable = false;

        public TableSourceNodeBuilder<K, V> withSourceName(string sourceName)
        {
            this.sourceName = sourceName;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withTopic(string topic)
        {
            this.topic = topic;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withMaterializedInternal(MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal)
        {
            this.materializedInternal = materializedInternal;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withConsumedInternal(ConsumedInternal<K, V> consumedInternal)
        {
            this.consumedInternal = consumedInternal;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withProcessorParameters(ProcessorParameters<K, V> processorParameters)
        {
            this.processorParameters = processorParameters;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withNodeName(string nodeName)
        {
            this.nodeName = nodeName;
            return this;
        }

        public TableSourceNodeBuilder<K, V> isGlobalKTable(bool isGlobaKTable)
        {
            this._isGlobalKTable = isGlobaKTable;

            return this;
        }

        public TableSourceNode<K, V> build()
        {
            return new TableSourceNode<K, V>(
                nodeName,
                sourceName,
                topic,
                consumedInternal,
                materializedInternal,
                processorParameters,
                _isGlobalKTable);
        }
    }
}
