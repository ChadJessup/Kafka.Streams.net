using Kafka.Streams.Nodes;
using Kafka.Streams.State.KeyValues;
using NodaTime;

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
        private readonly IClock clock;

        public TableSourceNodeBuilder(IClock clock)
        {
            this.clock = clock;
        }

        public TableSourceNodeBuilder<K, V> WithSourceName(string sourceName)
        {
            this.sourceName = sourceName;
            return this;
        }

        public TableSourceNodeBuilder<K, V> WithTopic(string topic)
        {
            this.topic = topic;
            return this;
        }

        public TableSourceNodeBuilder<K, V> WithMaterializedInternal(MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal)
        {
            this.materializedInternal = materializedInternal;
            return this;
        }

        public TableSourceNodeBuilder<K, V> WithConsumedInternal(ConsumedInternal<K, V> consumedInternal)
        {
            this.consumedInternal = consumedInternal;
            return this;
        }

        public TableSourceNodeBuilder<K, V> WithProcessorParameters(ProcessorParameters<K, V> processorParameters)
        {
            this.processorParameters = processorParameters;
            return this;
        }

        public TableSourceNodeBuilder<K, V> WithNodeName(string nodeName)
        {
            this.nodeName = nodeName;
            return this;
        }

        public TableSourceNodeBuilder<K, V> IsGlobalKTable(bool isGlobaKTable)
        {
            this._isGlobalKTable = isGlobaKTable;

            return this;
        }

        public TableSourceNode<K, V> Build()
        {
            return new TableSourceNode<K, V>(
                this.clock,
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
