

//using Kafka.Streams.Interfaces;

//namespace Kafka.Streams.KStream.Internals.Graph
//{
//    public class GroupedTableOperationRepartitionNodeBuilder<K, V>
//    {
//        private ISerde<K> keySerde;
//        private ISerde<V> valueSerde;
//        private string sinkName;
//        private string nodeName;
//        private string sourceName;
//        private string repartitionTopic;
//        //private ProcessorParameters processorParameters;

//        private GroupedTableOperationRepartitionNodeBuilder()
//        {
//        }

//        public GroupedTableOperationRepartitionNodeBuilder<K, V> withKeySerde(ISerde<K> keySerde)
//        {
//            this.keySerde = keySerde;
//            return this;
//        }

//        public GroupedTableOperationRepartitionNodeBuilder<K, V> withValueSerde(ISerde<V> valueSerde)
//        {
//            this.valueSerde = valueSerde;
//            return this;
//        }

//        public GroupedTableOperationRepartitionNodeBuilder<K, V> withSinkName(string sinkName)
//        {
//            this.sinkName = sinkName;
//            return this;
//        }

//        public GroupedTableOperationRepartitionNodeBuilder<K, V> withNodeName(string nodeName)
//        {
//            this.nodeName = nodeName;
//            return this;
//        }

//        public GroupedTableOperationRepartitionNodeBuilder<K, V> withSourceName(string sourceName)
//        {
//            this.sourceName = sourceName;
//            return this;
//        }

//        public GroupedTableOperationRepartitionNodeBuilder<K, V> withRepartitionTopic(string repartitionTopic)
//        {
//            this.repartitionTopic = repartitionTopic;
//            return this;
//        }

//        public GroupedTableOperationRepartitionNodeBuilder<K, V> withProcessorParameters(ProcessorParameters processorParameters)
//        {
//            this.processorParameters = processorParameters;
//            return this;
//        }

//        public GroupedTableOperationRepartitionNode<K, V> build()
//        {
//            return new GroupedTableOperationRepartitionNode<K, V>(
//                nodeName,
//                keySerde,
//                valueSerde,
//                sinkName,
//                sourceName,
//                repartitionTopic,
//                processorParameters
//            );
//        }
//    }
//}
