

//using Confluent.Kafka;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.Processors;
//using Kafka.Streams.Processors.Internals;

//namespace Kafka.Streams.KStream.Internals.Graph
//{
//    public class GroupedTableOperationRepartitionNode<K, V> : BaseRepartitionNode<K, V>
//    {
//        private GroupedTableOperationRepartitionNode(
//            string nodeName,
//            ISerde<K> keySerde,
//            ISerde<V> valueSerde,
//            string sinkName,
//            string sourceName,
//            string repartitionTopic,
//            ProcessorParameters processorParameters)
//            : base(
//                nodeName,
//                sourceName,
//                processorParameters,
//                keySerde,
//                valueSerde,
//                sinkName,
//                repartitionTopic)
//        {
//        }


//        ISerializer<V> getValueSerializer()
//        {
//            ISerializer<V> valueSerializer = valueSerde?.Serializer();
//            return unsafeCastChangedToValueSerializer(valueSerializer);
//        }


//        private ISerializer<V> unsafeCastChangedToValueSerializer(ISerializer<V> valueSerializer)
//        {
//            return (ISerializer<V>)new ChangedSerializer<V>(valueSerializer);
//        }


//        IDeserializer<V> getValueDeserializer()
//        {
//            IDeserializer<V> valueDeserializer = valueSerde?.Deserializer();
//            return unsafeCastChangedToValueDeserializer(valueDeserializer);
//        }


//        private IDeserializer<V> unsafeCastChangedToValueDeserializer(IDeserializer<V> valueDeserializer)
//        {
//            return (IDeserializer<V>)new ChangedDeserializer<V>(valueDeserializer);
//        }


//        public string ToString()
//        {
//            return "GroupedTableOperationRepartitionNode{} " + base.ToString();
//        }


//        public void WriteToTopology(InternalTopologyBuilder topologyBuilder)
//        {
//            ISerializer<K> keySerializer = keySerde != null ? keySerde.Serializer() : null;
//            IDeserializer<K> keyDeserializer = keySerde != null ? keySerde.Deserializer() : null;


//            topologyBuilder.AddInternalTopic(repartitionTopic);

//            topologyBuilder.AddSink(
//                sinkName,
//                repartitionTopic,
//                keySerializer,
//                getValueSerializer(),
//                null,
//                parentNodeNames()
//            );

//            topologyBuilder.AddSource(
//                null,
//                sourceName,
//                new FailOnInvalidTimestamp(),
//                keyDeserializer,
//                getValueDeserializer(),
//                repartitionTopic
//            );

//        }

//        public static GroupedTableOperationRepartitionNodeBuilder<K1, V1> groupedTableOperationNodeBuilder<K1, V1>()
//        {
//            return new GroupedTableOperationRepartitionNodeBuilder<K1, V1>();
//        }
//    }
//}
