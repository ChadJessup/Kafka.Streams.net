
//    public class StoreChangeLogger<K, V>
//    {
//        private string topic;
//        private int partition;
//        private IProcessorContext<K, V> context;
//        private IRecordCollector<K, V> collector;
//        private ISerializer<K> keySerializer;
//        private ISerializer<V> valueSerializer;

//        public StoreChangeLogger(string storeName,
//                          IProcessorContext<K, V> context,
//                          StateSerdes<K, V> serialization)
//            : this(storeName, context, context.taskId().partition, serialization)
//        {
//        }

//        private StoreChangeLogger(string storeName,
//                                  IProcessorContext<K, V> context,
//                                  int partition,
//                                  StateSerdes<K, V> serialization)
//        {
//            topic = ProcessorStateManager<K, V>.storeChangelogTopic(context.applicationId(), storeName);
//            this.context = context;
//            this.partition = partition;
//            this.collector = ((ISupplier)context).recordCollector();
//            keySerializer = serialization.keySerializer();
//            valueSerializer = serialization.valueSerializer();
//        }

//        public void logChange(K key,
//                       V value)
//        {
//            logChange(key, value, context.timestamp());
//        }

//        void logChange(K key,
//                       V value,
//                       long timestamp)
//        {
//            // Sending null headers to changelog topics (KIP-244)
//            collector.send(topic, key, value, null, partition, timestamp, keySerializer, valueSerializer);
//        }
//    }
//}