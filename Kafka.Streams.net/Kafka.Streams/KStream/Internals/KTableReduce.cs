using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableReduce<K, V> : IKTableProcessorSupplier<K, V, V>
    {
        private readonly KafkaStreamsContext context;
        private readonly string storeName;
        private readonly IReducer<V> addReducer;
        private readonly IReducer<V> removeReducer;

        private bool sendOldValues = false;

        public KTableReduce(
            KafkaStreamsContext context,
            string storeName,
            IReducer<V> addReducer,
            IReducer<V> removeReducer)
        {
            this.context = context;
            this.storeName = storeName;
            this.addReducer = addReducer;
            this.removeReducer = removeReducer;
        }

        public void EnableSendingOldValues()
        {
            sendOldValues = true;
        }

        public IKeyValueProcessor<K, IChange<V>> Get()
        {
            return null;//new KTableReduceProcessor<K, V>();
        }

        IKeyValueProcessor IProcessorSupplier.Get()
            => this.Get();

        public IKTableValueGetterSupplier<K, V> View()
        {
            return new KTableMaterializedValueGetterSupplier<K, V>(
                this.context,
                storeName);
        }
    }
}
