using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableReduce<K, V> : IKTableProcessorSupplier<K, V, V>
    {
        private readonly KafkaStreamsContext context;
        private readonly string storeName;
        private readonly Reducer<V> addReducer;
        private readonly Reducer<V> removeReducer;

        private bool sendOldValues = false;

        public KTableReduce(
            KafkaStreamsContext context,
            string storeName,
            Reducer<V> addReducer,
            Reducer<V> removeReducer)
        {
            this.context = context;
            this.storeName = storeName;
            this.addReducer = addReducer;
            this.removeReducer = removeReducer;
        }

        public void EnableSendingOldValues()
        {
            this.sendOldValues = true;
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
                this.storeName);
        }
    }
}
