using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableAggregate<K, V, T> : IKTableProcessorSupplier<K, V, T>
    {
        private readonly string? storeName;
        private readonly KafkaStreamsContext context;
        private readonly Initializer<T> initializer;
        private readonly Aggregator<K, V, T> add;
        private readonly Aggregator<K, V, T> subtractor;

        private bool sendOldValues = false;

        public KTableAggregate(
            KafkaStreamsContext context,
            string? storeName,
            Initializer<T> initializer,
            Aggregator<K, V, T> adder,
            Aggregator<K, V, T> subtractor)
        {
            this.context = context;
            this.storeName = storeName;
            this.initializer = initializer;
            this.add = adder;
            this.subtractor = subtractor;
        }

        public void EnableSendingOldValues()
        {
            this.sendOldValues = true;
        }

        public IKeyValueProcessor<K, IChange<V>> Get()
        {
            return null; //new KTableAggregateProcessor();
        }

        public IKTableValueGetterSupplier<K, T> View()
        {
            return new KTableMaterializedValueGetterSupplier<K, T>(
                this.context,
                this.storeName);
        }

        IKeyValueProcessor IProcessorSupplier.Get()
            => this.Get();
    }
}
