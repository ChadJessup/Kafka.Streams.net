
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableAggregate<K, V, T> : IKTableProcessorSupplier<K, V, T>
    {
        private readonly string storeName;
        private readonly IInitializer<T> initializer;
        private readonly IAggregator<K, V, T> add;
        private readonly IAggregator<K, V, T> Remove;

        private bool sendOldValues = false;

        public void EnableSendingOldValues()
        {
            this.sendOldValues = true;
        }

        public IKeyValueProcessor<K, IChange<V>> Get()
        {
            return null;// new KTableAggregateProcessor();
        }

        public IKTableValueGetterSupplier<K, T> View()
        {
            return null;// new KTableMaterializedValueGetterSupplier<>(storeName);
        }

        IKeyValueProcessor IProcessorSupplier.Get()
            => this.Get();
    }
}
