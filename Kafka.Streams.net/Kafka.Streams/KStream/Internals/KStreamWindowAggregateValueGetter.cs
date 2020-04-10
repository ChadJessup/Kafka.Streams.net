using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.TimeStamped;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamWindowAggregateValueGetter<K, V, Agg> : IKTableValueGetter<IWindowed<K>, Agg>
    {
        private ITimestampedWindowStore<K, Agg> windowStore;
        private readonly KafkaStreamsContext context;

        public KStreamWindowAggregateValueGetter(KafkaStreamsContext context)
        {
            this.context = context;
        }

        public void Init(IProcessorContext context, string storeName)
        {
            this.windowStore = (ITimestampedWindowStore<K, Agg>)context.GetStateStore(storeName);
        }

        public IValueAndTimestamp<Agg> Get(IWindowed<K> windowedKey)
        {
            K key = windowedKey.Key;
            var window = windowedKey.window;

            return this.windowStore.Fetch(key, window.Start());
        }

        public void Close() { }
    }
}
