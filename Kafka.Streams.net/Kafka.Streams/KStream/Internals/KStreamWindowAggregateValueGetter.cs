using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.TimeStamped;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamWindowAggregateValueGetter<K, V, Agg> : IKTableValueGetter<Windowed<K>, Agg>
    {
        private ITimestampedWindowStore<K, Agg> windowStore;

        public void init(IProcessorContext context, string storeName)
        {
            windowStore = (ITimestampedWindowStore<K, Agg>)context.getStateStore(storeName);
        }

        public ValueAndTimestamp<Agg> get(Windowed<K> windowedKey)
        {
            K key = windowedKey.Key;
            var window = windowedKey.window;

            return windowStore.fetch(key, window.Start());
        }

        public void close() { }
    }
}
