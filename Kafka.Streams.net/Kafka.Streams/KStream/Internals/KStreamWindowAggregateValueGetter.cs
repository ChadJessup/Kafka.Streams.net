using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.State;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamWindowAggregateValueGetter<K, V, Agg> : IKTableValueGetter<Windowed<K>, Agg>
    {
        private ITimestampedWindowStore<K, Agg> windowStore;

        public void init(IProcessorContext<Windowed<K>, Agg> context, string storeName)
        {
            windowStore = (ITimestampedWindowStore<K, Agg>)context.getStateStore(storeName);
        }

        public ValueAndTimestamp<Agg> get(Windowed<K> windowedKey)
        {
            K key = windowedKey.key;
            var window = windowedKey.window;

            return windowStore.fetch(key, window.start());
        }

        public void close() { }
    }
}
