using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;

namespace Kafka.Streams.KStream.Internals
{
    public interface IKTableValueGetter<K, V>
    {
        void init(IProcessorContext context, string storeName);

        ValueAndTimestamp<V>? get(K key);

        void close();
    }
}