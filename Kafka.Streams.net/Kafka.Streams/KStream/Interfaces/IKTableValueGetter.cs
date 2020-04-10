using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;

namespace Kafka.Streams.KStream.Internals
{
    public interface IKTableValueGetter<K, V>
    {
        void Init(IProcessorContext context, string storeName);

        IValueAndTimestamp<V>? Get(K key);

        void Close();
    }
}
