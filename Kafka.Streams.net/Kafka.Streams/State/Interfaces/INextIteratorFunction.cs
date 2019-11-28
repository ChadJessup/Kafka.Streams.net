using Kafka.Streams.State.KeyValue;

namespace Kafka.Streams.State.Interfaces
{
    public interface INextIteratorFunction<K, V, StoreType>
    {
        IKeyValueIterator<K, V> apply(StoreType store);
    }
}