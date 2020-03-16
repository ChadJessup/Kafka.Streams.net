using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.State.Interfaces
{
    public interface INextIteratorFunction<K, V, StoreType>
    {
        IKeyValueIterator<K, V> apply(StoreType store);
    }
}