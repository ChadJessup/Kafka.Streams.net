using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.State.Interfaces
{
    public interface INextIteratorFunction<K, V, StoreType>
    {
        IKeyValueIterator<K, V> Apply(StoreType store);
    }
}