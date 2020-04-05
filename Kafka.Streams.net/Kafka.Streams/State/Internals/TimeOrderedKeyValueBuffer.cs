using Confluent.Kafka;
using Kafka.Common.Interfaces;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Processors.Internals;

namespace Kafka.Streams.State.Internals
{
    public interface ITimeOrderedKeyValueBuffer<K, V> : IStateStore
    {
        void SetSerdesIfNull(ISerde<K> keySerde, ISerde<V> valueSerde);

        void EvictWhile(ISupplier<bool> predicate, IConsumer<K, Eviction<K, V>> callback);

        ValueAndTimestamp<V> PriorValueForBuffered(K key);

        void Put(long time, K key, Change<V> value, ProcessorRecordContext recordContext);

        int NumRecords();

        long BufferSize();

        long MinTimestamp();
    }
}
