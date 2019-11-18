
//using Confluent.Kafka;
//using Kafka.Common.Interfaces;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.Processors.Internals;

//namespace Kafka.Streams.State.Internals
//{
//    public interface TimeOrderedKeyValueBuffer<K, V> : IStateStore
//    {

//        void setSerdesIfNull(ISerde<K> keySerde, ISerde<V> valueSerde);

//        void evictWhile(ISupplier<bool> predicate, IConsumer<K, Eviction<K, V>> callback);

//        Maybe<ValueAndTimestamp<V>> priorValueForBuffered(K key);

//        void put(long time, K key, Change<V> value, ProcessorRecordContext recordContext);

//        int numRecords();

//        long bufferSize();

//        long minTimestamp();
//    }
//}