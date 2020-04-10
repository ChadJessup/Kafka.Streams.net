using System;
using Kafka.Common;
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Sessions;
using Kafka.Streams.State.TimeStamped;
using Kafka.Streams.State.Windowed;

namespace Kafka.Streams.Interfaces
{
    public interface IStoresFactory
    {
        ISessionBytesStoreSupplier InMemorySessionStore(string Name, TimeSpan retentionPeriod);
        IKeyValueBytesStoreSupplier PersistentKeyValueStore(string Name);
        ISessionBytesStoreSupplier PersistentSessionStore(string Name, long retentionPeriodMs);
        IWindowBytesStoreSupplier PersistentTimestampedWindowStore(string Name, TimeSpan retentionPeriod, TimeSpan windowSize, bool retainDuplicates);
        IWindowBytesStoreSupplier PersistentWindowStore(string Name, TimeSpan retentionPeriod, int numSegments, TimeSpan windowSize, bool retainDuplicates);
        IWindowBytesStoreSupplier PersistentWindowStore(string Name, TimeSpan retentionPeriod, TimeSpan windowSize, bool retainDuplicates);

        ITimestampedKeyValueBytesStoreSupplier PersistentTimestampedKeyValueStore(string Name);
        IStoreBuilder<ISessionStore<K, V>> SessionStoreBuilder<K, V>(KafkaStreamsContext context, ISessionBytesStoreSupplier supplier, ISerde<K> keySerde, ISerde<V> valueSerde) where V : class;
        IStoreBuilder<IKeyValueStore<K, V>> KeyValueStoreBuilder<K, V>(KafkaStreamsContext context, IKeyValueBytesStoreSupplier supplier, ISerde<K> keySerde, ISerde<V> valueSerde);
        IStoreBuilder<ITimestampedKeyValueStore<K, V>> TimestampedKeyValueStoreBuilder<K, V>(KafkaStreamsContext context, IKeyValueBytesStoreSupplier supplier, ISerde<K> keySerde, ISerde<V> valueSerde);
        IStoreBuilder<ITimestampedWindowStore<K, V>> TimestampedWindowStoreBuilder<K, V>(KafkaStreamsContext context, IWindowBytesStoreSupplier supplier, ISerde<K> keySerde, ISerde<V> valueSerde);
        IStoreBuilder<IWindowStore<K, V>> WindowStoreBuilder<K, V>(KafkaStreamsContext context, IWindowBytesStoreSupplier supplier, ISerde<K> keySerde, ISerde<V> valueSerde);
    }
}
