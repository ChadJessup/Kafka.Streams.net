using System;
using Kafka.Common;
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
        ISessionBytesStoreSupplier InMemorySessionStore(string name, TimeSpan retentionPeriod);
        IKeyValueBytesStoreSupplier PersistentKeyValueStore(string name);
        ISessionBytesStoreSupplier PersistentSessionStore(string name, long retentionPeriodMs);
        IWindowBytesStoreSupplier PersistentTimestampedWindowStore(string name, TimeSpan retentionPeriod, TimeSpan windowSize, bool retainDuplicates);
        IWindowBytesStoreSupplier PersistentWindowStore(string name, TimeSpan retentionPeriod, int numSegments, TimeSpan windowSize, bool retainDuplicates);
        IWindowBytesStoreSupplier PersistentWindowStore(string name, TimeSpan retentionPeriod, TimeSpan windowSize, bool retainDuplicates);

        ITimestampedKeyValueBytesStoreSupplier PersistentTimestampedKeyValueStore(string name);
        IStoreBuilder<ISessionStore<K, V>> SessionStoreBuilder<K, V>(ISessionBytesStoreSupplier supplier, ISerde<K> keySerde, ISerde<V> valueSerde) where V : class;
        IStoreBuilder<IKeyValueStore<K, V>> KeyValueStoreBuilder<K, V>(IClock clock, IKeyValueBytesStoreSupplier supplier, ISerde<K> keySerde, ISerde<V> valueSerde);
        IStoreBuilder<ITimestampedKeyValueStore<K, V>> TimestampedKeyValueStoreBuilder<K, V>(IClock clock, IKeyValueBytesStoreSupplier supplier, ISerde<K> keySerde, ISerde<V> valueSerde);
        IStoreBuilder<ITimestampedWindowStore<K, V>> TimestampedWindowStoreBuilder<K, V>(IClock clock, IWindowBytesStoreSupplier supplier, ISerde<K> keySerde, ISerde<V> valueSerde);
        IStoreBuilder<IWindowStore<K, V>> WindowStoreBuilder<K, V>(IClock clock, IWindowBytesStoreSupplier supplier, ISerde<K> keySerde, ISerde<V> valueSerde);
    }
}
