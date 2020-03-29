using Confluent.Kafka;
using Xunit;
using System;

namespace Kafka.Streams.Tests.State
{
    public class StoresTest
    {

        [Xunit.Fact]
        public void shouldThrowIfPersistentKeyValueStoreStoreNameIsNull()
        {
            Exception e = assertThrows(NullPointerException, () => Stores.persistentKeyValueStore(null));
            Assert.Equal("name cannot be null", e.getMessage());
        }

        [Xunit.Fact]
        public void shouldThrowIfPersistentTimestampedKeyValueStoreStoreNameIsNull()
        {
            Exception e = assertThrows(NullPointerException, () => Stores.persistentTimestampedKeyValueStore(null));
            Assert.Equal("name cannot be null", e.getMessage());
        }

        [Xunit.Fact]
        public void shouldThrowIfIMemoryKeyValueStoreStoreNameIsNull()
        {
            Exception e = assertThrows(NullPointerException, () => Stores.inMemoryKeyValueStore(null));
            Assert.Equal("name cannot be null", e.getMessage());
        }

        [Xunit.Fact]
        public void shouldThrowIfILruMapStoreNameIsNull()
        {
            Exception e = assertThrows(NullPointerException, () => Stores.lruMap(null, 0));
            Assert.Equal("name cannot be null", e.getMessage());
        }

        [Xunit.Fact]
        public void shouldThrowIfILruMapStoreCapacityIsNegative()
        {
            Exception e = assertThrows(IllegalArgumentException, () => Stores.lruMap("anyName", -1));
            Assert.Equal("maxCacheSize cannot be negative", e.getMessage());
        }

        [Xunit.Fact]
        public void shouldThrowIfIPersistentWindowStoreStoreNameIsNull()
        {
            Exception e = assertThrows(NullPointerException, () => Stores.persistentWindowStore(null, ZERO, ZERO, false));
            Assert.Equal("name cannot be null", e.getMessage());
        }

        [Xunit.Fact]
        public void shouldThrowIfIPersistentTimestampedWindowStoreStoreNameIsNull()
        {
            Exception e = assertThrows(NullPointerException, () => Stores.persistentTimestampedWindowStore(null, ZERO, ZERO, false));
            Assert.Equal("name cannot be null", e.getMessage());
        }

        [Xunit.Fact]
        public void shouldThrowIfIPersistentWindowStoreRetentionPeriodIsNegative()
        {
            Exception e = assertThrows(IllegalArgumentException, () => Stores.persistentWindowStore("anyName", ofMillis(-1L), ZERO, false));
            Assert.Equal("retentionPeriod cannot be negative", e.getMessage());
        }

        [Xunit.Fact]
        public void shouldThrowIfIPersistentTimestampedWindowStoreRetentionPeriodIsNegative()
        {
            Exception e = assertThrows(IllegalArgumentException, () => Stores.persistentTimestampedWindowStore("anyName", ofMillis(-1L), ZERO, false));
            Assert.Equal("retentionPeriod cannot be negative", e.getMessage());
        }

        @Deprecated
        [Xunit.Fact]
    public void shouldThrowIfIPersistentWindowStoreIfNumberOfSegmentsSmallerThanOne()
        {
            Exception e = assertThrows(IllegalArgumentException, () => Stores.persistentWindowStore("anyName", 0L, 1, 0L, false));
            Assert.Equal("numSegments cannot be smaller than 2", e.getMessage());
        }

        [Xunit.Fact]
        public void shouldThrowIfIPersistentWindowStoreIfWindowSizeIsNegative()
        {
            Exception e = assertThrows(IllegalArgumentException, () => Stores.persistentWindowStore("anyName", ofMillis(0L), ofMillis(-1L), false));
            Assert.Equal("windowSize cannot be negative", e.getMessage());
        }

        [Xunit.Fact]
        public void shouldThrowIfIPersistentTimestampedWindowStoreIfWindowSizeIsNegative()
        {
            Exception e = assertThrows(IllegalArgumentException, () => Stores.persistentTimestampedWindowStore("anyName", ofMillis(0L), ofMillis(-1L), false));
            Assert.Equal("windowSize cannot be negative", e.getMessage());
        }

        [Xunit.Fact]
        public void shouldThrowIfIPersistentSessionStoreStoreNameIsNull()
        {
            Exception e = assertThrows(NullPointerException, () => Stores.persistentSessionStore(null, ofMillis(0)));
            Assert.Equal("name cannot be null", e.getMessage());
        }

        [Xunit.Fact]
        public void shouldThrowIfIPersistentSessionStoreRetentionPeriodIsNegative()
        {
            Exception e = assertThrows(IllegalArgumentException, () => Stores.persistentSessionStore("anyName", ofMillis(-1)));
            Assert.Equal("retentionPeriod cannot be negative", e.getMessage());
        }

        [Xunit.Fact]
        public void shouldThrowIfSupplierIsNullForWindowStoreBuilder()
        {
            Exception e = assertThrows(NullPointerException, () => Stores.windowStoreBuilder(null, Serdes.ByteArray(), Serdes.ByteArray()));
            Assert.Equal("supplier cannot be null", e.getMessage());
        }

        [Xunit.Fact]
        public void shouldThrowIfSupplierIsNullForKeyValueStoreBuilder()
        {
            Exception e = assertThrows(NullPointerException, () => Stores.keyValueStoreBuilder(null, Serdes.ByteArray(), Serdes.ByteArray()));
            Assert.Equal("supplier cannot be null", e.getMessage());
        }

        [Xunit.Fact]
        public void shouldThrowIfSupplierIsNullForSessionStoreBuilder()
        {
            Exception e = assertThrows(NullPointerException, () => Stores.sessionStoreBuilder(null, Serdes.ByteArray(), Serdes.ByteArray()));
            Assert.Equal("supplier cannot be null", e.getMessage());
        }

        [Xunit.Fact]
        public void shouldCreateInMemoryKeyValueStore()
        {
            Assert.Equal(Stores.inMemoryKeyValueStore("memory").get(), instanceOf(InMemoryKeyValueStore));
        }

        [Xunit.Fact]
        public void shouldCreateMemoryNavigableCache()
        {
            Assert.Equal(Stores.lruMap("map", 10).get(), instanceOf(MemoryNavigableLRUCache));
        }

        [Xunit.Fact]
        public void shouldCreateRocksDbStore()
        {
            Assert.Equal(
                Stores.persistentKeyValueStore("store").get(),
                allOf(not(instanceOf(RocksDBTimestampedStore)), instanceOf(RocksDBStore)));
        }

        [Xunit.Fact]
        public void shouldCreateRocksDbTimestampedStore()
        {
            Assert.Equal(Stores.persistentTimestampedKeyValueStore("store").get(), instanceOf(RocksDBTimestampedStore));
        }

        [Xunit.Fact]
        public void shouldCreateRocksDbWindowStore()
        {
            WindowStore store = Stores.persistentWindowStore("store", ofMillis(1L), ofMillis(1L), false).get();
            StateStore wrapped = ((WrappedStateStore)store).wrapped();
            Assert.Equal(store, instanceOf(RocksDBWindowStore));
            Assert.Equal(wrapped, allOf(not(instanceOf(RocksDBTimestampedSegmentedBytesStore)), instanceOf(RocksDBSegmentedBytesStore)));
        }

        [Xunit.Fact]
        public void shouldCreateRocksDbTimestampedWindowStore()
        {
            WindowStore store = Stores.persistentTimestampedWindowStore("store", ofMillis(1L), ofMillis(1L), false).get();
            StateStore wrapped = ((WrappedStateStore)store).wrapped();
            Assert.Equal(store, instanceOf(RocksDBWindowStore));
            Assert.Equal(wrapped, instanceOf(RocksDBTimestampedSegmentedBytesStore));
        }

        [Xunit.Fact]
        public void shouldCreateRocksDbSessionStore()
        {
            Assert.Equal(Stores.persistentSessionStore("store", ofMillis(1)).get(), instanceOf(RocksDBSessionStore));
        }

        [Xunit.Fact]
        public void shouldBuildKeyValueStore()
        {
            KeyValueStore<string, string> store = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("name"),
                Serdes.String(),
                Serdes.String()
            ).build();
            Assert.Equal(store, not(nullValue()));
        }

        [Xunit.Fact]
        public void shouldBuildTimestampedKeyValueStore()
        {
            TimestampedKeyValueStore<string, string> store = Stores.timestampedKeyValueStoreBuilder(
                Stores.persistentTimestampedKeyValueStore("name"),
                Serdes.String(),
                Serdes.String()
            ).build();
            Assert.Equal(store, not(nullValue()));
        }

        [Xunit.Fact]
        public void shouldBuildTimestampedKeyValueStoreThatWrapsKeyValueStore()
        {
            TimestampedKeyValueStore<string, string> store = Stores.timestampedKeyValueStoreBuilder(
                Stores.persistentKeyValueStore("name"),
                Serdes.String(),
                Serdes.String()
            ).build();
            Assert.Equal(store, not(nullValue()));
        }

        [Xunit.Fact]
        public void shouldBuildTimestampedKeyValueStoreThatWrapsInMemoryKeyValueStore()
        {
            TimestampedKeyValueStore<string, string> store = Stores.timestampedKeyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("name"),
                Serdes.String(),
                Serdes.String()
            ).withLoggingDisabled().withCachingDisabled().build();
            Assert.Equal(store, not(nullValue()));
            Assert.Equal(((WrappedStateStore)store).wrapped(), instanceOf(TimestampedBytesStore));
        }

        [Xunit.Fact]
        public void shouldBuildWindowStore()
        {
            WindowStore<string, string> store = Stores.windowStoreBuilder(
                Stores.persistentWindowStore("store", ofMillis(3L), ofMillis(3L), true),
                Serdes.String(),
                Serdes.String()
            ).build();
            Assert.Equal(store, not(nullValue()));
        }

        [Xunit.Fact]
        public void shouldBuildTimestampedWindowStore()
        {
            TimestampedWindowStore<string, string> store = Stores.timestampedWindowStoreBuilder(
                Stores.persistentTimestampedWindowStore("store", ofMillis(3L), ofMillis(3L), true),
                Serdes.String(),
                Serdes.String()
            ).build();
            Assert.Equal(store, not(nullValue()));
        }

        [Xunit.Fact]
        public void shouldBuildTimestampedWindowStoreThatWrapsWindowStore()
        {
            TimestampedWindowStore<string, string> store = Stores.timestampedWindowStoreBuilder(
                Stores.persistentWindowStore("store", ofMillis(3L), ofMillis(3L), true),
                Serdes.String(),
                Serdes.String()
            ).build();
            Assert.Equal(store, not(nullValue()));
        }

        [Xunit.Fact]
        public void shouldBuildTimestampedWindowStoreThatWrapsInMemroyWindowStore()
        {
            TimestampedWindowStore<string, string> store = Stores.timestampedWindowStoreBuilder(
                Stores.inMemoryWindowStore("store", ofMillis(3L), ofMillis(3L), true),
                Serdes.String(),
                Serdes.String()
            ).withLoggingDisabled().withCachingDisabled().build();
            Assert.Equal(store, not(nullValue()));
            Assert.Equal(((WrappedStateStore)store).wrapped(), instanceOf(TimestampedBytesStore));
        }

        [Xunit.Fact]
        public void shouldBuildSessionStore()
        {
            SessionStore<string, string> store = Stores.sessionStoreBuilder(
                Stores.persistentSessionStore("name", ofMillis(10)),
                Serdes.String(),
                Serdes.String()
            ).build();
            Assert.Equal(store, not(nullValue()));
        }
    }
}
