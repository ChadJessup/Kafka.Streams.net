using Confluent.Kafka;
using Xunit;
using System;
using Kafka.Streams.KStream;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State;
using Kafka.Streams.State.TimeStamped;
using Kafka.Streams.State.Window;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.Sessions;
using NodaTime;

namespace Kafka.Streams.Tests.State
{
    public class StoresTest
    {

        [Xunit.Fact]
        public void ShouldThrowIfPersistentKeyValueStoreStoreNameIsNull()
        {
            Exception e = Assert.Throws<NullReferenceException>(() => Stores.PersistentKeyValueStore(null));
            Assert.Equal("name cannot be null", e.ToString());
        }

        [Xunit.Fact]
        public void ShouldThrowIfPersistentTimestampedKeyValueStoreStoreNameIsNull()
        {
            Exception e = Assert.Throws<NullReferenceException>(() => Stores.PersistentTimestampedKeyValueStore(null));
            Assert.Equal("name cannot be null", e.ToString());
        }

        //[Xunit.Fact]
        //public void ShouldThrowIfIMemoryKeyValueStoreStoreNameIsNull()
        //{
        //    Exception e = Assert.Throws<NullReferenceException>(() => Stores.InMemoryKeyValueStore(null));
        //    Assert.Equal("name cannot be null", e.ToString());
        //}

        //[Xunit.Fact]
        //public void ShouldThrowIfILruMapStoreNameIsNull()
        //{
        //    Exception e = Assert.Throws<NullReferenceException>(() => Stores.lruMap(null, 0));
        //    Assert.Equal("name cannot be null", e.ToString());
        //}

        //[Xunit.Fact]
        //public void ShouldThrowIfILruMapStoreCapacityIsNegative()
        //{
        //    Exception e = Assert.Throws<ArgumentException>(() => Stores.lruMap("anyName", -1));
        //    Assert.Equal("maxCacheSize cannot be negative", e.ToString());
        //}

        [Xunit.Fact]
        public void ShouldThrowIfIPersistentWindowStoreStoreNameIsNull()
        {
            Exception e = Assert.Throws<NullReferenceException>(() => Stores.PersistentWindowStore(null, TimeSpan.Zero, TimeSpan.Zero, false));
            Assert.Equal("name cannot be null", e.ToString());
        }

        [Xunit.Fact]
        public void ShouldThrowIfIPersistentTimestampedWindowStoreStoreNameIsNull()
        {
            Exception e = Assert.Throws<NullReferenceException>(() => Stores.PersistentTimestampedWindowStore(null, TimeSpan.Zero, TimeSpan.Zero, false));
            Assert.Equal("name cannot be null", e.ToString());
        }

        [Xunit.Fact]
        public void ShouldThrowIfIPersistentWindowStoreRetentionPeriodIsNegative()
        {
            Exception e = Assert.Throws<ArgumentException>(() => Stores.PersistentWindowStore("anyName", TimeSpan.FromMilliseconds(-1L), TimeSpan.Zero, false));
            Assert.Equal("retentionPeriod cannot be negative", e.ToString());
        }

        [Xunit.Fact]
        public void ShouldThrowIfIPersistentTimestampedWindowStoreRetentionPeriodIsNegative()
        {
            Exception e = Assert.Throws<ArgumentException>(() => Stores.PersistentTimestampedWindowStore("anyName", TimeSpan.FromMilliseconds(-1L), TimeSpan.Zero, false));
            Assert.Equal("retentionPeriod cannot be negative", e.ToString());
        }

        [Obsolete]
        [Xunit.Fact]
        public void ShouldThrowIfIPersistentWindowStoreIfNumberOfSegmentsSmallerThanOne()
        {
            Exception e = Assert.Throws<ArgumentException>(() => Stores.PersistentWindowStore("anyName", TimeSpan.Zero, 1, TimeSpan.Zero, false));
            Assert.Equal("numSegments cannot be smaller than 2", e.ToString());
        }

        [Xunit.Fact]
        public void ShouldThrowIfIPersistentWindowStoreIfWindowSizeIsNegative()
        {
            Exception e = Assert.Throws<ArgumentException>(() => Stores.PersistentWindowStore("anyName", TimeSpan.FromMilliseconds(0L), TimeSpan.FromMilliseconds(-1L), false));
            Assert.Equal("windowSize cannot be negative", e.ToString());
        }

        [Xunit.Fact]
        public void ShouldThrowIfIPersistentTimestampedWindowStoreIfWindowSizeIsNegative()
        {
            Exception e = Assert.Throws<ArgumentException>(() => Stores.PersistentTimestampedWindowStore("anyName", TimeSpan.FromMilliseconds(0L), TimeSpan.FromMilliseconds(-1L), false));
            Assert.Equal("windowSize cannot be negative", e.ToString());
        }

        [Xunit.Fact]
        public void ShouldThrowIfIPersistentSessionStoreStoreNameIsNull()
        {
            Exception e = Assert.Throws<NullReferenceException>(() => Stores.PersistentSessionStore(null, 0));
            Assert.Equal("name cannot be null", e.ToString());
        }

        [Xunit.Fact]
        public void ShouldThrowIfIPersistentSessionStoreRetentionPeriodIsNegative()
        {
            Exception e = Assert.Throws<ArgumentException>(() => Stores.PersistentSessionStore("anyName", TimeSpan.FromMilliseconds(-1)));
            Assert.Equal("retentionPeriod cannot be negative", e.ToString());
        }

        [Xunit.Fact]
        public void ShouldThrowIfSupplierIsNullForWindowStoreBuilder()
        {
            Exception e = Assert.Throws<NullReferenceException>(() => Stores.WindowStoreBuilder(null, null, Serdes.ByteArray(), Serdes.ByteArray()));
            Assert.Equal("supplier cannot be null", e.ToString());
        }

        [Xunit.Fact]
        public void ShouldThrowIfSupplierIsNullForKeyValueStoreBuilder()
        {
            Exception e = Assert.Throws<NullReferenceException>(() => Stores.KeyValueStoreBuilder(null, null, Serdes.ByteArray(), Serdes.ByteArray()));
            Assert.Equal("supplier cannot be null", e.ToString());
        }

        [Xunit.Fact]
        public void ShouldThrowIfSupplierIsNullForSessionStoreBuilder()
        {
            Exception e = Assert.Throws<NullReferenceException>(() => Stores.SessionStoreBuilder(null, Serdes.ByteArray(), Serdes.ByteArray()));
            Assert.Equal("supplier cannot be null", e.ToString());
        }

        //[Xunit.Fact]
        //public void ShouldCreateInMemoryKeyValueStore()
        //{
        //    Assert.IsAssignableFrom<IInMemoryKeyValueStore>(Stores.InMemoryKeyValueStore("memory").Get());
        //}

        //[Xunit.Fact]
        //public void ShouldCreateMemoryNavigableCache()
        //{
        //    Assert.IsAssignableFrom<MemoryNavigableLRUCache>(Stores.lruMap("map", 10).Get());
        //}

        //[Xunit.Fact]
        //public void ShouldCreateRocksDbStore()
        //{
        //    Assert.Equal(
        //        Stores.PersistentKeyValueStore("store").Get(),
        //        allOf(not(instanceOf(RocksDBTimestampedStore)), instanceOf(RocksDbStore)));
        //}

        //[Xunit.Fact]
        //public void ShouldCreateRocksDbTimestampedStore()
        //{
        //    Assert.Equal(Stores.PersistentTimestampedKeyValueStore("store").Get(), instanceOf(RocksDBTimestampedStore));
        //}

        //[Xunit.Fact]
        //public void ShouldCreateRocksDbWindowStore()
        //{
        //    IWindowStore store = Stores.PersistentWindowStore("store", FromMilliseconds(1L), FromMilliseconds(1L), false).Get();
        //    IStateStore wrapped = ((WrappedStateStore)store).wrapped();
        //    Assert.Equal(store, instanceOf(RocksDBWindowStore));
        //    Assert.Equal(wrapped, allOf(not(instanceOf(RocksDBTimestampedSegmentedBytesStore)), instanceOf(RocksDBSegmentedBytesStore)));
        //}

        //[Xunit.Fact]
        //public void ShouldCreateRocksDbTimestampedWindowStore()
        //{
        //    IWindowStore store = Stores.PersistentTimestampedWindowStore("store", FromMilliseconds(1L), FromMilliseconds(1L), false).Get();
        //    IStateStore wrapped = ((WrappedStateStore)store).wrapped();
        //    Assert.Equal(store, instanceOf(RocksDBWindowStore));
        //    Assert.Equal(wrapped, instanceOf(RocksDBTimestampedSegmentedBytesStore));
        //}

        //[Xunit.Fact]
        //public void ShouldCreateRocksDbSessionStore()
        //{
        //    Assert.Equal(Stores.PersistentSessionStore("store", FromMilliseconds(1)).Get(), instanceOf(RocksDBSessionStore));
        //}

        [Xunit.Fact]
        public void ShouldBuildKeyValueStore()
        {
            IKeyValueStore<string, string> store = Stores.KeyValueStoreBuilder(
                null,
                Stores.PersistentKeyValueStore("name"),
                Serdes.String(),
                Serdes.String()
            ).Build();

            Assert.NotNull(store);
        }

        [Xunit.Fact]
        public void ShouldBuildTimestampedKeyValueStore()
        {
            ITimestampedKeyValueStore<string, string> store = Stores.TimestampedKeyValueStoreBuilder(
                null,
                Stores.PersistentTimestampedKeyValueStore("name"),
                Serdes.String(),
                Serdes.String()
            ).Build();

            Assert.NotNull(store);
        }

        [Xunit.Fact]
        public void ShouldBuildTimestampedKeyValueStoreThatWrapsKeyValueStore()
        {
            ITimestampedKeyValueStore<string, string> store = Stores.TimestampedKeyValueStoreBuilder(
                null,
                Stores.PersistentKeyValueStore("name"),
                Serdes.String(),
                Serdes.String()
            ).Build();
            Assert.NotNull(store);
        }

        // [Xunit.Fact]
        // public void ShouldBuildTimestampedKeyValueStoreThatWrapsInMemoryKeyValueStore()
        // {
        //     ITimestampedKeyValueStore<string, string> store = Stores.TimestampedKeyValueStoreBuilder(
        //         null,
        //         Stores.InMemoryKeyValueStore("name"),
        //         Serdes.String(),
        //         Serdes.String()
        //     ).withLoggingDisabled().withCachingDisabled().Build();
        //     Assert.NotNull(store);
        // 
        //     Assert.IsAssignableFrom<ITimestampedBytesStore>(((WrappedStateStore)store).GetWrappedStateStore());
        // }

        [Xunit.Fact]
        public void ShouldBuildWindowStore()
        {
            IWindowStore<string, string> store = Stores.WindowStoreBuilder(
                null,
                Stores.PersistentWindowStore("store", TimeSpan.FromMilliseconds(3L), TimeSpan.FromMilliseconds(3L), true),
                Serdes.String(),
                Serdes.String()
            ).Build();
            Assert.NotNull(store);
        }

        [Xunit.Fact]
        public void ShouldBuildTimestampedWindowStore()
        {
            ITimestampedWindowStore<string, string> store = Stores.TimestampedWindowStoreBuilder(
                null,
                Stores.PersistentTimestampedWindowStore("store", TimeSpan.FromMilliseconds(3L), TimeSpan.FromMilliseconds(3L), true),
                Serdes.String(),
                Serdes.String()
            ).Build();
            Assert.NotNull(store);
        }

        [Xunit.Fact]
        public void ShouldBuildTimestampedWindowStoreThatWrapsWindowStore()
        {
            ITimestampedWindowStore<string, string> store = Stores.TimestampedWindowStoreBuilder(
                null,
                Stores.PersistentWindowStore("store", TimeSpan.FromMilliseconds(3L), TimeSpan.FromMilliseconds(3L), true),
                Serdes.String(),
                Serdes.String()
            ).Build();
            Assert.NotNull(store);
        }

        //[Xunit.Fact]
        //public void ShouldBuildTimestampedWindowStoreThatWrapsInMemroyWindowStore()
        //{
        //    ITimestampedWindowStore<string, string> store = Stores.TimestampedWindowStoreBuilder(
        //        null,
        //        Stores.InMemoryWindowStore("store", TimeSpan.FromMilliseconds(3L), TimeSpan.FromMilliseconds(3L), true),
        //        Serdes.String(),
        //        Serdes.String())
        //        .withLoggingDisabled()
        //        .withCachingDisabled()
        //        .Build();
        //
        //    Assert.NotNull(store);
        //    Assert.IsAssignableFrom<ITimestampedBytesStore>(((WrappedStateStore)store).GetWrappedStateStore());
        //}

        [Xunit.Fact]
        public void ShouldBuildSessionStore()
        {
            ISessionStore<string, string> store = Stores.SessionStoreBuilder(
                Stores.PersistentSessionStore("name", TimeSpan.FromMilliseconds(10)),
                Serdes.String(),
                Serdes.String()
            ).Build();
            Assert.NotNull(store);
        }
    }
}
