using Confluent.Kafka;
using Xunit;
using System;
using Kafka.Streams.KStream;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State;
using Kafka.Streams.State.TimeStamped;
using Kafka.Streams.State.Windowed;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.Sessions;


namespace Kafka.Streams.Tests.State
{
    public class StoresTest
    {
        [Fact]
        public void ShouldThrowIfPersistentKeyValueStoreStoreNameIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => Stores.PersistentKeyValueStore(null));
        }

        //[Fact]
        //public void ShouldThrowIfPersistentTimestampedKeyValueStoreStoreNameIsNull()
        //{
        //    Exception e = Assert.Throws<NullReferenceException>(() => Stores.PersistentTimestampedKeyValueStore(null));
        //    Assert.Equal("name cannot be null", e.ToString());
        //}

        //[Fact]
        //public void ShouldThrowIfIMemoryKeyValueStoreStoreNameIsNull()
        //{
        //    Exception e = Assert.Throws<NullReferenceException>(() => Stores.InMemoryKeyValueStore(null));
        //    Assert.Equal("name cannot be null", e.ToString());
        //}

        //[Fact]
        //public void ShouldThrowIfILruMapStoreNameIsNull()
        //{
        //    Exception e = Assert.Throws<NullReferenceException>(() => Stores.lruMap(null, 0));
        //    Assert.Equal("name cannot be null", e.ToString());
        //}

        //[Fact]
        //public void ShouldThrowIfILruMapStoreCapacityIsNegative()
        //{
        //    Exception e = Assert.Throws<ArgumentException>(() => Stores.lruMap("anyName", -1));
        //    Assert.Equal("maxCacheSize cannot be negative", e.ToString());
        //}

        [Fact]
        public void ShouldThrowIfIPersistentWindowStoreStoreNameIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => Stores.PersistentWindowStore(null, TimeSpan.Zero, TimeSpan.Zero, false));
        }

        [Fact]
        public void ShouldThrowIfPersistentTimestampedWindowStoreStoreNameIsNull()
        {
            Exception e = Assert.Throws<ArgumentNullException>(() => Stores.PersistentTimestampedWindowStore(null, TimeSpan.Zero, TimeSpan.Zero, false));
        }

        [Fact]
        public void ShouldThrowIfIPersistentWindowStoreRetentionPeriodIsNegative()
        {
            Exception e = Assert.Throws<ArgumentException>(() => Stores.PersistentWindowStore("anyName", TimeSpan.FromMilliseconds(-1L), TimeSpan.Zero, false));
        }

        [Fact]
        public void ShouldThrowIfIPersistentTimestampedWindowStoreRetentionPeriodIsNegative()
        {
            Exception e = Assert.Throws<ArgumentException>(() => Stores.PersistentTimestampedWindowStore("anyName", TimeSpan.FromMilliseconds(-1L), TimeSpan.Zero, false));
            Assert.Equal("retentionPeriod cannot be negative", e.Message);
        }

        [Obsolete]
        [Fact]
        public void ShouldThrowIfIPersistentWindowStoreIfNumberOfSegmentsSmallerThanOne()
        {
            Exception e = Assert.Throws<ArgumentException>(() => Stores.PersistentWindowStore("anyName", TimeSpan.Zero, 1, TimeSpan.Zero, false));
            Assert.Equal("numSegments cannot be smaller than 2", e.Message);
        }

        [Fact]
        public void ShouldThrowIfIPersistentWindowStoreIfWindowSizeIsNegative()
        {
            Exception e = Assert.Throws<ArgumentException>(() => Stores.PersistentWindowStore("anyName", TimeSpan.FromMilliseconds(0L), TimeSpan.FromMilliseconds(-1L), false));
            Assert.Equal("windowSize cannot be negative", e.Message);
        }

        [Fact]
        public void ShouldThrowIfIPersistentTimestampedWindowStoreIfWindowSizeIsNegative()
        {
            Exception e = Assert.Throws<ArgumentException>(() => Stores.PersistentTimestampedWindowStore("anyName", TimeSpan.FromMilliseconds(0L), TimeSpan.FromMilliseconds(-1L), false));
            Assert.Equal("windowSize cannot be negative", e.Message);
        }

        [Fact]
        public void ShouldThrowIfIPersistentSessionStoreStoreNameIsNull()
        {
            Exception e = Assert.Throws<ArgumentNullException>(() => Stores.PersistentSessionStore(null, 0));
        }

        //[Fact]
        //public void ShouldThrowIfIPersistentSessionStoreRetentionPeriodIsNegative()
        //{
        //    Exception e = Assert.Throws<ArgumentException>(() => Stores.PersistentSessionStore("anyName", TimeSpan.FromMilliseconds(-1)));
        //    Assert.Equal("retentionPeriod cannot be negative", e.Message);
        //}

        [Fact]
        public void ShouldThrowIfSupplierIsNullForWindowStoreBuilder()
        {
            Assert.Throws<ArgumentNullException>(() => Stores.WindowStoreBuilder(null, null, Serdes.ByteArray(), Serdes.ByteArray()));
        }

        [Fact]
        public void ShouldThrowIfSupplierIsNullForKeyValueStoreBuilder()
        {
            Assert.Throws<ArgumentNullException>(() => Stores.KeyValueStoreBuilder(null, null, Serdes.ByteArray(), Serdes.ByteArray()));
        }

        [Fact]
        public void ShouldThrowIfSupplierIsNullForSessionStoreBuilder()
        {
            Assert.Throws<ArgumentNullException>(() => Stores.SessionStoreBuilder(null, Serdes.ByteArray(), Serdes.ByteArray()));
        }

        //[Fact]
        //public void ShouldCreateInMemoryKeyValueStore()
        //{
        //    Assert.IsAssignableFrom<IInMemoryKeyValueStore>(Stores.InMemoryKeyValueStore("memory").Get());
        //}

        //[Fact]
        //public void ShouldCreateMemoryNavigableCache()
        //{
        //    Assert.IsAssignableFrom<MemoryNavigableLRUCache>(Stores.lruMap("map", 10).Get());
        //}

        //[Fact]
        //public void ShouldCreateRocksDbStore()
        //{
        //    Assert.Equal(
        //        Stores.PersistentKeyValueStore("store").Get(),
        //        allOf(not(instanceOf(RocksDBTimestampedStore)), instanceOf(RocksDbStore)));
        //}

        //[Fact]
        //public void ShouldCreateRocksDbTimestampedStore()
        //{
        //    Assert.Equal(Stores.PersistentTimestampedKeyValueStore("store").Get(), instanceOf(RocksDBTimestampedStore));
        //}

        //[Fact]
        //public void ShouldCreateRocksDbWindowStore()
        //{
        //    IWindowStore store = Stores.PersistentWindowStore("store", FromMilliseconds(1L), FromMilliseconds(1L), false).Get();
        //    IStateStore wrapped = ((WrappedStateStore)store).wrapped();
        //    Assert.Equal(store, instanceOf(RocksDBWindowStore));
        //    Assert.Equal(wrapped, allOf(not(instanceOf(RocksDBTimestampedSegmentedBytesStore)), instanceOf(RocksDBSegmentedBytesStore)));
        //}

        //[Fact]
        //public void ShouldCreateRocksDbTimestampedWindowStore()
        //{
        //    IWindowStore store = Stores.PersistentTimestampedWindowStore("store", FromMilliseconds(1L), FromMilliseconds(1L), false).Get();
        //    IStateStore wrapped = ((WrappedStateStore)store).wrapped();
        //    Assert.Equal(store, instanceOf(RocksDBWindowStore));
        //    Assert.Equal(wrapped, instanceOf(RocksDBTimestampedSegmentedBytesStore));
        //}

        //[Fact]
        //public void ShouldCreateRocksDbSessionStore()
        //{
        //    Assert.Equal(Stores.PersistentSessionStore("store", FromMilliseconds(1)).Get(), instanceOf(RocksDBSessionStore));
        //}

        [Fact(Skip = "RocksDb")]
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

        //[Fact]
        //public void ShouldBuildTimestampedKeyValueStore()
        //{
        //    ITimestampedKeyValueStore<string, string> store = Stores.TimestampedKeyValueStoreBuilder(
        //        null,
        //        Stores.PersistentTimestampedKeyValueStore("name"),
        //        Serdes.String(),
        //        Serdes.String()
        //    ).Build();
        //
        //    Assert.NotNull(store);
        //}

        [Fact(Skip ="RocksDb")]
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

        // [Fact]
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

        [Fact(Skip = "RocksDb")]
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

        [Fact(Skip = "RocksDb")]
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

        [Fact(Skip = "RocksDb")]
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

        //[Fact]
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

        //[Fact]
        //public void ShouldBuildSessionStore()
        //{
        //    ISessionStore<string, string> store = Stores.SessionStoreBuilder(
        //        Stores.PersistentSessionStore("name", TimeSpan.FromMilliseconds(10)),
        //        Serdes.String(),
        //        Serdes.String()
        //    ).Build();
        //
        //    Assert.NotNull(store);
        //}
    }
}
