using Kafka.Streams.Interfaces;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.TimeStamped;
using Kafka.Streams.State.Windowed;
using System;
using Xunit;

namespace Kafka.Streams.Tests.State
{
    public class StoresTest
    {
        private readonly StreamsBuilder streamsBuilder;
        private readonly IStoresFactory storesFactory;

        public StoresTest()
        {
            this.streamsBuilder = new StreamsBuilder();
            this.storesFactory = this.streamsBuilder.Context.StoresFactory;
        }

        [Fact]
        public void ShouldThrowIfPersistentKeyValueStoreStoreNameIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => this.storesFactory.PersistentKeyValueStore(null));
        }

        //[Fact]
        //public void ShouldThrowIfPersistentTimestampedKeyValueStoreStoreNameIsNull()
        //{
        //    Exception e = Assert.Throws<NullReferenceException>(() => this.storesFactory.PersistentTimestampedKeyValueStore(null));
        //    Assert.Equal("name cannot be null", e.ToString());
        //}

        //[Fact]
        //public void ShouldThrowIfIMemoryKeyValueStoreStoreNameIsNull()
        //{
        //    Exception e = Assert.Throws<NullReferenceException>(() => this.storesFactory.InMemoryKeyValueStore(null));
        //    Assert.Equal("name cannot be null", e.ToString());
        //}

        //[Fact]
        //public void ShouldThrowIfILruMapStoreNameIsNull()
        //{
        //    Exception e = Assert.Throws<NullReferenceException>(() => this.storesFactory.lruMap(null, 0));
        //    Assert.Equal("name cannot be null", e.ToString());
        //}

        //[Fact]
        //public void ShouldThrowIfILruMapStoreCapacityIsNegative()
        //{
        //    Exception e = Assert.Throws<ArgumentException>(() => this.storesFactory.lruMap("anyName", -1));
        //    Assert.Equal("maxCacheSize cannot be negative", e.ToString());
        //}

        [Fact]
        public void ShouldThrowIfIPersistentWindowStoreStoreNameIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => this.storesFactory.PersistentWindowStore(null, TimeSpan.Zero, TimeSpan.Zero, false));
        }

        [Fact]
        public void ShouldThrowIfPersistentTimestampedWindowStoreStoreNameIsNull()
        {
            Exception e = Assert.Throws<ArgumentNullException>(() => this.storesFactory.PersistentTimestampedWindowStore(null, TimeSpan.Zero, TimeSpan.Zero, false));
        }

        [Fact]
        public void ShouldThrowIfIPersistentWindowStoreRetentionPeriodIsNegative()
        {
            Exception e = Assert.Throws<ArgumentException>(() => this.storesFactory.PersistentWindowStore("anyName", TimeSpan.FromMilliseconds(-1L), TimeSpan.Zero, false));
        }

        [Fact]
        public void ShouldThrowIfIPersistentTimestampedWindowStoreRetentionPeriodIsNegative()
        {
            Exception e = Assert.Throws<ArgumentException>(() => this.storesFactory.PersistentTimestampedWindowStore("anyName", TimeSpan.FromMilliseconds(-1L), TimeSpan.Zero, false));
            Assert.Equal("retentionPeriod cannot be negative", e.Message);
        }

        [Obsolete]
        [Fact]
        public void ShouldThrowIfIPersistentWindowStoreIfNumberOfSegmentsSmallerThanOne()
        {
            Exception e = Assert.Throws<ArgumentException>(() => this.storesFactory.PersistentWindowStore("anyName", TimeSpan.Zero, 1, TimeSpan.Zero, false));
            Assert.Equal("numSegments cannot be smaller than 2", e.Message);
        }

        [Fact]
        public void ShouldThrowIfIPersistentWindowStoreIfWindowSizeIsNegative()
        {
            Exception e = Assert.Throws<ArgumentException>(() => this.storesFactory.PersistentWindowStore("anyName", TimeSpan.FromMilliseconds(0L), TimeSpan.FromMilliseconds(-1L), false));
            Assert.Equal("windowSize cannot be negative", e.Message);
        }

        [Fact]
        public void ShouldThrowIfIPersistentTimestampedWindowStoreIfWindowSizeIsNegative()
        {
            Exception e = Assert.Throws<ArgumentException>(() => this.storesFactory.PersistentTimestampedWindowStore("anyName", TimeSpan.FromMilliseconds(0L), TimeSpan.FromMilliseconds(-1L), false));
            Assert.Equal("windowSize cannot be negative", e.Message);
        }

        [Fact]
        public void ShouldThrowIfIPersistentSessionStoreStoreNameIsNull()
        {
            Exception e = Assert.Throws<ArgumentNullException>(() => this.storesFactory.PersistentSessionStore(null, 0));
        }

        //[Fact]
        //public void ShouldThrowIfIPersistentSessionStoreRetentionPeriodIsNegative()
        //{
        //    Exception e = Assert.Throws<ArgumentException>(() => this.storesFactory.PersistentSessionStore("anyName", TimeSpan.FromMilliseconds(-1)));
        //    Assert.Equal("retentionPeriod cannot be negative", e.Message);
        //}

        [Fact]
        public void ShouldThrowIfSupplierIsNullForWindowStoreBuilder()
        {
            Assert.Throws<ArgumentNullException>(() => this.storesFactory.WindowStoreBuilder(
                null,
                null,
                Serdes.ByteArray(),
                Serdes.ByteArray()));
        }

        [Fact]
        public void ShouldThrowIfSupplierIsNullForKeyValueStoreBuilder()
        {
            Assert.Throws<ArgumentNullException>(() => this.storesFactory.KeyValueStoreBuilder(null, null, Serdes.ByteArray(), Serdes.ByteArray()));
        }

        [Fact]
        public void ShouldThrowIfSupplierIsNullForSessionStoreBuilder()
        {
            Assert.Throws<ArgumentNullException>(() => this.storesFactory.SessionStoreBuilder(
                this.streamsBuilder.Context,
                null,
                Serdes.ByteArray(),
                Serdes.ByteArray()));
        }

        //[Fact]
        //public void ShouldCreateInMemoryKeyValueStore()
        //{
        //    Assert.IsAssignableFrom<IInMemoryKeyValueStore>(this.storesFactory.InMemoryKeyValueStore("memory").Get());
        //}

        //[Fact]
        //public void ShouldCreateMemoryNavigableCache()
        //{
        //    Assert.IsAssignableFrom<MemoryNavigableLRUCache>(this.storesFactory.lruMap("map", 10).Get());
        //}

        //[Fact]
        //public void ShouldCreateRocksDbStore()
        //{
        //    Assert.Equal(
        //        this.storesFactory.PersistentKeyValueStore("store").Get(),
        //        allOf(not(instanceOf(RocksDBTimestampedStore)), instanceOf(RocksDbStore)));
        //}

        //[Fact]
        //public void ShouldCreateRocksDbTimestampedStore()
        //{
        //    Assert.Equal(this.storesFactory.PersistentTimestampedKeyValueStore("store").Get(), instanceOf(RocksDBTimestampedStore));
        //}

        //[Fact]
        //public void ShouldCreateRocksDbWindowStore()
        //{
        //    IWindowStore store = this.storesFactory.PersistentWindowStore("store", FromMilliseconds(1L), FromMilliseconds(1L), false).Get();
        //    IStateStore wrapped = ((WrappedStateStore)store).wrapped();
        //    Assert.Equal(store, instanceOf(RocksDBWindowStore));
        //    Assert.Equal(wrapped, allOf(not(instanceOf(RocksDBTimestampedSegmentedBytesStore)), instanceOf(RocksDBSegmentedBytesStore)));
        //}

        //[Fact]
        //public void ShouldCreateRocksDbTimestampedWindowStore()
        //{
        //    IWindowStore store = this.storesFactory.PersistentTimestampedWindowStore("store", FromMilliseconds(1L), FromMilliseconds(1L), false).Get();
        //    IStateStore wrapped = ((WrappedStateStore)store).wrapped();
        //    Assert.Equal(store, instanceOf(RocksDBWindowStore));
        //    Assert.Equal(wrapped, instanceOf(RocksDBTimestampedSegmentedBytesStore));
        //}

        //[Fact]
        //public void ShouldCreateRocksDbSessionStore()
        //{
        //    Assert.Equal(this.storesFactory.PersistentSessionStore("store", FromMilliseconds(1)).Get(), instanceOf(RocksDBSessionStore));
        //}

        [Fact]
        public void ShouldBuildKeyValueStore()
        {
            IKeyValueStore<string, string> store = this.storesFactory.KeyValueStoreBuilder(
                this.streamsBuilder.Context,
                this.storesFactory.PersistentKeyValueStore("name"),
                Serdes.String(),
                Serdes.String())
            .Build();

            Assert.NotNull(store);
        }

        //[Fact]
        //public void ShouldBuildTimestampedKeyValueStore()
        //{
        //    ITimestampedKeyValueStore<string, string> store = this.storesFactory.TimestampedKeyValueStoreBuilder(
        //        null,
        //        this.storesFactory.PersistentTimestampedKeyValueStore("name"),
        //        Serdes.String(),
        //        Serdes.String()
        //    ).Build();
        //
        //    Assert.NotNull(store);
        //}

        [Fact]
        public void ShouldBuildTimestampedKeyValueStoreThatWrapsKeyValueStore()
        {
            ITimestampedKeyValueStore<string, string> store = this.storesFactory.TimestampedKeyValueStoreBuilder(
                this.streamsBuilder.Context,
                this.storesFactory.PersistentKeyValueStore("name"),
                Serdes.String(),
                Serdes.String()
            ).Build();
            Assert.NotNull(store);
        }

        // [Fact]
        // public void ShouldBuildTimestampedKeyValueStoreThatWrapsInMemoryKeyValueStore()
        // {
        //     ITimestampedKeyValueStore<string, string> store = this.storesFactory.TimestampedKeyValueStoreBuilder(
        //         null,
        //         this.storesFactory.InMemoryKeyValueStore("name"),
        //         Serdes.String(),
        //         Serdes.String()
        //     ).withLoggingDisabled().withCachingDisabled().Build();
        //     Assert.NotNull(store);
        // 
        //     Assert.IsAssignableFrom<ITimestampedBytesStore>(((WrappedStateStore)store).GetWrappedStateStore());
        // }

        [Fact]
        public void ShouldBuildWindowStore()
        {
            IWindowStore<string, string> store = this.storesFactory.WindowStoreBuilder(
                this.streamsBuilder.Context,
                this.storesFactory.PersistentWindowStore("store", TimeSpan.FromMilliseconds(3L), TimeSpan.FromMilliseconds(3L), true),
                Serdes.String(),
                Serdes.String()
            ).Build();
            Assert.NotNull(store);
        }

        [Fact]
        public void ShouldBuildTimestampedWindowStore()
        {
            ITimestampedWindowStore<string, string> store = this.storesFactory.TimestampedWindowStoreBuilder(
                this.streamsBuilder.Context,
                this.storesFactory.PersistentTimestampedWindowStore("store", TimeSpan.FromMilliseconds(3L), TimeSpan.FromMilliseconds(3L), true),
                Serdes.String(),
                Serdes.String()
            ).Build();
            Assert.NotNull(store);
        }

        [Fact]
        public void ShouldBuildTimestampedWindowStoreThatWrapsWindowStore()
        {
            ITimestampedWindowStore<string, string> store = this.storesFactory.TimestampedWindowStoreBuilder(
                this.streamsBuilder.Context,
                this.storesFactory.PersistentWindowStore("store", TimeSpan.FromMilliseconds(3L), TimeSpan.FromMilliseconds(3L), true),
                Serdes.String(),
                Serdes.String()
            ).Build();
            Assert.NotNull(store);
        }

        //[Fact]
        //public void ShouldBuildTimestampedWindowStoreThatWrapsInMemroyWindowStore()
        //{
        //    ITimestampedWindowStore<string, string> store = this.storesFactory.TimestampedWindowStoreBuilder(
        //        null,
        //        this.storesFactory.InMemoryWindowStore("store", TimeSpan.FromMilliseconds(3L), TimeSpan.FromMilliseconds(3L), true),
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
        //    ISessionStore<string, string> store = this.storesFactory.SessionStoreBuilder(
        //        this.storesFactory.PersistentSessionStore("name", TimeSpan.FromMilliseconds(10)),
        //        Serdes.String(),
        //        Serdes.String()
        //    ).Build();
        //
        //    Assert.NotNull(store);
        //}
    }
}
