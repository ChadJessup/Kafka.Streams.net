//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */

























//    public class TimestampedWindowStoreBuilderTest
//    {

//        
//    private WindowBytesStoreSupplier supplier;
//        
//    private RocksDBTimestampedWindowStore inner;
//        private TimestampedWindowStoreBuilder<string, string> builder;


//        public void SetUp()
//        {
//            expect(supplier.Get()).andReturn(inner);
//            expect(supplier.Name()).andReturn("Name");
//            expect(inner.Persistent()).andReturn(true).anyTimes();
//            replay(supplier, inner);

//            builder = new TimestampedWindowStoreBuilder<>(
//                supplier,
//                Serdes.String(),
//                Serdes.String(),
//                new MockTime());
//        }

//        [Fact]
//        public void ShouldHaveMeteredStoreAsOuterStore()
//        {
//            ITimestampedWindowStore<string, string> store = builder.Build();
//            Assert.Equal(store, instanceOf(MeteredTimestampedWindowStore));
//        }

//        [Fact]
//        public void ShouldHaveChangeLoggingStoreByDefault()
//        {
//            ITimestampedWindowStore<string, string> store = builder.Build();
//            IStateStore next = ((WrappedStateStore)store).wrapped();
//            Assert.Equal(next, instanceOf(ChangeLoggingTimestampedWindowBytesStore));
//        }

//        [Fact]
//        public void ShouldNotHaveChangeLoggingStoreWhenDisabled()
//        {
//            ITimestampedWindowStore<string, string> store = builder.WithLoggingDisabled().Build();
//            IStateStore next = ((WrappedStateStore)store).wrapped();
//            Assert.Equal(next, CoreMatchers.equalTo(inner));
//        }

//        [Fact]
//        public void ShouldHaveCachingStoreWhenEnabled()
//        {
//            ITimestampedWindowStore<string, string> store = builder.withCachingEnabled().Build();
//            IStateStore wrapped = ((WrappedStateStore)store).wrapped();
//            Assert.Equal(store, instanceOf(MeteredTimestampedWindowStore));
//            Assert.Equal(wrapped, instanceOf(CachingWindowStore));
//        }

//        [Fact]
//        public void ShouldHaveChangeLoggingStoreWhenLoggingEnabled()
//        {
//            ITimestampedWindowStore<string, string> store = builder
//                    .withLoggingEnabled(Collections.emptyMap())
//                    .Build();
//            IStateStore wrapped = ((WrappedStateStore)store).wrapped();
//            Assert.Equal(store, instanceOf(MeteredTimestampedWindowStore));
//            Assert.Equal(wrapped, instanceOf(ChangeLoggingTimestampedWindowBytesStore));
//            Assert.Equal(((WrappedStateStore)wrapped).wrapped(), CoreMatchers.equalTo(inner));
//        }

//        [Fact]
//        public void ShouldHaveCachingAndChangeLoggingWhenBothEnabled()
//        {
//            ITimestampedWindowStore<string, string> store = builder
//                    .withLoggingEnabled(Collections.emptyMap())
//                    .withCachingEnabled()
//                    .Build();
//            WrappedStateStore caching = (WrappedStateStore)((WrappedStateStore)store).wrapped();
//            WrappedStateStore changeLogging = (WrappedStateStore)caching.wrapped();
//            Assert.Equal(store, instanceOf(MeteredTimestampedWindowStore));
//            Assert.Equal(caching, instanceOf(CachingWindowStore));
//            Assert.Equal(changeLogging, instanceOf(ChangeLoggingTimestampedWindowBytesStore));
//            Assert.Equal(changeLogging.wrapped(), CoreMatchers.equalTo(inner));
//        }

//        [Fact]
//        public void ShouldNotWrapTimestampedByteStore()
//        {
//            reset(supplier);
//            expect(supplier.Get()).andReturn(new RocksDBTimestampedWindowStore(
//                new RocksDBTimestampedSegmentedBytesStore(
//                    "Name",
//                    "metric-scope",
//                    10L,
//                    5L,
//                    new WindowKeySchema()),
//                false,
//                1L));
//            expect(supplier.Name()).andReturn("Name");
//            replay(supplier);

//            ITimestampedWindowStore<string, string> store = builder
//                .WithLoggingDisabled()
//                .WithCachingDisabled()
//                .Build();
//            Assert.Equal(((WrappedStateStore)store).wrapped(), instanceOf(RocksDBTimestampedWindowStore));
//        }

//        [Fact]
//        public void ShouldWrapPlainKeyValueStoreAsTimestampStore()
//        {
//            reset(supplier);
//            expect(supplier.Get()).andReturn(new RocksDBWindowStore(
//                new RocksDBSegmentedBytesStore(
//                    "Name",
//                    "metric-scope",
//                    10L,
//                    5L,
//                    new WindowKeySchema()),
//                false,
//                1L));
//            expect(supplier.Name()).andReturn("Name");
//            replay(supplier);

//            ITimestampedWindowStore<string, string> store = builder
//                .WithLoggingDisabled()
//                .WithCachingDisabled()
//                .Build();
//            Assert.Equal(((WrappedStateStore)store).wrapped(), instanceOf(WindowToTimestampedWindowByteStoreAdapter));
//        }


//        [Fact]// (expected = NullReferenceException)
//        public void ShouldThrowNullPointerIfInnerIsNull()
//        {
//            new TimestampedWindowStoreBuilder<>(null, Serdes.String(), Serdes.String(), new MockTime());
//        }

//        [Fact]// (expected = NullReferenceException)
//        public void ShouldThrowNullPointerIfKeySerdeIsNull()
//        {
//            new TimestampedWindowStoreBuilder<>(supplier, null, Serdes.String(), new MockTime());
//        }

//        [Fact]// (expected = NullReferenceException)
//        public void ShouldThrowNullPointerIfValueSerdeIsNull()
//        {
//            new TimestampedWindowStoreBuilder<>(supplier, Serdes.String(), null, new MockTime());
//        }

//        [Fact]// (expected = NullReferenceException)
//        public void ShouldThrowNullPointerIfTimeIsNull()
//        {
//            new TimestampedWindowStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), null);
//        }

//    }
//}
///*






//*

//*





//*/

























