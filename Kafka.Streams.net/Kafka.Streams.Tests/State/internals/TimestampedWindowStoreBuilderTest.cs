//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */

























//    public class TimestampedWindowStoreBuilderTest
//    {

//        (type = MockType.NICE)
//    private WindowBytesStoreSupplier supplier;
//        (type = MockType.NICE)
//    private RocksDBTimestampedWindowStore inner;
//        private TimestampedWindowStoreBuilder<string, string> builder;


//        public void SetUp()
//        {
//            expect(supplier.Get()).andReturn(inner);
//            expect(supplier.name()).andReturn("name");
//            expect(inner.persistent()).andReturn(true).anyTimes();
//            replay(supplier, inner);

//            builder = new TimestampedWindowStoreBuilder<>(
//                supplier,
//                Serdes.String(),
//                Serdes.String(),
//                new MockTime());
//        }

//        [Xunit.Fact]
//        public void ShouldHaveMeteredStoreAsOuterStore()
//        {
//            ITimestampedWindowStore<string, string> store = builder.Build();
//            Assert.Equal(store, instanceOf(MeteredTimestampedWindowStore));
//        }

//        [Xunit.Fact]
//        public void ShouldHaveChangeLoggingStoreByDefault()
//        {
//            ITimestampedWindowStore<string, string> store = builder.Build();
//            IStateStore next = ((WrappedStateStore)store).wrapped();
//            Assert.Equal(next, instanceOf(ChangeLoggingTimestampedWindowBytesStore));
//        }

//        [Xunit.Fact]
//        public void ShouldNotHaveChangeLoggingStoreWhenDisabled()
//        {
//            ITimestampedWindowStore<string, string> store = builder.withLoggingDisabled().Build();
//            IStateStore next = ((WrappedStateStore)store).wrapped();
//            Assert.Equal(next, CoreMatchers.equalTo(inner));
//        }

//        [Xunit.Fact]
//        public void ShouldHaveCachingStoreWhenEnabled()
//        {
//            ITimestampedWindowStore<string, string> store = builder.withCachingEnabled().Build();
//            IStateStore wrapped = ((WrappedStateStore)store).wrapped();
//            Assert.Equal(store, instanceOf(MeteredTimestampedWindowStore));
//            Assert.Equal(wrapped, instanceOf(CachingWindowStore));
//        }

//        [Xunit.Fact]
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

//        [Xunit.Fact]
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

//        [Xunit.Fact]
//        public void ShouldNotWrapTimestampedByteStore()
//        {
//            reset(supplier);
//            expect(supplier.Get()).andReturn(new RocksDBTimestampedWindowStore(
//                new RocksDBTimestampedSegmentedBytesStore(
//                    "name",
//                    "metric-scope",
//                    10L,
//                    5L,
//                    new WindowKeySchema()),
//                false,
//                1L));
//            expect(supplier.name()).andReturn("name");
//            replay(supplier);

//            ITimestampedWindowStore<string, string> store = builder
//                .withLoggingDisabled()
//                .withCachingDisabled()
//                .Build();
//            Assert.Equal(((WrappedStateStore)store).wrapped(), instanceOf(RocksDBTimestampedWindowStore));
//        }

//        [Xunit.Fact]
//        public void ShouldWrapPlainKeyValueStoreAsTimestampStore()
//        {
//            reset(supplier);
//            expect(supplier.Get()).andReturn(new RocksDBWindowStore(
//                new RocksDBSegmentedBytesStore(
//                    "name",
//                    "metric-scope",
//                    10L,
//                    5L,
//                    new WindowKeySchema()),
//                false,
//                1L));
//            expect(supplier.name()).andReturn("name");
//            replay(supplier);

//            ITimestampedWindowStore<string, string> store = builder
//                .withLoggingDisabled()
//                .withCachingDisabled()
//                .Build();
//            Assert.Equal(((WrappedStateStore)store).wrapped(), instanceOf(WindowToTimestampedWindowByteStoreAdapter));
//        }


//        [Xunit.Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerIfInnerIsNull()
//        {
//            new TimestampedWindowStoreBuilder<>(null, Serdes.String(), Serdes.String(), new MockTime());
//        }

//        [Xunit.Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerIfKeySerdeIsNull()
//        {
//            new TimestampedWindowStoreBuilder<>(supplier, null, Serdes.String(), new MockTime());
//        }

//        [Xunit.Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerIfValueSerdeIsNull()
//        {
//            new TimestampedWindowStoreBuilder<>(supplier, Serdes.String(), null, new MockTime());
//        }

//        [Xunit.Fact]// (expected = NullPointerException)
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

























