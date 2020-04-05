//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */

























//    public class TimestampedKeyValueStoreBuilderTest
//    {

//        (type = MockType.NICE)
//    private IKeyValueBytesStoreSupplier supplier;
//        (type = MockType.NICE)
//    private RocksDBTimestampedStore inner;
//        private TimestampedKeyValueStoreBuilder<string, string> builder;


//        public void SetUp()
//        {
//            expect(supplier.Get()).andReturn(inner);
//            expect(supplier.name()).andReturn("name");
//            expect(inner.persistent()).andReturn(true).anyTimes();
//            replay(supplier, inner);

//            builder = new TimestampedKeyValueStoreBuilder<>(
//                supplier,
//                Serdes.String(),
//                Serdes.String(),
//                new MockTime()
//            );
//        }

//        [Fact]
//        public void ShouldHaveMeteredStoreAsOuterStore()
//        {
//            ITimestampedKeyValueStore<string, string> store = builder.Build();
//            Assert.Equal(store, instanceOf(MeteredTimestampedKeyValueStore));
//        }

//        [Fact]
//        public void ShouldHaveChangeLoggingStoreByDefault()
//        {
//            ITimestampedKeyValueStore<string, string> store = builder.Build();
//            Assert.Equal(store, instanceOf(MeteredTimestampedKeyValueStore));
//            IStateStore next = ((WrappedStateStore)store).wrapped();
//            Assert.Equal(next, instanceOf(ChangeLoggingTimestampedKeyValueBytesStore));
//        }

//        [Fact]
//        public void ShouldNotHaveChangeLoggingStoreWhenDisabled()
//        {
//            ITimestampedKeyValueStore<string, string> store = builder.withLoggingDisabled().Build();
//            IStateStore next = ((WrappedStateStore)store).wrapped();
//            Assert.Equal(next, CoreMatchers.equalTo(inner));
//        }

//        [Fact]
//        public void ShouldHaveCachingStoreWhenEnabled()
//        {
//            ITimestampedKeyValueStore<string, string> store = builder.withCachingEnabled().Build();
//            IStateStore wrapped = ((WrappedStateStore)store).wrapped();
//            Assert.Equal(store, instanceOf(MeteredTimestampedKeyValueStore));
//            Assert.Equal(wrapped, instanceOf(CachingKeyValueStore));
//        }

//        [Fact]
//        public void ShouldHaveChangeLoggingStoreWhenLoggingEnabled()
//        {
//            ITimestampedKeyValueStore<string, string> store = builder
//                    .withLoggingEnabled(Collections.emptyMap())
//                    .Build();
//            IStateStore wrapped = ((WrappedStateStore)store).wrapped();
//            Assert.Equal(store, instanceOf(MeteredTimestampedKeyValueStore));
//            Assert.Equal(wrapped, instanceOf(ChangeLoggingTimestampedKeyValueBytesStore));
//            Assert.Equal(((WrappedStateStore)wrapped).wrapped(), CoreMatchers.equalTo(inner));
//        }

//        [Fact]
//        public void ShouldHaveCachingAndChangeLoggingWhenBothEnabled()
//        {
//            ITimestampedKeyValueStore<string, string> store = builder
//                    .withLoggingEnabled(Collections.emptyMap())
//                    .withCachingEnabled()
//                    .Build();
//            WrappedStateStore caching = (WrappedStateStore)((WrappedStateStore)store).wrapped();
//            WrappedStateStore changeLogging = (WrappedStateStore)caching.wrapped();
//            Assert.Equal(store, instanceOf(MeteredTimestampedKeyValueStore));
//            Assert.Equal(caching, instanceOf(CachingKeyValueStore));
//            Assert.Equal(changeLogging, instanceOf(ChangeLoggingTimestampedKeyValueBytesStore));
//            Assert.Equal(changeLogging.wrapped(), CoreMatchers.equalTo(inner));
//        }

//        [Fact]
//        public void ShouldNotWrapTimestampedByteStore()
//        {
//            reset(supplier);
//            expect(supplier.Get()).andReturn(new RocksDBTimestampedStore("name"));
//            expect(supplier.name()).andReturn("name");
//            replay(supplier);

//            ITimestampedKeyValueStore<string, string> store = builder
//                .withLoggingDisabled()
//                .withCachingDisabled()
//                .Build();
//            Assert.Equal(((WrappedStateStore)store).wrapped(), instanceOf(RocksDBTimestampedStore));
//        }

//        [Fact]
//        public void ShouldWrapPlainKeyValueStoreAsTimestampStore()
//        {
//            reset(supplier);
//            expect(supplier.Get()).andReturn(new RocksDbStore("name"));
//            expect(supplier.name()).andReturn("name");
//            replay(supplier);

//            ITimestampedKeyValueStore<string, string> store = builder
//                .withLoggingDisabled()
//                .withCachingDisabled()
//                .Build();
//            Assert.Equal(((WrappedStateStore)store).wrapped(), instanceOf(KeyValueToTimestampedKeyValueByteStoreAdapter));
//        }


//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerIfInnerIsNull()
//        {
//            new TimestampedKeyValueStoreBuilder<>(null, Serdes.String(), Serdes.String(), new MockTime());
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerIfKeySerdeIsNull()
//        {
//            new TimestampedKeyValueStoreBuilder<>(supplier, null, Serdes.String(), new MockTime());
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerIfValueSerdeIsNull()
//        {
//            new TimestampedKeyValueStoreBuilder<>(supplier, Serdes.String(), null, new MockTime());
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerIfTimeIsNull()
//        {
//            new TimestampedKeyValueStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), null);
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerIfMetricsScopeIsNull()
//        {
//            new TimestampedKeyValueStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), new MockTime());
//        }

//    }
//}
///*






//*

//*





//*/

























