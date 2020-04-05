//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */
























//    public class KeyValueStoreBuilderTest
//    {

//        (type = MockType.NICE)
//    private IKeyValueBytesStoreSupplier supplier;
//        (type = MockType.NICE)
//    private IKeyValueStore<Bytes, byte[]> inner;
//        private KeyValueStoreBuilder<string, string> builder;


//        public void SetUp()
//        {
//            EasyMock.expect(supplier.Get()).andReturn(inner);
//            EasyMock.expect(supplier.name()).andReturn("name");
//            EasyMock.replay(supplier);
//            builder = new KeyValueStoreBuilder<>(
//                supplier,
//                Serdes.String(),
//                Serdes.String(),
//                new MockTime()
//            );
//        }

//        [Xunit.Fact]
//        public void ShouldHaveMeteredStoreAsOuterStore()
//        {
//            IKeyValueStore<string, string> store = builder.Build();
//            Assert.Equal(store, instanceOf(MeteredKeyValueStore));
//        }

//        [Xunit.Fact]
//        public void ShouldHaveChangeLoggingStoreByDefault()
//        {
//            IKeyValueStore<string, string> store = builder.Build();
//            Assert.Equal(store, instanceOf(MeteredKeyValueStore));
//            IStateStore next = ((WrappedStateStore)store).wrapped();
//            Assert.Equal(next, instanceOf(ChangeLoggingKeyValueBytesStore));
//        }

//        [Xunit.Fact]
//        public void ShouldNotHaveChangeLoggingStoreWhenDisabled()
//        {
//            IKeyValueStore<string, string> store = builder.withLoggingDisabled().Build();
//            IStateStore next = ((WrappedStateStore)store).wrapped();
//            Assert.Equal(next, CoreMatchers.equalTo(inner));
//        }

//        [Xunit.Fact]
//        public void ShouldHaveCachingStoreWhenEnabled()
//        {
//            IKeyValueStore<string, string> store = builder.withCachingEnabled().Build();
//            IStateStore wrapped = ((WrappedStateStore)store).wrapped();
//            Assert.Equal(store, instanceOf(MeteredKeyValueStore));
//            Assert.Equal(wrapped, instanceOf(CachingKeyValueStore));
//        }

//        [Xunit.Fact]
//        public void ShouldHaveChangeLoggingStoreWhenLoggingEnabled()
//        {
//            IKeyValueStore<string, string> store = builder
//                    .withLoggingEnabled(Collections.emptyMap())
//                    .Build();
//            IStateStore wrapped = ((WrappedStateStore)store).wrapped();
//            Assert.Equal(store, instanceOf(MeteredKeyValueStore));
//            Assert.Equal(wrapped, instanceOf(ChangeLoggingKeyValueBytesStore));
//            Assert.Equal(((WrappedStateStore)wrapped).wrapped(), CoreMatchers.equalTo(inner));
//        }

//        [Xunit.Fact]
//        public void ShouldHaveCachingAndChangeLoggingWhenBothEnabled()
//        {
//            IKeyValueStore<string, string> store = builder
//                    .withLoggingEnabled(Collections.emptyMap())
//                    .withCachingEnabled()
//                    .Build();
//            WrappedStateStore caching = (WrappedStateStore)((WrappedStateStore)store).wrapped();
//            WrappedStateStore changeLogging = (WrappedStateStore)caching.wrapped();
//            Assert.Equal(store, instanceOf(MeteredKeyValueStore));
//            Assert.Equal(caching, instanceOf(CachingKeyValueStore));
//            Assert.Equal(changeLogging, instanceOf(ChangeLoggingKeyValueBytesStore));
//            Assert.Equal(changeLogging.wrapped(), CoreMatchers.equalTo(inner));
//        }


//        [Xunit.Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerIfInnerIsNull()
//        {
//            new KeyValueStoreBuilder<>(null, Serdes.String(), Serdes.String(), new MockTime());
//        }

//        [Xunit.Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerIfKeySerdeIsNull()
//        {
//            new KeyValueStoreBuilder<>(supplier, null, Serdes.String(), new MockTime());
//        }

//        [Xunit.Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerIfValueSerdeIsNull()
//        {
//            new KeyValueStoreBuilder<>(supplier, Serdes.String(), null, new MockTime());
//        }

//        [Xunit.Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerIfTimeIsNull()
//        {
//            new KeyValueStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), null);
//        }

//        [Xunit.Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerIfMetricsScopeIsNull()
//        {
//            new KeyValueStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), new MockTime());
//        }

//    }
//}
///*






//*

//*





//*/
























