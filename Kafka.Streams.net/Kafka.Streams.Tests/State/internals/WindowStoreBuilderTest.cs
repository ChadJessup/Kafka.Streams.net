//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */

























//    public class WindowStoreBuilderTest
//    {

//        (type = MockType.NICE)
//    private WindowBytesStoreSupplier supplier;
//        (type = MockType.NICE)
//    private IWindowStore<Bytes, byte[]> inner;
//        private WindowStoreBuilder<string, string> builder;


//        public void SetUp()
//        {
//            expect(supplier.Get()).andReturn(inner);
//            expect(supplier.name()).andReturn("name");
//            replay(supplier);

//            builder = new WindowStoreBuilder<>(
//                supplier,
//                Serdes.String(),
//                Serdes.String(),
//                new MockTime());
//        }

//        [Xunit.Fact]
//        public void ShouldHaveMeteredStoreAsOuterStore()
//        {
//            IWindowStore<string, string> store = builder.Build();
//            Assert.Equal(store, instanceOf(MeteredWindowStore));
//        }

//        [Xunit.Fact]
//        public void ShouldHaveChangeLoggingStoreByDefault()
//        {
//            IWindowStore<string, string> store = builder.Build();
//            IStateStore next = ((WrappedStateStore)store).wrapped();
//            Assert.Equal(next, instanceOf(ChangeLoggingWindowBytesStore));
//        }

//        [Xunit.Fact]
//        public void ShouldNotHaveChangeLoggingStoreWhenDisabled()
//        {
//            IWindowStore<string, string> store = builder.withLoggingDisabled().Build();
//            IStateStore next = ((WrappedStateStore)store).wrapped();
//            Assert.Equal(next, CoreMatchers.equalTo(inner));
//        }

//        [Xunit.Fact]
//        public void ShouldHaveCachingStoreWhenEnabled()
//        {
//            IWindowStore<string, string> store = builder.withCachingEnabled().Build();
//            IStateStore wrapped = ((WrappedStateStore)store).wrapped();
//            Assert.Equal(store, instanceOf(MeteredWindowStore));
//            Assert.Equal(wrapped, instanceOf(CachingWindowStore));
//        }

//        [Xunit.Fact]
//        public void ShouldHaveChangeLoggingStoreWhenLoggingEnabled()
//        {
//            IWindowStore<string, string> store = builder
//                    .withLoggingEnabled(Collections.emptyMap())
//                    .Build();
//            IStateStore wrapped = ((WrappedStateStore)store).wrapped();
//            Assert.Equal(store, instanceOf(MeteredWindowStore));
//            Assert.Equal(wrapped, instanceOf(ChangeLoggingWindowBytesStore));
//            Assert.Equal(((WrappedStateStore)wrapped).wrapped(), CoreMatchers.equalTo(inner));
//        }

//        [Xunit.Fact]
//        public void ShouldHaveCachingAndChangeLoggingWhenBothEnabled()
//        {
//            IWindowStore<string, string> store = builder
//                    .withLoggingEnabled(Collections.emptyMap())
//                    .withCachingEnabled()
//                    .Build();
//            WrappedStateStore caching = (WrappedStateStore)((WrappedStateStore)store).wrapped();
//            WrappedStateStore changeLogging = (WrappedStateStore)caching.wrapped();
//            Assert.Equal(store, instanceOf(MeteredWindowStore));
//            Assert.Equal(caching, instanceOf(CachingWindowStore));
//            Assert.Equal(changeLogging, instanceOf(ChangeLoggingWindowBytesStore));
//            Assert.Equal(changeLogging.wrapped(), CoreMatchers.equalTo(inner));
//        }


//        [Xunit.Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerIfInnerIsNull()
//        {
//            new WindowStoreBuilder<>(null, Serdes.String(), Serdes.String(), new MockTime());
//        }

//        [Xunit.Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerIfKeySerdeIsNull()
//        {
//            new WindowStoreBuilder<>(supplier, null, Serdes.String(), new MockTime());
//        }

//        [Xunit.Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerIfValueSerdeIsNull()
//        {
//            new WindowStoreBuilder<>(supplier, Serdes.String(), null, new MockTime());
//        }

//        [Xunit.Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerIfTimeIsNull()
//        {
//            new WindowStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), null);
//        }

//    }
//}
///*






//*

//*





//*/

























