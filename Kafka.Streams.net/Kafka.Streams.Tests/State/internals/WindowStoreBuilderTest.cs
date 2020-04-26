//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */

























//    public class WindowStoreBuilderTest
//    {

//        
//    private WindowBytesStoreSupplier supplier;
//        
//    private IWindowStore<Bytes, byte[]> inner;
//        private WindowStoreBuilder<string, string> builder;


//        public void SetUp()
//        {
//            expect(supplier.Get()).andReturn(inner);
//            expect(supplier.Name()).andReturn("Name");
//            replay(supplier);

//            builder = new WindowStoreBuilder<>(
//                supplier,
//                Serdes.String(),
//                Serdes.String(),
//                new MockTime());
//        }

//        [Fact]
//        public void ShouldHaveMeteredStoreAsOuterStore()
//        {
//            IWindowStore<string, string> store = builder.Build();
//            Assert.Equal(store, instanceOf(MeteredWindowStore));
//        }

//        [Fact]
//        public void ShouldHaveChangeLoggingStoreByDefault()
//        {
//            IWindowStore<string, string> store = builder.Build();
//            IStateStore next = ((WrappedStateStore)store).wrapped();
//            Assert.Equal(next, instanceOf(ChangeLoggingWindowBytesStore));
//        }

//        [Fact]
//        public void ShouldNotHaveChangeLoggingStoreWhenDisabled()
//        {
//            IWindowStore<string, string> store = builder.WithLoggingDisabled().Build();
//            IStateStore next = ((WrappedStateStore)store).wrapped();
//            Assert.Equal(next, CoreMatchers.equalTo(inner));
//        }

//        [Fact]
//        public void ShouldHaveCachingStoreWhenEnabled()
//        {
//            IWindowStore<string, string> store = builder.withCachingEnabled().Build();
//            IStateStore wrapped = ((WrappedStateStore)store).wrapped();
//            Assert.Equal(store, instanceOf(MeteredWindowStore));
//            Assert.Equal(wrapped, instanceOf(CachingWindowStore));
//        }

//        [Fact]
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

//        [Fact]
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


//        [Fact]// (expected = NullReferenceException)
//        public void ShouldThrowNullPointerIfInnerIsNull()
//        {
//            new WindowStoreBuilder<>(null, Serdes.String(), Serdes.String(), new MockTime());
//        }

//        [Fact]// (expected = NullReferenceException)
//        public void ShouldThrowNullPointerIfKeySerdeIsNull()
//        {
//            new WindowStoreBuilder<>(supplier, null, Serdes.String(), new MockTime());
//        }

//        [Fact]// (expected = NullReferenceException)
//        public void ShouldThrowNullPointerIfValueSerdeIsNull()
//        {
//            new WindowStoreBuilder<>(supplier, Serdes.String(), null, new MockTime());
//        }

//        [Fact]// (expected = NullReferenceException)
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

























