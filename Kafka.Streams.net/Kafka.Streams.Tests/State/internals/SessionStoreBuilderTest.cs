//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */




























//    public class SessionStoreBuilderTest
//    {

//        (type = MockType.NICE)
//    private SessionBytesStoreSupplier supplier;
//        (type = MockType.NICE)
//    private ISessionStore<Bytes, byte[]> inner;
//        private SessionStoreBuilder<string, string> builder;


//        public void SetUp()
//        {// throws Exception

//            expect(supplier.Get()).andReturn(inner);
//            expect(supplier.Name()).andReturn("Name");
//            replay(supplier);

//            builder = new SessionStoreBuilder<>(
//                supplier,
//                Serdes.String(),
//                Serdes.String(),
//                new MockTime());
//        }

//        [Fact]
//        public void ShouldHaveMeteredStoreAsOuterStore()
//        {
//            ISessionStore<string, string> store = builder.Build();
//            Assert.Equal(store, instanceOf(MeteredSessionStore));
//        }

//        [Fact]
//        public void ShouldHaveChangeLoggingStoreByDefault()
//        {
//            ISessionStore<string, string> store = builder.Build();
//            IStateStore next = ((WrappedStateStore)store).wrapped();
//            Assert.Equal(next, instanceOf(ChangeLoggingSessionBytesStore));
//        }

//        [Fact]
//        public void ShouldNotHaveChangeLoggingStoreWhenDisabled()
//        {
//            ISessionStore<string, string> store = builder.WithLoggingDisabled().Build();
//            IStateStore next = ((WrappedStateStore)store).wrapped();
//            Assert.Equal(next, CoreMatchers.< IStateStore > equalTo(inner));
//        }

//        [Fact]
//        public void ShouldHaveCachingStoreWhenEnabled()
//        {
//            ISessionStore<string, string> store = builder.withCachingEnabled().Build();
//            IStateStore wrapped = ((WrappedStateStore)store).wrapped();
//            Assert.Equal(store, instanceOf(MeteredSessionStore));
//            Assert.Equal(wrapped, instanceOf(CachingSessionStore));
//        }

//        [Fact]
//        public void ShouldHaveChangeLoggingStoreWhenLoggingEnabled()
//        {
//            ISessionStore<string, string> store = builder
//                    .withLoggingEnabled(Collections.< string, string > emptyMap())
//                    .Build();
//            IStateStore wrapped = ((WrappedStateStore)store).wrapped();
//            Assert.Equal(store, instanceOf(MeteredSessionStore));
//            Assert.Equal(wrapped, instanceOf(ChangeLoggingSessionBytesStore));
//            Assert.Equal(((WrappedStateStore)wrapped).wrapped(), CoreMatchers.< IStateStore > equalTo(inner));
//        }

//        [Fact]
//        public void ShouldHaveCachingAndChangeLoggingWhenBothEnabled()
//        {
//            ISessionStore<string, string> store = builder
//                    .withLoggingEnabled(Collections.< string, string > emptyMap())
//                    .withCachingEnabled()
//                    .Build();
//            WrappedStateStore caching = (WrappedStateStore)((WrappedStateStore)store).wrapped();
//            WrappedStateStore changeLogging = (WrappedStateStore)caching.wrapped();
//            Assert.Equal(store, instanceOf(MeteredSessionStore));
//            Assert.Equal(caching, instanceOf(CachingSessionStore));
//            Assert.Equal(changeLogging, instanceOf(ChangeLoggingSessionBytesStore));
//            Assert.Equal(changeLogging.wrapped(), CoreMatchers.< IStateStore > equalTo(inner));
//        }

//        [Fact]
//        public void ShouldThrowNullPointerIfInnerIsNull()
//        {
//            Exception e = Assert.Throws<NullReferenceException>(() => new SessionStoreBuilder<>(null, Serdes.String(), Serdes.String(), new MockTime()));
//            Assert.Equal(e.ToString(), ("supplier cannot be null"));
//        }

//        [Fact]
//        public void ShouldThrowNullPointerIfKeySerdeIsNull()
//        {
//            Exception e = Assert.Throws<NullReferenceException>(() => new SessionStoreBuilder<>(supplier, null, Serdes.String(), new MockTime()));
//            Assert.Equal(e.ToString(), ("Name cannot be null"));
//        }

//        [Fact]
//        public void ShouldThrowNullPointerIfValueSerdeIsNull()
//        {
//            Exception e = Assert.Throws<NullReferenceException>(() => new SessionStoreBuilder<>(supplier, Serdes.String(), null, new MockTime()));
//            Assert.Equal(e.ToString(), ("Name cannot be null"));
//        }

//        [Fact]
//        public void ShouldThrowNullPointerIfTimeIsNull()
//        {
//            reset(supplier);
//            expect(supplier.Name()).andReturn("Name");
//            replay(supplier);
//            Exception e = Assert.Throws<NullReferenceException>(() => new SessionStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), null));
//            Assert.Equal(e.ToString(), ("time cannot be null"));
//        }

//        [Fact]
//        public void ShouldThrowNullPointerIfMetricsScopeIsNull()
//        {
//            Exception e = Assert.Throws<NullReferenceException>(() => new SessionStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), new MockTime()));
//            Assert.Equal(e.ToString(), ("Name cannot be null"));
//        }

//    }
//}
///*






//*

//*





//*/




























