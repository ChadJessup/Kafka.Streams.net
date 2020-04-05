//namespace Kafka.Streams.Tests.Processor.Internals
//{
//    /*






//    *

//    *





//    */
































//    public class TimestampedKeyValueStoreMaterializerTest
//    {

//        private readonly string storePrefix = "prefix";
//        (type = MockType.NICE)
//    private InternalNameProvider nameProvider;

//        [Xunit.Fact]
//        public void ShouldCreateBuilderThatBuildsMeteredStoreWithCachingAndLoggingEnabled()
//        {
//            MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>> materialized =
//                new MaterializedInternal<>(Materialized.As("store"), nameProvider, storePrefix);

//            TimestampedKeyValueStoreMaterializer<string, string> materializer = new TimestampedKeyValueStoreMaterializer<>(materialized);
//            IStoreBuilder<ITimestampedKeyValueStore<string, string>> builder = materializer.materialize();
//            ITimestampedKeyValueStore<string, string> store = builder.Build();
//            WrappedStateStore caching = (WrappedStateStore)((WrappedStateStore)store).wrapped();
//            IStateStore logging = caching.wrapped();
//            Assert.Equal(store, instanceOf(MeteredTimestampedKeyValueStore));
//            Assert.Equal(caching, instanceOf(CachingKeyValueStore));
//            Assert.Equal(logging, instanceOf(ChangeLoggingTimestampedKeyValueBytesStore));
//        }

//        [Xunit.Fact]
//        public void ShouldCreateBuilderThatBuildsStoreWithCachingDisabled()
//        {
//            MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>> materialized = new MaterializedInternal<>(
//                Materialized<string, string, IKeyValueStore<Bytes, byte[]>>.As("store").withCachingDisabled(), nameProvider, storePrefix
//            );
//            TimestampedKeyValueStoreMaterializer<string, string> materializer = new TimestampedKeyValueStoreMaterializer<>(materialized);
//            IStoreBuilder<ITimestampedKeyValueStore<string, string>> builder = materializer.materialize();
//            ITimestampedKeyValueStore<string, string> store = builder.Build();
//            WrappedStateStore logging = (WrappedStateStore)((WrappedStateStore)store).wrapped();
//            Assert.Equal(logging, instanceOf(ChangeLoggingKeyValueBytesStore));
//        }

//        [Xunit.Fact]
//        public void ShouldCreateBuilderThatBuildsStoreWithLoggingDisabled()
//        {
//            MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>> materialized = new MaterializedInternal<>(
//                Materialized<string, string, IKeyValueStore<Bytes, byte[]>>.As("store").withLoggingDisabled(), nameProvider, storePrefix
//            );
//            TimestampedKeyValueStoreMaterializer<string, string> materializer = new TimestampedKeyValueStoreMaterializer<>(materialized);
//            IStoreBuilder<ITimestampedKeyValueStore<string, string>> builder = materializer.materialize();
//            ITimestampedKeyValueStore<string, string> store = builder.Build();
//            WrappedStateStore caching = (WrappedStateStore)((WrappedStateStore)store).wrapped();
//            Assert.Equal(caching, instanceOf(CachingKeyValueStore));
//            Assert.Equal(caching.wrapped(), not(instanceOf(ChangeLoggingKeyValueBytesStore)));
//        }

//        [Xunit.Fact]
//        public void ShouldCreateBuilderThatBuildsStoreWithCachingAndLoggingDisabled()
//        {
//            MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>> materialized = new MaterializedInternal<>(
//                Materialized<string, string, IKeyValueStore<Bytes, byte[]>>.As("store").withCachingDisabled().withLoggingDisabled(), nameProvider, storePrefix
//            );
//            TimestampedKeyValueStoreMaterializer<string, string> materializer = new TimestampedKeyValueStoreMaterializer<>(materialized);
//            IStoreBuilder<ITimestampedKeyValueStore<string, string>> builder = materializer.materialize();
//            ITimestampedKeyValueStore<string, string> store = builder.Build();
//            IStateStore wrapped = ((WrappedStateStore)store).wrapped();
//            Assert.Equal(wrapped, not(instanceOf(CachingKeyValueStore)));
//            Assert.Equal(wrapped, not(instanceOf(ChangeLoggingKeyValueBytesStore)));
//        }

//        [Xunit.Fact]
//        public void ShouldCreateKeyValueStoreWithTheProvidedInnerStore()
//        {
//            IKeyValueBytesStoreSupplier supplier = EasyMock.createNiceMock(IKeyValueBytesStoreSupplier);
//            InMemoryKeyValueStore store = new InMemoryKeyValueStore("name");
//            EasyMock.expect(supplier.name()).andReturn("name").anyTimes();
//            EasyMock.expect(supplier.Get()).andReturn(store);
//            EasyMock.replay(supplier);

//            MaterializedInternal<string, int, IKeyValueStore<Bytes, byte[]>> materialized =
//                new MaterializedInternal<>(Materialized.As(supplier), nameProvider, storePrefix);
//            TimestampedKeyValueStoreMaterializer<string, int> materializer = new TimestampedKeyValueStoreMaterializer<>(materialized);
//            IStoreBuilder<ITimestampedKeyValueStore<string, int>> builder = materializer.materialize();
//            ITimestampedKeyValueStore<string, int> built = builder.Build();

//            Assert.Equal(store.name(), CoreMatchers.equalTo(built.name()));
//        }

//    }
//}
///*






//*

//*





//*/
































