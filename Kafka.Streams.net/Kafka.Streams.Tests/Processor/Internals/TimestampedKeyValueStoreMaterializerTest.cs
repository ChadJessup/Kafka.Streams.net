using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Metered;
using Kafka.Streams.State.TimeStamped;
using Moq;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class TimestampedKeyValueStoreMaterializerTest
    {
        private readonly string storePrefix = "prefix";

        private IInternalNameProvider nameProvider;

        [Fact]
        public void ShouldCreateBuilderThatBuildsMeteredStoreWithCachingAndLoggingEnabled()
        {
            MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>> materialized =
                new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>("store"), nameProvider, storePrefix);

            TimestampedKeyValueStoreMaterializer<string, string> materializer = new TimestampedKeyValueStoreMaterializer<string, string>(materialized);
            IStoreBuilder<ITimestampedKeyValueStore<string, string>> builder = materializer.Materialize();
            ITimestampedKeyValueStore<string, string> store = builder.Build();
            WrappedStateStore caching = (WrappedStateStore)((WrappedStateStore)store).GetWrappedStateStore();
            IStateStore logging = caching.GetWrappedStateStore();
            Assert.IsAssignableFrom<MeteredTimestampedKeyValueStore<string, string>>(store);
            Assert.IsAssignableFrom<CachingKeyValueStore>(caching);
            Assert.IsAssignableFrom<ChangeLoggingTimestampedKeyValueBytesStore>(logging);
        }

        [Fact]
        public void ShouldCreateBuilderThatBuildsStoreWithCachingDisabled()
        {
            MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>> materialized = new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(
                Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>("store").WithCachingDisabled(), nameProvider, storePrefix);

            TimestampedKeyValueStoreMaterializer<string, string> materializer = new TimestampedKeyValueStoreMaterializer<string, string>(materialized);
            IStoreBuilder<ITimestampedKeyValueStore<string, string>> builder = materializer.Materialize();
            ITimestampedKeyValueStore<string, string> store = builder.Build();
            WrappedStateStore logging = (WrappedStateStore)((WrappedStateStore)store).wrapped();
            Assert.IsAssignableFrom<ChangeLoggingKeyValueBytesStore>(logging);
        }

        [Fact]
        public void ShouldCreateBuilderThatBuildsStoreWithLoggingDisabled()
        {
            MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>> materialized = new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(
                Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>("store").WithLoggingDisabled(), nameProvider, storePrefix);

            TimestampedKeyValueStoreMaterializer<string, string> materializer = new TimestampedKeyValueStoreMaterializer<string, string>(materialized);
            IStoreBuilder<ITimestampedKeyValueStore<string, string>> builder = materializer.Materialize();
            ITimestampedKeyValueStore<string, string> store = builder.Build();
            WrappedStateStore caching = (WrappedStateStore)((WrappedStateStore)store).GetWrappedStateStore();
            Assert.IsAssignableFrom<CachingKeyValueStore>(caching);
            Assert.IsNotType<ChangeLoggingKeyValueBytesStore>(caching.GetWrappedStateStore());
        }

        [Fact]
        public void ShouldCreateBuilderThatBuildsStoreWithCachingAndLoggingDisabled()
        {
            MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>> materialized = new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(
                Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>("store").WithCachingDisabled().WithLoggingDisabled(), nameProvider, storePrefix);

            TimestampedKeyValueStoreMaterializer<string, string> materializer = new TimestampedKeyValueStoreMaterializer<string, string>(materialized);
            IStoreBuilder<ITimestampedKeyValueStore<string, string>> builder = materializer.materialize();
            ITimestampedKeyValueStore<string, string> store = builder.Build();
            IStateStore wrapped = ((WrappedStateStore)store).GetWrappedStateStore();
            Assert.IsNotType<CachingKeyValueStore>(wrapped);
            Assert.IsNotType<ChangeLoggingKeyValueBytesStore>(wrapped);
        }

        [Fact]
        public void ShouldCreateKeyValueStoreWithTheProvidedInnerStore()
        {
            IKeyValueBytesStoreSupplier supplier = Mock.Of<IKeyValueBytesStoreSupplier>();
            InMemoryKeyValueStore store = new InMemoryKeyValueStore("Name");
            EasyMock.expect(supplier.Name).andReturn("Name").anyTimes();
            EasyMock.expect(supplier.Get()).andReturn(store);
            EasyMock.replay(supplier);

            MaterializedInternal<string, int, IKeyValueStore<Bytes, byte[]>> materialized =
                new MaterializedInternal<string, int, IKeyValueStore<Bytes, byte[]>>(Materialized.As<string, int, IKeyValueStore<Bytes, byte[]>>(supplier), nameProvider, storePrefix);
            TimestampedKeyValueStoreMaterializer<string, int> materializer = new TimestampedKeyValueStoreMaterializer<string, int>(materialized);
            IStoreBuilder<ITimestampedKeyValueStore<string, int>> builder = materializer.Materialize();
            ITimestampedKeyValueStore<string, int> built = builder.Build();

            Assert.Equal(store.Name(), CoreMatchers.equalTo(built.Name));
        }
    }
}
