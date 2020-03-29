/*






 *

 *





 */
































public class TimestampedKeyValueStoreMaterializerTest {

    private string storePrefix = "prefix";
    @Mock(type = MockType.NICE)
    private InternalNameProvider nameProvider;

    [Xunit.Fact]
    public void ShouldCreateBuilderThatBuildsMeteredStoreWithCachingAndLoggingEnabled() {
        MaterializedInternal<string, string, KeyValueStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.As("store"), nameProvider, storePrefix);

        TimestampedKeyValueStoreMaterializer<string, string> materializer = new TimestampedKeyValueStoreMaterializer<>(materialized);
        StoreBuilder<TimestampedKeyValueStore<string, string>> builder = materializer.materialize();
        TimestampedKeyValueStore<string, string> store = builder.build();
        WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        StateStore logging = caching.wrapped();
        Assert.Equal(store, instanceOf(MeteredTimestampedKeyValueStore));
        Assert.Equal(caching, instanceOf(CachingKeyValueStore));
        Assert.Equal(logging, instanceOf(ChangeLoggingTimestampedKeyValueBytesStore));
    }

    [Xunit.Fact]
    public void ShouldCreateBuilderThatBuildsStoreWithCachingDisabled() {
        MaterializedInternal<string, string, KeyValueStore<Bytes, byte[]>> materialized = new MaterializedInternal<>(
            Materialized<string, string, KeyValueStore<Bytes, byte[]>>.As("store").withCachingDisabled(), nameProvider, storePrefix
        );
        TimestampedKeyValueStoreMaterializer<string, string> materializer = new TimestampedKeyValueStoreMaterializer<>(materialized);
        StoreBuilder<TimestampedKeyValueStore<string, string>> builder = materializer.materialize();
        TimestampedKeyValueStore<string, string> store = builder.build();
        WrappedStateStore logging = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        Assert.Equal(logging, instanceOf(ChangeLoggingKeyValueBytesStore));
    }

    [Xunit.Fact]
    public void ShouldCreateBuilderThatBuildsStoreWithLoggingDisabled() {
        MaterializedInternal<string, string, KeyValueStore<Bytes, byte[]>> materialized = new MaterializedInternal<>(
            Materialized<string, string, KeyValueStore<Bytes, byte[]>>.As("store").withLoggingDisabled(), nameProvider, storePrefix
        );
        TimestampedKeyValueStoreMaterializer<string, string> materializer = new TimestampedKeyValueStoreMaterializer<>(materialized);
        StoreBuilder<TimestampedKeyValueStore<string, string>> builder = materializer.materialize();
        TimestampedKeyValueStore<string, string> store = builder.build();
        WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        Assert.Equal(caching, instanceOf(CachingKeyValueStore));
        Assert.Equal(caching.wrapped(), not(instanceOf(ChangeLoggingKeyValueBytesStore)));
    }

    [Xunit.Fact]
    public void ShouldCreateBuilderThatBuildsStoreWithCachingAndLoggingDisabled() {
        MaterializedInternal<string, string, KeyValueStore<Bytes, byte[]>> materialized = new MaterializedInternal<>(
            Materialized<string, string, KeyValueStore<Bytes, byte[]>>.As("store").withCachingDisabled().withLoggingDisabled(), nameProvider, storePrefix
        );
        TimestampedKeyValueStoreMaterializer<string, string> materializer = new TimestampedKeyValueStoreMaterializer<>(materialized);
        StoreBuilder<TimestampedKeyValueStore<string, string>> builder = materializer.materialize();
        TimestampedKeyValueStore<string, string> store = builder.build();
        StateStore wrapped = ((WrappedStateStore) store).wrapped();
        Assert.Equal(wrapped, not(instanceOf(CachingKeyValueStore)));
        Assert.Equal(wrapped, not(instanceOf(ChangeLoggingKeyValueBytesStore)));
    }

    [Xunit.Fact]
    public void ShouldCreateKeyValueStoreWithTheProvidedInnerStore() {
        KeyValueBytesStoreSupplier supplier = EasyMock.createNiceMock(KeyValueBytesStoreSupplier);
        InMemoryKeyValueStore store = new InMemoryKeyValueStore("name");
        EasyMock.expect(supplier.name()).andReturn("name").anyTimes();
        EasyMock.expect(supplier.get()).andReturn(store);
        EasyMock.replay(supplier);

        MaterializedInternal<string, int, KeyValueStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.As(supplier), nameProvider, storePrefix);
        TimestampedKeyValueStoreMaterializer<string, int> materializer = new TimestampedKeyValueStoreMaterializer<>(materialized);
        StoreBuilder<TimestampedKeyValueStore<string, int>> builder = materializer.materialize();
        TimestampedKeyValueStore<string, int> built = builder.build();

        Assert.Equal(store.name(), CoreMatchers.equalTo(built.name()));
    }

}