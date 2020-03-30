/*






 *

 *





 */

























public class TimestampedKeyValueStoreBuilderTest {

    (type = MockType.NICE)
    private KeyValueBytesStoreSupplier supplier;
    (type = MockType.NICE)
    private RocksDBTimestampedStore inner;
    private TimestampedKeyValueStoreBuilder<string, string> builder;

    
    public void SetUp() {
        expect(supplier.get()).andReturn(inner);
        expect(supplier.name()).andReturn("name");
        expect(inner.persistent()).andReturn(true).anyTimes();
        replay(supplier, inner);

        builder = new TimestampedKeyValueStoreBuilder<>(
            supplier,
            Serdes.String(),
            Serdes.String(),
            new MockTime()
        );
    }

    [Xunit.Fact]
    public void ShouldHaveMeteredStoreAsOuterStore() {
        TimestampedKeyValueStore<string, string> store = builder.build();
        Assert.Equal(store, instanceOf(MeteredTimestampedKeyValueStore));
    }

    [Xunit.Fact]
    public void ShouldHaveChangeLoggingStoreByDefault() {
        TimestampedKeyValueStore<string, string> store = builder.build();
        Assert.Equal(store, instanceOf(MeteredTimestampedKeyValueStore));
        StateStore next = ((WrappedStateStore) store).wrapped();
        Assert.Equal(next, instanceOf(ChangeLoggingTimestampedKeyValueBytesStore));
    }

    [Xunit.Fact]
    public void ShouldNotHaveChangeLoggingStoreWhenDisabled() {
        TimestampedKeyValueStore<string, string> store = builder.withLoggingDisabled().build();
        StateStore next = ((WrappedStateStore) store).wrapped();
        Assert.Equal(next, CoreMatchers.equalTo(inner));
    }

    [Xunit.Fact]
    public void ShouldHaveCachingStoreWhenEnabled() {
        TimestampedKeyValueStore<string, string> store = builder.withCachingEnabled().build();
        StateStore wrapped = ((WrappedStateStore) store).wrapped();
        Assert.Equal(store, instanceOf(MeteredTimestampedKeyValueStore));
        Assert.Equal(wrapped, instanceOf(CachingKeyValueStore));
    }

    [Xunit.Fact]
    public void ShouldHaveChangeLoggingStoreWhenLoggingEnabled() {
        TimestampedKeyValueStore<string, string> store = builder
                .withLoggingEnabled(Collections.emptyMap())
                .build();
        StateStore wrapped = ((WrappedStateStore) store).wrapped();
        Assert.Equal(store, instanceOf(MeteredTimestampedKeyValueStore));
        Assert.Equal(wrapped, instanceOf(ChangeLoggingTimestampedKeyValueBytesStore));
        Assert.Equal(((WrappedStateStore) wrapped).wrapped(), CoreMatchers.equalTo(inner));
    }

    [Xunit.Fact]
    public void ShouldHaveCachingAndChangeLoggingWhenBothEnabled() {
        TimestampedKeyValueStore<string, string> store = builder
                .withLoggingEnabled(Collections.emptyMap())
                .withCachingEnabled()
                .build();
        WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        WrappedStateStore changeLogging = (WrappedStateStore) caching.wrapped();
        Assert.Equal(store, instanceOf(MeteredTimestampedKeyValueStore));
        Assert.Equal(caching, instanceOf(CachingKeyValueStore));
        Assert.Equal(changeLogging, instanceOf(ChangeLoggingTimestampedKeyValueBytesStore));
        Assert.Equal(changeLogging.wrapped(), CoreMatchers.equalTo(inner));
    }

    [Xunit.Fact]
    public void ShouldNotWrapTimestampedByteStore() {
        reset(supplier);
        expect(supplier.get()).andReturn(new RocksDBTimestampedStore("name"));
        expect(supplier.name()).andReturn("name");
        replay(supplier);

        TimestampedKeyValueStore<string, string> store = builder
            .withLoggingDisabled()
            .withCachingDisabled()
            .build();
        Assert.Equal(((WrappedStateStore) store).wrapped(), instanceOf(RocksDBTimestampedStore));
    }

    [Xunit.Fact]
    public void ShouldWrapPlainKeyValueStoreAsTimestampStore() {
        reset(supplier);
        expect(supplier.get()).andReturn(new RocksDBStore("name"));
        expect(supplier.name()).andReturn("name");
        replay(supplier);

        TimestampedKeyValueStore<string, string> store = builder
            .withLoggingDisabled()
            .withCachingDisabled()
            .build();
        Assert.Equal(((WrappedStateStore) store).wrapped(), instanceOf(KeyValueToTimestampedKeyValueByteStoreAdapter));
    }

    
    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerIfInnerIsNull() {
        new TimestampedKeyValueStoreBuilder<>(null, Serdes.String(), Serdes.String(), new MockTime());
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerIfKeySerdeIsNull() {
        new TimestampedKeyValueStoreBuilder<>(supplier, null, Serdes.String(), new MockTime());
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerIfValueSerdeIsNull() {
        new TimestampedKeyValueStoreBuilder<>(supplier, Serdes.String(), null, new MockTime());
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerIfTimeIsNull() {
        new TimestampedKeyValueStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerIfMetricsScopeIsNull() {
        new TimestampedKeyValueStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), new MockTime());
    }

}