/*






 *

 *





 */

























public class TimestampedKeyValueStoreBuilderTest {

    @Mock(type = MockType.NICE)
    private KeyValueBytesStoreSupplier supplier;
    @Mock(type = MockType.NICE)
    private RocksDBTimestampedStore inner;
    private TimestampedKeyValueStoreBuilder<string, string> builder;

    
    public void setUp() {
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
    public void shouldHaveMeteredStoreAsOuterStore() {
        TimestampedKeyValueStore<string, string> store = builder.build();
        Assert.Equal(store, instanceOf(MeteredTimestampedKeyValueStore));
    }

    [Xunit.Fact]
    public void shouldHaveChangeLoggingStoreByDefault() {
        TimestampedKeyValueStore<string, string> store = builder.build();
        Assert.Equal(store, instanceOf(MeteredTimestampedKeyValueStore));
        StateStore next = ((WrappedStateStore) store).wrapped();
        Assert.Equal(next, instanceOf(ChangeLoggingTimestampedKeyValueBytesStore));
    }

    [Xunit.Fact]
    public void shouldNotHaveChangeLoggingStoreWhenDisabled() {
        TimestampedKeyValueStore<string, string> store = builder.withLoggingDisabled().build();
        StateStore next = ((WrappedStateStore) store).wrapped();
        Assert.Equal(next, CoreMatchers.equalTo(inner));
    }

    [Xunit.Fact]
    public void shouldHaveCachingStoreWhenEnabled() {
        TimestampedKeyValueStore<string, string> store = builder.withCachingEnabled().build();
        StateStore wrapped = ((WrappedStateStore) store).wrapped();
        Assert.Equal(store, instanceOf(MeteredTimestampedKeyValueStore));
        Assert.Equal(wrapped, instanceOf(CachingKeyValueStore));
    }

    [Xunit.Fact]
    public void shouldHaveChangeLoggingStoreWhenLoggingEnabled() {
        TimestampedKeyValueStore<string, string> store = builder
                .withLoggingEnabled(Collections.emptyMap())
                .build();
        StateStore wrapped = ((WrappedStateStore) store).wrapped();
        Assert.Equal(store, instanceOf(MeteredTimestampedKeyValueStore));
        Assert.Equal(wrapped, instanceOf(ChangeLoggingTimestampedKeyValueBytesStore));
        Assert.Equal(((WrappedStateStore) wrapped).wrapped(), CoreMatchers.equalTo(inner));
    }

    [Xunit.Fact]
    public void shouldHaveCachingAndChangeLoggingWhenBothEnabled() {
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
    public void shouldNotWrapTimestampedByteStore() {
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
    public void shouldWrapPlainKeyValueStoreAsTimestampStore() {
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
    public void shouldThrowNullPointerIfInnerIsNull() {
        new TimestampedKeyValueStoreBuilder<>(null, Serdes.String(), Serdes.String(), new MockTime());
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerIfKeySerdeIsNull() {
        new TimestampedKeyValueStoreBuilder<>(supplier, null, Serdes.String(), new MockTime());
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerIfValueSerdeIsNull() {
        new TimestampedKeyValueStoreBuilder<>(supplier, Serdes.String(), null, new MockTime());
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerIfTimeIsNull() {
        new TimestampedKeyValueStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerIfMetricsScopeIsNull() {
        new TimestampedKeyValueStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), new MockTime());
    }

}