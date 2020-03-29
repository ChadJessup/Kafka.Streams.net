/*






 *

 *





 */

























public class TimestampedWindowStoreBuilderTest {

    @Mock(type = MockType.NICE)
    private WindowBytesStoreSupplier supplier;
    @Mock(type = MockType.NICE)
    private RocksDBTimestampedWindowStore inner;
    private TimestampedWindowStoreBuilder<string, string> builder;

    
    public void setUp() {
        expect(supplier.get()).andReturn(inner);
        expect(supplier.name()).andReturn("name");
        expect(inner.persistent()).andReturn(true).anyTimes();
        replay(supplier, inner);

        builder = new TimestampedWindowStoreBuilder<>(
            supplier,
            Serdes.String(),
            Serdes.String(),
            new MockTime());
    }

    [Xunit.Fact]
    public void shouldHaveMeteredStoreAsOuterStore() {
        TimestampedWindowStore<string, string> store = builder.build();
        Assert.Equal(store, instanceOf(MeteredTimestampedWindowStore));
    }

    [Xunit.Fact]
    public void shouldHaveChangeLoggingStoreByDefault() {
        TimestampedWindowStore<string, string> store = builder.build();
        StateStore next = ((WrappedStateStore) store).wrapped();
        Assert.Equal(next, instanceOf(ChangeLoggingTimestampedWindowBytesStore));
    }

    [Xunit.Fact]
    public void shouldNotHaveChangeLoggingStoreWhenDisabled() {
        TimestampedWindowStore<string, string> store = builder.withLoggingDisabled().build();
        StateStore next = ((WrappedStateStore) store).wrapped();
        Assert.Equal(next, CoreMatchers.equalTo(inner));
    }

    [Xunit.Fact]
    public void shouldHaveCachingStoreWhenEnabled() {
        TimestampedWindowStore<string, string> store = builder.withCachingEnabled().build();
        StateStore wrapped = ((WrappedStateStore) store).wrapped();
        Assert.Equal(store, instanceOf(MeteredTimestampedWindowStore));
        Assert.Equal(wrapped, instanceOf(CachingWindowStore));
    }

    [Xunit.Fact]
    public void shouldHaveChangeLoggingStoreWhenLoggingEnabled() {
        TimestampedWindowStore<string, string> store = builder
                .withLoggingEnabled(Collections.emptyMap())
                .build();
        StateStore wrapped = ((WrappedStateStore) store).wrapped();
        Assert.Equal(store, instanceOf(MeteredTimestampedWindowStore));
        Assert.Equal(wrapped, instanceOf(ChangeLoggingTimestampedWindowBytesStore));
        Assert.Equal(((WrappedStateStore) wrapped).wrapped(), CoreMatchers.equalTo(inner));
    }

    [Xunit.Fact]
    public void shouldHaveCachingAndChangeLoggingWhenBothEnabled() {
        TimestampedWindowStore<string, string> store = builder
                .withLoggingEnabled(Collections.emptyMap())
                .withCachingEnabled()
                .build();
        WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        WrappedStateStore changeLogging = (WrappedStateStore) caching.wrapped();
        Assert.Equal(store, instanceOf(MeteredTimestampedWindowStore));
        Assert.Equal(caching, instanceOf(CachingWindowStore));
        Assert.Equal(changeLogging, instanceOf(ChangeLoggingTimestampedWindowBytesStore));
        Assert.Equal(changeLogging.wrapped(), CoreMatchers.equalTo(inner));
    }

    [Xunit.Fact]
    public void shouldNotWrapTimestampedByteStore() {
        reset(supplier);
        expect(supplier.get()).andReturn(new RocksDBTimestampedWindowStore(
            new RocksDBTimestampedSegmentedBytesStore(
                "name",
                "metric-scope",
                10L,
                5L,
                new WindowKeySchema()),
            false,
            1L));
        expect(supplier.name()).andReturn("name");
        replay(supplier);

        TimestampedWindowStore<string, string> store = builder
            .withLoggingDisabled()
            .withCachingDisabled()
            .build();
        Assert.Equal(((WrappedStateStore) store).wrapped(), instanceOf(RocksDBTimestampedWindowStore));
    }

    [Xunit.Fact]
    public void shouldWrapPlainKeyValueStoreAsTimestampStore() {
        reset(supplier);
        expect(supplier.get()).andReturn(new RocksDBWindowStore(
            new RocksDBSegmentedBytesStore(
                "name",
                "metric-scope",
                10L,
                5L,
                new WindowKeySchema()),
            false,
            1L));
        expect(supplier.name()).andReturn("name");
        replay(supplier);

        TimestampedWindowStore<string, string> store = builder
            .withLoggingDisabled()
            .withCachingDisabled()
            .build();
        Assert.Equal(((WrappedStateStore) store).wrapped(), instanceOf(WindowToTimestampedWindowByteStoreAdapter));
    }

    
    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerIfInnerIsNull() {
        new TimestampedWindowStoreBuilder<>(null, Serdes.String(), Serdes.String(), new MockTime());
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerIfKeySerdeIsNull() {
        new TimestampedWindowStoreBuilder<>(supplier, null, Serdes.String(), new MockTime());
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerIfValueSerdeIsNull() {
        new TimestampedWindowStoreBuilder<>(supplier, Serdes.String(), null, new MockTime());
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerIfTimeIsNull() {
        new TimestampedWindowStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), null);
    }

}