/*






 *

 *





 */
























public class KeyValueStoreBuilderTest {

    @Mock(type = MockType.NICE)
    private KeyValueBytesStoreSupplier supplier;
    @Mock(type = MockType.NICE)
    private KeyValueStore<Bytes, byte[]> inner;
    private KeyValueStoreBuilder<string, string> builder;

    
    public void setUp() {
        EasyMock.expect(supplier.get()).andReturn(inner);
        EasyMock.expect(supplier.name()).andReturn("name");
        EasyMock.replay(supplier);
        builder = new KeyValueStoreBuilder<>(
            supplier,
            Serdes.String(),
            Serdes.String(),
            new MockTime()
        );
    }

    [Xunit.Fact]
    public void shouldHaveMeteredStoreAsOuterStore() {
        KeyValueStore<string, string> store = builder.build();
        Assert.Equal(store, instanceOf(MeteredKeyValueStore));
    }

    [Xunit.Fact]
    public void shouldHaveChangeLoggingStoreByDefault() {
        KeyValueStore<string, string> store = builder.build();
        Assert.Equal(store, instanceOf(MeteredKeyValueStore));
        StateStore next = ((WrappedStateStore) store).wrapped();
        Assert.Equal(next, instanceOf(ChangeLoggingKeyValueBytesStore));
    }

    [Xunit.Fact]
    public void shouldNotHaveChangeLoggingStoreWhenDisabled() {
        KeyValueStore<string, string> store = builder.withLoggingDisabled().build();
        StateStore next = ((WrappedStateStore) store).wrapped();
        Assert.Equal(next, CoreMatchers.equalTo(inner));
    }

    [Xunit.Fact]
    public void shouldHaveCachingStoreWhenEnabled() {
        KeyValueStore<string, string> store = builder.withCachingEnabled().build();
        StateStore wrapped = ((WrappedStateStore) store).wrapped();
        Assert.Equal(store, instanceOf(MeteredKeyValueStore));
        Assert.Equal(wrapped, instanceOf(CachingKeyValueStore));
    }

    [Xunit.Fact]
    public void shouldHaveChangeLoggingStoreWhenLoggingEnabled() {
        KeyValueStore<string, string> store = builder
                .withLoggingEnabled(Collections.emptyMap())
                .build();
        StateStore wrapped = ((WrappedStateStore) store).wrapped();
        Assert.Equal(store, instanceOf(MeteredKeyValueStore));
        Assert.Equal(wrapped, instanceOf(ChangeLoggingKeyValueBytesStore));
        Assert.Equal(((WrappedStateStore) wrapped).wrapped(), CoreMatchers.equalTo(inner));
    }

    [Xunit.Fact]
    public void shouldHaveCachingAndChangeLoggingWhenBothEnabled() {
        KeyValueStore<string, string> store = builder
                .withLoggingEnabled(Collections.emptyMap())
                .withCachingEnabled()
                .build();
        WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        WrappedStateStore changeLogging = (WrappedStateStore) caching.wrapped();
        Assert.Equal(store, instanceOf(MeteredKeyValueStore));
        Assert.Equal(caching, instanceOf(CachingKeyValueStore));
        Assert.Equal(changeLogging, instanceOf(ChangeLoggingKeyValueBytesStore));
        Assert.Equal(changeLogging.wrapped(), CoreMatchers.equalTo(inner));
    }

    
    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerIfInnerIsNull() {
        new KeyValueStoreBuilder<>(null, Serdes.String(), Serdes.String(), new MockTime());
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerIfKeySerdeIsNull() {
        new KeyValueStoreBuilder<>(supplier, null, Serdes.String(), new MockTime());
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerIfValueSerdeIsNull() {
        new KeyValueStoreBuilder<>(supplier, Serdes.String(), null, new MockTime());
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerIfTimeIsNull() {
        new KeyValueStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerIfMetricsScopeIsNull() {
        new KeyValueStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), new MockTime());
    }

}