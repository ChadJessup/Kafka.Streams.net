/*






 *

 *





 */

























public class WindowStoreBuilderTest {

    @Mock(type = MockType.NICE)
    private WindowBytesStoreSupplier supplier;
    @Mock(type = MockType.NICE)
    private WindowStore<Bytes, byte[]> inner;
    private WindowStoreBuilder<string, string> builder;

    
    public void setUp() {
        expect(supplier.get()).andReturn(inner);
        expect(supplier.name()).andReturn("name");
        replay(supplier);

        builder = new WindowStoreBuilder<>(
            supplier,
            Serdes.String(),
            Serdes.String(),
            new MockTime());
    }

    [Xunit.Fact]
    public void shouldHaveMeteredStoreAsOuterStore() {
        WindowStore<string, string> store = builder.build();
        Assert.Equal(store, instanceOf(MeteredWindowStore));
    }

    [Xunit.Fact]
    public void shouldHaveChangeLoggingStoreByDefault() {
        WindowStore<string, string> store = builder.build();
        StateStore next = ((WrappedStateStore) store).wrapped();
        Assert.Equal(next, instanceOf(ChangeLoggingWindowBytesStore));
    }

    [Xunit.Fact]
    public void shouldNotHaveChangeLoggingStoreWhenDisabled() {
        WindowStore<string, string> store = builder.withLoggingDisabled().build();
        StateStore next = ((WrappedStateStore) store).wrapped();
        Assert.Equal(next, CoreMatchers.equalTo(inner));
    }

    [Xunit.Fact]
    public void shouldHaveCachingStoreWhenEnabled() {
        WindowStore<string, string> store = builder.withCachingEnabled().build();
        StateStore wrapped = ((WrappedStateStore) store).wrapped();
        Assert.Equal(store, instanceOf(MeteredWindowStore));
        Assert.Equal(wrapped, instanceOf(CachingWindowStore));
    }

    [Xunit.Fact]
    public void shouldHaveChangeLoggingStoreWhenLoggingEnabled() {
        WindowStore<string, string> store = builder
                .withLoggingEnabled(Collections.emptyMap())
                .build();
        StateStore wrapped = ((WrappedStateStore) store).wrapped();
        Assert.Equal(store, instanceOf(MeteredWindowStore));
        Assert.Equal(wrapped, instanceOf(ChangeLoggingWindowBytesStore));
        Assert.Equal(((WrappedStateStore) wrapped).wrapped(), CoreMatchers.equalTo(inner));
    }

    [Xunit.Fact]
    public void shouldHaveCachingAndChangeLoggingWhenBothEnabled() {
        WindowStore<string, string> store = builder
                .withLoggingEnabled(Collections.emptyMap())
                .withCachingEnabled()
                .build();
        WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        WrappedStateStore changeLogging = (WrappedStateStore) caching.wrapped();
        Assert.Equal(store, instanceOf(MeteredWindowStore));
        Assert.Equal(caching, instanceOf(CachingWindowStore));
        Assert.Equal(changeLogging, instanceOf(ChangeLoggingWindowBytesStore));
        Assert.Equal(changeLogging.wrapped(), CoreMatchers.equalTo(inner));
    }

    
    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerIfInnerIsNull() {
        new WindowStoreBuilder<>(null, Serdes.String(), Serdes.String(), new MockTime());
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerIfKeySerdeIsNull() {
        new WindowStoreBuilder<>(supplier, null, Serdes.String(), new MockTime());
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerIfValueSerdeIsNull() {
        new WindowStoreBuilder<>(supplier, Serdes.String(), null, new MockTime());
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerIfTimeIsNull() {
        new WindowStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), null);
    }

}