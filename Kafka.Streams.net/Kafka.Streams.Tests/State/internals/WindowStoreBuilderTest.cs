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

    
    public void SetUp() {
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
    public void ShouldHaveMeteredStoreAsOuterStore() {
        WindowStore<string, string> store = builder.build();
        Assert.Equal(store, instanceOf(MeteredWindowStore));
    }

    [Xunit.Fact]
    public void ShouldHaveChangeLoggingStoreByDefault() {
        WindowStore<string, string> store = builder.build();
        StateStore next = ((WrappedStateStore) store).wrapped();
        Assert.Equal(next, instanceOf(ChangeLoggingWindowBytesStore));
    }

    [Xunit.Fact]
    public void ShouldNotHaveChangeLoggingStoreWhenDisabled() {
        WindowStore<string, string> store = builder.withLoggingDisabled().build();
        StateStore next = ((WrappedStateStore) store).wrapped();
        Assert.Equal(next, CoreMatchers.equalTo(inner));
    }

    [Xunit.Fact]
    public void ShouldHaveCachingStoreWhenEnabled() {
        WindowStore<string, string> store = builder.withCachingEnabled().build();
        StateStore wrapped = ((WrappedStateStore) store).wrapped();
        Assert.Equal(store, instanceOf(MeteredWindowStore));
        Assert.Equal(wrapped, instanceOf(CachingWindowStore));
    }

    [Xunit.Fact]
    public void ShouldHaveChangeLoggingStoreWhenLoggingEnabled() {
        WindowStore<string, string> store = builder
                .withLoggingEnabled(Collections.emptyMap())
                .build();
        StateStore wrapped = ((WrappedStateStore) store).wrapped();
        Assert.Equal(store, instanceOf(MeteredWindowStore));
        Assert.Equal(wrapped, instanceOf(ChangeLoggingWindowBytesStore));
        Assert.Equal(((WrappedStateStore) wrapped).wrapped(), CoreMatchers.equalTo(inner));
    }

    [Xunit.Fact]
    public void ShouldHaveCachingAndChangeLoggingWhenBothEnabled() {
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
    public void ShouldThrowNullPointerIfInnerIsNull() {
        new WindowStoreBuilder<>(null, Serdes.String(), Serdes.String(), new MockTime());
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerIfKeySerdeIsNull() {
        new WindowStoreBuilder<>(supplier, null, Serdes.String(), new MockTime());
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerIfValueSerdeIsNull() {
        new WindowStoreBuilder<>(supplier, Serdes.String(), null, new MockTime());
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerIfTimeIsNull() {
        new WindowStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), null);
    }

}