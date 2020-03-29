/*






 *

 *





 */




























public class SessionStoreBuilderTest {

    @Mock(type = MockType.NICE)
    private SessionBytesStoreSupplier supplier;
    @Mock(type = MockType.NICE)
    private SessionStore<Bytes, byte[]> inner;
    private SessionStoreBuilder<string, string> builder;

    
    public void SetUp() {// throws Exception

        expect(supplier.get()).andReturn(inner);
        expect(supplier.name()).andReturn("name");
        replay(supplier);

        builder = new SessionStoreBuilder<>(
            supplier,
            Serdes.String(),
            Serdes.String(),
            new MockTime());
    }

    [Xunit.Fact]
    public void ShouldHaveMeteredStoreAsOuterStore() {
        SessionStore<string, string> store = builder.build();
        Assert.Equal(store, instanceOf(MeteredSessionStore));
    }

    [Xunit.Fact]
    public void ShouldHaveChangeLoggingStoreByDefault() {
        SessionStore<string, string> store = builder.build();
        StateStore next = ((WrappedStateStore) store).wrapped();
        Assert.Equal(next, instanceOf(ChangeLoggingSessionBytesStore));
    }

    [Xunit.Fact]
    public void ShouldNotHaveChangeLoggingStoreWhenDisabled() {
        SessionStore<string, string> store = builder.withLoggingDisabled().build();
        StateStore next = ((WrappedStateStore) store).wrapped();
        Assert.Equal(next, CoreMatchers.<StateStore>equalTo(inner));
    }

    [Xunit.Fact]
    public void ShouldHaveCachingStoreWhenEnabled() {
        SessionStore<string, string> store = builder.withCachingEnabled().build();
        StateStore wrapped = ((WrappedStateStore) store).wrapped();
        Assert.Equal(store, instanceOf(MeteredSessionStore));
        Assert.Equal(wrapped, instanceOf(CachingSessionStore));
    }

    [Xunit.Fact]
    public void ShouldHaveChangeLoggingStoreWhenLoggingEnabled() {
        SessionStore<string, string> store = builder
                .withLoggingEnabled(Collections.<string, string>emptyMap())
                .build();
        StateStore wrapped = ((WrappedStateStore) store).wrapped();
        Assert.Equal(store, instanceOf(MeteredSessionStore));
        Assert.Equal(wrapped, instanceOf(ChangeLoggingSessionBytesStore));
        Assert.Equal(((WrappedStateStore) wrapped).wrapped(), CoreMatchers.<StateStore>equalTo(inner));
    }

    [Xunit.Fact]
    public void ShouldHaveCachingAndChangeLoggingWhenBothEnabled() {
        SessionStore<string, string> store = builder
                .withLoggingEnabled(Collections.<string, string>emptyMap())
                .withCachingEnabled()
                .build();
        WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        WrappedStateStore changeLogging = (WrappedStateStore) caching.wrapped();
        Assert.Equal(store, instanceOf(MeteredSessionStore));
        Assert.Equal(caching, instanceOf(CachingSessionStore));
        Assert.Equal(changeLogging, instanceOf(ChangeLoggingSessionBytesStore));
        Assert.Equal(changeLogging.wrapped(), CoreMatchers.<StateStore>equalTo(inner));
    }

    [Xunit.Fact]
    public void ShouldThrowNullPointerIfInnerIsNull() {
        Exception e = assertThrows(NullPointerException, () => new SessionStoreBuilder<>(null, Serdes.String(), Serdes.String(), new MockTime()));
        Assert.Equal(e.getMessage(), ("supplier cannot be null"));
    }

    [Xunit.Fact]
    public void ShouldThrowNullPointerIfKeySerdeIsNull() {
        Exception e = assertThrows(NullPointerException, () => new SessionStoreBuilder<>(supplier, null, Serdes.String(), new MockTime()));
        Assert.Equal(e.getMessage(), ("name cannot be null"));
    }

    [Xunit.Fact]
    public void ShouldThrowNullPointerIfValueSerdeIsNull() {
        Exception e = assertThrows(NullPointerException, () => new SessionStoreBuilder<>(supplier, Serdes.String(), null, new MockTime()));
        Assert.Equal(e.getMessage(), ("name cannot be null"));
    }

    [Xunit.Fact]
    public void ShouldThrowNullPointerIfTimeIsNull() {
        reset(supplier);
        expect(supplier.name()).andReturn("name");
        replay(supplier);
        Exception e = assertThrows(NullPointerException, () => new SessionStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), null));
        Assert.Equal(e.getMessage(), ("time cannot be null"));
    }

    [Xunit.Fact]
    public void ShouldThrowNullPointerIfMetricsScopeIsNull() {
        Exception e = assertThrows(NullPointerException, () => new SessionStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), new MockTime()));
        Assert.Equal(e.getMessage(), ("name cannot be null"));
    }

}