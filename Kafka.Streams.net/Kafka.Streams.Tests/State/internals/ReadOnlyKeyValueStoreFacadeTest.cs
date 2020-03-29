/*






 *

 *





 */




















public class ReadOnlyKeyValueStoreFacadeTest {
    @Mock
    private TimestampedKeyValueStore<string, string> mockedKeyValueTimestampStore;
    @Mock
    private KeyValueIterator<string, ValueAndTimestamp<string>> mockedKeyValueTimestampIterator;

    private ReadOnlyKeyValueStoreFacade<string, string> readOnlyKeyValueStoreFacade;

    
    public void setup() {
        readOnlyKeyValueStoreFacade = new ReadOnlyKeyValueStoreFacade<>(mockedKeyValueTimestampStore);
    }

    [Xunit.Fact]
    public void shouldReturnPlainValueOnGet() {
        expect(mockedKeyValueTimestampStore.get("key"))
            .andReturn(ValueAndTimestamp.make("value", 42L));
        expect(mockedKeyValueTimestampStore.get("unknownKey"))
            .andReturn(null);
        replay(mockedKeyValueTimestampStore);

        Assert.Equal(readOnlyKeyValueStoreFacade.get("key"), is("value"));
        assertNull(readOnlyKeyValueStoreFacade.get("unknownKey"));
        verify(mockedKeyValueTimestampStore);
    }

    [Xunit.Fact]
    public void shouldReturnPlainKeyValuePairsForRangeIterator() {
        expect(mockedKeyValueTimestampIterator.next())
            .andReturn(KeyValuePair.Create("key1", ValueAndTimestamp.make("value1", 21L)))
            .andReturn(KeyValuePair.Create("key2", ValueAndTimestamp.make("value2", 42L)));
        expect(mockedKeyValueTimestampStore.range("key1", "key2")).andReturn(mockedKeyValueTimestampIterator);
        replay(mockedKeyValueTimestampIterator, mockedKeyValueTimestampStore);

        KeyValueIterator<string, string> iterator = readOnlyKeyValueStoreFacade.range("key1", "key2");
        Assert.Equal(iterator.next(), is(KeyValuePair.Create("key1", "value1")));
        Assert.Equal(iterator.next(), is(KeyValuePair.Create("key2", "value2")));
        verify(mockedKeyValueTimestampIterator, mockedKeyValueTimestampStore);
    }

    [Xunit.Fact]
    public void shouldReturnPlainKeyValuePairsForAllIterator() {
        expect(mockedKeyValueTimestampIterator.next())
            .andReturn(KeyValuePair.Create("key1", ValueAndTimestamp.make("value1", 21L)))
            .andReturn(KeyValuePair.Create("key2", ValueAndTimestamp.make("value2", 42L)));
        expect(mockedKeyValueTimestampStore.all()).andReturn(mockedKeyValueTimestampIterator);
        replay(mockedKeyValueTimestampIterator, mockedKeyValueTimestampStore);

        KeyValueIterator<string, string> iterator = readOnlyKeyValueStoreFacade.all();
        Assert.Equal(iterator.next(), is(KeyValuePair.Create("key1", "value1")));
        Assert.Equal(iterator.next(), is(KeyValuePair.Create("key2", "value2")));
        verify(mockedKeyValueTimestampIterator, mockedKeyValueTimestampStore);
    }

    [Xunit.Fact]
    public void shouldForwardApproximateNumEntries() {
        expect(mockedKeyValueTimestampStore.approximateNumEntries()).andReturn(42L);
        replay(mockedKeyValueTimestampStore);

        Assert.Equal(readOnlyKeyValueStoreFacade.approximateNumEntries(), is(42L));
        verify(mockedKeyValueTimestampStore);
    }
}
