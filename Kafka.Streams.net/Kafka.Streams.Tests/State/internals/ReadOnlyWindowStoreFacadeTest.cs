/*






 *

 *





 */

























public class ReadOnlyWindowStoreFacadeTest {
    @Mock
    private TimestampedWindowStore<string, string> mockedWindowTimestampStore;
    @Mock
    private WindowStoreIterator<ValueAndTimestamp<string>> mockedWindowTimestampIterator;
    @Mock
    private KeyValueIterator<Windowed<string>, ValueAndTimestamp<string>> mockedKeyValueWindowTimestampIterator;

    private ReadOnlyWindowStoreFacade<string, string> readOnlyWindowStoreFacade;

    
    public void Setup() {
        readOnlyWindowStoreFacade = new ReadOnlyWindowStoreFacade<>(mockedWindowTimestampStore);
    }

    [Xunit.Fact]
    public void ShouldReturnPlainKeyValuePairsOnSingleKeyFetch() {
        expect(mockedWindowTimestampStore.fetch("key1", 21L))
            .andReturn(ValueAndTimestamp.make("value1", 42L));
        expect(mockedWindowTimestampStore.fetch("unknownKey", 21L))
            .andReturn(null);
        replay(mockedWindowTimestampStore);

        Assert.Equal(readOnlyWindowStoreFacade.fetch("key1", 21L), is("value1"));
        assertNull(readOnlyWindowStoreFacade.fetch("unknownKey", 21L));

        verify(mockedWindowTimestampStore);
    }

    [Xunit.Fact]
    public void ShouldReturnPlainKeyValuePairsOnSingleKeyFetchLongParameters() {
        expect(mockedWindowTimestampIterator.next())
            .andReturn(KeyValuePair.Create(21L, ValueAndTimestamp.make("value1", 22L)))
            .andReturn(KeyValuePair.Create(42L, ValueAndTimestamp.make("value2", 23L)));
        expect(mockedWindowTimestampStore.fetch("key1", 21L, 42L))
            .andReturn(mockedWindowTimestampIterator);
        replay(mockedWindowTimestampIterator, mockedWindowTimestampStore);

        WindowStoreIterator<string> iterator =
            readOnlyWindowStoreFacade.fetch("key1", 21L, 42L);

        Assert.Equal(iterator.next(), is(KeyValuePair.Create(21L, "value1")));
        Assert.Equal(iterator.next(), is(KeyValuePair.Create(42L, "value2")));
        verify(mockedWindowTimestampIterator, mockedWindowTimestampStore);
    }

    [Xunit.Fact]
    public void ShouldReturnPlainKeyValuePairsOnSingleKeyFetchInstantParameters() {
        expect(mockedWindowTimestampIterator.next())
            .andReturn(KeyValuePair.Create(21L, ValueAndTimestamp.make("value1", 22L)))
            .andReturn(KeyValuePair.Create(42L, ValueAndTimestamp.make("value2", 23L)));
        expect(mockedWindowTimestampStore.fetch("key1", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L)))
            .andReturn(mockedWindowTimestampIterator);
        replay(mockedWindowTimestampIterator, mockedWindowTimestampStore);

        WindowStoreIterator<string> iterator =
            readOnlyWindowStoreFacade.fetch("key1", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L));

        Assert.Equal(iterator.next(), is(KeyValuePair.Create(21L, "value1")));
        Assert.Equal(iterator.next(), is(KeyValuePair.Create(42L, "value2")));
        verify(mockedWindowTimestampIterator, mockedWindowTimestampStore);
    }

    [Xunit.Fact]
    public void ShouldReturnPlainKeyValuePairsOnRangeFetchLongParameters() {
        expect(mockedKeyValueWindowTimestampIterator.next())
            .andReturn(KeyValuePair.Create(
                new Windowed<>("key1", new TimeWindow(21L, 22L)),
                ValueAndTimestamp.make("value1", 22L)))
            .andReturn(KeyValuePair.Create(
                new Windowed<>("key2", new TimeWindow(42L, 43L)),
                ValueAndTimestamp.make("value2", 100L)));
        expect(mockedWindowTimestampStore.fetch("key1", "key2", 21L, 42L))
            .andReturn(mockedKeyValueWindowTimestampIterator);
        replay(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);

        KeyValueIterator<Windowed<string>, string> iterator =
            readOnlyWindowStoreFacade.fetch("key1", "key2", 21L, 42L);

        Assert.Equal(iterator.next(), is(KeyValuePair.Create(new Windowed<>("key1", new TimeWindow(21L, 22L)), "value1")));
        Assert.Equal(iterator.next(), is(KeyValuePair.Create(new Windowed<>("key2", new TimeWindow(42L, 43L)), "value2")));
        verify(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);
    }

    [Xunit.Fact]
    public void ShouldReturnPlainKeyValuePairsOnRangeFetchInstantParameters() {
        expect(mockedKeyValueWindowTimestampIterator.next())
            .andReturn(KeyValuePair.Create(
                new Windowed<>("key1", new TimeWindow(21L, 22L)),
                ValueAndTimestamp.make("value1", 22L)))
            .andReturn(KeyValuePair.Create(
                new Windowed<>("key2", new TimeWindow(42L, 43L)),
                ValueAndTimestamp.make("value2", 100L)));
        expect(mockedWindowTimestampStore.fetch("key1", "key2", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L)))
            .andReturn(mockedKeyValueWindowTimestampIterator);
        replay(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);

        KeyValueIterator<Windowed<string>, string> iterator =
            readOnlyWindowStoreFacade.fetch("key1", "key2", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L));

        Assert.Equal(iterator.next(), is(KeyValuePair.Create(new Windowed<>("key1", new TimeWindow(21L, 22L)), "value1")));
        Assert.Equal(iterator.next(), is(KeyValuePair.Create(new Windowed<>("key2", new TimeWindow(42L, 43L)), "value2")));
        verify(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);
    }

    [Xunit.Fact]
    public void ShouldReturnPlainKeyValuePairsOnFetchAllLongParameters() {
        expect(mockedKeyValueWindowTimestampIterator.next())
            .andReturn(KeyValuePair.Create(
                new Windowed<>("key1", new TimeWindow(21L, 22L)),
                ValueAndTimestamp.make("value1", 22L)))
            .andReturn(KeyValuePair.Create(
                new Windowed<>("key2", new TimeWindow(42L, 43L)),
                ValueAndTimestamp.make("value2", 100L)));
        expect(mockedWindowTimestampStore.fetchAll(21L, 42L))
            .andReturn(mockedKeyValueWindowTimestampIterator);
        replay(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);

        KeyValueIterator<Windowed<string>, string> iterator =
            readOnlyWindowStoreFacade.fetchAll(21L, 42L);

        Assert.Equal(iterator.next(), is(KeyValuePair.Create(new Windowed<>("key1", new TimeWindow(21L, 22L)), "value1")));
        Assert.Equal(iterator.next(), is(KeyValuePair.Create(new Windowed<>("key2", new TimeWindow(42L, 43L)), "value2")));
        verify(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);
    }

    [Xunit.Fact]
    public void ShouldReturnPlainKeyValuePairsOnFetchAllInstantParameters() {
        expect(mockedKeyValueWindowTimestampIterator.next())
            .andReturn(KeyValuePair.Create(
                new Windowed<>("key1", new TimeWindow(21L, 22L)),
                ValueAndTimestamp.make("value1", 22L)))
            .andReturn(KeyValuePair.Create(
                new Windowed<>("key2", new TimeWindow(42L, 43L)),
                ValueAndTimestamp.make("value2", 100L)));
        expect(mockedWindowTimestampStore.fetchAll(Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L)))
            .andReturn(mockedKeyValueWindowTimestampIterator);
        replay(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);

        KeyValueIterator<Windowed<string>, string> iterator =
            readOnlyWindowStoreFacade.fetchAll(Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L));

        Assert.Equal(iterator.next(), is(KeyValuePair.Create(new Windowed<>("key1", new TimeWindow(21L, 22L)), "value1")));
        Assert.Equal(iterator.next(), is(KeyValuePair.Create(new Windowed<>("key2", new TimeWindow(42L, 43L)), "value2")));
        verify(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);
    }

    [Xunit.Fact]
    public void ShouldReturnPlainKeyValuePairsOnAll() {
        expect(mockedKeyValueWindowTimestampIterator.next())
            .andReturn(KeyValuePair.Create(
                new Windowed<>("key1", new TimeWindow(21L, 22L)),
                ValueAndTimestamp.make("value1", 22L)))
            .andReturn(KeyValuePair.Create(
                new Windowed<>("key2", new TimeWindow(42L, 43L)),
                ValueAndTimestamp.make("value2", 100L)));
        expect(mockedWindowTimestampStore.all()).andReturn(mockedKeyValueWindowTimestampIterator);
        replay(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);

        KeyValueIterator<Windowed<string>, string> iterator = readOnlyWindowStoreFacade.all();

        Assert.Equal(iterator.next(), is(KeyValuePair.Create(new Windowed<>("key1", new TimeWindow(21L, 22L)), "value1")));
        Assert.Equal(iterator.next(), is(KeyValuePair.Create(new Windowed<>("key2", new TimeWindow(42L, 43L)), "value2")));
        verify(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);
    }
}
