/*






 *

 *





 */





















public class InMemoryWindowStoreTest : WindowBytesStoreTest {

    private static readonly string STORE_NAME = "InMemoryWindowStore";

    
    WindowStore<K, V> BuildWindowStore<K, V>(long retentionPeriod,
        long windowSize,
        bool retainDuplicates,
        Serde<K> keySerde,
        Serde<V> valueSerde) {
        return Stores.windowStoreBuilder(
            Stores.inMemoryWindowStore(
                STORE_NAME,
                ofMillis(retentionPeriod),
                ofMillis(windowSize),
                retainDuplicates),
            keySerde,
            valueSerde)
            .build();
    }

    
    string GetMetricsScope() {
        return new InMemoryWindowBytesStoreSupplier(null, 0, 0, false).metricsScope();
    }

    
    void SetClassLoggerToDebug() {
        LogCaptureAppender.setClassLoggerToDebug(InMemoryWindowStore);
    }

    [Xunit.Fact]
    public void ShouldRestore() {
        // should be empty initially
        Assert.False(windowStore.all().hasNext());

        StateSerdes<int, string> serdes = new StateSerdes<>("", Serdes.Int(),
            Serdes.String());

        List<KeyValuePair<byte[], byte[]>> restorableEntries = new LinkedList<>();

        restorableEntries
            .add(new KeyValuePair<>(toStoreKeyBinary(1, 0L, 0, serdes).get(), serdes.rawValue("one")));
        restorableEntries.add(new KeyValuePair<>(toStoreKeyBinary(2, WINDOW_SIZE, 0, serdes).get(),
            serdes.rawValue("two")));
        restorableEntries.add(new KeyValuePair<>(toStoreKeyBinary(3, 2 * WINDOW_SIZE, 0, serdes).get(),
            serdes.rawValue("three")));

        context.restore(STORE_NAME, restorableEntries);
        KeyValueIterator<Windowed<int>, string> iterator = windowStore
            .fetchAll(0L, 2 * WINDOW_SIZE);

        Assert.Equal(windowedPair(1, "one", 0L), iterator.next());
        Assert.Equal(windowedPair(2, "two", WINDOW_SIZE), iterator.next());
        Assert.Equal(windowedPair(3, "three", 2 * WINDOW_SIZE), iterator.next());
        Assert.False(iterator.hasNext());
    }

    [Xunit.Fact]
    public void ShouldNotExpireFromOpenIterator() {

        windowStore.put(1, "one", 0L);
        windowStore.put(1, "two", 10L);

        windowStore.put(2, "one", 5L);
        windowStore.put(2, "two", 15L);

        WindowStoreIterator<string> iterator1 = windowStore.fetch(1, 0L, 50L);
        WindowStoreIterator<string> iterator2 = windowStore.fetch(2, 0L, 50L);

        // This put expires all four previous records, but they should still be returned from already open iterators
        windowStore.put(1, "four", 2 * RETENTION_PERIOD);

        Assert.Equal(new KeyValuePair<>(0L, "one"), iterator1.next());
        Assert.Equal(new KeyValuePair<>(5L, "one"), iterator2.next());

        Assert.Equal(new KeyValuePair<>(15L, "two"), iterator2.next());
        Assert.Equal(new KeyValuePair<>(10L, "two"), iterator1.next());

        Assert.False(iterator1.hasNext());
        Assert.False(iterator2.hasNext());

        iterator1.close();
        iterator2.close();

        // Make sure expired records are removed now that open iterators are closed
        Assert.False(windowStore.fetch(1, 0L, 50L).hasNext());
    }

    [Xunit.Fact]
    public void TestExpiration() {

        long currentTime = 0;
        setCurrentTime(currentTime);
        windowStore.put(1, "one");

        currentTime += RETENTION_PERIOD / 4;
        setCurrentTime(currentTime);
        windowStore.put(1, "two");

        currentTime += RETENTION_PERIOD / 4;
        setCurrentTime(currentTime);
        windowStore.put(1, "three");

        currentTime += RETENTION_PERIOD / 4;
        setCurrentTime(currentTime);
        windowStore.put(1, "four");

        // increase current time to the full RETENTION_PERIOD to expire first record
        currentTime = currentTime + RETENTION_PERIOD / 4;
        setCurrentTime(currentTime);
        windowStore.put(1, "five");

        KeyValueIterator<Windowed<int>, string> iterator = windowStore
            .fetchAll(0L, currentTime);

        // effect of this put (expires next oldest record, adds new one) should not be reflected in the already fetched results
        currentTime = currentTime + RETENTION_PERIOD / 4;
        setCurrentTime(currentTime);
        windowStore.put(1, "six");

        // should only have middle 4 values, as (only) the first record was expired at the time of the fetch
        // and the last was inserted after the fetch
        Assert.Equal(windowedPair(1, "two", RETENTION_PERIOD / 4), iterator.next());
        Assert.Equal(windowedPair(1, "three", RETENTION_PERIOD / 2), iterator.next());
        Assert.Equal(windowedPair(1, "four", 3 * (RETENTION_PERIOD / 4)), iterator.next());
        Assert.Equal(windowedPair(1, "five", RETENTION_PERIOD), iterator.next());
        Assert.False(iterator.hasNext());

        iterator = windowStore.fetchAll(0L, currentTime);

        // If we fetch again after the last put, the second oldest record should have expired and newest should appear in results
        Assert.Equal(windowedPair(1, "three", RETENTION_PERIOD / 2), iterator.next());
        Assert.Equal(windowedPair(1, "four", 3 * (RETENTION_PERIOD / 4)), iterator.next());
        Assert.Equal(windowedPair(1, "five", RETENTION_PERIOD), iterator.next());
        Assert.Equal(windowedPair(1, "six", 5 * (RETENTION_PERIOD / 4)), iterator.next());
        Assert.False(iterator.hasNext());
    }
    
}
