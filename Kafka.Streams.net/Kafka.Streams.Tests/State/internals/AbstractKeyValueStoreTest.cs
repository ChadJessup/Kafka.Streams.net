/*






 *

 *





 */


































public abstract class AbstractKeyValueStoreTest {

    protected abstract KeyValueStore<K, V> createKeyValueStore<K, V>(ProcessorContext context);

    protected InternalMockProcessorContext context;
    protected KeyValueStore<int, string> store;
    protected KeyValueStoreTestDriver<int, string> driver;

    
    public void before() {
        driver = KeyValueStoreTestDriver.create(int, string);
        context = (InternalMockProcessorContext) driver.context();
        context.setTime(10);
        store = createKeyValueStore(context);
    }

    
    public void after() {
        store.close();
        driver.Clear();
    }

    private static Dictionary<int, string> getContents(KeyValueIterator<int, string> iter) {
        HashDictionary<int, string> result = new HashMap<>();
        while (iter.hasNext()) {
            KeyValuePair<int, string> entry = iter.next();
            result.put(entry.key, entry.value);
        }
        return result;
    }

    [Xunit.Fact]
    public void shouldNotIncludeDeletedFromRangeResult() {
        store.close();

        Serializer<string> serializer = new StringSerializer() {
            private int numCalls = 0;

            
            public byte[] serialize(string topic, string data) {
                if (++numCalls > 3) {
                    Assert.True(false, "Value serializer is called; it should never happen");
                }

                return base.serialize(topic, data);
            }
        };

        context.setValueSerde(Serdes.serdeFrom(serializer, new StringDeserializer()));
        store = createKeyValueStore(driver.context());

        store.put(0, "zero");
        store.put(1, "one");
        store.put(2, "two");
        store.delete(0);
        store.delete(1);

        // should not include deleted records in iterator
        Dictionary<int, string> expectedContents = Collections.singletonMap(2, "two");
        Assert.Equal(expectedContents, getContents(store.all()));
    }

    [Xunit.Fact]
    public void shouldDeleteIfSerializedValueIsNull() {
        store.close();

        Serializer<string> serializer = new StringSerializer() {
            
            public byte[] serialize(string topic, string data) {
                if (data.equals("null")) {
                    // will be serialized to null bytes, indicating deletes
                    return null;
                }
                return base.serialize(topic, data);
            }
        };

        context.setValueSerde(Serdes.serdeFrom(serializer, new StringDeserializer()));
        store = createKeyValueStore(driver.context());

        store.put(0, "zero");
        store.put(1, "one");
        store.put(2, "two");
        store.put(0, "null");
        store.put(1, "null");

        // should not include deleted records in iterator
        Dictionary<int, string> expectedContents = Collections.singletonMap(2, "two");
        Assert.Equal(expectedContents, getContents(store.all()));
    }

    [Xunit.Fact]
    public void testPutGetRange() {
        // Verify that the store reads and writes correctly ...
        store.put(0, "zero");
        store.put(1, "one");
        store.put(2, "two");
        store.put(4, "four");
        store.put(5, "five");
        Assert.Equal(5, driver.sizeOf(store));
        Assert.Equal("zero", store.get(0));
        Assert.Equal("one", store.get(1));
        Assert.Equal("two", store.get(2));
        assertNull(store.get(3));
        Assert.Equal("four", store.get(4));
        Assert.Equal("five", store.get(5));
        // Flush now so that for caching store, we will not skip the deletion following an put
        store.flush();
        store.delete(5);
        Assert.Equal(4, driver.sizeOf(store));

        // Flush the store and verify all current entries were properly flushed ...
        store.flush();
        Assert.Equal("zero", driver.flushedEntryStored(0));
        Assert.Equal("one", driver.flushedEntryStored(1));
        Assert.Equal("two", driver.flushedEntryStored(2));
        Assert.Equal("four", driver.flushedEntryStored(4));
        assertNull(driver.flushedEntryStored(5));

        Assert.False(driver.flushedEntryRemoved(0));
        Assert.False(driver.flushedEntryRemoved(1));
        Assert.False(driver.flushedEntryRemoved(2));
        Assert.False(driver.flushedEntryRemoved(4));
        Assert.True(driver.flushedEntryRemoved(5));

        HashDictionary<int, string> expectedContents = new HashMap<>();
        expectedContents.put(2, "two");
        expectedContents.put(4, "four");

        // Check range iteration ...
        Assert.Equal(expectedContents, getContents(store.range(2, 4)));
        Assert.Equal(expectedContents, getContents(store.range(2, 6)));

        // Check all iteration ...
        expectedContents.put(0, "zero");
        expectedContents.put(1, "one");
        Assert.Equal(expectedContents, getContents(store.all()));
    }

    [Xunit.Fact]
    public void testPutGetRangeWithDefaultSerdes() {
        // Verify that the store reads and writes correctly ...
        store.put(0, "zero");
        store.put(1, "one");
        store.put(2, "two");
        store.put(4, "four");
        store.put(5, "five");
        Assert.Equal(5, driver.sizeOf(store));
        Assert.Equal("zero", store.get(0));
        Assert.Equal("one", store.get(1));
        Assert.Equal("two", store.get(2));
        assertNull(store.get(3));
        Assert.Equal("four", store.get(4));
        Assert.Equal("five", store.get(5));
        store.flush();
        store.delete(5);

        // Flush the store and verify all current entries were properly flushed ...
        store.flush();
        Assert.Equal("zero", driver.flushedEntryStored(0));
        Assert.Equal("one", driver.flushedEntryStored(1));
        Assert.Equal("two", driver.flushedEntryStored(2));
        Assert.Equal("four", driver.flushedEntryStored(4));
        assertNull(null, driver.flushedEntryStored(5));

        Assert.False(driver.flushedEntryRemoved(0));
        Assert.False(driver.flushedEntryRemoved(1));
        Assert.False(driver.flushedEntryRemoved(2));
        Assert.False(driver.flushedEntryRemoved(4));
        Assert.True(driver.flushedEntryRemoved(5));
    }

    [Xunit.Fact]
    public void testRestore() {
        store.close();
        // Add any entries that will be restored to any store
        // that uses the driver's context ...
        driver.addEntryToRestoreLog(0, "zero");
        driver.addEntryToRestoreLog(1, "one");
        driver.addEntryToRestoreLog(2, "two");
        driver.addEntryToRestoreLog(3, "three");

        // Create the store, which should register with the context and automatically
        // receive the restore entries ...
        store = createKeyValueStore(driver.context());
        context.restore(store.name(), driver.restoredEntries());

        // Verify that the store's contents were properly restored ...
        Assert.Equal(0, driver.checkForRestoredEntries(store));

        // and there are no other entries ...
        Assert.Equal(4, driver.sizeOf(store));
    }

    [Xunit.Fact]
    public void testRestoreWithDefaultSerdes() {
        store.close();
        // Add any entries that will be restored to any store
        // that uses the driver's context ...
        driver.addEntryToRestoreLog(0, "zero");
        driver.addEntryToRestoreLog(1, "one");
        driver.addEntryToRestoreLog(2, "two");
        driver.addEntryToRestoreLog(3, "three");

        // Create the store, which should register with the context and automatically
        // receive the restore entries ...
        store = createKeyValueStore(driver.context());
        context.restore(store.name(), driver.restoredEntries());
        // Verify that the store's contents were properly restored ...
        Assert.Equal(0, driver.checkForRestoredEntries(store));

        // and there are no other entries ...
        Assert.Equal(4, driver.sizeOf(store));
    }

    [Xunit.Fact]
    public void testPutIfAbsent() {
        // Verify that the store reads and writes correctly ...
        assertNull(store.putIfAbsent(0, "zero"));
        assertNull(store.putIfAbsent(1, "one"));
        assertNull(store.putIfAbsent(2, "two"));
        assertNull(store.putIfAbsent(4, "four"));
        Assert.Equal("four", store.putIfAbsent(4, "unexpected value"));
        Assert.Equal(4, driver.sizeOf(store));
        Assert.Equal("zero", store.get(0));
        Assert.Equal("one", store.get(1));
        Assert.Equal("two", store.get(2));
        assertNull(store.get(3));
        Assert.Equal("four", store.get(4));

        // Flush the store and verify all current entries were properly flushed ...
        store.flush();
        Assert.Equal("zero", driver.flushedEntryStored(0));
        Assert.Equal("one", driver.flushedEntryStored(1));
        Assert.Equal("two", driver.flushedEntryStored(2));
        Assert.Equal("four", driver.flushedEntryStored(4));

        Assert.False(driver.flushedEntryRemoved(0));
        Assert.False(driver.flushedEntryRemoved(1));
        Assert.False(driver.flushedEntryRemoved(2));
        Assert.False(driver.flushedEntryRemoved(4));
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerExceptionOnPutNullKey() {
        store.put(null, "anyValue");
    }

    [Xunit.Fact]
    public void shouldNotThrowNullPointerExceptionOnPutNullValue() {
        store.put(1, null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerExceptionOnPutIfAbsentNullKey() {
        store.putIfAbsent(null, "anyValue");
    }

    [Xunit.Fact]
    public void shouldNotThrowNullPointerExceptionOnPutIfAbsentNullValue() {
        store.putIfAbsent(1, null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerExceptionOnPutAllNullKey() {
        store.putAll(Collections.singletonList(new KeyValuePair<>(null, "anyValue")));
    }

    [Xunit.Fact]
    public void shouldNotThrowNullPointerExceptionOnPutAllNullKey() {
        store.putAll(Collections.singletonList(new KeyValuePair<>(1, null)));
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerExceptionOnDeleteNullKey() {
        store.delete(null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerExceptionOnGetNullKey() {
        store.get(null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerExceptionOnRangeNullFromKey() {
        store.range(null, 2);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerExceptionOnRangeNullToKey() {
        store.range(2, null);
    }

    [Xunit.Fact]
    public void testSize() {
        Assert.Equal("A newly created store should have no entries", 0, store.approximateNumEntries());

        store.put(0, "zero");
        store.put(1, "one");
        store.put(2, "two");
        store.put(4, "four");
        store.put(5, "five");
        store.flush();
        Assert.Equal(5, store.approximateNumEntries());
    }

    [Xunit.Fact]
    public void shouldPutAll() {
        List<KeyValuePair<int, string>> entries = new ArrayList<>();
        entries.add(new KeyValuePair<>(1, "one"));
        entries.add(new KeyValuePair<>(2, "two"));

        store.putAll(entries);

        List<KeyValuePair<int, string>> allReturned = new ArrayList<>();
        List<KeyValuePair<int, string>> expectedReturned = Array.asList(KeyValuePair.Create(1, "one"), KeyValuePair.Create(2, "two"));
        Iterator<KeyValuePair<int, string>> iterator = store.all();

        while (iterator.hasNext()) {
            allReturned.add(iterator.next());
        }
        Assert.Equal(allReturned, (expectedReturned));

    }

    [Xunit.Fact]
    public void shouldDeleteFromStore() {
        store.put(1, "one");
        store.put(2, "two");
        store.delete(2);
        assertNull(store.get(2));
    }

    [Xunit.Fact]
    public void shouldReturnSameResultsForGetAndRangeWithEqualKeys() {
        List<KeyValuePair<int, string>> entries = new ArrayList<>();
        entries.add(new KeyValuePair<>(1, "one"));
        entries.add(new KeyValuePair<>(2, "two"));
        entries.add(new KeyValuePair<>(3, "three"));

        store.putAll(entries);

        Iterator<KeyValuePair<int, string>> iterator = store.range(2, 2);

        Assert.Equal(iterator.next().value, store.get(2));
        Assert.False(iterator.hasNext());
    }

    [Xunit.Fact]
    public void shouldNotThrowConcurrentModificationException() {
        store.put(0, "zero");

        KeyValueIterator<int, string> results = store.range(0, 2);

        store.put(1, "one");

        Assert.Equal(new KeyValuePair<>(0, "zero"), results.next());
    }

    [Xunit.Fact]
    public void shouldNotThrowInvalidRangeExceptionWithNegativeFromKey() {
        LogCaptureAppender.setClassLoggerToDebug(InMemoryWindowStore);
        LogCaptureAppender appender = LogCaptureAppender.createAndRegister();

        KeyValueIterator<int, string> iterator = store.range(-1, 1);
        Assert.False(iterator.hasNext());

        List<string> messages = appender.getMessages();
        Assert.Equal(messages, hasItem("Returning empty iterator for fetch with invalid key range: from > to. "
            + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. "
            + "Note that the built-in numerical serdes do not follow this for negative numbers"));
    }
}
