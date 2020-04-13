//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */


































//    public abstract class AbstractKeyValueStoreTest
//    {

//        protected abstract IKeyValueStore<K, V> CreateKeyValueStore<K, V>(ProcessorContext context);

//        protected InternalMockProcessorContext context;
//        protected IKeyValueStore<int, string> store;
//        protected KeyValueStoreTestDriver<int, string> driver;


//        public void Before()
//        {
//            driver = KeyValueStoreTestDriver.Create(int, string);
//            context = (InternalMockProcessorContext)driver.context;
//            context.setTime(10);
//            store = createKeyValueStore(context);
//        }


//        public void After()
//        {
//            store.Close();
//            driver.Clear();
//        }

//        private static Dictionary<int, string> GetContents(IKeyValueIterator<int, string> iter)
//        {
//            HashDictionary<int, string> result = new HashMap<>();
//            while (iter.HasNext())
//            {
//                KeyValuePair<int, string> entry = iter.MoveNext();
//                result.Put(entry.Key, entry.Value);
//            }
//            return result;
//        }

//        [Fact]
//        public void ShouldNotIncludeDeletedFromRangeResult()
//        {
//            store.Close();

//            Serializer<string> serializer = new Serdes.String().Serializer()
//            {
//            private int numCalls = 0;


//        public byte[] Serialize(string topic, string data)
//        {
//            if (++numCalls > 3)
//            {
//                Assert.True(false, "Value serializer is called; it should never happen");
//            }

//            return base.Serialize(topic, data);
//        }
//    };

//    context.setValueSerde(Serdes.SerdeFrom(serializer, new Serdes.String().Deserializer()));
//        store = createKeyValueStore(driver.context);

//    store.Put(0, "zero");
//        store.Put(1, "one");
//        store.Put(2, "two");
//        store.Delete(0);
//        store.Delete(1);

//        // should not include deleted records in iterator
//        Dictionary<int, string> expectedContents = Collections.singletonMap(2, "two");
//    Assert.Equal(expectedContents, getContents(store.All()));
//    }

//    [Fact]
//    public void ShouldDeleteIfSerializedValueIsNull()
//    {
//        store.Close();

//        Serializer<string> serializer = new Serdes.String().Serializer()
//        {


//            public byte[] serialize(string topic, string data)
//        {
//            if (data.Equals("null"))
//            {
//                // will be serialized to null bytes, indicating deletes
//                return null;
//            }
//            return base.Serialize(topic, data);
//        }
//    };

//    context.setValueSerde(Serdes.SerdeFrom(serializer, new Serdes.String().Deserializer()));
//        store = createKeyValueStore(driver.context);

//    store.Put(0, "zero");
//        store.Put(1, "one");
//        store.Put(2, "two");
//        store.Put(0, "null");
//        store.Put(1, "null");

//        // should not include deleted records in iterator
//        Dictionary<int, string> expectedContents = Collections.singletonMap(2, "two");
//    Assert.Equal(expectedContents, getContents(store.All()));
//    }

//    [Fact]
//    public void TestPutGetRange()
//    {
//        // Verify that the store reads and writes correctly ...
//        store.Put(0, "zero");
//        store.Put(1, "one");
//        store.Put(2, "two");
//        store.Put(4, "four");
//        store.Put(5, "five");
//        Assert.Equal(5, driver.sizeOf(store));
//        Assert.Equal("zero", store.Get(0));
//        Assert.Equal("one", store.Get(1));
//        Assert.Equal("two", store.Get(2));
//        Assert.Null(store.Get(3));
//        Assert.Equal("four", store.Get(4));
//        Assert.Equal("five", store.Get(5));
//        // Flush now so that for caching store, we will not skip the deletion following an Put
//        store.Flush();
//        store.Delete(5);
//        Assert.Equal(4, driver.sizeOf(store));

//        // Flush the store and verify All current entries were properly flushed ...
//        store.Flush();
//        Assert.Equal("zero", driver.flushedEntryStored(0));
//        Assert.Equal("one", driver.flushedEntryStored(1));
//        Assert.Equal("two", driver.flushedEntryStored(2));
//        Assert.Equal("four", driver.flushedEntryStored(4));
//        Assert.Null(driver.flushedEntryStored(5));

//        Assert.False(driver.flushedEntryRemoved(0));
//        Assert.False(driver.flushedEntryRemoved(1));
//        Assert.False(driver.flushedEntryRemoved(2));
//        Assert.False(driver.flushedEntryRemoved(4));
//        Assert.True(driver.flushedEntryRemoved(5));

//        HashDictionary<int, string> expectedContents = new HashMap<>();
//        expectedContents.Put(2, "two");
//        expectedContents.Put(4, "four");

//        // Check range iteration ...
//        Assert.Equal(expectedContents, getContents(store.Range(2, 4)));
//        Assert.Equal(expectedContents, getContents(store.Range(2, 6)));

//        // Check All iteration ...
//        expectedContents.Put(0, "zero");
//        expectedContents.Put(1, "one");
//        Assert.Equal(expectedContents, getContents(store.All()));
//    }

//    [Fact]
//    public void TestPutGetRangeWithDefaultSerdes()
//    {
//        // Verify that the store reads and writes correctly ...
//        store.Put(0, "zero");
//        store.Put(1, "one");
//        store.Put(2, "two");
//        store.Put(4, "four");
//        store.Put(5, "five");
//        Assert.Equal(5, driver.sizeOf(store));
//        Assert.Equal("zero", store.Get(0));
//        Assert.Equal("one", store.Get(1));
//        Assert.Equal("two", store.Get(2));
//        Assert.Null(store.Get(3));
//        Assert.Equal("four", store.Get(4));
//        Assert.Equal("five", store.Get(5));
//        store.Flush();
//        store.Delete(5);

//        // Flush the store and verify All current entries were properly flushed ...
//        store.Flush();
//        Assert.Equal("zero", driver.flushedEntryStored(0));
//        Assert.Equal("one", driver.flushedEntryStored(1));
//        Assert.Equal("two", driver.flushedEntryStored(2));
//        Assert.Equal("four", driver.flushedEntryStored(4));
//        Assert.Null(null, driver.flushedEntryStored(5));

//        Assert.False(driver.flushedEntryRemoved(0));
//        Assert.False(driver.flushedEntryRemoved(1));
//        Assert.False(driver.flushedEntryRemoved(2));
//        Assert.False(driver.flushedEntryRemoved(4));
//        Assert.True(driver.flushedEntryRemoved(5));
//    }

//    [Fact]
//    public void TestRestore()
//    {
//        store.Close();
//        // Add any entries that will be restored to any store
//        // that uses the driver's context ...
//        driver.addEntryToRestoreLog(0, "zero");
//        driver.addEntryToRestoreLog(1, "one");
//        driver.addEntryToRestoreLog(2, "two");
//        driver.addEntryToRestoreLog(3, "three");

//        // Create the store, which should register with the context and automatically
//        // receive the restore entries ...
//        store = createKeyValueStore(driver.context);
//        context.restore(store.Name(), driver.restoredEntries());

//        // Verify that the store's contents were properly restored ...
//        Assert.Equal(0, driver.checkForRestoredEntries(store));

//        // and there are no other entries ...
//        Assert.Equal(4, driver.sizeOf(store));
//    }

//    [Fact]
//    public void TestRestoreWithDefaultSerdes()
//    {
//        store.Close();
//        // Add any entries that will be restored to any store
//        // that uses the driver's context ...
//        driver.addEntryToRestoreLog(0, "zero");
//        driver.addEntryToRestoreLog(1, "one");
//        driver.addEntryToRestoreLog(2, "two");
//        driver.addEntryToRestoreLog(3, "three");

//        // Create the store, which should register with the context and automatically
//        // receive the restore entries ...
//        store = createKeyValueStore(driver.context);
//        context.restore(store.Name(), driver.restoredEntries());
//        // Verify that the store's contents were properly restored ...
//        Assert.Equal(0, driver.checkForRestoredEntries(store));

//        // and there are no other entries ...
//        Assert.Equal(4, driver.sizeOf(store));
//    }

//    [Fact]
//    public void TestPutIfAbsent()
//    {
//        // Verify that the store reads and writes correctly ...
//        Assert.Null(store.PutIfAbsent(0, "zero"));
//        Assert.Null(store.PutIfAbsent(1, "one"));
//        Assert.Null(store.PutIfAbsent(2, "two"));
//        Assert.Null(store.PutIfAbsent(4, "four"));
//        Assert.Equal("four", store.PutIfAbsent(4, "unexpected value"));
//        Assert.Equal(4, driver.sizeOf(store));
//        Assert.Equal("zero", store.Get(0));
//        Assert.Equal("one", store.Get(1));
//        Assert.Equal("two", store.Get(2));
//        Assert.Null(store.Get(3));
//        Assert.Equal("four", store.Get(4));

//        // Flush the store and verify All current entries were properly flushed ...
//        store.Flush();
//        Assert.Equal("zero", driver.flushedEntryStored(0));
//        Assert.Equal("one", driver.flushedEntryStored(1));
//        Assert.Equal("two", driver.flushedEntryStored(2));
//        Assert.Equal("four", driver.flushedEntryStored(4));

//        Assert.False(driver.flushedEntryRemoved(0));
//        Assert.False(driver.flushedEntryRemoved(1));
//        Assert.False(driver.flushedEntryRemoved(2));
//        Assert.False(driver.flushedEntryRemoved(4));
//    }

//    [Fact]// (expected = NullPointerException)
//    public void ShouldThrowNullPointerExceptionOnPutNullKey()
//    {
//        store.Put(null, "anyValue");
//    }

//    [Fact]
//    public void ShouldNotThrowNullPointerExceptionOnPutNullValue()
//    {
//        store.Put(1, null);
//    }

//    [Fact]// (expected = NullPointerException)
//    public void ShouldThrowNullPointerExceptionOnPutIfAbsentNullKey()
//    {
//        store.PutIfAbsent(null, "anyValue");
//    }

//    [Fact]
//    public void ShouldNotThrowNullPointerExceptionOnPutIfAbsentNullValue()
//    {
//        store.PutIfAbsent(1, null);
//    }

//    [Fact]// (expected = NullPointerException)
//    public void ShouldThrowNullPointerExceptionOnPutAllNullKey()
//    {
//        store.PutAll(Collections.singletonList(KeyValuePair.Create(null, "anyValue")));
//    }

//    [Fact]
//    public void ShouldNotThrowNullPointerExceptionOnPutAllNullKey()
//    {
//        store.PutAll(Collections.singletonList(KeyValuePair.Create(1, null)));
//    }

//    [Fact]// (expected = NullPointerException)
//    public void ShouldThrowNullPointerExceptionOnDeleteNullKey()
//    {
//        store.Delete(null);
//    }

//    [Fact]// (expected = NullPointerException)
//    public void ShouldThrowNullPointerExceptionOnGetNullKey()
//    {
//        store.Get(null);
//    }

//    [Fact]// (expected = NullPointerException)
//    public void ShouldThrowNullPointerExceptionOnRangeNullFromKey()
//    {
//        store.Range(null, 2);
//    }

//    [Fact]// (expected = NullPointerException)
//    public void ShouldThrowNullPointerExceptionOnRangeNullToKey()
//    {
//        store.Range(2, null);
//    }

//    [Fact]
//    public void TestSize()
//    {
//        Assert.Equal("A newly created store should have no entries", 0, store.approximateNumEntries);

//        store.Put(0, "zero");
//        store.Put(1, "one");
//        store.Put(2, "two");
//        store.Put(4, "four");
//        store.Put(5, "five");
//        store.Flush();
//        Assert.Equal(5, store.approximateNumEntries);
//    }

//    [Fact]
//    public void ShouldPutAll()
//    {
//        List<KeyValuePair<int, string>> entries = new List<KeyValuePair<int, string>>();
//        entries.Add(KeyValuePair.Create(1, "one"));
//        entries.Add(KeyValuePair.Create(2, "two"));

//        store.PutAll(entries);

//        List<KeyValuePair<int, string>> allReturned = new List<KeyValuePair<int, string>>();
//        List<KeyValuePair<int, string>> expectedReturned = Arrays.asList(KeyValuePair.Create(1, "one"), KeyValuePair.Create(2, "two"));
//        Iterator<KeyValuePair<int, string>> iterator = store.All();

//        while (iterator.HasNext())
//        {
//            allReturned.Add(iterator.MoveNext());
//        }
//        Assert.Equal(allReturned, (expectedReturned));

//    }

//    [Fact]
//    public void ShouldDeleteFromStore()
//    {
//        store.Put(1, "one");
//        store.Put(2, "two");
//        store.Delete(2);
//        Assert.Null(store.Get(2));
//    }

//    [Fact]
//    public void ShouldReturnSameResultsForGetAndRangeWithEqualKeys()
//    {
//        List<KeyValuePair<int, string>> entries = new List<KeyValuePair<int, string>>();
//        entries.Add(KeyValuePair.Create(1, "one"));
//        entries.Add(KeyValuePair.Create(2, "two"));
//        entries.Add(KeyValuePair.Create(3, "three"));

//        store.PutAll(entries);

//        Iterator<KeyValuePair<int, string>> iterator = store.Range(2, 2);

//        Assert.Equal(iterator.MoveNext().Value, store.Get(2));
//        Assert.False(iterator.HasNext());
//    }

//    [Fact]
//    public void ShouldNotThrowConcurrentModificationException()
//    {
//        store.Put(0, "zero");

//        IKeyValueIterator<int, string> results = store.Range(0, 2);

//        store.Put(1, "one");

//        Assert.Equal(KeyValuePair.Create(0, "zero"), results.MoveNext());
//    }

//    [Fact]
//    public void ShouldNotThrowInvalidRangeExceptionWithNegativeFromKey()
//    {
//        LogCaptureAppender.setClassLoggerToDebug(InMemoryWindowStore);
//        LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();

//        IKeyValueIterator<int, string> iterator = store.Range(-1, 1);
//        Assert.False(iterator.HasNext());

//        List<string> messages = appender.getMessages();
//        Assert.Equal(messages, hasItem("Returning empty iterator for Fetch with invalid key range: from > to. "
//            + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. "
//            + "Note that the built-in numerical serdes do not follow this for negative numbers"));
//    }
//}
//}
///*






//*

//*





//*/





































//// should not include deleted records in iterator




//// should not include deleted records in iterator





















