/*






 *

 *





 */














































public class RocksDBStoreTest {
    private static bool enableBloomFilters = false;
    static string DB_NAME = "db-name";

    private File dir;
    private Serializer<string> stringSerializer = new StringSerializer();
    private Deserializer<string> stringDeserializer = new StringDeserializer();

    InternalMockProcessorContext context;
    RocksDBStore rocksDBStore;

    
    public void setUp() {
        Properties props = StreamsTestUtils.getStreamsConfig();
        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, MockRocksDbConfigSetter);
        rocksDBStore = getRocksDBStore();
        dir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext(dir,
            Serdes.String(),
            Serdes.String(),
            new StreamsConfig(props));
    }

    RocksDBStore getRocksDBStore() {
        return new RocksDBStore(DB_NAME);
    }

    
    public void tearDown() {
        rocksDBStore.close();
    }

    [Xunit.Fact]
    public void shouldRespectBulkloadOptionsDuringInit() {
        rocksDBStore.init(context, rocksDBStore);

        StateRestoreListener restoreListener = context.getRestoreListener(rocksDBStore.name());

        restoreListener.onRestoreStart(null, rocksDBStore.name(), 0L, 0L);

        Assert.Equal(rocksDBStore.getOptions().level0FileNumCompactionTrigger(), (1 << 30));
        Assert.Equal(rocksDBStore.getOptions().level0SlowdownWritesTrigger(), (1 << 30));
        Assert.Equal(rocksDBStore.getOptions().level0StopWritesTrigger(), (1 << 30));

        restoreListener.onRestoreEnd(null, rocksDBStore.name(), 0L);

        Assert.Equal(rocksDBStore.getOptions().level0FileNumCompactionTrigger(), (10));
        Assert.Equal(rocksDBStore.getOptions().level0SlowdownWritesTrigger(), (20));
        Assert.Equal(rocksDBStore.getOptions().level0StopWritesTrigger(), (36));
    }

    [Xunit.Fact]
    public void shouldNotThrowExceptionOnRestoreWhenThereIsPreExistingRocksDbFiles() {
        rocksDBStore.init(context, rocksDBStore);

        string message = "how can a 4 ounce bird carry a 2lb coconut";
        int intKey = 1;
        for (int i = 0; i < 2000000; i++) {
            rocksDBStore.put(new Bytes(stringSerializer.serialize(null, "theKeyIs" + intKey++)),
                             stringSerializer.serialize(null, message));
        }

        List<KeyValuePair<byte[], byte[]>> restoreBytes = new ArrayList<>();

        byte[] restoredKey = "restoredKey".getBytes(UTF_8);
        byte[] restoredValue = "restoredValue".getBytes(UTF_8);
        restoreBytes.add(KeyValuePair.Create(restoredKey, restoredValue));

        context.restore(DB_NAME, restoreBytes);

        Assert.Equal(
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "restoredKey")))),
            equalTo("restoredValue"));
    }

    [Xunit.Fact]
    public void shouldCallRocksDbConfigSetter() {
        MockRocksDbConfigSetter.called = false;

        rocksDBStore.openDB(context);

        Assert.True(MockRocksDbConfigSetter.called);
    }

    [Xunit.Fact]
    public void shouldThrowProcessorStateExceptionOnOpeningReadOnlyDir() {
        File tmpDir = TestUtils.tempDirectory();
        InternalMockProcessorContext tmpContext = new InternalMockProcessorContext(tmpDir, new StreamsConfig(StreamsTestUtils.getStreamsConfig()));

        Assert.True(tmpDir.setReadOnly());

        try {
            rocksDBStore.openDB(tmpContext);
            Assert.True(false, "Should have thrown ProcessorStateException");
        } catch (ProcessorStateException e) {
            // this is good, do nothing
        }
    }

    [Xunit.Fact]
    public void shouldPutAll() {
        List<KeyValuePair<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValuePair<>(
            new Bytes(stringSerializer.serialize(null, "1")),
            stringSerializer.serialize(null, "a")));
        entries.add(new KeyValuePair<>(
            new Bytes(stringSerializer.serialize(null, "2")),
            stringSerializer.serialize(null, "b")));
        entries.add(new KeyValuePair<>(
            new Bytes(stringSerializer.serialize(null, "3")),
            stringSerializer.serialize(null, "c")));

        rocksDBStore.init(context, rocksDBStore);
        rocksDBStore.putAll(entries);
        rocksDBStore.flush();

        Assert.Equal(
            "a",
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "1")))));
        Assert.Equal(
            "b",
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "2")))));
        Assert.Equal(
            "c",
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "3")))));
    }

    [Xunit.Fact]
    public void shouldTogglePrepareForBulkloadSetting() {
        rocksDBStore.init(context, rocksDBStore);
        RocksDBStore.RocksDBBatchingRestoreCallback restoreListener =
            (RocksDBStore.RocksDBBatchingRestoreCallback) rocksDBStore.batchingStateRestoreCallback;

        restoreListener.onRestoreStart(null, null, 0, 0);
        Assert.True("Should have set bulk loading to true", rocksDBStore.isPrepareForBulkload());

        restoreListener.onRestoreEnd(null, null, 0);
        Assert.False("Should have set bulk loading to false", rocksDBStore.isPrepareForBulkload());
    }

    [Xunit.Fact]
    public void shouldTogglePrepareForBulkloadSettingWhenPrexistingSstFiles() {
        List<KeyValuePair<byte[], byte[]>> entries = getKeyValueEntries();

        rocksDBStore.init(context, rocksDBStore);
        context.restore(rocksDBStore.name(), entries);

        RocksDBStore.RocksDBBatchingRestoreCallback restoreListener =
            (RocksDBStore.RocksDBBatchingRestoreCallback) rocksDBStore.batchingStateRestoreCallback;

        restoreListener.onRestoreStart(null, null, 0, 0);
        Assert.True("Should have not set bulk loading to true", rocksDBStore.isPrepareForBulkload());

        restoreListener.onRestoreEnd(null, null, 0);
        Assert.False("Should have set bulk loading to false", rocksDBStore.isPrepareForBulkload());
    }

    [Xunit.Fact]
    public void shouldRestoreAll() {
        List<KeyValuePair<byte[], byte[]>> entries = getKeyValueEntries();

        rocksDBStore.init(context, rocksDBStore);
        context.restore(rocksDBStore.name(), entries);

        Assert.Equal(
            "a",
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "1")))));
        Assert.Equal(
            "b",
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "2")))));
        Assert.Equal(
            "c",
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "3")))));
    }

    [Xunit.Fact]
    public void shouldPutOnlyIfAbsentValue() {
        rocksDBStore.init(context, rocksDBStore);
        Bytes keyBytes = new Bytes(stringSerializer.serialize(null, "one"));
        byte[] valueBytes = stringSerializer.serialize(null, "A");
        byte[] valueBytesUpdate = stringSerializer.serialize(null, "B");

        rocksDBStore.putIfAbsent(keyBytes, valueBytes);
        rocksDBStore.putIfAbsent(keyBytes, valueBytesUpdate);

        string retrievedValue = stringDeserializer.deserialize(null, rocksDBStore.get(keyBytes));
        Assert.Equal("A", retrievedValue);
    }

    [Xunit.Fact]
    public void shouldHandleDeletesOnRestoreAll() {
        List<KeyValuePair<byte[], byte[]>> entries = getKeyValueEntries();
        entries.add(new KeyValuePair<>("1".getBytes(UTF_8), null));

        rocksDBStore.init(context, rocksDBStore);
        context.restore(rocksDBStore.name(), entries);

        KeyValueIterator<Bytes, byte[]> iterator = rocksDBStore.all();
        HashSet<string> keys = new HashSet<>();

        while (iterator.hasNext()) {
            keys.add(stringDeserializer.deserialize(null, iterator.next().key.get()));
        }

        Assert.Equal(keys, (Utils.mkSet("2", "3")));
    }

    [Xunit.Fact]
    public void shouldHandleDeletesAndPutbackOnRestoreAll() {
        List<KeyValuePair<byte[], byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValuePair<>("1".getBytes(UTF_8), "a".getBytes(UTF_8)));
        entries.add(new KeyValuePair<>("2".getBytes(UTF_8), "b".getBytes(UTF_8)));
        // this will be deleted
        entries.add(new KeyValuePair<>("1".getBytes(UTF_8), null));
        entries.add(new KeyValuePair<>("3".getBytes(UTF_8), "c".getBytes(UTF_8)));
        // this will restore key "1" as WriteBatch applies updates in order
        entries.add(new KeyValuePair<>("1".getBytes(UTF_8), "restored".getBytes(UTF_8)));

        rocksDBStore.init(context, rocksDBStore);
        context.restore(rocksDBStore.name(), entries);

        KeyValueIterator<Bytes, byte[]> iterator = rocksDBStore.all();
        HashSet<string> keys = new HashSet<>();

        while (iterator.hasNext()) {
            keys.add(stringDeserializer.deserialize(null, iterator.next().key.get()));
        }

        Assert.Equal(keys, (Utils.mkSet("1", "2", "3")));

        Assert.Equal(
            "restored",
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "1")))));
        Assert.Equal(
            "b",
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "2")))));
        Assert.Equal(
            "c",
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "3")))));
    }

    [Xunit.Fact]
    public void shouldRestoreThenDeleteOnRestoreAll() {
        List<KeyValuePair<byte[], byte[]>> entries = getKeyValueEntries();

        rocksDBStore.init(context, rocksDBStore);

        context.restore(rocksDBStore.name(), entries);

        Assert.Equal(
            "a",
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "1")))));
        Assert.Equal(
            "b",
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "2")))));
        Assert.Equal(
            "c",
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "3")))));

        entries.Clear();

        entries.add(new KeyValuePair<>("2".getBytes(UTF_8), "b".getBytes(UTF_8)));
        entries.add(new KeyValuePair<>("3".getBytes(UTF_8), "c".getBytes(UTF_8)));
        entries.add(new KeyValuePair<>("1".getBytes(UTF_8), null));

        context.restore(rocksDBStore.name(), entries);

        KeyValueIterator<Bytes, byte[]> iterator = rocksDBStore.all();
        HashSet<string> keys = new HashSet<>();

        while (iterator.hasNext()) {
            keys.add(stringDeserializer.deserialize(null, iterator.next().key.get()));
        }

        Assert.Equal(keys, (Utils.mkSet("2", "3")));
    }

    [Xunit.Fact]
    public void shouldThrowNullPointerExceptionOnNullPut() {
        rocksDBStore.init(context, rocksDBStore);
        try {
            rocksDBStore.put(null, stringSerializer.serialize(null, "someVal"));
            Assert.True(false, "Should have thrown NullPointerException on null put()");
        } catch (NullPointerException e) {
            // this is good
        }
    }

    [Xunit.Fact]
    public void shouldThrowNullPointerExceptionOnNullPutAll() {
        rocksDBStore.init(context, rocksDBStore);
        try {
            rocksDBStore.put(null, stringSerializer.serialize(null, "someVal"));
            Assert.True(false, "Should have thrown NullPointerException on null put()");
        } catch (NullPointerException e) {
            // this is good
        }
    }

    [Xunit.Fact]
    public void shouldThrowNullPointerExceptionOnNullGet() {
        rocksDBStore.init(context, rocksDBStore);
        try {
            rocksDBStore.get(null);
            Assert.True(false, "Should have thrown NullPointerException on null get()");
        } catch (NullPointerException e) {
            // this is good
        }
    }

    [Xunit.Fact]
    public void shouldThrowNullPointerExceptionOnDelete() {
        rocksDBStore.init(context, rocksDBStore);
        try {
            rocksDBStore.delete(null);
            Assert.True(false, "Should have thrown NullPointerException on deleting null key");
        } catch (NullPointerException e) {
            // this is good
        }
    }

    [Xunit.Fact]
    public void shouldThrowNullPointerExceptionOnRange() {
        rocksDBStore.init(context, rocksDBStore);
        try {
            rocksDBStore.range(null, new Bytes(stringSerializer.serialize(null, "2")));
            Assert.True(false, "Should have thrown NullPointerException on deleting null key");
        } catch (NullPointerException e) {
            // this is good
        }
    }

    [Xunit.Fact]// (expected = ProcessorStateException)
    public void shouldThrowProcessorStateExceptionOnPutDeletedDir(){ //throws IOException
        rocksDBStore.init(context, rocksDBStore);
        Utils.delete(dir);
        rocksDBStore.put(
            new Bytes(stringSerializer.serialize(null, "anyKey")),
            stringSerializer.serialize(null, "anyValue"));
        rocksDBStore.flush();
    }

    [Xunit.Fact]
    public void shouldHandleToggleOfEnablingBloomFilters() {

        Properties props = StreamsTestUtils.getStreamsConfig();
        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, TestingBloomFilterRocksDBConfigSetter);
        rocksDBStore = getRocksDBStore();
        dir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext(dir,
            Serdes.String(),
            Serdes.String(),
            new StreamsConfig(props));

        enableBloomFilters = false;
        rocksDBStore.init(context, rocksDBStore);

        List<string> expectedValues = new ArrayList<>();
        expectedValues.add("a");
        expectedValues.add("b");
        expectedValues.add("c");

        List<KeyValuePair<byte[], byte[]>> keyValues = getKeyValueEntries();
        foreach (KeyValuePair<byte[], byte[]> keyValue in keyValues) {
            rocksDBStore.put(new Bytes(keyValue.key), keyValue.value);
        }

        int expectedIndex = 0;
        foreach (KeyValuePair<byte[], byte[]> keyValue in keyValues) {
            byte[] valBytes = rocksDBStore.get(new Bytes(keyValue.key));
            Assert.Equal(new string(valBytes, UTF_8), is(expectedValues.get(expectedIndex++)));
        }
        Assert.False(TestingBloomFilterRocksDBConfigSetter.bloomFiltersSet);

        rocksDBStore.close();
        expectedIndex = 0;

        // reopen with Bloom Filters enabled
        // should open fine without errors
        enableBloomFilters = true;
        rocksDBStore.init(context, rocksDBStore);

        foreach (KeyValuePair<byte[], byte[]> keyValue in keyValues) {
            byte[] valBytes = rocksDBStore.get(new Bytes(keyValue.key));
            Assert.Equal(new string(valBytes, UTF_8), is(expectedValues.get(expectedIndex++)));
        }

        Assert.True(TestingBloomFilterRocksDBConfigSetter.bloomFiltersSet);
    }

    public static class MockRocksDbConfigSetter : RocksDBConfigSetter {
        static bool called;

        
        public void setConfig(string storeName, Options options, Dictionary<string, object> configs) {
            called = true;

            options.setLevel0FileNumCompactionTrigger(10);
        }
    }

    public static class TestingBloomFilterRocksDBConfigSetter : RocksDBConfigSetter {

        static bool bloomFiltersSet;
        static Filter filter;
        static Cache cache;

        
        public void setConfig(string storeName, Options options, Dictionary<string, object> configs) {
            BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
            cache = new LRUCache(50 * 1024 * 1024L);
            tableConfig.setBlockCache(cache);
            tableConfig.setBlockSize(4096L);
            if (enableBloomFilters) {
                filter = new BloomFilter();
                tableConfig.setFilter(filter);
                options.optimizeFiltersForHits();
                bloomFiltersSet = true;
            } else {
                options.setOptimizeFiltersForHits(false);
                bloomFiltersSet = false;
            }

            options.setTableFormatConfig(tableConfig);
        }

        
        public void close(string storeName, Options options) {
            if (filter != null) {
                filter.close();
            }
            cache.close();
        }
    }

    private List<KeyValuePair<byte[], byte[]>> getKeyValueEntries() {
        List<KeyValuePair<byte[], byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValuePair<>("1".getBytes(UTF_8), "a".getBytes(UTF_8)));
        entries.add(new KeyValuePair<>("2".getBytes(UTF_8), "b".getBytes(UTF_8)));
        entries.add(new KeyValuePair<>("3".getBytes(UTF_8), "c".getBytes(UTF_8)));
        return entries;
    }

}
