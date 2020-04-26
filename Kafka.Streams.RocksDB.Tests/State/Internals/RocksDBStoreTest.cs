//using Confluent.Kafka;
//using Kafka.Streams.Configs;
//using Kafka.Streams.Errors;
//using Kafka.Streams.KStream;
//using Kafka.Streams.State.Interfaces;
//using Kafka.Streams.State.RocksDbState;
//using Kafka.Streams.Tests.Helpers;
//using System.Collections.Generic;
//using System.IO;
//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */














































//    public class RocksDBStoreTest
//    {
//        private static bool enableBloomFilters = false;
//        const string DB_NAME = "db-Name";

//        private DirectoryInfo dir;
//        private ISerializer<string> stringSerializer = Serdes.String().Serializer;
//        private IDeserializer<string> stringDeserializer = Serdes.String().Deserializer;

//        InternalMockProcessorContext context;
//        RocksDbStore rocksDBStore;


//        public void SetUp()
//        {
//            StreamsConfig props = StreamsTestUtils.getStreamsConfig();
//            props.Put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, MockRocksDbConfigSetter);
//            rocksDBStore = GetRocksDBStore();
//            dir = TestUtils.GetTempDirectory();
//            context = new InternalMockProcessorContext(dir,
//                Serdes.String(),
//                Serdes.String(),
//                new StreamsConfig(props));
//        }

//        RocksDbStore GetRocksDBStore()
//        {
//            return new RocksDbStore(DB_NAME);
//        }


//        public void TearDown()
//        {
//            rocksDBStore.Close();
//        }

//        [Fact]
//        public void ShouldRespectBulkloadOptionsDuringInit()
//        {
//            rocksDBStore.Init(context, rocksDBStore);

//            IStateRestoreListener restoreListener = context.getRestoreListener(rocksDBStore.Name());

//            restoreListener.onRestoreStart(null, rocksDBStore.Name(), 0L, 0L);

//            Assert.Equal(rocksDBStore.getOptions().level0FileNumCompactionTrigger(), (1 << 30));
//            Assert.Equal(rocksDBStore.getOptions().level0SlowdownWritesTrigger(), (1 << 30));
//            Assert.Equal(rocksDBStore.getOptions().level0StopWritesTrigger(), (1 << 30));

//            restoreListener.onRestoreEnd(null, rocksDBStore.Name(), 0L);

//            Assert.Equal(rocksDBStore.getOptions().level0FileNumCompactionTrigger(), (10));
//            Assert.Equal(rocksDBStore.getOptions().level0SlowdownWritesTrigger(), (20));
//            Assert.Equal(rocksDBStore.getOptions().level0StopWritesTrigger(), (36));
//        }

//        [Fact]
//        public void ShouldNotThrowExceptionOnRestoreWhenThereIsPreExistingRocksDbFiles()
//        {
//            rocksDBStore.Init(context, rocksDBStore);

//            string message = "how can a 4 ounce bird carry a 2lb coconut";
//            int intKey = 1;
//            for (int i = 0; i < 2000000; i++)
//            {
//                rocksDBStore.Put(new Bytes(stringSerializer.Serialize(null, "theKeyIs" + intKey++)),
//                                 stringSerializer.Serialize(null, message));
//            }

//            List<KeyValuePair<byte[], byte[]>> restoreBytes = new List<KeyValuePair<byte[], byte[]>>();

//            byte[] restoredKey = "restoredKey".getBytes(UTF_8);
//            byte[] restoredValue = "restoredValue".getBytes(UTF_8);
//            restoreBytes.Add(KeyValuePair.Create(restoredKey, restoredValue));

//            context.restore(DB_NAME, restoreBytes);

//            Assert.Equal(
//                stringDeserializer.deserialize(
//                    null,
//                    rocksDBStore.Get(new Bytes(stringSerializer.Serialize(null, "restoredKey")))),
//                equalTo("restoredValue"));
//        }

//        [Fact]
//        public void ShouldCallRocksDbConfigSetter()
//        {
//            MockRocksDbConfigSetter.called = false;

//            rocksDBStore.OpenDb(context);

//            Assert.True(MockRocksDbConfigSetter.called);
//        }

//        [Fact]
//        public void ShouldThrowProcessorStateExceptionOnOpeningReadOnlyDir()
//        {
//            var tmpDir = TestUtils.GetTempDirectory();
//            InternalMockProcessorContext tmpContext = new InternalMockProcessorContext(tmpDir, new StreamsConfig(StreamsTestUtils.getStreamsConfig()));

//            Assert.True(tmpDir.setReadOnly());

//            try
//            {
//                rocksDBStore.OpenDb(tmpContext);
//                Assert.True(false, "Should have thrown ProcessorStateException");
//            }
//            catch (ProcessorStateException e)
//            {
//                // this is good, do nothing
//            }
//        }

//        [Fact]
//        public void ShouldPutAll()
//        {
//            List<KeyValuePair<Bytes, byte[]>> entries = new List<KeyValuePair<Bytes, byte[]>>();
//            entries.Add(KeyValuePair.Create(
//                new Bytes(stringSerializer.Serialize(null, "1")),
//                stringSerializer.Serialize(null, "a")));
//            entries.Add(KeyValuePair.Create(
//                new Bytes(stringSerializer.Serialize(null, "2")),
//                stringSerializer.Serialize(null, "b")));
//            entries.Add(KeyValuePair.Create(
//                new Bytes(stringSerializer.Serialize(null, "3")),
//                stringSerializer.Serialize(null, "c")));

//            rocksDBStore.Init(context, rocksDBStore);
//            rocksDBStore.putAll(entries);
//            rocksDBStore.Flush();

//            Assert.Equal(
//                "a",
//                stringDeserializer.deserialize(
//                    null,
//                    rocksDBStore.Get(new Bytes(stringSerializer.Serialize(null, "1")))));
//            Assert.Equal(
//                "b",
//                stringDeserializer.deserialize(
//                    null,
//                    rocksDBStore.Get(new Bytes(stringSerializer.Serialize(null, "2")))));
//            Assert.Equal(
//                "c",
//                stringDeserializer.deserialize(
//                    null,
//                    rocksDBStore.Get(new Bytes(stringSerializer.Serialize(null, "3")))));
//        }

//        [Fact]
//        public void ShouldTogglePrepareForBulkloadSetting()
//        {
//            rocksDBStore.Init(context, rocksDBStore);
//            RocksDbStore.RocksDBBatchingRestoreCallback restoreListener =
//                (RocksDbStore.RocksDBBatchingRestoreCallback)rocksDBStore.batchingStateRestoreCallback;

//            restoreListener.onRestoreStart(null, null, 0, 0);
//            Assert.True("Should have set bulk loading to true", rocksDBStore.isPrepareForBulkload());

//            restoreListener.onRestoreEnd(null, null, 0);
//            Assert.False("Should have set bulk loading to false", rocksDBStore.isPrepareForBulkload());
//        }

//        [Fact]
//        public void ShouldTogglePrepareForBulkloadSettingWhenPrexistingSstFiles()
//        {
//            List<KeyValuePair<byte[], byte[]>> entries = GetKeyValueEntries();

//            rocksDBStore.Init(context, rocksDBStore);
//            context.restore(rocksDBStore.Name(), entries);

//            RocksDbStore.RocksDBBatchingRestoreCallback restoreListener =
//                (RocksDbStore.RocksDBBatchingRestoreCallback)rocksDBStore.batchingStateRestoreCallback;

//            restoreListener.onRestoreStart(null, null, 0, 0);
//            Assert.True("Should have not set bulk loading to true", rocksDBStore.isPrepareForBulkload());

//            restoreListener.onRestoreEnd(null, null, 0);
//            Assert.False("Should have set bulk loading to false", rocksDBStore.isPrepareForBulkload());
//        }

//        [Fact]
//        public void ShouldRestoreAll()
//        {
//            List<KeyValuePair<byte[], byte[]>> entries = GetKeyValueEntries();

//            rocksDBStore.Init(context, rocksDBStore);
//            context.restore(rocksDBStore.Name(), entries);

//            Assert.Equal(
//                "a",
//                stringDeserializer.Deserialize(
//                    null,
//                    rocksDBStore.Get(new Bytes(stringSerializer.Serialize(null, "1")))));
//            Assert.Equal(
//                "b",
//                stringDeserializer.Deserialize(
//                    null,
//                    rocksDBStore.Get(new Bytes(stringSerializer.Serialize(null, "2")))));
//            Assert.Equal(
//                "c",
//                stringDeserializer.Deserialize(
//                    null,
//                    rocksDBStore.Get(new Bytes(stringSerializer.Serialize(null, "3")))));
//        }

//        [Fact]
//        public void ShouldPutOnlyIfAbsentValue()
//        {
//            rocksDBStore.Init(context, rocksDBStore);
//            Bytes keyBytes = new Bytes(stringSerializer.Serialize(null, "one"));
//            byte[] valueBytes = stringSerializer.Serialize(null, "A");
//            byte[] valueBytesUpdate = stringSerializer.Serialize(null, "B");

//            rocksDBStore.putIfAbsent(keyBytes, valueBytes);
//            rocksDBStore.putIfAbsent(keyBytes, valueBytesUpdate);

//            string retrievedValue = stringDeserializer.deserialize(null, rocksDBStore.Get(keyBytes));
//            Assert.Equal("A", retrievedValue);
//        }

//        [Fact]
//        public void ShouldHandleDeletesOnRestoreAll()
//        {
//            List<KeyValuePair<byte[], byte[]>> entries = GetKeyValueEntries();
//            entries.Add(KeyValuePair.Create("1".getBytes(UTF_8), null));

//            rocksDBStore.Init(context, rocksDBStore);
//            context.restore(rocksDBStore.Name(), entries);

//            IKeyValueIterator<Bytes, byte[]> iterator = rocksDBStore.All();
//            HashSet<string> keys = new HashSet<>();

//            while (iterator.MoveNext())
//            {
//                keys.Add(stringDeserializer.deserialize(null, iterator.MoveNext().key.Get()));
//            }

//            Assert.Equal(keys, (Utils.mkSet("2", "3")));
//        }

//        [Fact]
//        public void ShouldHandleDeletesAndPutbackOnRestoreAll()
//        {
//            List<KeyValuePair<byte[], byte[]>> entries = new List<KeyValuePair<byte[], byte[]>>();
//            entries.Add(KeyValuePair.Create("1".getBytes(UTF_8), "a".getBytes(UTF_8)));
//            entries.Add(KeyValuePair.Create("2".getBytes(UTF_8), "b".getBytes(UTF_8)));
//            // this will be deleted
//            entries.Add(KeyValuePair.Create("1".getBytes(UTF_8), null));
//            entries.Add(KeyValuePair.Create("3".getBytes(UTF_8), "c".getBytes(UTF_8)));
//            // this will restore key "1" as WriteBatch applies updates in order
//            entries.Add(KeyValuePair.Create("1".getBytes(UTF_8), "restored".getBytes(UTF_8)));

//            rocksDBStore.Init(context, rocksDBStore);
//            context.restore(rocksDBStore.Name(), entries);

//            IKeyValueIterator<Bytes, byte[]> iterator = rocksDBStore.All();
//            HashSet<string> keys = new HashSet<>();

//            while (iterator.MoveNext())
//            {
//                keys.Add(stringDeserializer.deserialize(null, iterator.MoveNext().key.Get()));
//            }

//            Assert.Equal(keys, (Utils.mkSet("1", "2", "3")));

//            Assert.Equal(
//                "restored",
//                stringDeserializer.deserialize(
//                    null,
//                    rocksDBStore.Get(new Bytes(stringSerializer.Serialize(null, "1")))));
//            Assert.Equal(
//                "b",
//                stringDeserializer.deserialize(
//                    null,
//                    rocksDBStore.Get(new Bytes(stringSerializer.Serialize(null, "2")))));
//            Assert.Equal(
//                "c",
//                stringDeserializer.deserialize(
//                    null,
//                    rocksDBStore.Get(new Bytes(stringSerializer.Serialize(null, "3")))));
//        }

//        [Fact]
//        public void ShouldRestoreThenDeleteOnRestoreAll()
//        {
//            List<KeyValuePair<byte[], byte[]>> entries = GetKeyValueEntries();

//            rocksDBStore.Init(context, rocksDBStore);

//            context.restore(rocksDBStore.Name(), entries);

//            Assert.Equal(
//                "a",
//                stringDeserializer.deserialize(
//                    null,
//                    rocksDBStore.Get(new Bytes(stringSerializer.Serialize(null, "1")))));
//            Assert.Equal(
//                "b",
//                stringDeserializer.deserialize(
//                    null,
//                    rocksDBStore.Get(new Bytes(stringSerializer.Serialize(null, "2")))));
//            Assert.Equal(
//                "c",
//                stringDeserializer.deserialize(
//                    null,
//                    rocksDBStore.Get(new Bytes(stringSerializer.Serialize(null, "3")))));

//            entries.Clear();

//            entries.Add(KeyValuePair.Create("2".getBytes(UTF_8), "b".getBytes(UTF_8)));
//            entries.Add(KeyValuePair.Create("3".getBytes(UTF_8), "c".getBytes(UTF_8)));
//            entries.Add(KeyValuePair.Create("1".getBytes(UTF_8), null));

//            context.restore(rocksDBStore.Name(), entries);

//            IKeyValueIterator<Bytes, byte[]> iterator = rocksDBStore.All();
//            HashSet<string> keys = new HashSet<>();

//            while (iterator.MoveNext())
//            {
//                keys.Add(stringDeserializer.deserialize(null, iterator.MoveNext().key.Get()));
//            }

//            Assert.Equal(keys, (Utils.mkSet("2", "3")));
//        }

//        [Fact]
//        public void ShouldThrowNullPointerExceptionOnNullPut()
//        {
//            rocksDBStore.Init(context, rocksDBStore);
//            try
//            {
//                rocksDBStore.Put(null, stringSerializer.Serialize(null, "someVal"));
//                Assert.True(false, "Should have thrown NullReferenceException on null Put()");
//            }
//            catch (NullReferenceException e)
//            {
//                // this is good
//            }
//        }

//        [Fact]
//        public void ShouldThrowNullPointerExceptionOnNullPutAll()
//        {
//            rocksDBStore.Init(context, rocksDBStore);
//            try
//            {
//                rocksDBStore.Put(null, stringSerializer.Serialize(null, "someVal"));
//                Assert.True(false, "Should have thrown NullReferenceException on null Put()");
//            }
//            catch (NullReferenceException e)
//            {
//                // this is good
//            }
//        }

//        [Fact]
//        public void ShouldThrowNullPointerExceptionOnNullGet()
//        {
//            rocksDBStore.Init(context, rocksDBStore);
//            try
//            {
//                rocksDBStore.Get(null);
//                Assert.True(false, "Should have thrown NullReferenceException on null get()");
//            }
//            catch (NullReferenceException e)
//            {
//                // this is good
//            }
//        }

//        [Fact]
//        public void ShouldThrowNullPointerExceptionOnDelete()
//        {
//            rocksDBStore.Init(context, rocksDBStore);
//            try
//            {
//                rocksDBStore.delete(null);
//                Assert.True(false, "Should have thrown NullReferenceException on deleting null key");
//            }
//            catch (NullReferenceException e)
//            {
//                // this is good
//            }
//        }

//        [Fact]
//        public void ShouldThrowNullPointerExceptionOnRange()
//        {
//            rocksDBStore.Init(context, rocksDBStore);
//            try
//            {
//                rocksDBStore.Range(null, new Bytes(stringSerializer.Serialize(null, "2")));
//                Assert.True(false, "Should have thrown NullReferenceException on deleting null key");
//            }
//            catch (NullReferenceException e)
//            {
//                // this is good
//            }
//        }

//        [Fact]// (expected = ProcessorStateException)
//        public void ShouldThrowProcessorStateExceptionOnPutDeletedDir()
//        { //throws IOException
//            rocksDBStore.Init(context, rocksDBStore);
//            Utils.delete(dir);
//            rocksDBStore.Put(
//                new Bytes(stringSerializer.Serialize(null, "anyKey")),
//                stringSerializer.Serialize(null, "anyValue"));
//            rocksDBStore.Flush();
//        }

//        [Fact]
//        public void ShouldHandleToggleOfEnablingBloomFilters()
//        {

//            StreamsConfig props = StreamsTestUtils.getStreamsConfig();
//            props.Put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, TestingBloomFilterRocksDBConfigSetter);
//            rocksDBStore = GetRocksDBStore();
//            dir = TestUtils.GetTempDirectory();
//            context = new InternalMockProcessorContext(dir,
//                Serdes.String(),
//                Serdes.String(),
//                new StreamsConfig(props));

//            enableBloomFilters = false;
//            rocksDBStore.Init(context, rocksDBStore);

//            List<string> expectedValues = new List<string>();
//            expectedValues.Add("a");
//            expectedValues.Add("b");
//            expectedValues.Add("c");

//            List<KeyValuePair<byte[], byte[]>> keyValues = GetKeyValueEntries();
//            foreach (KeyValuePair<byte[], byte[]> keyValue in keyValues)
//            {
//                rocksDBStore.Put(new Bytes(keyValue.key), keyValue.value);
//            }

//            int expectedIndex = 0;
//            foreach (KeyValuePair<byte[], byte[]> keyValue in keyValues)
//            {
//                byte[] valBytes = rocksDBStore.Get(new Bytes(keyValue.key));
//                Assert.Equal(new string(valBytes, UTF_8), (expectedValues.Get(expectedIndex++)));
//            }
//            Assert.False(TestingBloomFilterRocksDBConfigSetter.bloomFiltersSet);

//            rocksDBStore.Close();
//            expectedIndex = 0;

//            // reopen with Bloom Filters enabled
//            // should open fine without errors
//            enableBloomFilters = true;
//            rocksDBStore.Init(context, rocksDBStore);

//            foreach (KeyValuePair<byte[], byte[]> keyValue in keyValues)
//            {
//                byte[] valBytes = rocksDBStore.Get(new Bytes(keyValue.key));
//                Assert.Equal(new string(valBytes, UTF_8), (expectedValues.Get(expectedIndex++)));
//            }

//            Assert.True(TestingBloomFilterRocksDBConfigSetter.bloomFiltersSet);
//        }

//        public static class MockRocksDbConfigSetter : RocksDBConfigSetter
//        {
//            static bool called;


//            public void SetConfig(string storeName, Options options, Dictionary<string, object> configs)
//            {
//                called = true;

//                options.setLevel0FileNumCompactionTrigger(10);
//            }
//        }

//        public static class TestingBloomFilterRocksDBConfigSetter : RocksDBConfigSetter
//        {

//            static bool bloomFiltersSet;
//            static Filter filter;
//            static Cache cache;


//            public void SetConfig(string storeName, Options options, Dictionary<string, object> configs)
//            {
//                BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
//                cache = new LRUCache(50 * 1024 * 1024L);
//                tableConfig.setBlockCache(cache);
//                tableConfig.setBlockSize(4096L);
//                if (enableBloomFilters)
//                {
//                    filter = new BloomFilter();
//                    tableConfig.setFilter(filter);
//                    options.optimizeFiltersForHits();
//                    bloomFiltersSet = true;
//                }
//                else
//                {
//                    options.setOptimizeFiltersForHits(false);
//                    bloomFiltersSet = false;
//                }

//                options.setTableFormatConfig(tableConfig);
//            }


//            public void Close(string storeName, Options options)
//            {
//                if (filter != null)
//                {
//                    filter.Close();
//                }
//                cache.Close();
//            }
//        }

//        private List<KeyValuePair<byte[], byte[]>> GetKeyValueEntries()
//        {
//            List<KeyValuePair<byte[], byte[]>> entries = new List<KeyValuePair<byte[], byte[]>>();
//            entries.Add(KeyValuePair.Create("1".getBytes(UTF_8), "a".getBytes(UTF_8)));
//            entries.Add(KeyValuePair.Create("2".getBytes(UTF_8), "b".getBytes(UTF_8)));
//            entries.Add(KeyValuePair.Create("3".getBytes(UTF_8), "c".getBytes(UTF_8)));
//            return entries;
//        }

//    }
//}
///*






//*

//*





//*/














































