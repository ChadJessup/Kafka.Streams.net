namespace Kafka.Streams.Tests.State.Internals
{
    /*






    *

    *





    */


























    public class RocksDBTimestampedStoreTest : RocksDBStoreTest
    {

        RocksDBStore GetRocksDBStore()
        {
            return new RocksDBTimestampedStore(DB_NAME);
        }

        [Xunit.Fact]
        public void ShouldMigrateDataFromDefaultToTimestampColumnFamily()
        {// throws Exception
            PrepareOldStore();

            LogCaptureAppender.setClassLoggerToDebug(RocksDBTimestampedStore);

            LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();
            rocksDBStore.init(context, rocksDBStore);
            Assert.Equal(appender.getMessages(), hasItem("Opening store " + DB_NAME + " in upgrade mode"));
            LogCaptureAppender.Unregister(appender);

            // approx: 7 entries on old CF, 0 in new CF
            Assert.Equal(rocksDBStore.approximateNumEntries(), (7L));

            // get()

            // should be no-op on both CF
            Assert.Equal(rocksDBStore.get(new Bytes("unknown".getBytes())), new IsNull<>());
            // approx: 7 entries on old CF, 0 in new CF
            Assert.Equal(rocksDBStore.approximateNumEntries(), (7L));

            // should migrate key1 from old to new CF
            // must return timestamp plus value, ie, it's not 1 byte but 9 bytes
            Assert.Equal(rocksDBStore.get(new Bytes("key1".getBytes())).Length, (8 + 1));
            // one delete on old CF, one put on new CF
            // approx: 6 entries on old CF, 1 in new CF
            Assert.Equal(rocksDBStore.approximateNumEntries(), (7L));

            // put()

            // should migrate key2 from old to new CF with new value
            rocksDBStore.put(new Bytes("key2".getBytes()), "timestamp+22".getBytes());
            // one delete on old CF, one put on new CF
            // approx: 5 entries on old CF, 2 in new CF
            Assert.Equal(rocksDBStore.approximateNumEntries(), (7L));

            // should delete key3 from old and new CF
            rocksDBStore.put(new Bytes("key3".getBytes()), null);
            // count is off by one, due to two delete operations (even if one does not delete anything)
            // approx: 4 entries on old CF, 1 in new CF
            Assert.Equal(rocksDBStore.approximateNumEntries(), (5L));

            // should add new key8 to new CF
            rocksDBStore.put(new Bytes("key8".getBytes()), "timestamp+88888888".getBytes());
            // one delete on old CF, one put on new CF
            // approx: 3 entries on old CF, 2 in new CF
            Assert.Equal(rocksDBStore.approximateNumEntries(), (5L));

            // putIfAbsent()

            // should migrate key4 from old to new CF with old value
            Assert.Equal(rocksDBStore.putIfAbsent(new Bytes("key4".getBytes()), "timestamp+4444".getBytes()).Length, (8 + 4));
            // one delete on old CF, one put on new CF
            // approx: 2 entries on old CF, 3 in new CF
            Assert.Equal(rocksDBStore.approximateNumEntries(), (5L));

            // should add new key11 to new CF
            Assert.Equal(rocksDBStore.putIfAbsent(new Bytes("key11".getBytes()), "timestamp+11111111111".getBytes()), new IsNull<>());
            // one delete on old CF, one put on new CF
            // approx: 1 entries on old CF, 4 in new CF
            Assert.Equal(rocksDBStore.approximateNumEntries(), (5L));

            // should not delete key5 but migrate to new CF
            Assert.Equal(rocksDBStore.putIfAbsent(new Bytes("key5".getBytes()), null).Length, (8 + 5));
            // one delete on old CF, one put on new CF
            // approx: 0 entries on old CF, 5 in new CF
            Assert.Equal(rocksDBStore.approximateNumEntries(), (5L));

            // should be no-op on both CF
            Assert.Equal(rocksDBStore.putIfAbsent(new Bytes("key12".getBytes()), null), new IsNull<>());
            // two delete operation, however, only one is counted because old CF count was zero before already
            // approx: 0 entries on old CF, 4 in new CF
            Assert.Equal(rocksDBStore.approximateNumEntries(), (4L));

            // delete()

            // should delete key6 from old and new CF
            Assert.Equal(rocksDBStore.delete(new Bytes("key6".getBytes())).Length, (8 + 6));
            // two delete operation, however, only one is counted because old CF count was zero before already
            // approx: 0 entries on old CF, 3 in new CF
            Assert.Equal(rocksDBStore.approximateNumEntries(), (3L));


            IteratorsShouldNotMigrateData();
            Assert.Equal(rocksDBStore.approximateNumEntries(), (3L));

            rocksDBStore.close();

            VerifyOldAndNewColumnFamily();
        }

        private void IteratorsShouldNotMigrateData()
        {
            // iterating should not migrate any data, but return all key over both CF (plus surrogate timestamps for old CF)
            KeyValueIterator<Bytes, byte[]> itAll = rocksDBStore.all();
            {
                KeyValuePair<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key1".getBytes(), keyValue.key.get());
                // unknown timestamp == -1 plus value == 1
                assertArrayEquals(new byte[] { -1, -1, -1, -1, -1, -1, -1, -1, '1' }, keyValue.value);
            }
            {
                KeyValuePair<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key11".getBytes(), keyValue.key.get());
                assertArrayEquals(new byte[] { 't', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1' }, keyValue.value);
            }
            {
                KeyValuePair<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key2".getBytes(), keyValue.key.get());
                assertArrayEquals(new byte[] { 't', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '2', '2' }, keyValue.value);
            }
            {
                KeyValuePair<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key4".getBytes(), keyValue.key.get());
                // unknown timestamp == -1 plus value == 4444
                assertArrayEquals(new byte[] { -1, -1, -1, -1, -1, -1, -1, -1, '4', '4', '4', '4' }, keyValue.value);
            }
            {
                KeyValuePair<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key5".getBytes(), keyValue.key.get());
                // unknown timestamp == -1 plus value == 55555
                assertArrayEquals(new byte[] { -1, -1, -1, -1, -1, -1, -1, -1, '5', '5', '5', '5', '5' }, keyValue.value);
            }
            {
                KeyValuePair<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key7".getBytes(), keyValue.key.get());
                // unknown timestamp == -1 plus value == 7777777
                assertArrayEquals(new byte[] { -1, -1, -1, -1, -1, -1, -1, -1, '7', '7', '7', '7', '7', '7', '7' }, keyValue.value);
            }
            {
                KeyValuePair<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key8".getBytes(), keyValue.key.get());
                assertArrayEquals(new byte[] { 't', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '8', '8', '8', '8', '8', '8', '8', '8' }, keyValue.value);
            }
            Assert.False(itAll.hasNext());
            itAll.close();

            KeyValueIterator<Bytes, byte[]> it =
                rocksDBStore.range(new Bytes("key2".getBytes()), new Bytes("key5".getBytes()));
            {
                KeyValuePair<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key2".getBytes(), keyValue.key.get());
                assertArrayEquals(new byte[] { 't', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '2', '2' }, keyValue.value);
            }
            {
                KeyValuePair<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key4".getBytes(), keyValue.key.get());
                // unknown timestamp == -1 plus value == 4444
                assertArrayEquals(new byte[] { -1, -1, -1, -1, -1, -1, -1, -1, '4', '4', '4', '4' }, keyValue.value);
            }
            {
                KeyValuePair<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key5".getBytes(), keyValue.key.get());
                // unknown timestamp == -1 plus value == 55555
                assertArrayEquals(new byte[] { -1, -1, -1, -1, -1, -1, -1, -1, '5', '5', '5', '5', '5' }, keyValue.value);
            }
            Assert.False(it.hasNext());
            it.close();
        }

        private void VerifyOldAndNewColumnFamily()
        {// throws Exception
            DBOptions dbOptions = new DBOptions();
            ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();

            List<ColumnFamilyDescriptor> columnFamilyDescriptors = asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
                new ColumnFamilyDescriptor("keyValueWithTimestamp".getBytes(StandardCharsets.UTF_8), columnFamilyOptions));
            List<ColumnFamilyHandle> columnFamilies = new ArrayList<>(columnFamilyDescriptors.Count);

            RocksDB db = RocksDB.open(
                dbOptions,
                new File(new File(context.stateDir(), "rocksdb"), DB_NAME).getAbsolutePath(),
                columnFamilyDescriptors,
                columnFamilies);

            ColumnFamilyHandle noTimestampColumnFamily = columnFamilies.get(0);
            ColumnFamilyHandle withTimestampColumnFamily = columnFamilies.get(1);

            Assert.Equal(db.get(noTimestampColumnFamily, "unknown".getBytes()), new IsNull<>());
            Assert.Equal(db.get(noTimestampColumnFamily, "key1".getBytes()), new IsNull<>());
            Assert.Equal(db.get(noTimestampColumnFamily, "key2".getBytes()), new IsNull<>());
            Assert.Equal(db.get(noTimestampColumnFamily, "key3".getBytes()), new IsNull<>());
            Assert.Equal(db.get(noTimestampColumnFamily, "key4".getBytes()), new IsNull<>());
            Assert.Equal(db.get(noTimestampColumnFamily, "key5".getBytes()), new IsNull<>());
            Assert.Equal(db.get(noTimestampColumnFamily, "key6".getBytes()), new IsNull<>());
            Assert.Equal(db.get(noTimestampColumnFamily, "key7".getBytes()).Length, (7));
            Assert.Equal(db.get(noTimestampColumnFamily, "key8".getBytes()), new IsNull<>());
            Assert.Equal(db.get(noTimestampColumnFamily, "key11".getBytes()), new IsNull<>());
            Assert.Equal(db.get(noTimestampColumnFamily, "key12".getBytes()), new IsNull<>());

            Assert.Equal(db.get(withTimestampColumnFamily, "unknown".getBytes()), new IsNull<>());
            Assert.Equal(db.get(withTimestampColumnFamily, "key1".getBytes()).Length, (8 + 1));
            Assert.Equal(db.get(withTimestampColumnFamily, "key2".getBytes()).Length, (12));
            Assert.Equal(db.get(withTimestampColumnFamily, "key3".getBytes()), new IsNull<>());
            Assert.Equal(db.get(withTimestampColumnFamily, "key4".getBytes()).Length, (8 + 4));
            Assert.Equal(db.get(withTimestampColumnFamily, "key5".getBytes()).Length, (8 + 5));
            Assert.Equal(db.get(withTimestampColumnFamily, "key6".getBytes()), new IsNull<>());
            Assert.Equal(db.get(withTimestampColumnFamily, "key7".getBytes()), new IsNull<>());
            Assert.Equal(db.get(withTimestampColumnFamily, "key8".getBytes()).Length, (18));
            Assert.Equal(db.get(withTimestampColumnFamily, "key11".getBytes()).Length, (21));
            Assert.Equal(db.get(withTimestampColumnFamily, "key12".getBytes()), new IsNull<>());

            db.close();

            // check that still in upgrade mode
            LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();
            rocksDBStore.init(context, rocksDBStore);
            Assert.Equal(appender.getMessages(), hasItem("Opening store " + DB_NAME + " in upgrade mode"));
            LogCaptureAppender.Unregister(appender);
            rocksDBStore.close();

            // clear old CF
            columnFamilies.Clear();
            db = RocksDB.open(
                dbOptions,
                new File(new File(context.stateDir(), "rocksdb"), DB_NAME).getAbsolutePath(),
                columnFamilyDescriptors,
                columnFamilies);

            noTimestampColumnFamily = columnFamilies.get(0);
            db.delete(noTimestampColumnFamily, "key7".getBytes());
            db.close();

            // check that still in regular mode
            appender = LogCaptureAppender.CreateAndRegister();
            rocksDBStore.init(context, rocksDBStore);
            Assert.Equal(appender.getMessages(), hasItem("Opening store " + DB_NAME + " in regular mode"));
            LogCaptureAppender.Unregister(appender);
        }

        private void PrepareOldStore()
        {
            RocksDBStore keyValueStore = new RocksDBStore(DB_NAME);
            keyValueStore.init(context, keyValueStore);

            keyValueStore.put(new Bytes("key1".getBytes()), "1".getBytes());
            keyValueStore.put(new Bytes("key2".getBytes()), "22".getBytes());
            keyValueStore.put(new Bytes("key3".getBytes()), "333".getBytes());
            keyValueStore.put(new Bytes("key4".getBytes()), "4444".getBytes());
            keyValueStore.put(new Bytes("key5".getBytes()), "55555".getBytes());
            keyValueStore.put(new Bytes("key6".getBytes()), "666666".getBytes());
            keyValueStore.put(new Bytes("key7".getBytes()), "7777777".getBytes());

            keyValueStore.close();
        }

    }
}
/*






*

*





*/


























