//using Kafka.Common.Utils;
//using Kafka.Streams.Errors;
//using RocksDbSharp;
//using System.Collections.Generic;

//namespace Kafka.Streams.State.Internals
//{
//    public class DualColumnFamilyAccessor : IRocksDbAccessor
//    {
//        private ColumnFamilyHandle oldColumnFamily;
//        private ColumnFamilyHandle newColumnFamily;

//        private DualColumnFamilyAccessor(ColumnFamilyHandle oldColumnFamily,
//                                         ColumnFamilyHandle newColumnFamily)
//        {
//            this.oldColumnFamily = oldColumnFamily;
//            this.newColumnFamily = newColumnFamily;
//        }


//        public void put(byte[] key,
//                        byte[] valueWithTimestamp)
//        {
//            if (valueWithTimestamp == null)
//            {
//                try
//                {
//                    db.delete(oldColumnFamily, wOptions, key);
//                }
//                catch (RocksDbException e)
//                {
//                    // string string.Format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
//                    throw new ProcessorStateException("Error while removing key from store " + name, e);
//                }
//                try
//                {
//                    db.delete(newColumnFamily, wOptions, key);
//                }
//                catch (RocksDbException e)
//                {
//                    // string string.Format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
//                    throw new ProcessorStateException("Error while removing key from store " + name, e);
//                }
//            }
//            else
//            {
//                try
//                {
//                    db.delete(oldColumnFamily, wOptions, key);
//                }
//                catch (RocksDbException e)
//                {
//                    // string string.Format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
//                    throw new ProcessorStateException("Error while removing key from store " + name, e);
//                }
//                try
//                {
//                    db.Add(newColumnFamily, wOptions, key, valueWithTimestamp);
//                }
//                catch (RocksDbException e)
//                {
//                    // string string.Format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
//                    throw new ProcessorStateException("Error while putting key/value into store " + name, e);
//                }
//            }
//        }


//        public void prepareBatch(List<KeyValue<Bytes, byte[]>> entries,
//                                 WriteBatch batch)
//        {
//            foreach (KeyValue<Bytes, byte[]> entry in entries)
//            {
//                entry.key = entry.key ?? throw new System.ArgumentNullException("key cannot be null", nameof(entry.key));
//                addToBatch(entry.key(), entry.value, batch);
//            }
//        }


//        public byte[] get(byte[] key)
//        {
//            byte[] valueWithTimestamp = db[newColumnFamily, key];
//            if (valueWithTimestamp != null)
//            {
//                return valueWithTimestamp;
//            }

//            byte[] plainValue = db[oldColumnFamily, key];
//            if (plainValue != null)
//            {
//                byte[] valueWithUnknownTimestamp = convertToTimestampedFormat(plainValue);
//                // this does only work, because the changelog topic contains correct data already
//                // for other string.Format changes, we cannot take this short cut and can only migrate data
//                // from old to new store on put()
//                put(key, valueWithUnknownTimestamp);
//                return valueWithUnknownTimestamp;
//            }

//            return null;
//        }


//        public byte[] getOnly(byte[] key)
//        {
//            byte[] valueWithTimestamp = db[newColumnFamily, key];
//            if (valueWithTimestamp != null)
//            {
//                return valueWithTimestamp;
//            }

//            byte[] plainValue = db[oldColumnFamily, key];
//            if (plainValue != null)
//            {
//                return convertToTimestampedFormat(plainValue);
//            }

//            return null;
//        }


//        public IKeyValueIterator<Bytes, byte[]> range(Bytes from,
//                                                     Bytes to)
//        {
//            return new RocksDbDualCFRangeIterator(
//                name,
//                db.newIterator(newColumnFamily),
//                db.newIterator(oldColumnFamily),
//                from,
//                to);
//        }


//        public IKeyValueIterator<Bytes, byte[]> all()
//        {
//            RocksIterator innerIterWithTimestamp = db.newIterator(newColumnFamily);
//            innerIterWithTimestamp.seekToFirst();
//            RocksIterator innerIterNoTimestamp = db.newIterator(oldColumnFamily);
//            innerIterNoTimestamp.seekToFirst();
//            return new RocksDbDualCFIterator(name, innerIterWithTimestamp, innerIterNoTimestamp);
//        }


//        public long approximateNumEntries()
//        {
//            return db.getLongProperty(oldColumnFamily, "rocksdb.estimate-num-keys")
//                + db.getLongProperty(newColumnFamily, "rocksdb.estimate-num-keys");
//        }


//        public void flush()
//        {
//            db.flush(fOptions, oldColumnFamily);
//            db.flush(fOptions, newColumnFamily);
//        }


//        public void prepareBatchForRestore(List<KeyValue<byte[], byte[]>> records,
//                                           WriteBatch batch)
//        {
//            foreach (KeyValue<byte[], byte[]> record in records)
//            {
//                addToBatch(record.key, record.value, batch);
//            }
//        }


//        public void addToBatch(byte[] key,
//                               byte[] value,
//                               WriteBatch batch)
//        {
//            if (value == null)
//            {
//                batch.delete(oldColumnFamily, key);
//                batch.delete(newColumnFamily, key);
//            }
//            else
//            {
//                batch.delete(oldColumnFamily, key);
//                batch.Add(newColumnFamily, key, value);
//            }
//        }


//        public void close()
//        {
//            oldColumnFamily.close();
//            newColumnFamily.close();
//        }



//        public void toggleDbForBulkLoading()
//        {
//            try
//            {
//                db.compactRange(oldColumnFamily, true, 1, 0);
//            }
//            catch (RocksDbException e)
//            {
//                throw new ProcessorStateException("Error while range compacting during restoring  store " + name, e);
//            }
//            try
//            {
//                db.compactRange(newColumnFamily, true, 1, 0);
//            }
//            catch (RocksDbException e)
//            {
//                throw new ProcessorStateException("Error while range compacting during restoring  store " + name, e);
//            }
//        }
//    }
//}
