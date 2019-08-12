//using Kafka.Common.Utils;
//using Kafka.Streams.State.Interfaces;
//using RocksDbSharp;
//using System.Collections.Generic;
//using Kafka.Streams.Errors;
//using System;

//namespace Kafka.Streams.State.Internals
//{
//    public class SingleColumnFamilyAccessor : IRocksDbAccessor
//    {
//        private ColumnFamilyHandle columnFamily;

//        public SingleColumnFamilyAccessor(ColumnFamilyHandle columnFamily)
//        {
//            this.columnFamily = columnFamily;
//        }


//        public void put(byte[] key,
//                        byte[] value)
//        {
//            if (value == null)
//            {
//                try
//                {
//                    db.delete(columnFamily, wOptions, key);
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
//                    db.Add(columnFamily, wOptions, key, value);
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
//                addToBatch(entry.key, entry.value, batch);
//            }
//        }


//        public byte[] get(byte[] key)
//        {
//            return db[columnFamily, key];
//        }


//        public byte[] getOnly(byte[] key)
//        {
//            return db[columnFamily, key];
//        }


//        public IKeyValueIterator<Bytes, byte[]> range(Bytes from,
//                                                     Bytes to)
//        {
//            return new RocksDbRangeIterator(
//                name,
//                db.newIterator(columnFamily),
//                openIterators,
//                from,
//                to);
//        }


//        public IKeyValueIterator<Bytes, byte[]> all()
//        {
//            RocksIterator innerIterWithTimestamp = db.newIterator(columnFamily);
//            innerIterWithTimestamp.seekToFirst();
//            return new RocksDbIterator(name, innerIterWithTimestamp, openIterators);
//        }


//        public long approximateNumEntries()
//        {
//            return db.getLongProperty(columnFamily, "rocksdb.estimate-num-keys");
//        }


//        public void flush()
//        {
//            db.flush(fOptions, columnFamily);
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
//                batch.Delete(columnFamily, key);
//            }
//            else
//            {
//                batch.Add(columnFamily, key, value);
//            }
//        }


//        public void close()
//        {
//            columnFamily.close();
//        }



//        public void toggleDbForBulkLoading()
//        {
//            try
//            {
//                db.compactRange(columnFamily, true, 1, 0);
//            }
//            catch (RocksDbException e)
//            {
//                throw new ProcessorStateException("Error while range compacting during restoring  store " + name, e);
//            }
//        }
//    }
//}