using Kafka.Streams.Errors;
using Kafka.Streams.State.KeyValues;
using RocksDbSharp;
using System.Collections.Generic;

namespace Kafka.Streams.State.RocksDbState
{
    public class SingleColumnFamilyAccessor : IRocksDbAccessor
    {
        private readonly string name;
        private readonly RocksDb db;
        private readonly WriteOptions wOptions;
        private readonly HashSet<IKeyValueIterator<Bytes, byte[]>> openIterators;
        private readonly ColumnFamilyHandle columnFamily;

        public SingleColumnFamilyAccessor(
            string name,
            RocksDb db,
            WriteOptions writeOptions,
            HashSet<IKeyValueIterator<Bytes, byte[]>> openIterators,
            ColumnFamilyHandle columnFamily)
        {
            this.name = name;
            this.db = db;
            this.wOptions = writeOptions;
            this.openIterators = openIterators;
            this.columnFamily = columnFamily;
        }

        public void put(byte[] key,
                        byte[] value)
        {
            if (value == null)
            {
                try
                {
                    db.Remove(key, columnFamily, wOptions);
                }
                catch (RocksDbException e)
                {
                    // string string.Format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                    throw new ProcessorStateException("Error while removing key from store " + name, e);
                }
            }
            else
            {
                try
                {
                    db.Put(key, value, columnFamily, wOptions);
                }
                catch (RocksDbException e)
                {
                    // string string.Format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                    throw new ProcessorStateException("Error while putting key/value into store " + name, e);
                }
            }
        }

        public void prepareBatch(
            List<KeyValue<Bytes, byte[]>> entries,
            WriteBatch batch)
        {
            foreach (KeyValue<Bytes, byte[]> entry in entries)
            {
                addToBatch(entry.Key.get(), entry.Value, batch);
            }
        }

        public byte[] get(byte[] key)
        {
            return db.Get(key, columnFamily);
        }

        public byte[] getOnly(byte[] key)
        {
            return db.Get(key, columnFamily);
        }

        public IKeyValueIterator<Bytes, byte[]> range(
            Bytes from,
            Bytes to)
        {
            return new RocksDbRangeIterator(
                name,
                db.NewIterator(columnFamily),
                openIterators,
                from,
                to);
        }


        public IKeyValueIterator<Bytes, byte[]> all()
        {
            Iterator innerIterWithTimestamp = db.NewIterator(columnFamily);
            innerIterWithTimestamp.SeekToFirst();
            return new RocksDbIterator(
                name,
                innerIterWithTimestamp,
                openIterators);
        }


        public long approximateNumEntries()
        {
            return long.Parse(db.GetProperty("rocksdb.estimate-num-keys", columnFamily));
        }


        public void flush()
        {
            //db.flush(fOptions, columnFamily);
        }


        public void prepareBatchForRestore(List<KeyValue<byte[], byte[]>> records,
                                           WriteBatch batch)
        {
            foreach (KeyValue<byte[], byte[]> record in records)
            {
                addToBatch(record.Key, record.Value, batch);
            }
        }

        public void addToBatch(
            byte[] key,
            byte[] value,
            WriteBatch batch)
        {
            if (value == null)
            {
                batch.Delete(key, columnFamily);
            }
            else
            {
                batch.Put(key, value, columnFamily);
            }
        }


        public void close()
        {
            //columnFamily.Close();
        }

        public void toggleDbForBulkLoading()
        {
            try
            {
                //db.compactRange(columnFamily, true, 1, 0);
            }
            catch (RocksDbException e)
            {
                throw new ProcessorStateException("Error while range compacting during restoring  store " + name, e);
            }
        }
    }
}