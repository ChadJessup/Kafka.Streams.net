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

        public void Put(byte[] key,
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

        public void PrepareBatch(
            List<KeyValuePair<Bytes, byte[]>> entries,
            WriteBatch batch)
        {
            foreach (KeyValuePair<Bytes, byte[]> entry in entries)
            {
                AddToBatch(entry.Key.Get(), entry.Value, batch);
            }
        }

        public byte[] Get(byte[] key)
        {
            return db.Get(key, columnFamily);
        }

        public byte[] GetOnly(byte[] key)
        {
            return db.Get(key, columnFamily);
        }

        public IKeyValueIterator<Bytes, byte[]> Range(
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


        public IKeyValueIterator<Bytes, byte[]> All()
        {
            Iterator innerIterWithTimestamp = db.NewIterator(columnFamily);
            innerIterWithTimestamp.SeekToFirst();
            return new RocksDbIterator(
                name,
                innerIterWithTimestamp,
                openIterators);
        }


        public long ApproximateNumEntries()
        {
            return long.Parse(db.GetProperty("rocksdb.estimate-num-keys", columnFamily));
        }


        public void Flush()
        {
            //db.flush(fOptions, columnFamily);
        }


        public void PrepareBatchForRestore(List<KeyValuePair<byte[], byte[]>> records,
                                           WriteBatch batch)
        {
            foreach (KeyValuePair<byte[], byte[]> record in records)
            {
                AddToBatch(record.Key, record.Value, batch);
            }
        }

        public void AddToBatch(
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


        public void Close()
        {
            //columnFamily.Close();
        }

        public void ToggleDbForBulkLoading()
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