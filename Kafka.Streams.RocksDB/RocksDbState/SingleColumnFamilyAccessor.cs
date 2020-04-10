using Kafka.Streams.Errors;
using Kafka.Streams.State.KeyValues;
using RocksDbSharp;
using System.Collections.Generic;

namespace Kafka.Streams.RocksDbState
{
    public class SingleColumnFamilyAccessor : IRocksDbAccessor
    {
        private readonly string Name;
        private readonly RocksDb db;
        private readonly WriteOptions wOptions;
        private readonly HashSet<IKeyValueIterator<Bytes, byte[]>> openIterators;
        private readonly ColumnFamilyHandle columnFamily;

        public SingleColumnFamilyAccessor(
            string Name,
            RocksDb db,
            WriteOptions writeOptions,
            HashSet<IKeyValueIterator<Bytes, byte[]>> openIterators,
            ColumnFamilyHandle columnFamily)
        {
            this.Name = Name;
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
                    this.db.Remove(key, this.columnFamily, this.wOptions);
                }
                catch (RocksDbException e)
                {
                    // string string.Format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                    throw new ProcessorStateException("Error while removing key from store " + this.Name, e);
                }
            }
            else
            {
                try
                {
                    this.db.Put(key, value, this.columnFamily, this.wOptions);
                }
                catch (RocksDbException e)
                {
                    // string string.Format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                    throw new ProcessorStateException("Error while putting key/value into store " + this.Name, e);
                }
            }
        }

        public void PrepareBatch(
            List<KeyValuePair<Bytes, byte[]>> entries,
            WriteBatch batch)
        {
            foreach (KeyValuePair<Bytes, byte[]> entry in entries)
            {
                this.AddToBatch(entry.Key.Get(), entry.Value, batch);
            }
        }

        public byte[] Get(byte[] key)
        {
            return this.db.Get(key, this.columnFamily);
        }

        public byte[] GetOnly(byte[] key)
        {
            return this.db.Get(key, this.columnFamily);
        }

        public IKeyValueIterator<Bytes, byte[]> Range(
            Bytes from,
            Bytes to)
        {
            return new RocksDbRangeIterator(
                this.Name,
                this.db.NewIterator(this.columnFamily),
                this.openIterators,
                from,
                to);
        }


        public IKeyValueIterator<Bytes, byte[]> All()
        {
            Iterator innerIterWithTimestamp = this.db.NewIterator(this.columnFamily);
            innerIterWithTimestamp.SeekToFirst();
            return new RocksDbIterator(
                this.Name,
                innerIterWithTimestamp,
                this.openIterators);
        }


        public long ApproximateNumEntries()
        {
            return long.Parse(this.db.GetProperty("rocksdb.estimate-num-keys", this.columnFamily));
        }


        public void Flush()
        {
            //db.Flush(fOptions, columnFamily);
        }


        public void PrepareBatchForRestore(List<KeyValuePair<byte[], byte[]>> records,
                                           WriteBatch batch)
        {
            foreach (KeyValuePair<byte[], byte[]> record in records)
            {
                this.AddToBatch(record.Key, record.Value, batch);
            }
        }

        public void AddToBatch(
            byte[] key,
            byte[] value,
            WriteBatch batch)
        {
            if (value == null)
            {
                batch.Delete(key, this.columnFamily);
            }
            else
            {
                batch.Put(key, value, this.columnFamily);
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
                throw new ProcessorStateException("Error while range compacting during restoring  store " + this.Name, e);
            }
        }
    }
}