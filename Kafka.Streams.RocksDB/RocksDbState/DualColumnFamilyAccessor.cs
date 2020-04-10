using Kafka.Streams.Errors;
using Kafka.Streams.Internals;
using Kafka.Streams.State.KeyValues;
using RocksDbSharp;
using System.Collections.Generic;

namespace Kafka.Streams.RocksDbState
{
    public class DualColumnFamilyAccessor : IRocksDbAccessor
    {
        private readonly ColumnFamilyHandle oldColumnFamily;
        private readonly ColumnFamilyHandle newColumnFamily;
        private readonly RocksDb db;
        private readonly WriteOptions wOptions;
        private readonly string Name;

        public DualColumnFamilyAccessor(
            string Name,
            RocksDb db,
            WriteOptions writeOptions,
            ColumnFamilyHandle oldColumnFamily,
            ColumnFamilyHandle newColumnFamily)
        {
            this.db = db;
            this.wOptions = writeOptions;

            this.oldColumnFamily = oldColumnFamily;
            this.newColumnFamily = newColumnFamily;
        }

        public void Put(
            byte[] key,
            byte[] valueWithTimestamp)
        {
            if (valueWithTimestamp == null)
            {
                try
                {
                    this.db.Remove(key, this.oldColumnFamily, this.wOptions);
                }
                catch (RocksDbException e)
                {
                    // string string.Format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                    throw new ProcessorStateException("Error while removing key from store " + this.Name, e);
                }

                try
                {
                    this.db.Remove(key, this.newColumnFamily, this.wOptions);
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
                    this.db.Remove(key, this.oldColumnFamily, this.wOptions);
                }
                catch (RocksDbException e)
                {
                    // string string.Format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                    throw new ProcessorStateException("Error while removing key from store " + this.Name, e);
                }

                try
                {
                    this.db.Put(key, valueWithTimestamp, this.newColumnFamily, this.wOptions);
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
            var valueWithTimestamp = this.db.Get(key, this.newColumnFamily);

            if (valueWithTimestamp != null)
            {
                return valueWithTimestamp;
            }

            var plainValue = this.db.Get(key, this.oldColumnFamily);

            if (plainValue != null)
            {
                var valueWithUnknownTimestamp = ApiUtils.ConvertToTimestampedFormat(plainValue);
                // this does only work, because the changelog topic contains correct data already
                // for other string.Format changes, we cannot take this short cut and can only migrate data
                // from old to new store on Put()
                this.Put(key, valueWithUnknownTimestamp);
                return valueWithUnknownTimestamp;
            }

            return null;
        }


        public byte[] GetOnly(byte[] key)
        {
            var valueWithTimestamp = this.db.Get(key, this.newColumnFamily);

            if (valueWithTimestamp != null)
            {
                return valueWithTimestamp;
            }

            var plainValue = this.db.Get(key, this.oldColumnFamily);

            if (plainValue != null)
            {
                return ApiUtils.ConvertToTimestampedFormat(plainValue);
            }

            return null;
        }


        public IKeyValueIterator<Bytes, byte[]> Range(
            Bytes from,
            Bytes to)
        {
            return new RocksDbDualCFRangeIterator(
                this.Name,
                this.db.NewIterator(this.newColumnFamily),
                this.db.NewIterator(this.oldColumnFamily),
                from,
                to);
        }

        public IKeyValueIterator<Bytes, byte[]> All()
        {
            Iterator innerIterWithTimestamp = this.db.NewIterator(this.newColumnFamily);
            innerIterWithTimestamp.SeekToFirst();
            Iterator innerIterNoTimestamp = this.db.NewIterator(this.oldColumnFamily);
            innerIterNoTimestamp.SeekToFirst();
            return new RocksDbDualCFIterator(this.Name, innerIterWithTimestamp, innerIterNoTimestamp);
        }


        public long ApproximateNumEntries()
        {
            return long.Parse(this.db.GetProperty("rocksdb.estimate-num-keys", this.oldColumnFamily))
                + long.Parse(this.db.GetProperty("rocksdb.estimate-num-keys", this.newColumnFamily));
        }


        public void Flush()
        {
            // db.Flush(fOptions, oldColumnFamily);
            // db.Flush(fOptions, newColumnFamily);
        }

        public void PrepareBatchForRestore(
            List<KeyValuePair<byte[], byte[]>> records,
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
                batch.Delete(key, this.oldColumnFamily);
                batch.Delete(key, this.newColumnFamily);
            }
            else
            {
                batch.Delete(key, this.oldColumnFamily);
                batch.Put(key, value, this.newColumnFamily);
            }
        }


        public void Close()
        {
            // oldColumnFamily.Close();
            // newColumnFamily.Close();
        }

        public void ToggleDbForBulkLoading()
        {
            try
            {
                //db.CompactRange(1, 0, oldColumnFamily);
            }
            catch (RocksDbException e)
            {
                throw new ProcessorStateException("Error while range compacting during restoring  store " + this.Name, e);
            }

            try
            {
                //db.CompactRange(newColumnFamily, true, 1, 0);
            }
            catch (RocksDbException e)
            {
                throw new ProcessorStateException("Error while range compacting during restoring  store " + this.Name, e);
            }
        }
    }
}
