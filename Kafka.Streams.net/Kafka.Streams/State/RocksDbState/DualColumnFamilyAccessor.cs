﻿using Kafka.Streams.Errors;
using Kafka.Streams.Internals;
using Kafka.Streams.State.KeyValues;
using RocksDbSharp;
using System.Collections.Generic;

namespace Kafka.Streams.State.RocksDbState
{
    public class DualColumnFamilyAccessor : IRocksDbAccessor
    {
        private readonly ColumnFamilyHandle oldColumnFamily;
        private readonly ColumnFamilyHandle newColumnFamily;
        private readonly RocksDb db;
        private readonly WriteOptions wOptions;
        private readonly string name;

        public DualColumnFamilyAccessor(
            string name,
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
                    db.Remove(key, oldColumnFamily, wOptions);
                }
                catch (RocksDbException e)
                {
                    // string string.Format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                    throw new ProcessorStateException("Error while removing key from store " + name, e);
                }

                try
                {
                    db.Remove(key, newColumnFamily, wOptions);
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
                    db.Remove(key, oldColumnFamily, wOptions);
                }
                catch (RocksDbException e)
                {
                    // string string.Format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                    throw new ProcessorStateException("Error while removing key from store " + name, e);
                }

                try
                {
                    db.Put(key, valueWithTimestamp, newColumnFamily, wOptions);
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
            var valueWithTimestamp = db.Get(key, newColumnFamily);

            if (valueWithTimestamp != null)
            {
                return valueWithTimestamp;
            }

            var plainValue = db.Get(key, oldColumnFamily);

            if (plainValue != null)
            {
                var valueWithUnknownTimestamp = ApiUtils.ConvertToTimestampedFormat(plainValue);
                // this does only work, because the changelog topic contains correct data already
                // for other string.Format changes, we cannot take this short cut and can only migrate data
                // from old to new store on put()
                Put(key, valueWithUnknownTimestamp);
                return valueWithUnknownTimestamp;
            }

            return null;
        }


        public byte[] GetOnly(byte[] key)
        {
            var valueWithTimestamp = db.Get(key, newColumnFamily);

            if (valueWithTimestamp != null)
            {
                return valueWithTimestamp;
            }

            var plainValue = db.Get(key, oldColumnFamily);

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
                name,
                db.NewIterator(newColumnFamily),
                db.NewIterator(oldColumnFamily),
                from,
                to);
        }

        public IKeyValueIterator<Bytes, byte[]> All()
        {
            Iterator innerIterWithTimestamp = db.NewIterator(newColumnFamily);
            innerIterWithTimestamp.SeekToFirst();
            Iterator innerIterNoTimestamp = db.NewIterator(oldColumnFamily);
            innerIterNoTimestamp.SeekToFirst();
            return new RocksDbDualCFIterator(name, innerIterWithTimestamp, innerIterNoTimestamp);
        }


        public long ApproximateNumEntries()
        {
            return long.Parse(db.GetProperty("rocksdb.estimate-num-keys", oldColumnFamily))
                + long.Parse(db.GetProperty("rocksdb.estimate-num-keys", newColumnFamily));
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
                batch.Delete(key, oldColumnFamily);
                batch.Delete(key, newColumnFamily);
            }
            else
            {
                batch.Delete(key, oldColumnFamily);
                batch.Put(key, value, newColumnFamily);
            }
        }


        public void Close()
        {
            // oldColumnFamily.close();
            // newColumnFamily.close();
        }

        public void ToggleDbForBulkLoading()
        {
            try
            {
                //db.CompactRange(1, 0, oldColumnFamily);
            }
            catch (RocksDbException e)
            {
                throw new ProcessorStateException("Error while range compacting during restoring  store " + name, e);
            }

            try
            {
                //db.CompactRange(newColumnFamily, true, 1, 0);
            }
            catch (RocksDbException e)
            {
                throw new ProcessorStateException("Error while range compacting during restoring  store " + name, e);
            }
        }
    }
}
