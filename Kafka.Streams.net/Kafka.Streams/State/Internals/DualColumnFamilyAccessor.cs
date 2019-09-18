using Kafka.Common.Utils;
using Kafka.Streams.Errors;
using Kafka.Streams.Internals;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.State.Interfaces;
using RocksDbSharp;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public class DualColumnFamilyAccessor : IRocksDbAccessor
    {
        private readonly ColumnFamilyHandle oldColumnFamily;
        private readonly ColumnFamilyHandle newColumnFamily;
        private readonly RocksDb db;
        private readonly WriteOptions wOptions;
        private readonly string name;
        private readonly HashSet<IKeyValueIterator<Bytes, byte[]>> openIterators;

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

        public void put(
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

        public void prepareBatch(
            List<KeyValue<Bytes, byte[]>> entries,
            WriteBatch batch)
        {
            foreach (KeyValue<Bytes, byte[]> entry in entries)
            {
                entry.key = entry.key ?? throw new ArgumentNullException(nameof(entry.key));

                addToBatch(entry.key.get(), entry.value, batch);
            }
        }


        public byte[] get(byte[] key)
        {
            byte[] valueWithTimestamp = db.Get(key, newColumnFamily);

            if (valueWithTimestamp != null)
            {
                return valueWithTimestamp;
            }

            byte[] plainValue = db.Get(key, oldColumnFamily);

            if (plainValue != null)
            {
                byte[] valueWithUnknownTimestamp = ApiUtils.convertToTimestampedFormat(plainValue);
                // this does only work, because the changelog topic contains correct data already
                // for other string.Format changes, we cannot take this short cut and can only migrate data
                // from old to new store on put()
                put(key, valueWithUnknownTimestamp);
                return valueWithUnknownTimestamp;
            }

            return null;
        }


        public byte[] getOnly(byte[] key)
        {
            byte[] valueWithTimestamp = db.Get(key, newColumnFamily);

            if (valueWithTimestamp != null)
            {
                return valueWithTimestamp;
            }

            byte[] plainValue = db.Get(key, oldColumnFamily);

            if (plainValue != null)
            {
                return ApiUtils.convertToTimestampedFormat(plainValue);
            }

            return null;
        }


        public IKeyValueIterator<Bytes, byte[]> range(
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

        public IKeyValueIterator<Bytes, byte[]> all()
        {
            Iterator innerIterWithTimestamp = db.NewIterator(newColumnFamily);
            innerIterWithTimestamp.SeekToFirst();
            Iterator innerIterNoTimestamp = db.NewIterator(oldColumnFamily);
            innerIterNoTimestamp.SeekToFirst();
            return new RocksDbDualCFIterator(name, innerIterWithTimestamp, innerIterNoTimestamp);
        }


        public long approximateNumEntries()
        {
            return long.Parse(db.GetProperty("rocksdb.estimate-num-keys", oldColumnFamily))
                + long.Parse(db.GetProperty("rocksdb.estimate-num-keys", newColumnFamily));
        }


        public void flush()
        {
            // db.Flush(fOptions, oldColumnFamily);
            // db.Flush(fOptions, newColumnFamily);
        }

        public void prepareBatchForRestore(
            List<KeyValue<byte[], byte[]>> records,
            WriteBatch batch)
        {
            foreach (KeyValue<byte[], byte[]> record in records)
            {
                addToBatch(record.key, record.value, batch);
            }
        }


        public void addToBatch(
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


        public void close()
        {
            // oldColumnFamily.close();
            // newColumnFamily.close();
        }

        public void toggleDbForBulkLoading()
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
