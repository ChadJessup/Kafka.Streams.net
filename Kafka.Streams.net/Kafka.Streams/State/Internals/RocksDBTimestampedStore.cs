using Kafka.Common.Utils;
using Kafka.Streams.State;
using Microsoft.Extensions.Logging;
using RocksDbSharp;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Kafka.Streams.State.Internals
{
    /**
     * A persistent key-(value-timestamp) store based on RocksDB.
     */
    public class RocksDBTimestampedStore : RocksDBStore, ITimestampedBytesStore
    {
        private static ILogger log = new LoggerFactory().CreateLogger<RocksDBTimestampedStore>();

        public RocksDBTimestampedStore(string name)
            : base(name)
        {
        }

        RocksDBTimestampedStore(string name,
                                string parentDir)
            : base(name, parentDir)
        {
        }


        void openRocksDB(DBOptions dbOptions,
                         ColumnFamilyOptions columnFamilyOptions)
        {
            List<ColumnFamilyDescriptor> columnFamilyDescriptors = asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
                new ColumnFamilyDescriptor("keyValueWithTimestamp".getBytes(System.Text.Encoding.UTF8), columnFamilyOptions));
            List<ColumnFamilyHandle> columnFamilies = new List<ColumnFamilyHandle>(columnFamilyDescriptors.size());

            try
            {
                db = RocksDB.open(dbOptions, dbDir.FullName, columnFamilyDescriptors, columnFamilies);

                ColumnFamilyHandle noTimestampColumnFamily = columnFamilies[0];

                RocksIterator noTimestampsIter = db.newIterator(noTimestampColumnFamily);
                noTimestampsIter.seekToFirst();
                if (noTimestampsIter.isValid())
                {
                    log.LogInformation("Opening store {} in upgrade mode", name);
                    dbAccessor = new DualColumnFamilyAccessor(noTimestampColumnFamily, columnFamilies[1]);
                }
                else
                {
                    log.LogInformation("Opening store {} in regular mode", name);
                    dbAccessor = new SingleColumnFamilyAccessor(columnFamilies[1]);
                    noTimestampColumnFamily.close();
                }
                noTimestampsIter.close();
            }
            catch (RocksDBException e)
            {
                if ("Column family not found: : keyValueWithTimestamp".Equals(e.getMessage()))
                {
                    try
                    {
                        db = RocksDB.open(dbOptions, dbDir.FullName, columnFamilyDescriptors.subList(0, 1), columnFamilies);
                        columnFamilies.Add(db.createColumnFamily(columnFamilyDescriptors[1]));
                    }
                    catch (RocksDBException fatal)
                    {
                        throw new ProcessorStateException("Error opening store " + name + " at location " + dbDir.ToString(), fatal);
                    }
                    log.LogInformation("Opening store {} in upgrade mode", name);
                    dbAccessor = new DualColumnFamilyAccessor(columnFamilies[0], columnFamilies[1]);
                }
                else
                {
                    throw new ProcessorStateException("Error opening store " + name + " at location " + dbDir.ToString(), e);
                }
            }
        }

        private class DualColumnFamilyAccessor : IRocksDBAccessor
        {
            private ColumnFamilyHandle oldColumnFamily;
            private ColumnFamilyHandle newColumnFamily;

            private DualColumnFamilyAccessor(ColumnFamilyHandle oldColumnFamily,
                                             ColumnFamilyHandle newColumnFamily)
            {
                this.oldColumnFamily = oldColumnFamily;
                this.newColumnFamily = newColumnFamily;
            }


            public void put(byte[] key,
                            byte[] valueWithTimestamp)
            {
                if (valueWithTimestamp == null)
                {
                    try
                    {
                        db.delete(oldColumnFamily, wOptions, key);
                    }
                    catch (RocksDBException e)
                    {
                        // string format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                        throw new ProcessorStateException("Error while removing key from store " + name, e);
                    }
                    try
                    {
                        db.delete(newColumnFamily, wOptions, key);
                    }
                    catch (RocksDBException e)
                    {
                        // string format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                        throw new ProcessorStateException("Error while removing key from store " + name, e);
                    }
                }
                else
                {
                    try
                    {
                        db.delete(oldColumnFamily, wOptions, key);
                    }
                    catch (RocksDBException e)
                    {
                        // string format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                        throw new ProcessorStateException("Error while removing key from store " + name, e);
                    }
                    try
                    {
                        db.Add(newColumnFamily, wOptions, key, valueWithTimestamp);
                    }
                    catch (RocksDBException e)
                    {
                        // string format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                        throw new ProcessorStateException("Error while putting key/value into store " + name, e);
                    }
                }
            }


            public void prepareBatch(List<KeyValue<Bytes, byte[]>> entries,
                                     WriteBatch batch)
            {
                foreach (KeyValue<Bytes, byte[]> entry in entries)
                {
                    entry.key = entry.key ?? throw new System.ArgumentNullException("key cannot be null", nameof(entry.key));
               .AddToBatch(entry.key(), entry.value, batch);
                }
            }


            public byte[] get(byte[] key)
            {
                byte[]
            valueWithTimestamp = db[newColumnFamily, key];
                if (valueWithTimestamp != null)
                {
                    return valueWithTimestamp;
                }

                byte[]
            plainValue = db[oldColumnFamily, key];
                if (plainValue != null)
                {
                    byte[] valueWithUnknownTimestamp = convertToTimestampedFormat(plainValue);
                    // this does only work, because the changelog topic contains correct data already
                    // for other format changes, we cannot take this short cut and can only migrate data
                    // from old to new store on put()
                    put(key, valueWithUnknownTimestamp);
                    return valueWithUnknownTimestamp;
                }

                return null;
            }


            public byte[] getOnly(byte[] key)
            {
                byte[] valueWithTimestamp = db[newColumnFamily, key];
                if (valueWithTimestamp != null)
                {
                    return valueWithTimestamp;
                }

                byte[]
            plainValue = db[oldColumnFamily, key];
                if (plainValue != null)
                {
                    return convertToTimestampedFormat(plainValue);
                }

                return null;
            }


            public IKeyValueIterator<Bytes, byte[]> range(Bytes from,
                                                         Bytes to)
            {
                return new RocksDBDualCFRangeIterator(
                    name,
                    db.newIterator(newColumnFamily),
                    db.newIterator(oldColumnFamily),
                    from,
                    to);
            }


            public IKeyValueIterator<Bytes, byte[]> all()
            {
                RocksIterator innerIterWithTimestamp = db.newIterator(newColumnFamily);
                innerIterWithTimestamp.seekToFirst();
                RocksIterator innerIterNoTimestamp = db.newIterator(oldColumnFamily);
                innerIterNoTimestamp.seekToFirst();
                return new RocksDBDualCFIterator(name, innerIterWithTimestamp, innerIterNoTimestamp);
            }


            public long approximateNumEntries()
            {
                return db.getLongProperty(oldColumnFamily, "rocksdb.estimate-num-keys")
                    + db.getLongProperty(newColumnFamily, "rocksdb.estimate-num-keys");
            }


            public void flush()
            {
                db.flush(fOptions, oldColumnFamily);
                db.flush(fOptions, newColumnFamily);
            }


            public void prepareBatchForRestore(Collection<KeyValue<byte[], byte[]>> records,
                                               WriteBatch batch)
            {
                foreach (KeyValue<byte[], byte[]> record in records)
                {
               .AddToBatch(record.key, record.value, batch);
                }
            }


            public void addToBatch(byte[] key,
                                   byte[] value,
                                   WriteBatch batch)
            {
                if (value == null)
                {
                    batch.delete(oldColumnFamily, key);
                    batch.delete(newColumnFamily, key);
                }
                else
                {
                    batch.delete(oldColumnFamily, key);
                    batch.Add(newColumnFamily, key, value);
                }
            }


            public void close()
            {
                oldColumnFamily.close();
                newColumnFamily.close();
            }



            public void toggleDbForBulkLoading()
            {
                try
                {
                    db.compactRange(oldColumnFamily, true, 1, 0);
                }
                catch (RocksDBException e)
                {
                    throw new ProcessorStateException("Error while range compacting during restoring  store " + name, e);
                }
                try
                {
                    db.compactRange(newColumnFamily, true, 1, 0);
                }
                catch (RocksDBException e)
                {
                    throw new ProcessorStateException("Error while range compacting during restoring  store " + name, e);
                }
            }
        }

        private class RocksDBDualCFIterator : AbstractIterator<KeyValue<Bytes, byte[]>>
        : IKeyValueIterator<Bytes, byte[]>
        {

            // RocksDB's JNI interface does not expose getters/setters that allow the
            // comparator to be pluggable, and the default is lexicographic, so it's
            // safe to just force lexicographic comparator here for now.
            private Comparator<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;

            private string storeName;
            private RocksIterator iterWithTimestamp;
            private RocksIterator iterNoTimestamp;

            private volatile bool open = true;

            private byte[] nextWithTimestamp;
            private byte[] nextNoTimestamp;
            private KeyValue<Bytes, byte[]> next;

            RocksDBDualCFIterator(string storeName,
                                  RocksIterator iterWithTimestamp,
                                  RocksIterator iterNoTimestamp)
            {
                this.iterWithTimestamp = iterWithTimestamp;
                this.iterNoTimestamp = iterNoTimestamp;
                this.storeName = storeName;
            }

            [MethodImpl(MethodImplOptions.Synchronized)]
            public bool hasNext()
            {
                if (!open)
                {
                    throw new InvalidStateStoreException(string.Format("RocksDB iterator for store %s has closed", storeName));
                }
                return base.hasNext();
            }

            [MethodImpl(MethodImplOptions.Synchronized)]
            public KeyValue<Bytes, byte[]> next()
            {
                return base.next();
            }


            public KeyValue<Bytes, byte[]> makeNext()
            {
                if (nextNoTimestamp == null && iterNoTimestamp.isValid())
                {
                    nextNoTimestamp = iterNoTimestamp.key();
                }

                if (nextWithTimestamp == null && iterWithTimestamp.isValid())
                {
                    nextWithTimestamp = iterWithTimestamp.key();
                }

                if (nextNoTimestamp == null && !iterNoTimestamp.isValid())
                {
                    if (nextWithTimestamp == null && !iterWithTimestamp.isValid())
                    {
                        return allDone();
                    }
                    else
                    {
                        next = KeyValue.pair(new Bytes(nextWithTimestamp), iterWithTimestamp.value());
                        nextWithTimestamp = null;
                        iterWithTimestamp.next();
                    }
                }
                else
                {
                    if (nextWithTimestamp == null)
                    {
                        next = KeyValue.pair(new Bytes(nextNoTimestamp), convertToTimestampedFormat(iterNoTimestamp.value()));
                        nextNoTimestamp = null;
                        iterNoTimestamp.next();
                    }
                    else
                    {
                        if (comparator.compare(nextNoTimestamp, nextWithTimestamp) <= 0)
                        {
                            next = KeyValue.pair(new Bytes(nextNoTimestamp), convertToTimestampedFormat(iterNoTimestamp.value()));
                            nextNoTimestamp = null;
                            iterNoTimestamp.next();
                        }
                        else
                        {
                            next = KeyValue.pair(new Bytes(nextWithTimestamp), iterWithTimestamp.value());
                            nextWithTimestamp = null;
                            iterWithTimestamp.next();
                        }
                    }
                }

                return next;
            }


            public void Remove()
            {
                throw new InvalidOperationException("RocksDB iterator does not support Remove()");
            }

            [MethodImpl(MethodImplOptions.Synchronized)]
            public void close()
            {
                openIterators.Remove(this);
                iterNoTimestamp.close();
                iterWithTimestamp.close();
                open = false;
            }


            public Bytes peekNextKey()
            {
                if (!hasNext())
                {
                    throw new NoSuchElementException();
                }
                return next.key;
            }
        }

        private class RocksDBDualCFRangeIterator : RocksDBDualCFIterator
        {
            // RocksDB's JNI interface does not expose getters/setters that allow the
            // comparator to be pluggable, and the default is lexicographic, so it's
            // safe to just force lexicographic comparator here for now.
            private Comparator<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;
            private byte[] upperBoundKey;

            RocksDBDualCFRangeIterator(string storeName,
                                       RocksIterator iterWithTimestamp,
                                       RocksIterator iterNoTimestamp,
                                       Bytes from,
                                       Bytes to)
            {
                base(storeName, iterWithTimestamp, iterNoTimestamp);
                iterWithTimestamp.seek(from());
                iterNoTimestamp.seek(from());
                upperBoundKey = to[];
                if (upperBoundKey == null)
                {
                    throw new NullPointerException("RocksDBDualCFRangeIterator: upperBoundKey is null for key " + to);
                }
            }


            public KeyValue<Bytes, byte[]> makeNext()
            {
                KeyValue<Bytes, byte[]> next = base.makeNext();

                if (next == null)
                {
                    return allDone();
                }
                else
                {
                    if (comparator.compare(next.key(), upperBoundKey) <= 0)
                    {
                        return next;
                    }
                    else
                    {
                        return allDone();
                    }
                }
            }
        }
    }
}