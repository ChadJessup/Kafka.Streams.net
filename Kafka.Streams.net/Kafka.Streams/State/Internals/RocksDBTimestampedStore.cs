/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

using Kafka.Common.Utils.AbstractIterator;
using Kafka.Common.Utils.Bytes;
using Kafka.Streams.KeyValue;
using Kafka.Streams.Errors.InvalidStateStoreException;
using Kafka.Streams.Errors.ProcessorStateException;
using Kafka.Streams.State.KeyValueIterator;
using Kafka.Streams.State.TimestampedBytesStore;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import static java.util.Arrays.asList;
import static org.apache.kafka.streams.state.TimestampedBytesStore.convertToTimestampedFormat;

/**
 * A persistent key-(value-timestamp) store based on RocksDB.
 */
public class RocksDBTimestampedStore : RocksDBStore : TimestampedBytesStore
{
    private static Logger log = LoggerFactory.getLogger(RocksDBTimestampedStore.class);

    RocksDBTimestampedStore(string name)
{
        super(name);
    }

    RocksDBTimestampedStore(string name,
                            string parentDir)
{
        super(name, parentDir);
    }

    @Override
    void openRocksDB(DBOptions dbOptions,
                     ColumnFamilyOptions columnFamilyOptions)
{
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = asList(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
            new ColumnFamilyDescriptor("keyValueWithTimestamp".getBytes(StandardCharsets.UTF_8), columnFamilyOptions));
        List<ColumnFamilyHandle> columnFamilies = new ArrayList<>(columnFamilyDescriptors.size());

        try
{
            db = RocksDB.open(dbOptions, dbDir.getAbsolutePath(), columnFamilyDescriptors, columnFamilies);

            ColumnFamilyHandle noTimestampColumnFamily = columnFamilies.get(0);

            RocksIterator noTimestampsIter = db.newIterator(noTimestampColumnFamily);
            noTimestampsIter.seekToFirst();
            if (noTimestampsIter.isValid())
{
                log.info("Opening store {} in upgrade mode", name);
                dbAccessor = new DualColumnFamilyAccessor(noTimestampColumnFamily, columnFamilies.get(1));
            } else
{
                log.info("Opening store {} in regular mode", name);
                dbAccessor = new SingleColumnFamilyAccessor(columnFamilies.get(1));
                noTimestampColumnFamily.close();
            }
            noTimestampsIter.close();
        } catch (RocksDBException e)
{
            if ("Column family not found: : keyValueWithTimestamp".Equals(e.getMessage()))
{
                try
{
                    db = RocksDB.open(dbOptions, dbDir.getAbsolutePath(), columnFamilyDescriptors.subList(0, 1), columnFamilies);
                    columnFamilies.add(db.createColumnFamily(columnFamilyDescriptors.get(1)));
                } catch (RocksDBException fatal)
{
                    throw new ProcessorStateException("Error opening store " + name + " at location " + dbDir.toString(), fatal);
                }
                log.info("Opening store {} in upgrade mode", name);
                dbAccessor = new DualColumnFamilyAccessor(columnFamilies.get(0), columnFamilies.get(1));
            } else
{
                throw new ProcessorStateException("Error opening store " + name + " at location " + dbDir.toString(), e);
            }
        }
    }



    private class DualColumnFamilyAccessor : RocksDBAccessor
{
        private ColumnFamilyHandle oldColumnFamily;
        private ColumnFamilyHandle newColumnFamily;

        private DualColumnFamilyAccessor(ColumnFamilyHandle oldColumnFamily,
                                         ColumnFamilyHandle newColumnFamily)
{
            this.oldColumnFamily = oldColumnFamily;
            this.newColumnFamily = newColumnFamily;
        }

        @Override
        public void put(byte[] key,
                        byte[] valueWithTimestamp)
{
            if (valueWithTimestamp == null)
{
                try
{
                    db.delete(oldColumnFamily, wOptions, key);
                } catch (RocksDBException e)
{
                    // string format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                    throw new ProcessorStateException("Error while removing key from store " + name, e);
                }
                try
{
                    db.delete(newColumnFamily, wOptions, key);
                } catch (RocksDBException e)
{
                    // string format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                    throw new ProcessorStateException("Error while removing key from store " + name, e);
                }
            } else
{
                try
{
                    db.delete(oldColumnFamily, wOptions, key);
                } catch (RocksDBException e)
{
                    // string format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                    throw new ProcessorStateException("Error while removing key from store " + name, e);
                }
                try
{
                    db.put(newColumnFamily, wOptions, key, valueWithTimestamp);
                } catch (RocksDBException e)
{
                    // string format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                    throw new ProcessorStateException("Error while putting key/value into store " + name, e);
                }
            }
        }

        @Override
        public void prepareBatch(List<KeyValue<Bytes, byte[]>> entries,
                                 WriteBatch batch) throws RocksDBException
{
            for (KeyValue<Bytes, byte[]> entry : entries)
{
                Objects.requireNonNull(entry.key, "key cannot be null");
                addToBatch(entry.key.get(), entry.value, batch);
            }
        }

        @Override
        public byte[] get(byte[] key) throws RocksDBException
{
            byte[] valueWithTimestamp = db.get(newColumnFamily, key);
            if (valueWithTimestamp != null)
{
                return valueWithTimestamp;
            }

            byte[] plainValue = db.get(oldColumnFamily, key);
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

        @Override
        public byte[] getOnly(byte[] key) throws RocksDBException
{
            byte[] valueWithTimestamp = db.get(newColumnFamily, key);
            if (valueWithTimestamp != null)
{
                return valueWithTimestamp;
            }

            byte[] plainValue = db.get(oldColumnFamily, key);
            if (plainValue != null)
{
                return convertToTimestampedFormat(plainValue);
            }

            return null;
        }

        @Override
        public KeyValueIterator<Bytes, byte[]> range(Bytes from,
                                                     Bytes to)
{
            return new RocksDBDualCFRangeIterator(
                name,
                db.newIterator(newColumnFamily),
                db.newIterator(oldColumnFamily),
                from,
                to);
        }

        @Override
        public KeyValueIterator<Bytes, byte[]> all()
{
            RocksIterator innerIterWithTimestamp = db.newIterator(newColumnFamily);
            innerIterWithTimestamp.seekToFirst();
            RocksIterator innerIterNoTimestamp = db.newIterator(oldColumnFamily);
            innerIterNoTimestamp.seekToFirst();
            return new RocksDBDualCFIterator(name, innerIterWithTimestamp, innerIterNoTimestamp);
        }

        @Override
        public long approximateNumEntries() throws RocksDBException
{
            return db.getLongProperty(oldColumnFamily, "rocksdb.estimate-num-keys")
                + db.getLongProperty(newColumnFamily, "rocksdb.estimate-num-keys");
        }

        @Override
        public void flush() throws RocksDBException
{
            db.flush(fOptions, oldColumnFamily);
            db.flush(fOptions, newColumnFamily);
        }

        @Override
        public void prepareBatchForRestore(Collection<KeyValue<byte[], byte[]>> records,
                                           WriteBatch batch) throws RocksDBException
{
            for (KeyValue<byte[], byte[]> record : records)
{
                addToBatch(record.key, record.value, batch);
            }
        }

        @Override
        public void addToBatch(byte[] key,
                               byte[] value,
                               WriteBatch batch) throws RocksDBException
{
            if (value == null)
{
                batch.delete(oldColumnFamily, key);
                batch.delete(newColumnFamily, key);
            } else
{
                batch.delete(oldColumnFamily, key);
                batch.put(newColumnFamily, key, value);
            }
        }

        @Override
        public void close()
{
            oldColumnFamily.close();
            newColumnFamily.close();
        }

        @Override
        @SuppressWarnings("deprecation")
        public void toggleDbForBulkLoading()
{
            try
{
                db.compactRange(oldColumnFamily, true, 1, 0);
            } catch (RocksDBException e)
{
                throw new ProcessorStateException("Error while range compacting during restoring  store " + name, e);
            }
            try
{
                db.compactRange(newColumnFamily, true, 1, 0);
            } catch (RocksDBException e)
{
                throw new ProcessorStateException("Error while range compacting during restoring  store " + name, e);
            }
        }
    }

    private class RocksDBDualCFIterator : AbstractIterator<KeyValue<Bytes, byte[]>>
        : KeyValueIterator<Bytes, byte[]>
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

        @Override
        public synchronized bool hasNext()
{
            if (!open)
{
                throw new InvalidStateStoreException(string.format("RocksDB iterator for store %s has closed", storeName));
            }
            return super.hasNext();
        }

        @Override
        public synchronized KeyValue<Bytes, byte[]> next()
{
            return super.next();
        }

        @Override
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
                } else
{
                    next = KeyValue.pair(new Bytes(nextWithTimestamp), iterWithTimestamp.value());
                    nextWithTimestamp = null;
                    iterWithTimestamp.next();
                }
            } else
{
                if (nextWithTimestamp == null)
{
                    next = KeyValue.pair(new Bytes(nextNoTimestamp), convertToTimestampedFormat(iterNoTimestamp.value()));
                    nextNoTimestamp = null;
                    iterNoTimestamp.next();
                } else
{
                    if (comparator.compare(nextNoTimestamp, nextWithTimestamp) <= 0)
{
                        next = KeyValue.pair(new Bytes(nextNoTimestamp), convertToTimestampedFormat(iterNoTimestamp.value()));
                        nextNoTimestamp = null;
                        iterNoTimestamp.next();
                    } else
{
                        next = KeyValue.pair(new Bytes(nextWithTimestamp), iterWithTimestamp.value());
                        nextWithTimestamp = null;
                        iterWithTimestamp.next();
                    }
                }
            }

            return next;
        }

        @Override
        public void remove()
{
            throw new UnsupportedOperationException("RocksDB iterator does not support remove()");
        }

        @Override
        public synchronized void close()
{
            openIterators.remove(this);
            iterNoTimestamp.close();
            iterWithTimestamp.close();
            open = false;
        }

        @Override
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
            super(storeName, iterWithTimestamp, iterNoTimestamp);
            iterWithTimestamp.seek(from.get());
            iterNoTimestamp.seek(from.get());
            upperBoundKey = to.get();
            if (upperBoundKey == null)
{
                throw new NullPointerException("RocksDBDualCFRangeIterator: upperBoundKey is null for key " + to);
            }
        }

        @Override
        public KeyValue<Bytes, byte[]> makeNext()
{
            KeyValue<Bytes, byte[]> next = super.makeNext();

            if (next == null)
{
                return allDone();
            } else
{
                if (comparator.compare(next.key.get(), upperBoundKey) <= 0)
{
                    return next;
                } else
{
                    return allDone();
                }
            }
        }
    }

}
