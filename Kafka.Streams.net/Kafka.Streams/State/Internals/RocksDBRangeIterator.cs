/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for.Additional information regarding copyright ownership.
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
namespace Kafka.Streams.State.Internals;

using Kafka.Common.Utils.Bytes;
using Kafka.Streams.KeyValue;
using Kafka.Streams.State.KeyValueIterator;





class RocksDBRangeIterator : RocksDbIterator
{
    // RocksDB's JNI interface does not expose getters/setters that allow the
    // comparator to be pluggable, and the default is lexicographic, so it's
    // safe to just force lexicographic comparator here for now.
    private Comparator<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;
    private byte[] rawToKey;

    RocksDBRangeIterator(string storeName,
                         RocksIterator iter,
                         HashSet<KeyValueIterator<Bytes, byte[]>> openIterators,
                         Bytes from,
                         Bytes to)
{
        super(storeName, iter, openIterators);
        iter.seek(from());
        rawToKey = to[];
        if (rawToKey == null)
{
            throw new NullPointerException("RocksDBRangeIterator: RawToKey is null for key " + to);
        }
    }

    public override KeyValue<Bytes, byte[]> makeNext()
{
        KeyValue<Bytes, byte[]> next = super.makeNext();

        if (next == null)
{
            return allDone();
        } else
{
            if (comparator.compare(next.key(), rawToKey) <= 0)
{
                return next;
            } else
{
                return allDone();
            }
        }
    }
}

