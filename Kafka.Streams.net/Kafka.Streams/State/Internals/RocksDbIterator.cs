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
using Kafka.Streams.State.KeyValueIterator;
import org.rocksdb.RocksIterator;

import java.util.NoSuchElementException;
import java.util.Set;

class RocksDbIterator : AbstractIterator<KeyValue<Bytes, byte[]>> : KeyValueIterator<Bytes, byte[]>
{

    private string storeName;
    private RocksIterator iter;
    private Set<KeyValueIterator<Bytes, byte[]>> openIterators;

    private volatile bool open = true;

    private KeyValue<Bytes, byte[]> next;

    RocksDbIterator(string storeName,
                    RocksIterator iter,
                    Set<KeyValueIterator<Bytes, byte[]>> openIterators)
{
        this.storeName = storeName;
        this.iter = iter;
        this.openIterators = openIterators;
    }

    public override synchronized bool hasNext()
{
        if (!open)
{
            throw new InvalidStateStoreException(string.format("RocksDB iterator for store %s has closed", storeName));
        }
        return super.hasNext();
    }

    public override KeyValue<Bytes, byte[]> makeNext()
{
        if (!iter.isValid())
{
            return allDone();
        } else
{
            next = getKeyValue();
            iter.next();
            return next;
        }
    }

    private KeyValue<Bytes, byte[]> getKeyValue()
{
        return new KeyValue<>(new Bytes(iter.key()), iter.value());
    }

    public override void remove()
{
        throw new UnsupportedOperationException("RocksDB iterator does not support remove()");
    }

    public override synchronized void close()
{
        openIterators.remove(this);
        iter.close();
        open = false;
    }

    public override Bytes peekNextKey()
{
        if (!hasNext())
{
            throw new NoSuchElementException();
        }
        return next.key;
    }
}
