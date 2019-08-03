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

import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
using Kafka.Common.Utils.Bytes;
using Kafka.Streams.KeyValue;
using Kafka.Streams.Processor.IProcessorContext;
using Kafka.Streams.Processor.IStateStore;
using Kafka.Streams.State.KeyValueIterator;
using Kafka.Streams.State.KeyValueStore;

import java.util.Iterator;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryKeyValueStore : KeyValueStore<Bytes, byte[]>
{
    private string name;
    private ConcurrentNavigableMap<Bytes, byte[]> map = new ConcurrentSkipListMap<>();
    private volatile bool open = false;

    private static Logger LOG = LoggerFactory.getLogger(InMemoryKeyValueStore.class);

    public InMemoryKeyValueStore(string name)
{
        this.name = name;
    }

    public override string name()
{
        return name;
    }

    public override void init(IProcessorContext context,
                     IStateStore root)
{

        if (root != null)
{
            // register the store
            context.register(root, (key, value) ->
{
                // this is a delete
                if (value == null)
{
                    delete(Bytes.wrap(key));
                } else
{
                    put(Bytes.wrap(key), value);
                }
            });
        }

        open = true;
    }

    public override bool persistent()
{
        return false;
    }

    public override bool isOpen()
{
        return open;
    }

    public override byte[] get(Bytes key)
{
        return map.get(key);
    }

    public override void put(Bytes key, byte[] value)
{
        if (value == null)
{
            map.remove(key);
        } else
{
            map.put(key, value);
        }
    }

    public override byte[] putIfAbsent(Bytes key, byte[] value)
{
        byte[] originalValue = get(key);
        if (originalValue == null)
{
            put(key, value);
        }
        return originalValue;
    }

    public override void putAll(List<KeyValue<Bytes, byte[]>> entries)
{
        for (KeyValue<Bytes, byte[]> entry : entries)
{
            put(entry.key, entry.value);
        }
    }

    public override byte[] delete(Bytes key)
{
        return map.remove(key);
    }

    public override KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to)
{

        if (from.compareTo(to) > 0)
{
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. "
                + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        return new DelegatingPeekingKeyValueIterator<>(
            name,
            new InMemoryKeyValueIterator(map.subMap(from, true, to, true).entrySet().iterator()));
    }

    public override KeyValueIterator<Bytes, byte[]> all()
{
        return new DelegatingPeekingKeyValueIterator<>(
            name,
            new InMemoryKeyValueIterator(map.entrySet().iterator()));
    }

    public override long approximateNumEntries()
{
        return map.size();
    }

    public override void flush()
{
        // do-nothing since it is in-memory
    }

    public override void close()
{
        map.clear();
        open = false;
    }

    private static class InMemoryKeyValueIterator : KeyValueIterator<Bytes, byte[]>
{
        private Iterator<Map.Entry<Bytes, byte[]>> iter;

        private InMemoryKeyValueIterator(Iterator<Map.Entry<Bytes, byte[]>> iter)
{
            this.iter = iter;
        }

        @Override
        public bool hasNext()
{
            return iter.hasNext();
        }

        @Override
        public KeyValue<Bytes, byte[]> next()
{
            Map.Entry<Bytes, byte[]> entry = iter.next();
            return new KeyValue<>(entry.getKey(), entry.getValue());
        }

        @Override
        public void remove()
{
            iter.remove();
        }

        @Override
        public void close()
{
            // do nothing
        }

        @Override
        public Bytes peekNextKey()
{
            throw new UnsupportedOperationException("peekNextKey() not supported in " + GetType().getName());
        }
    }
}
