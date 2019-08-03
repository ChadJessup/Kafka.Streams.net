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

using Kafka.Common.Utils.Bytes;
using Kafka.Streams.KeyValue;
using Kafka.Streams.Processor.IProcessorContext;
using Kafka.Streams.Processor.IStateStore;
using Kafka.Streams.State.KeyValueIterator;
using Kafka.Streams.State.KeyValueStore;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An in-memory LRU cache store based on HashSet and HashMap.
 */
public class MemoryLRUCache : KeyValueStore<Bytes, byte[]>
{

    public interface EldestEntryRemovalListener
{
        void apply(Bytes key, byte[] value);
    }

    private string name;
    protected Dictionary<Bytes, byte[]> map;

    private bool restoring = false; // TODO: this is a sub-optimal solution to avoid logging during restoration.
                                       // in the future we should augment the StateRestoreCallback with onComplete etc to better resolve this.
    private volatile bool open = true;

    private EldestEntryRemovalListener listener;

    MemoryLRUCache(string name, int maxCacheSize)
{
        this.name = name;

        // leave room for one extra entry to handle adding an entry before the oldest can be removed
        this.map = new LinkedHashMap<Bytes, byte[]>(maxCacheSize + 1, 1.01f, true)
{
            private static long serialVersionUID = 1L;

            @Override
            protected bool removeEldestEntry(Map.Entry<Bytes, byte[]> eldest)
{
                bool evict = super.size() > maxCacheSize;
                if (evict && !restoring && listener != null)
{
                    listener.apply(eldest.getKey(), eldest.getValue());
                }
                return evict;
            }
        };
    }

    void setWhenEldestRemoved(EldestEntryRemovalListener listener)
{
        this.listener = listener;
    }

    public override string name()
{
        return this.name;
    }

    public override void init(IProcessorContext context, IStateStore root)
{

        // register the store
        context.register(root, (key, value) ->
{
            restoring = true;
            put(Bytes.wrap(key), value);
            restoring = false;
        });
    }

    public override bool persistent()
{
        return false;
    }

    public override bool isOpen()
{
        return open;
    }

    public override synchronized byte[] get(Bytes key)
{
        Objects.requireNonNull(key);

        return this.map.get(key);
    }

    public override synchronized void put(Bytes key, byte[] value)
{
        Objects.requireNonNull(key);
        if (value == null)
{
            delete(key);
        } else
{
            this.map.put(key, value);
        }
    }

    public override synchronized byte[] putIfAbsent(Bytes key, byte[] value)
{
        Objects.requireNonNull(key);
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

    public override synchronized byte[] delete(Bytes key)
{
        Objects.requireNonNull(key);
        return this.map.remove(key);
    }

    /**
     * @throws UnsupportedOperationException at every invocation
     */
    public override KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to)
{
        throw new UnsupportedOperationException("MemoryLRUCache does not support range() function.");
    }

    /**
     * @throws UnsupportedOperationException at every invocation
     */
    public override KeyValueIterator<Bytes, byte[]> all()
{
        throw new UnsupportedOperationException("MemoryLRUCache does not support all() function.");
    }

    public override long approximateNumEntries()
{
        return this.map.size();
    }

    public override void flush()
{
        // do-nothing since it is in-memory
    }

    public override void close()
{
        open = false;
    }

    public int size()
{
        return this.map.size();
    }
}
