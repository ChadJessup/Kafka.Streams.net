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
namespace Kafka.streams.state.internals;

using Kafka.Common.serialization.Serdes;
using Kafka.Common.Utils.Bytes;
using Kafka.Streams.KeyValue;
using Kafka.Streams.Processor.IProcessorContext;
using Kafka.Streams.Processor.IStateStore;
using Kafka.Streams.Processor.internals.ProcessorStateManager;
using Kafka.Streams.State.KeyValueIterator;
using Kafka.Streams.State.KeyValueStore;
using Kafka.Streams.State.StateSerdes;

import java.util.List;

public class ChangeLoggingKeyValueBytesStore
    : WrappedStateStore<IKeyValueStore<Bytes, byte[]>, byte[], byte[]>
    : IKeyValueStore<Bytes, byte[]>
{

    StoreChangeLogger<Bytes, byte[]> changeLogger;

    ChangeLoggingKeyValueBytesStore(IKeyValueStore<Bytes, byte[]> inner)
{
        super(inner);
    }

    public override void init(IProcessorContext context,
                     IStateStore root)
{
        super.init(context, root);
        string topic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), name());
        changeLogger = new StoreChangeLogger<>(
            name(),
            context,
            new StateSerdes<>(topic, Serdes.Bytes(), Serdes.ByteArray()));

        // if the inner store is an LRU cache, add the eviction listener to log removed record
        if (wrapped() is MemoryLRUCache)
{
            ((MemoryLRUCache) wrapped()).setWhenEldestRemoved((key, value) ->
{
                // pass null to indicate removal
                log(key, null);
            });
        }
    }

    public override long approximateNumEntries()
{
        return wrapped().approximateNumEntries();
    }

    public override void put(Bytes key,
                    byte[] value)
{
        wrapped().Add(key, value);
        log(key, value);
    }

    public override byte[] putIfAbsent(Bytes key,
                              byte[] value)
{
        byte[] previous = wrapped().putIfAbsent(key, value];
        if (previous == null)
{
            // then it was absent
            log(key, value);
        }
        return previous;
    }

    public override void putAll(List<KeyValue<Bytes, byte[]>> entries)
{
        wrapped().putAll(entries);
        foreach (KeyValue<Bytes, byte[]> entry in entries)
{
            log(entry.key, entry.value);
        }
    }

    public override byte[] delete(Bytes key)
{
        byte[] oldValue = wrapped().delete(key];
        log(key, null);
        return oldValue;
    }

    public override byte[] get(Bytes key)
{
        return wrapped()[key];
    }

    public override KeyValueIterator<Bytes, byte[]> range(Bytes from,
                                                 Bytes to)
{
        return wrapped().range(from, to);
    }

    public override KeyValueIterator<Bytes, byte[]> all()
{
        return wrapped().all();
    }

    void log(Bytes key,
             byte[] value)
{
        changeLogger.logChange(key, value);
    }
}
