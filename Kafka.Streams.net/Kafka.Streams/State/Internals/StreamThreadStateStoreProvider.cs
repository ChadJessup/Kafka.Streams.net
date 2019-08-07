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

using Kafka.Streams.Errors.InvalidStateStoreException;
using Kafka.Streams.Processor.IStateStore;
using Kafka.Streams.Processor.Internals.StreamThread;
using Kafka.Streams.Processor.Internals.Task;
using Kafka.Streams.State.QueryableStoreType;
using Kafka.Streams.State.QueryableStoreTypes;
using Kafka.Streams.State.TimestampedKeyValueStore;
using Kafka.Streams.State.TimestampedWindowStore;





/**
 * Wrapper over StreamThread that : StateStoreProvider
 */
public class StreamThreadStateStoreProvider : StateStoreProvider
{

    private StreamThread streamThread;

    public StreamThreadStateStoreProvider(StreamThread streamThread)
{
        this.streamThread = streamThread;
    }


    public override List<T> stores(string storeName, QueryableStoreType<T> queryableStoreType)
{
        if (streamThread.state() == StreamThread.State.DEAD)
{
            return Collections.emptyList();
        }
        if (!streamThread.isRunningAndNotRebalancing())
{
            throw new InvalidStateStoreException("Cannot get state store " + storeName + " because the stream thread is " +
                    streamThread.state() + ", not RUNNING");
        }
        List<T> stores = new List<>();
        foreach (Task streamTask in streamThread.tasks().Values)
{
            IStateStore store = streamTask.getStore(storeName);
            if (store != null && queryableStoreType.accepts(store))
{
                if (!store.isOpen())
{
                    throw new InvalidStateStoreException("Cannot get state store " + storeName + " for task " + streamTask +
                            " because the store is not open. The state store may have migrated to another instances.");
                }
                if (store is TimestampedKeyValueStore && queryableStoreType is QueryableStoreTypes.KeyValueStoreType)
{
                    stores.Add((T) new ReadOnlyKeyValueStoreFacade((TimestampedKeyValueStore<object, object>) store));
                } else if (store is TimestampedWindowStore && queryableStoreType is QueryableStoreTypes.WindowStoreType)
{
                    stores.Add((T) new ReadOnlyWindowStoreFacade((TimestampedWindowStore<object, object>) store));
                } else
{
                    stores.Add((T) store);
                }
            }
        }
        return stores;
    }

}
