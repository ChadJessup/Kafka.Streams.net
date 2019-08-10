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
using Kafka.Streams.Errors;
using Kafka.Streams.IProcessor.Interfaces;
using Kafka.Streams.IProcessor.Internals;
using Kafka.Streams.State.Interfaces;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    /**
     * Wrapper over StreamThread that : StateStoreProvider
     */
    public class StreamThreadStateStoreProvider : IStateStoreProvider
    {
        private StreamThread streamThread;

        public StreamThreadStateStoreProvider(StreamThread streamThread)
        {
            this.streamThread = streamThread;
        }

        public List<T> stores<T>(string storeName, IQueryableStoreType<T> queryableStoreType)
        {
            if (streamThread.state() == StreamThread.State.DEAD)
            {
                return new List<T>();
            }

            if (!streamThread.isRunningAndNotRebalancing())
            {
                throw new InvalidStateStoreException("Cannot get state store " + storeName + " because the stream thread is " +
                        streamThread.state() + ", not RUNNING");
            }
            List<T> stores = new List<T>();
            foreach (ITask streamTask in streamThread.tasks().Values)
            {
                IStateStore store = streamTask.getStore(storeName);
                if (store != null && queryableStoreType.accepts(store))
                {
                    if (!store.isOpen())
                    {
                        throw new InvalidStateStoreException("Cannot get state store " + storeName + " for task " + streamTask +
                                " because the store is not open. The state store may have migrated to another instances.");
                    }
                    if (store is ITimestampedKeyValueStore && queryableStoreType is QueryableStoreTypes.KeyValueStoreType)
                    {
                        stores.Add((T)new ReadOnlyKeyValueStoreFacade((ITimestampedKeyValueStore<object, object>)store));
                    }
                    else if (store is ITimestampedWindowStore && queryableStoreType is QueryableStoreTypes.WindowStoreType)
                    {
                        stores.Add((T)new ReadOnlyWindowStoreFacade((ITimestampedWindowStore<object, object>)store));
                    }
                    else
                    {
                        stores.Add((T)store);
                    }
                }
            }
            return stores;
        }

    }
}