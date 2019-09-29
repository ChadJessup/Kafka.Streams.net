using Kafka.Streams.Errors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.Tasks;
using Kafka.Streams.Threads.KafkaStream;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    /**
     * Wrapper over KafkaStreamThread that : StateStoreProvider
     */
    public class StreamThreadStateStoreProvider : IStateStoreProvider
    {
        private readonly KafkaStreamThread streamThread;

        public StreamThreadStateStoreProvider(KafkaStreamThread streamThread)
        {
            this.streamThread = streamThread;
        }

        public List<T> stores<T>(string storeName, IQueryableStoreType<T> queryableStoreType)
        {
            if (streamThread.State.CurrentState == KafkaStreamThreadStates.DEAD)
            {
                return new List<T>();
            }

            if (!streamThread.isRunningAndNotRebalancing())
            {
                throw new InvalidStateStoreException("Cannot get state store " + storeName + " because the stream thread is " +
                        streamThread.State.CurrentState + ", not RUNNING");
            }

            var stores = new List<T>();
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
                    if (store is ITimestampedKeyValueStore<object, object> && queryableStoreType is KeyValueStoreType<object, object>)
                    {
                        stores.Add((T)(object)new ReadOnlyKeyValueStoreFacade<object, object>((ITimestampedKeyValueStore<object, object>)store));
                    }
                    else if (store is ITimestampedWindowStore<object, object> && queryableStoreType is WindowStoreType<object, object>)
                    {
                        //stores.Add((T)(object)new ReadOnlyWindowStoreFacade<object, object>((ITimestampedWindowStore<object, object>)store));
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