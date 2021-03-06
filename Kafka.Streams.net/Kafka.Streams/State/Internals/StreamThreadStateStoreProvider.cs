using Kafka.Streams.Errors;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Queryable;
using Kafka.Streams.State.ReadOnly;
using Kafka.Streams.State.TimeStamped;
using Kafka.Streams.State.Windowed;
using Kafka.Streams.Tasks;
using Kafka.Streams.Threads.Stream;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    /**
     * Wrapper over KafkaStreamThread that : StateStoreProvider
     */
    public class StreamThreadStateStoreProvider : IStateStoreProvider
    {
        private readonly IStreamThread streamThread;

        public StreamThreadStateStoreProvider(IStreamThread streamThread)
        {
            this.streamThread = streamThread;
        }

        public List<T> Stores<T>(string storeName, IQueryableStoreType<T> queryableStoreType)
        {
            if (this.streamThread.State.CurrentState == StreamThreadStates.DEAD)
            {
                return new List<T>();
            }

            if (!this.streamThread.IsRunningAndNotRebalancing())
            {
                throw new InvalidStateStoreException($"Cannot get state store {storeName} because the stream thread is " +
                        $"{this.streamThread.State.CurrentState}, not RUNNING");
            }

            var stores = new List<T>();
            foreach (ITask streamTask in this.streamThread.Tasks().Values)
            {
                IStateStore store = streamTask.GetStore(storeName);
                if (store != null && queryableStoreType.Accepts(store))
                {
                    if (!store.IsOpen())
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