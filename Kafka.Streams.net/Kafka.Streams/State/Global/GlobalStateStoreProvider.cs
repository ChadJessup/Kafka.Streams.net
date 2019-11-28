using Kafka.Streams.Errors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Queryable;
using Kafka.Streams.State.TimeStamped;
using Kafka.Streams.State.Window;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public class GlobalStateStoreProvider : IStateStoreProvider
    {
        private readonly Dictionary<string, IStateStore> globalStateStores;

        public GlobalStateStoreProvider(Dictionary<string, IStateStore> globalStateStores)
        {
            this.globalStateStores = globalStateStores;
        }

        public List<T> stores<T>(string storeName, IQueryableStoreType<T> queryableStoreType)
        {
            IStateStore store = globalStateStores[storeName];
            if (store == null || !queryableStoreType.accepts(store))
            {
                return new List<T>();
            }
            if (!store.isOpen())
            {
                throw new InvalidStateStoreException("the state store, " + storeName + ", is not open.");
            }
            if (store is ITimestampedKeyValueStore<object, object> && queryableStoreType is KeyValueStoreType<object, object>)
            {
                //return (List<T>)new ReadOnlyKeyValueStoreFacade<object, object>((ITimestampedKeyValueStore<object, object>)store);
            }
            else if (store is ITimestampedWindowStore<object, object> && queryableStoreType is WindowStoreType<object, object>)
            {
                //return (List<T>)new ReadOnlyWindowStoreFacade<object, object>((ITimestampedWindowStore<object, object>)store);
            }
            return (List<T>)store;
        }
    }
}