namespace Kafka.Streams.Tests.State.Internals
{
    /*






    *

    *





    */











    public class InMemoryKeyValueLoggedStoreTest : AbstractKeyValueStoreTest
    {



        protected KeyValueStore<K, V> CreateKeyValueStore<K, V>(ProcessorContext context)
        {
            StoreBuilder storeBuilder = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("my-store"),
                (Serde<K>)context.keySerde(),
                (Serde<V>)context.valueSerde())
                .withLoggingEnabled(Collections.singletonMap("retention.ms", "1000"));

            StateStore store = storeBuilder.build();
            store.init(context, store);

            return (KeyValueStore<K, V>)store;
        }
    }
}
/*






*

*





*/











