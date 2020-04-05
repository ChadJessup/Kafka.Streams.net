//using Kafka.Streams.KStream;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.State;
//using Kafka.Streams.State.KeyValues;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    public class InMemoryKeyValueLoggedStoreTest : AbstractKeyValueStoreTest
//    {
//        protected IKeyValueStore<K, V> CreateKeyValueStore<K, V>(IProcessorContext context)
//        {
//            var storeBuilder = Stores.KeyValueStoreBuilder(
//                Stores.InMemoryKeyValueStore("my-store"),
//                (Serde<K>)context.keySerde,
//                (Serde<V>)context.valueSerde)
//                .withLoggingEnabled(Collections.singletonMap("retention.ms", "1000"));

//            IStateStore store = storeBuilder.Build();
//            store.Init(context, store);

//            return (IKeyValueStore<K, V>)store;
//        }
//    }
//}
