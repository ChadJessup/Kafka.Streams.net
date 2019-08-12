
namespace Kafka.Streams.State
{
    public class WindowStoreType<K, V> : QueryableStoreTypeMatcher<ReadOnlyWindowStore<K, V>>
    {
        WindowStoreType()
            : base(Collections.singleton(ReadOnlyWindowStore))
        {
        }


        public ReadOnlyWindowStore<K, V> create(StateStoreProvider storeProvider,
                                                string storeName)
        {
            return new CompositeReadOnlyWindowStore<>(storeProvider, this, storeName);
        }
    }
}
