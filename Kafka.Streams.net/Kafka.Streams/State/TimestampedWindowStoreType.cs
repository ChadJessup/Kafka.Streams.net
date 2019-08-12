





















namespace Kafka.Streams.State
{

    public static class TimestampedWindowStoreType<K, V>
        : QueryableStoreTypeMatcher<ReadOnlyWindowStore<K, ValueAndTimestamp<V>>>
    {

        TimestampedWindowStoreType()
    : base(new HashSet<>(Arrays.asList(
        ITimestampedWindowStore,
        ReadOnlyWindowStore)))
        {
        }


        public ReadOnlyWindowStore<K, ValueAndTimestamp<V>> create(StateStoreProvider storeProvider,
                                                                   string storeName)
        {
            return new CompositeReadOnlyWindowStore<>(storeProvider, this, storeName);
        }
    }
}
