namespace Kafka.Streams.KStream.Internals
{
    public interface IKTableValueGetterSupplier<K, V>
    {
        IKTableValueGetter<K, V> Get();

        string[] StoreNames();
    }
}
