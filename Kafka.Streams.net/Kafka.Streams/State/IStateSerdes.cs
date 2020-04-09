using Confluent.Kafka;

namespace Kafka.Streams.State
{
    public interface IStateSerdes<K, V>
    {
        string Topic { get; }
        IDeserializer<K> KeyDeserializer();
        K KeyFrom(byte[] rawKey);
        ISerializer<K> KeySerializer();
        byte[] RawKey(K key);
        byte[] RawValue(V value);
        IDeserializer<V> ValueDeserializer();
        V ValueFrom(byte[] RawValue);
        ISerializer<V> ValueSerializer();
    }
}
