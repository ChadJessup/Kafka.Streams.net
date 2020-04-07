

namespace Kafka.Streams.KStream.Internals
{
    [System.Obsolete]
    public class SerializedInternal<K, V> : Serialized<K, V>
    {
        public SerializedInternal(ISerialized<K, V> serialized)
            : base(serialized)
        {
        }
    }
}
