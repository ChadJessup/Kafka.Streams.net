
using Confluent.Kafka;
using Kafka.Streams.Interfaces;

namespace Kafka.Streams.KStream.Internals
{
    public class ConsumedInternal<K, V> : Consumed<K, V>
    {
        public ConsumedInternal(Consumed<K, V> consumed)
            : base(consumed)
        {
        }

        public ConsumedInternal(
            ISerde<K> keySerde,
            ISerde<V> valSerde,
            ITimestampExtractor timestampExtractor,
            AutoOffsetReset? offsetReset)
            : this(Consumed.With(keySerde, valSerde, timestampExtractor, offsetReset))
        {
        }

        public ConsumedInternal()
            : this(Consumed.With<K, V>(null, null))
        {
        }

        public IDeserializer<K> KeyDeserializer()
        {
            return this.KeySerde?.Deserializer;
        }

        public IDeserializer<V> ValueDeserializer()
        {
            return this.ValueSerde?.Deserializer;
        }

        public AutoOffsetReset? OffsetResetPolicy()
        {
            return this.ResetPolicy;
        }

        public string Name => this.ProcessorName;
    }
}
