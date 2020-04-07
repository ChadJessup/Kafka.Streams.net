
using Confluent.Kafka;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Internals;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public class ValueAndTimestampDeserializer<V> : IDeserializer<ValueAndTimestamp<V>>
    {
        private static readonly IDeserializer<long> LONG_DESERIALIZER = Serdes.Long().Deserializer;

        public IDeserializer<V> valueDeserializer;
        private readonly IDeserializer<long> timestampDeserializer;

        public ValueAndTimestampDeserializer(IDeserializer<V> valueDeserializer)
        {
            valueDeserializer = valueDeserializer ?? throw new ArgumentNullException(nameof(valueDeserializer));

            this.valueDeserializer = valueDeserializer;
            timestampDeserializer = Serdes.Long().Deserializer;
        }

        public void Configure(
            IDictionary<string, string> configs,
            bool isKey)
        {
            // valueDeserializer.Configure(configs, isKey);
            // timestampDeserializer.Configure(configs, isKey);
        }

        public ValueAndTimestamp<V> Deserialize(
            string topic,
            byte[] valueAndTimestamp)
        {
            if (valueAndTimestamp == null)
            {
                return null;
            }

            var timestamp = timestampDeserializer.Deserialize(RawTimestamp(valueAndTimestamp), false, new SerializationContext(MessageComponentType.Value, topic));
            V value = valueDeserializer.Deserialize(RawValue(valueAndTimestamp), false, new SerializationContext(MessageComponentType.Value, topic));

            return ValueAndTimestamp.Make(value, timestamp);
        }

        public void Close()
        {
            // valueDeserializer.close();
            // timestampDeserializer.close();
        }

        static byte[] RawValue(byte[] rawValueAndTimestamp)
        {
            var rawValueLength = rawValueAndTimestamp.Length - 8;

            return new ByteBuffer()
                .Allocate(rawValueLength)
                .Add(rawValueAndTimestamp)//, 8, rawValueLength)
                .Array();
        }

        private static byte[] RawTimestamp(byte[] rawValueAndTimestamp)
        {
            return new ByteBuffer()
                .Allocate(8)
                .Add(rawValueAndTimestamp)//.UnionWith(, 0, 8 })
                .Array();
        }

        public ValueAndTimestamp<V> Deserialize(
            ReadOnlySpan<byte> data,
            bool isNull,
            SerializationContext context)
        {
            return Deserialize(context.Topic, data.ToArray());
        }
    }
}
