using Confluent.Kafka;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Internals;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public static class ValueAndTimestampDeserializer
    {
        public static byte[] RawValue(byte[] rawValueAndTimestamp)
        {
            if (rawValueAndTimestamp is null)
            {
                throw new ArgumentNullException(nameof(rawValueAndTimestamp));
            }

            var rawValueLength = rawValueAndTimestamp.Length - 8;

            return new ByteBuffer()
                .Allocate(rawValueLength)
                .Add(rawValueAndTimestamp)//, 8, rawValueLength)
                .Array();
        }

        public static byte[] RawTimestamp(byte[] rawValueAndTimestamp)
        {
            return new ByteBuffer()
                .Allocate(8)
                .Add(rawValueAndTimestamp)//.UnionWith(, 0, 8 })
                .Array();
        }

        public static byte[]? ConvertToTimestampedFormat(byte[] plainValue)
        {
            if (plainValue == null)
            {
                return null;
            }

            return new ByteBuffer()
                .Allocate(8 + plainValue.Length)
                .PutLong(-1)
                .Add(plainValue)
                .Array();
        }
    }

    public class ValueAndTimestampDeserializer<V> : IDeserializer<IValueAndTimestamp<V>>
    {
        private static readonly IDeserializer<long> LONG_DESERIALIZER = Serdes.Long().Deserializer;

        public IDeserializer<V> valueDeserializer { get; }
        private readonly IDeserializer<long> timestampDeserializer;

        public ValueAndTimestampDeserializer(IDeserializer<V> valueDeserializer)
        {
            valueDeserializer = valueDeserializer ?? throw new ArgumentNullException(nameof(valueDeserializer));

            this.valueDeserializer = valueDeserializer;
            this.timestampDeserializer = Serdes.Long().Deserializer;
        }

        public void Configure(
            IDictionary<string, string> configs,
            bool isKey)
        {
            // valueDeserializer.Configure(configs, isKey);
            // timestampDeserializer.Configure(configs, isKey);
        }

        public IValueAndTimestamp<V>? Deserialize(string topic, byte[] valueAndTimestamp)
        {
            if (valueAndTimestamp == null)
            {
                return null;
            }

            var timestamp = Timestamp.UnixTimestampMsToDateTime(this.timestampDeserializer.Deserialize(ValueAndTimestampDeserializer.RawTimestamp(valueAndTimestamp), false, new SerializationContext(MessageComponentType.Value, topic)));
            V value = this.valueDeserializer.Deserialize(ValueAndTimestampDeserializer.RawValue(valueAndTimestamp), false, new SerializationContext(MessageComponentType.Value, topic));

            return ValueAndTimestamp.Make(value, timestamp);
        }

        public void Close()
        {
            // valueDeserializer.Close();
            // timestampDeserializer.Close();
        }

        public IValueAndTimestamp<V>? Deserialize(
            ReadOnlySpan<byte> data,
            bool isNull,
            SerializationContext context)
        {
            return this.Deserialize(context.Topic, data.ToArray());
        }
    }
}
