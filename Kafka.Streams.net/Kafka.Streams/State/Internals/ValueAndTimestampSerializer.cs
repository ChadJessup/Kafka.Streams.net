
using Confluent.Kafka;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Internals;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{

    public class ValueAndTimestampSerializer<V> : ISerializer<ValueAndTimestamp<V>>
    {
        public ISerializer<V> valueSerializer;
        private readonly ISerializer<long> timestampSerializer;

        public ValueAndTimestampSerializer(ISerializer<V> valueSerializer)
        {
            valueSerializer = valueSerializer ?? throw new ArgumentNullException(nameof(valueSerializer));

            this.valueSerializer = valueSerializer;
            timestampSerializer = Serdes.Long().Serializer;
        }

        public void Configure(
            IDictionary<string, string> configs,
            bool isKey)
        {
            // valueSerializer.Configure(configs, isKey);
            // timestampSerializer.Configure(configs, isKey);
        }

        public byte[] Serialize(
            ValueAndTimestamp<V> data,
            SerializationContext context)
        {
            if (data == null)
            {
                return null;
            }

            return Serialize(context.Topic, data.Value, data.Timestamp);
        }

        public byte[] Serialize(
            string topic,
            V data,
            long timestamp)
        {
            if (data == null)
            {
                return null;
            }

            var RawValue = valueSerializer.Serialize(data, new SerializationContext(MessageComponentType.Value, topic));
            var rawTimestamp = timestampSerializer.Serialize(timestamp, new SerializationContext(MessageComponentType.Value, topic));

            return new ByteBuffer()
                .Allocate(rawTimestamp.Length + RawValue.Length)
                .Add(rawTimestamp)
                .Add(RawValue)
                .Array();
        }

        public void Close()
        {
            // valueSerializer.close();
            // timestampSerializer.close();
        }
    }
}
