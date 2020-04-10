
using Confluent.Kafka;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Internals;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{

    public class ValueAndTimestampSerializer<V> : ISerializer<IValueAndTimestamp<V>>
    {
        public ISerializer<V> valueSerializer;
        private readonly ISerializer<long> timestampSerializer;

        public ValueAndTimestampSerializer(ISerializer<V> valueSerializer)
        {
            valueSerializer = valueSerializer ?? throw new ArgumentNullException(nameof(valueSerializer));

            this.valueSerializer = valueSerializer;
            this.timestampSerializer = Serdes.Long().Serializer;
        }

        public void Configure(
            IDictionary<string, string> configs,
            bool isKey)
        {
            // valueSerializer.Configure(configs, isKey);
            // timestampSerializer.Configure(configs, isKey);
        }

        public byte[] Serialize(
            IValueAndTimestamp<V> data,
            SerializationContext context)
        {
            if (data == null)
            {
                return null;
            }

            return this.Serialize(context.Topic, data.Value, data.Timestamp);
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

            var RawValue = this.valueSerializer.Serialize(data, new SerializationContext(MessageComponentType.Value, topic));
            var rawTimestamp = this.timestampSerializer.Serialize(timestamp, new SerializationContext(MessageComponentType.Value, topic));

            return new ByteBuffer()
                .Allocate(rawTimestamp.Length + RawValue.Length)
                .Add(rawTimestamp)
                .Add(RawValue)
                .Array();
        }

        public void Close()
        {
            // valueSerializer.Close();
            // timestampSerializer.Close();
        }
    }
}
