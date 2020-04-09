using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream;

namespace Kafka.Streams.Serialization
{
    public class BytesSerdes : ISerde<Bytes>, ISerializer<Bytes>, IDeserializer<Bytes>
    {
        public ISerializer<Bytes> Serializer => this;
        public IDeserializer<Bytes> Deserializer => this;

        public void Close()
        {
        }

        public void Configure(IDictionary<string, string?> configs, bool isKey)
        {
        }

        public Bytes Deserialize(
            ReadOnlySpan<byte> data,
            bool isNull,
            SerializationContext context)
        {
            return new Bytes(Deserializers.ByteArray.Deserialize(data, isNull, context));
        }

        public void Dispose()
        {
        }

        public byte[] Serialize(
            Bytes data,
            SerializationContext context)
        {
            return Serializers.ByteArray.Serialize(data.Get(), context);
        }
    }
}
