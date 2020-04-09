using System;
using System.Security.Cryptography;
using Kafka.Streams;

namespace Confluent.Kafka
{
    public static class SerdeExtensions
    {
        public static T Deserialize<T>(this IDeserializer<T> deserializer, string topic, Bytes data, bool isKey)
            => Deserialize<T>(deserializer, topic, data.Get(), isKey);

        public static T Deserialize<T>(this IDeserializer<T> deserializer, string topic, ReadOnlySpan<byte> data, bool isKey)
        {
            if (deserializer is null)
            {
                throw new ArgumentNullException(nameof(deserializer));
            }

            return deserializer.Deserialize(
                data,
                data == null,
                new SerializationContext(isKey
                    ? MessageComponentType.Key
                    : MessageComponentType.Value,
                    topic));
        }

        public static byte[] Serialize<T>(this ISerializer<T> serializer, string topic, T data, bool isKey)
        {
            if (serializer is null)
            {
                throw new ArgumentNullException(nameof(serializer));
            }

            return serializer.Serialize(
                data,
                new SerializationContext(isKey
                    ? MessageComponentType.Key
                    : MessageComponentType.Value,
                    topic));
        }
    }
}
