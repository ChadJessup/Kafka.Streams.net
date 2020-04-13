using Confluent.Kafka;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Serialization;
using System;

namespace Kafka.Streams.KStream
{
    public static class Serdes
    {
        public static Serde<T> SerdeFrom<T>(ISerializer<T> serializer, IDeserializer<T> deserializer)
            => new Serde<T>(serializer, deserializer);

        public static Serde<T> SerdeFrom<T>()
            => Activator.CreateInstance<Serde<T>>();

        public static object SerdeFrom(Type type)
            => Activator.CreateInstance(type);

        public static ISerde<string> String()
            => new Serde<string>(Serializers.Utf8, Deserializers.Utf8);

        public static ISerde<long> Long()
            => new Serde<long>(Serializers.Int64, Deserializers.Int64);

        public static ISerde<double> Double()
            => new Serde<double>(Serializers.Double, Deserializers.Double);

        public static ISerde<int> Int()
            => new Serde<int>(Serializers.Int32, Deserializers.Int32);

        public static ISerde<byte[]> ByteArray()
            => new Serde<byte[]>(Serializers.ByteArray, Deserializers.ByteArray);

        public static ISerde<Bytes> Bytes()
            => new BytesSerdes();
    }
}
