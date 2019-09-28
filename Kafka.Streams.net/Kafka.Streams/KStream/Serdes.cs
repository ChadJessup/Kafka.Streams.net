﻿using Confluent.Kafka;
using Kafka.Streams.Interfaces;
using System;

namespace Kafka.Streams.KStream
{
    public static class Serdes
    {
        public static Serde<T> serdeFrom<T>()
            => Activator.CreateInstance<Serde<T>>();
        public static object serdeFrom(Type type)
            => Activator.CreateInstance(type);

        public static ISerde<string> String()
            => new Serde<string>(Serializers.Utf8, Deserializers.Utf8);

        public static ISerde<long> Long()
            => new Serde<long>(Serializers.Int64, Deserializers.Int64);
    }
}