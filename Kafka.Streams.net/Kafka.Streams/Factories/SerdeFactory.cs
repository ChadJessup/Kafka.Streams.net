using Confluent.Kafka;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Factories
{
    public interface ISerdeFactory<T>
    {
    }

    public class SerdeFactory<T> : ISerdeFactory<T>
    {
        private readonly SerializerFactory serializerFactory;
        private readonly DeserializerFactory deserializerFactory;

        public SerdeFactory(SerializerFactory serializerFactory, DeserializerFactory deserializerFactory)
        {
            this.serializerFactory = serializerFactory;
            this.deserializerFactory = deserializerFactory;
        }

        public ISerializer<T> GetSerializer()
            => this.serializerFactory.GetSerializer<T>();

        public IDeserializer<T> GetDeserializer()
            => this.deserializerFactory.GetDeserializer<T>();

        public ISerde<T> GetSerde()
        {
            return new Serde<T>(
                this.serializerFactory.GetSerializer<T>(),
                this.deserializerFactory.GetDeserializer<T>());
        }
    }
}
