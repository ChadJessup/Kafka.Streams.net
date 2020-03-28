using Confluent.Kafka;
using Kafka.Streams.Factories;
using Kafka.Streams.Interfaces;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream
{
    public class Serde<T> : ISerde<T>
    {
        public Serde(SerdeFactory<T> factory)
        {
            if (factory is null)
            {
                throw new ArgumentNullException(nameof(factory));
            }

            this.Serializer = factory.GetSerializer();
            this.Deserializer = factory.GetDeserializer();
        }

        public Serde(ISerializer<T> serializer, IDeserializer<T> deserializer)
        {
            this.Serializer = serializer;
            this.Deserializer = deserializer;
        }

        public virtual void Configure(IDictionary<string, string?> configs, bool isKey)
        {
        }

        public ISerializer<T> Serializer { get; }
        public IDeserializer<T> Deserializer { get; }

        public virtual void Close()
        {
        }

        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                }

                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}
