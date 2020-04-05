using Confluent.Kafka;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream;
using System.Collections.Generic;

namespace Kafka.Streams.Tests.State.Internals
{
    class SerdeThatDoesntHandleNull : ISerde<string>
    {
        public ISerializer<string> Serializer { get; } = Serdes.String().Serializer;
        public IDeserializer<string> Deserializer { get; } = Serdes.String().Deserializer;

        public void Close()
        {
            throw new System.NotImplementedException();
        }

        public void Configure(IDictionary<string, string?> configs, bool isKey)
        {
            throw new System.NotImplementedException();
        }

        public void Dispose()
        {
            throw new System.NotImplementedException();
        }

        //        public IDeserializer<string> Deserializer()
        //        {
        //            return new Serdes.String().Deserializer()
        //            {
        //
        //
        //            public string deserialize(string topic, byte[] data)
        //            {
        //                if (data == null)
        //                {
        //                    throw new NullPointerException();
        //                }
        //                return base.deserialize(topic, data);
        //            }
        //        };
    }
}
