using Confluent.Kafka;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public class InternalConsumerConfig : ConsumerConfig
    {

        //public InternalConsumerConfig(Dictionary<string, object> props)
        //    : base(ConsumerConfig.AddDeserializerToConfig(props, new ByteArrayDeserializer(), new ByteArrayDeserializer()), false)
        //{
        //}
    }
}
