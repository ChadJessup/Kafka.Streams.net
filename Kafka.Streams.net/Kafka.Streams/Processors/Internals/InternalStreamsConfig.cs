using Kafka.Streams.Configs;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public class InternalStreamsConfig : StreamsConfig
    {
        public InternalStreamsConfig(Dictionary<string, object> props)
            : base(props, false)
        {
        }
    }
}