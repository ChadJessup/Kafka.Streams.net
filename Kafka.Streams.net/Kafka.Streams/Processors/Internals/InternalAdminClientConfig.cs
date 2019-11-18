using Confluent.Kafka;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public class InternalAdminClientConfig : AdminClientConfig
    {
        private InternalAdminClientConfig(Dictionary<object, object> props)
    //        : base(props, false)
        {
        }
    }
}
