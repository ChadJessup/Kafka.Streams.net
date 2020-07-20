using System;
using System.Collections.Generic;
using Kafka.Common;

namespace Kafka.Streams.Processors.Internals
{
    internal class CopartitionedTopicsEnforcer
    {
        internal void enforce(HashSet<string> copartitionGroup, Dictionary<string, InternalTopicConfig> allRepartitionTopicsNumPartitions, Cluster metadata)
        {
            throw new NotImplementedException();
        }
    }
}