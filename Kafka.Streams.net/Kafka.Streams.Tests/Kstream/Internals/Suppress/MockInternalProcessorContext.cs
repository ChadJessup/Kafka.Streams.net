using Kafka.Streams.Nodes;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Tests.Kstream.Internals.Suppress
{
    internal class MockInternalProcessorContext
    {
        internal void SetRecordMetadata(string v1, int v2, long v3, object p, long timestamp)
        {
            throw new NotImplementedException();
        }

        internal void setCurrentNode(ProcessorNode processorNode)
        {
            throw new NotImplementedException();
        }

        internal IEnumerable<object> Forwarded()
        {
            throw new NotImplementedException();
        }
    }
}