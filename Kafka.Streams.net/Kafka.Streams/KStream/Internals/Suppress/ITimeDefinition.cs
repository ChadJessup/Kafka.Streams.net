using Kafka.Streams.Processors.Interfaces;
using System;

namespace Kafka.Streams.KStream.Internals.Suppress
{
    public interface ITimeDefinition<K>
    {
        DateTime Time(IProcessorContext context, K key);

        TimeDefinitionType Type();
    }
}
