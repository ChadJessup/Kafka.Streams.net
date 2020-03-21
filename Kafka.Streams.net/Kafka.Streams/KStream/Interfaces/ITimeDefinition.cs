﻿using Kafka.Streams.KStream.Internals.Suppress;
using Kafka.Streams.Processors.Interfaces;

namespace Kafka.Streams.KStream.Interfaces
{
    /**
     * This interface should never be instantiated outside of this.
     */
    public interface ITimeDefinition<K>
    {
        long Time(IProcessorContext context, K key);

        TimeDefinitionType Type();
    }
}
