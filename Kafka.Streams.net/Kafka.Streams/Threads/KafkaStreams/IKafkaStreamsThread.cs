using Kafka.Streams.Threads.KafkaStream;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Threads.KafkaStreams
{
    public interface IKafkaStreamsThread : IThread<KafkaStreamsThreadStates>, IDisposable
    {
        Dictionary<long, KafkaStreamThreadState> ThreadStates { get; }
    }
}