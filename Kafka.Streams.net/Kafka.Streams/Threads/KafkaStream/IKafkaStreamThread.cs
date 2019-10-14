using Kafka.Streams.Tasks;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Threads.KafkaStream
{
    public interface IKafkaStreamThread : IThread<KafkaStreamThreadStates>, IDisposable
    {
        Dictionary<TaskId, StreamTask> Tasks();
        bool IsRunningAndNotRebalancing();
    }
}