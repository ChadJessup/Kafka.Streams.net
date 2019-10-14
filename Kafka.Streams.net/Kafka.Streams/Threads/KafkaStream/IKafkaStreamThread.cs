using Kafka.Streams.Tasks;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Kafka.Streams.Threads.KafkaStream
{
    public interface IKafkaStreamThread : IThread<KafkaStreamThreadStates>, IDisposable
    {
        Dictionary<TaskId, StreamTask> Tasks();
        bool IsRunningAndNotRebalancing();
    }
}