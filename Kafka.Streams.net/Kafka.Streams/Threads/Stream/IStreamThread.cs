using Confluent.Kafka;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Tasks;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Threads.Stream
{
    public interface IStreamThread : IThread<StreamThreadStates>, IDisposable
    {
        Dictionary<TaskId, StreamTask> Tasks();
        bool IsRunningAndNotRebalancing();
        IConsumerRebalanceListener RebalanceListener { get; }
        ITaskManager TaskManager { get; }
        IConsumer<byte[], byte[]> Consumer { get; }
        void RunOnce();
        void Shutdown();
        bool MaybeCommit();
        void SetNow(long now);
    }
}