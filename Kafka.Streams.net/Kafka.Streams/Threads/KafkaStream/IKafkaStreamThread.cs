using Confluent.Kafka;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Tasks;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Threads.KafkaStream
{
    public interface IKafkaStreamThread : IThread<KafkaStreamThreadStates>, IDisposable
    {
        Dictionary<TaskId, StreamTask> Tasks();
        bool IsRunningAndNotRebalancing();
        IConsumerRebalanceListener RebalanceListener { get; }
        TaskManager TaskManager { get; }
        IConsumer<byte[], byte[]> Consumer { get; }
        void RunOnce();
        void Shutdown();
    }
}