using Kafka.Streams.Configs;
using Kafka.Streams.Processors;
using Kafka.Streams.Tasks;
using Kafka.Streams.Topologies;
using System;

namespace Kafka.Streams.Threads
{
    public interface IThreadContext<out TThread, TThreadStateMachine, TThreadStates>
        where TThread : IThread<TThreadStates>
        where TThreadStateMachine : IStateMachine<TThreadStates>
        where TThreadStates : Enum
    {
        StreamsConfig StreamsConfig { get; }
        TThread Thread { get; }
        TThreadStateMachine State { get; }
        ThreadMetadata Metadata { get; }
        TaskManager TaskManager { get; }
        InternalTopologyBuilder InternalTopologyBuilder { get; }
        string GetTaskProducerClientId(string threadClientId, TaskId taskId);
        string GetThreadProducerClientId(string threadClientId);
        string GetConsumerClientId(string threadClientId);
        string GetRestoreConsumerClientId(string threadClientId);
        string GetSharedAdminClientId(string clientId);
        void SetStateListener(IStateListener listener);
    }
}