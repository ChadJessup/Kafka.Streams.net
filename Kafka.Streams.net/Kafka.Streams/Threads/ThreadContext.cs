using Kafka.Streams.Configs;
using Kafka.Streams.Processors;
using Kafka.Streams.Tasks;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.Logging;
using System;

namespace Kafka.Streams.Threads
{
    public class ThreadContext2<TThread, TThreadStateMachine, TThreadStates> : IThreadContext<TThread, TThreadStateMachine, TThreadStates>
        where TThreadStateMachine : IStateMachine<TThreadStates>
        where TThread : IThread<TThreadStates>
        where TThreadStates : Enum
    {
        public ThreadContext2(
            ILogger<IThreadContext<TThread, TThreadStateMachine, TThreadStates>> logger,
            TThread thread,
            TThreadStateMachine threadState,
            StreamsConfig streamsConfig)
        {
            this.Logger = logger;
            this.StreamsConfig = streamsConfig;
            //this.Metadata = new ThreadMetadata(;
            this.State = threadState;
            this.Thread = thread;

//            this.Thread.Context = this;
        }

        public ILogger<IThreadContext<TThread, TThreadStateMachine, TThreadStates>> Logger { get; }
        public IStateListener StateListener { get; protected set; }
        public StreamsConfig StreamsConfig { get; }
        public TThread Thread { get; }
        public TThreadStateMachine State { get; }
        public ThreadMetadata Metadata { get; }
        public TaskManager TaskManager { get; }
        public InternalTopologyBuilder InternalTopologyBuilder { get; }
        
        public string GetTaskProducerClientId(string threadClientId, TaskId taskId)
            => $"{threadClientId}-{taskId}-producer";

        public string GetThreadProducerClientId(string threadClientId)
            => $"{threadClientId}-producer";

        public string GetConsumerClientId(string threadClientId)
            => $"{threadClientId}-consumer";

        public string GetRestoreConsumerClientId(string threadClientId)
            => $"{threadClientId}-restore-consumer";

        // currently admin client is shared among all threads
        public string GetSharedAdminClientId(string clientId)
            => $"{clientId}-admin";

        /**
         * Set the {@link KafkaStreamThread.StateListener} to be notified when state changes. Note this API is internal to
         * Kafka Streams and is not intended to be used by an external application.
         */
        public virtual void SetStateListener(IStateListener listener)
            => this.StateListener = listener;
    }
}