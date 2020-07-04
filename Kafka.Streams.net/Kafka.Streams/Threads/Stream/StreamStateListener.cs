using Kafka.Streams.Threads.GlobalStream;
using Kafka.Streams.Threads.KafkaStreams;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Kafka.Streams.Threads.Stream
{
    /**
     * Class that handles stream thread transitions
     */
    public class StreamStateListener :
        IStateListener<KafkaStreamsThreadStates>,
        IStateListener<StreamThreadStates>,
        IStateListener<GlobalStreamThreadStates>
    {
        private readonly ILogger<StreamStateListener> logger;

        private Dictionary<long, StreamThreadState> threadStates = new Dictionary<long, StreamThreadState>();
        private readonly IKafkaStreamsThread kafkaStreams;
        private readonly IGlobalStreamThread? globalThread;

        // this lock should always be held before the state lock
        private readonly object threadStatesLock;

        public StreamStateListener(
            ILogger<StreamStateListener> logger,
            IGlobalStreamThread? globalThread,
            IKafkaStreamsThread kafkaStreams)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.kafkaStreams = kafkaStreams ?? throw new ArgumentNullException(nameof(kafkaStreams));
            this.globalThread = globalThread;

            this.threadStatesLock = new object();
        }

        /**
         * If All threads are dead set to ERROR
         */
        private void MaybeSetError()
        {
            // check if we have at least one thread running
            foreach (var state in this.threadStates.Values)
            {
                if (state.CurrentState != StreamThreadStates.DEAD)
                {
                    return;
                }
            }

            if (this.kafkaStreams.State.SetState(KafkaStreamsThreadStates.ERROR))
            {
                this.logger.LogError("All stream threads have died. The instance will be in error state and should be closed.");
            }
        }

        /**
         * If All threads are up, including the global thread, set to RUNNING
         */
        private void MaybeSetRunning()
        {
            // state can be transferred to RUNNING if All threads are either RUNNING or DEAD
            foreach (var state in this.threadStates.Values)
            {
                if (state.CurrentState != StreamThreadStates.RUNNING
                    && state.CurrentState != StreamThreadStates.DEAD)
                {
                    return;
                }
            }

            // the global state thread is relevant only if it is started. There are cases
            // when we don't have a global state thread at All, e.g., when we don't have global KTables
            if (this.globalThread != null
                && this.globalThread.State.CurrentState != GlobalStreamThreadStates.RUNNING)
            {
                return;
            }

            this.kafkaStreams.State.SetState(KafkaStreamsThreadStates.RUNNING);
        }

        public void SetThreadStates(Dictionary<long, StreamThreadState> threadStates)
        {
            this.threadStates = threadStates;
        }

        public void OnChange(IThread thread, object newState, object oldState)
        {
            switch (thread)
            {
                case IStreamThread streamThread:
                    this.OnChange(streamThread, (StreamThreadStates)newState, (StreamThreadStates)oldState);
                    break;
                case IKafkaStreamsThread kafkaStreamsThread:
                    this.OnChange(kafkaStreamsThread, (KafkaStreamsThreadStates)newState, (KafkaStreamsThreadStates)oldState);
                    break;
                default:
                    throw new ArgumentException(nameof(thread));
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void OnChange(
            IThread<KafkaStreamsThreadStates> thread,
            KafkaStreamsThreadStates abstractNewState,
            KafkaStreamsThreadStates abstractOldState)
        {
            lock (this.threadStatesLock)
            {
                // StreamThreads first
                if (thread is StreamThread kafkaStreamThread && kafkaStreamThread.State is StreamThreadState state)
                {

                }
                else if (thread is GlobalStreamThread)
                {
                }
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void OnChange(
            IThread<StreamThreadStates> thread,
            StreamThreadStates newState,
            StreamThreadStates oldState)
        {
            lock (this.threadStatesLock)
            {
                var state = thread.State as StreamThreadState;

                if (this.threadStates.TryAdd(thread.ManagedThreadId, state))
                {
                    this.threadStates[thread.ManagedThreadId] = state;
                }

                switch (newState)
                {
                    case StreamThreadStates.PARTITIONS_REVOKED:
                        this.kafkaStreams.State.SetState(KafkaStreamsThreadStates.REBALANCING);
                        break;
                    case StreamThreadStates.RUNNING:
                        this.MaybeSetRunning();
                        break;
                    case StreamThreadStates.DEAD:
                        this.MaybeSetError();
                        break;
                    case StreamThreadStates.PARTITIONS_ASSIGNED:
                    case StreamThreadStates.PENDING_SHUTDOWN:
                    case StreamThreadStates.STARTING:
                    case StreamThreadStates.UNKNOWN:
                    case StreamThreadStates.CREATED:
                    default:
                        break;
                }
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void OnChange(
            IThread<GlobalStreamThreadStates> thread,
            GlobalStreamThreadStates newState,
            GlobalStreamThreadStates oldState)
        {
            lock (this.threadStatesLock)
            {
                // global stream thread has different invariants
                this.globalThread?.State.SetState(newState);

                // special case when global thread is dead
                if (newState == GlobalStreamThreadStates.DEAD)
                {
                    if (this.kafkaStreams.State.SetState(KafkaStreamsThreadStates.ERROR))
                    {
                        this.logger.LogError("Global thread has died. The instance will be in error state and should be closed.");
                    }
                }
            }
        }
    }
}
