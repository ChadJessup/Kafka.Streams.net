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
    public class StreamStateListener : IStateListener
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
         * If all threads are dead set to ERROR
         */
        private void MaybeSetError()
        {
            // check if we have at least one thread running
            foreach (var state in threadStates.Values)
            {
                if (state.CurrentState != StreamThreadStates.DEAD)
                {
                    return;
                }
            }

            if (this.kafkaStreams.State.SetState(KafkaStreamsThreadStates.ERROR))
            {
                logger.LogError("All stream threads have died. The instance will be in error state and should be closed.");
            }
        }

        /**
         * If all threads are up, including the global thread, set to RUNNING
         */
        private void MaybeSetRunning()
        {
            // state can be transferred to RUNNING if all threads are either RUNNING or DEAD
            foreach (var state in threadStates.Values)
            {
                if (state.CurrentState != StreamThreadStates.RUNNING
                    && state.CurrentState != StreamThreadStates.DEAD)
                {
                    return;
                }
            }

            // the global state thread is relevant only if it is started. There are cases
            // when we don't have a global state thread at all, e.g., when we don't have global KTables
            if (this.globalThread != null
                && this.globalThread.State.CurrentState != GlobalStreamThreadStates.RUNNING)
            {
                return;
            }

            this.kafkaStreams.State.SetState(KafkaStreamsThreadStates.RUNNING);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void onChange<States>(
            IThread<States> thread,
            States abstractNewState,
            States abstractOldState)
                where States : Enum
        {
            lock (threadStatesLock)
            {
                // StreamThreads first
                if (thread is StreamThread kafkaStreamThread && kafkaStreamThread.State is StreamThreadState state)
                {
                    var newState = (StreamThreadStates)(object)abstractNewState;
                    if (threadStates.TryAdd(kafkaStreamThread.ManagedThreadId, state))
                    {
                        threadStates[kafkaStreamThread.ManagedThreadId] = state;
                    }

                    if (newState == StreamThreadStates.PARTITIONS_REVOKED)
                    {
                        this.kafkaStreams.State.SetState(KafkaStreamsThreadStates.REBALANCING);
                    }
                    else if (newState == StreamThreadStates.RUNNING)
                    {
                        MaybeSetRunning();
                    }
                    else if (newState == StreamThreadStates.DEAD)
                    {
                        MaybeSetError();
                    }
                }
                else if (thread is GlobalStreamThread)
                {
                    // global stream thread has different invariants
                    var newState = (GlobalStreamThreadStates)(object)abstractNewState;
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

        public void SetThreadStates(Dictionary<long, StreamThreadState> threadStates)
        {
            this.threadStates = threadStates;
        }
    }
}
