using Kafka.Streams.Threads.GlobalStream;
using Kafka.Streams.Threads.KafkaStream;
using Kafka.Streams.Threads.KafkaStreams;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Kafka.Streams.Threads
{
    /**
     * Class that handles stream thread transitions
     */
    public class StreamStateListener : IStateListener
    {
        private readonly ILogger<StreamStateListener> logger;

        private Dictionary<long, KafkaStreamThreadState> threadStates;
        private readonly IKafkaStreamsThread kafkaStreams;
        private readonly IGlobalStreamThread globalThread;
        
        // this lock should always be held before the state lock
        private readonly object threadStatesLock;

        public StreamStateListener(
            ILogger<StreamStateListener> logger,
            IGlobalStreamThread globalThread,
            IKafkaStreamsThread kafkaStreams)
        {
            this.logger = logger;

            this.kafkaStreams = kafkaStreams;
            this.globalThread = globalThread;
            
            this.threadStatesLock = new object();
        }

        /**
         * If all threads are dead set to ERROR
         */
        private void maybeSetError()
        {
            // check if we have at least one thread running
            foreach (var state in threadStates.Values)
            {
                if (state.CurrentState != KafkaStreamThreadStates.DEAD)
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
        private void maybeSetRunning()
        {
            // state can be transferred to RUNNING if all threads are either RUNNING or DEAD
            foreach (var state in threadStates.Values)
            {
                if (state.CurrentState != KafkaStreamThreadStates.RUNNING
                    && state.CurrentState != KafkaStreamThreadStates.DEAD)
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
                if (thread is KafkaStreamThread)
                {
                    KafkaStreamThreadStates newState = (KafkaStreamThreadStates)(object)abstractNewState;
                    threadStates.Add(thread.ManagedThreadId, null);// newState);

                    if (newState == KafkaStreamThreadStates.PARTITIONS_REVOKED)
                    {
                        this.kafkaStreams.State.SetState(KafkaStreamsThreadStates.REBALANCING);
                    }
                    else if (newState == KafkaStreamThreadStates.RUNNING)
                    {
                        maybeSetRunning();
                    }
                    else if (newState == KafkaStreamThreadStates.DEAD)
                    {
                        maybeSetError();
                    }
                }
                else if (thread is GlobalStreamThread)
                {
                    // global stream thread has different invariants
                    GlobalStreamThreadStates newState = (GlobalStreamThreadStates)(object)abstractNewState;
                    this.globalThread.State.SetState(newState);

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

        public void SetThreadStates(Dictionary<long, KafkaStreamThreadState> threadStates)
        {
            this.threadStates = threadStates;
        }
    }
}
