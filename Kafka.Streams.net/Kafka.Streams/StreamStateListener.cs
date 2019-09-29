/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Threads.GlobalStream;
using Kafka.Streams.Threads.KafkaStreams;
using Kafka.Streams.Threads.KafkaStream;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Kafka.Streams
{
    /**
     * Class that handles stream thread transitions
     */
    public class StreamStateListener : IStateListener
    {
        private readonly ILogger<StreamStateListener> logger;
        private readonly KafkaStreamsThreadState kafkaStreamsState;
        private readonly Dictionary<long, KafkaStreamThreadState> threadState;
        private readonly GlobalStreamThreadState globalThreadState;
        // this lock should always be held before the state lock
        private readonly object threadStatesLock;

        public StreamStateListener(
            ILogger<StreamStateListener> logger,
            Dictionary<long, KafkaStreamThreadState> threadState,
            GlobalStreamThreadState globalThreadState,
            KafkaStreamsThreadState kafkaStreamsState)
        {
            this.logger = logger;
            this.kafkaStreamsState = kafkaStreamsState;
            this.threadState = threadState;
            this.globalThreadState = globalThreadState;
            this.threadStatesLock = new object();
        }

        /**
         * If all threads are dead set to ERROR
         */
        private void maybeSetError()
        {
            // check if we have at least one thread running
            foreach (var state in threadState.Values)
            {
                if (state.CurrentState != KafkaStreamThreadStates.DEAD)
                {
                    return;
                }
            }

            if (this.kafkaStreamsState.setState(KafkaStreamsThreadStates.ERROR))
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
            foreach (var state in threadState.Values)
            {
                if (state.CurrentState != KafkaStreamThreadStates.RUNNING
                    && state.CurrentState != KafkaStreamThreadStates.DEAD)
                {
                    return;
                }
            }

            // the global state thread is relevant only if it is started. There are cases
            // when we don't have a global state thread at all, e.g., when we don't have global KTables
            if (globalThreadState != null
                && globalThreadState.CurrentState != GlobalStreamThreadStates.RUNNING)
            {
                return;
            }

            this.kafkaStreamsState.setState(KafkaStreamsThreadStates.RUNNING);
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
//                    threadState.Add(thread.Thread.ManagedThreadId, newState);

                    if (newState == KafkaStreamThreadStates.PARTITIONS_REVOKED)
                    {
                        this.kafkaStreamsState.setState(KafkaStreamsThreadStates.REBALANCING);
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
                    globalThreadState.setState(newState);

                    // special case when global thread is dead
                    if (newState == GlobalStreamThreadStates.DEAD)
                    {
                        if (this.kafkaStreamsState.setState(KafkaStreamsThreadStates.ERROR))
                        {
                            this.logger.LogError("Global thread has died. The instance will be in error state and should be closed.");
                        }
                    }
                }
            }
        }
    }
}
