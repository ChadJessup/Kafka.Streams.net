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
using Kafka.Streams.Processor.Internals;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Kafka.Streams
{
    /**
     * Class that handles stream thread transitions
     */
    public class StreamStateListener : IStateListener
    {
        private Dictionary<long, StreamThread.State> threadState;
        private GlobalStreamThread.State globalThreadState;
        // this lock should always be held before the state lock
        private object threadStatesLock;

        StreamStateListener(
            Dictionary<long, StreamThread.State> threadState,
            GlobalStreamThread.State globalThreadState)
        {
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
            //foreach (StreamThread.State state in threadState.Values)
            //{
            //    if (state != KafkaStreamsStates.DEAD)
            //    {
            //        return;
            //    }
            //}

            //if (setState(KafkaStreamsStates.ERROR))
            //{
            //    log.LogError("All stream threads have died. The instance will be in error state and should be closed.");
            //}
        }

        /**
         * If all threads are up, including the global thread, set to RUNNING
         */
        private void maybeSetRunning()
        {
            // state can be transferred to RUNNING if all threads are either RUNNING or DEAD
            //foreach (StreamThread.State state in threadState.values())
            //{
            //    if (state != StreamThread.State.RUNNING && state != StreamThread.State.DEAD)
            //    {
            //        return;
            //    }
            //}

            // the global state thread is relevant only if it is started. There are cases
            // when we don't have a global state thread at all, e.g., when we don't have global KTables
            //if (globalThreadState != null && globalThreadState != GlobalStreamThread.State.RUNNING)
            //{
            //    return;
            //}

            //setState(State.RUNNING);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void onChange(
            KafkaStreamsStates abstractNewState,
            KafkaStreamsStates abstractOldState)
        {
            lock (threadStatesLock)
            {
                // StreamThreads first
                //if (thread is StreamThread)
                //{
                //    //                      StreamThread.State newState = (StreamThread.State)abstractNewState;
                //    //                      threadState.put(thread.getId(), newState);

                //    if (newState == StreamThread.State.PARTITIONS_REVOKED)
                //    {
                //        //                            setState(State.REBALANCING);
                //    }
                //    else if (newState == StreamThread.State.RUNNING)
                //    {
                //        maybeSetRunning();
                //    }
                //    else if (newState == StreamThread.State.DEAD)
                //    {
                //        maybeSetError();
                //    }
                //}
                //else if (thread is GlobalStreamThread)
                //{
                //    // global stream thread has different invariants
                //    GlobalStreamThread.State newState = (GlobalStreamThread.State)abstractNewState;
                //    globalThreadState = newState;

                //    // special case when global thread is dead
                //    if (newState == GlobalStreamThread.State.DEAD)
                //    {
                //        if (setState(State.ERROR))
                //        {
                //            log.error("Global thread has died. The instance will be in error state and should be closed.");
                //        }
                //    }
                //}
            }
        }
    }
}
