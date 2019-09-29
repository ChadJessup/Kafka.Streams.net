/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for.Additional information regarding copyright ownership.
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
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.Threads.GlobalStream
{
    /**
     * This is the thread responsible for keeping all Global State Stores updated.
     * It delegates most of the responsibility to the internal StateConsumer
     */
    public class GlobalStreamThreadState : IStateMachine<GlobalStreamThreadStates>
    {
        private readonly ILogger<GlobalStreamThreadState> logger;
        private Dictionary<GlobalStreamThreadStates, StateTransition<GlobalStreamThreadStates>> validTransitions = new Dictionary<GlobalStreamThreadStates, StateTransition<GlobalStreamThreadStates>>();
        private readonly object stateLock = new object();
        private readonly string logPrefix = "";

        public GlobalStreamThreadState(ILogger<GlobalStreamThreadState> logger)
        {
            this.logger = logger;
            this.Thread = null;
        }

        public GlobalStreamThreadStates CurrentState { get; private set; } = GlobalStreamThreadStates.CREATED;
        public IStateListener StateListener { get; }
        public IThread<GlobalStreamThreadStates> Thread { get; }

        public bool isRunning()
            => this.CurrentState == GlobalStreamThreadStates.RUNNING;

        public void setTransitions(IEnumerable<StateTransition<GlobalStreamThreadStates>> validTransitions)
        {
            this.validTransitions =
                validTransitions.ToDictionary(k => k.StartingState, v => v);
        }

        public bool isValidTransition(GlobalStreamThreadStates newState)
        => this.validTransitions.ContainsKey(newState)
                ? this.validTransitions[newState].PossibleTransitions.Contains(newState)
                : false;

        public bool setState(GlobalStreamThreadStates newState)
        {
            GlobalStreamThreadStates oldState = this.CurrentState;

            lock (stateLock)
            {
                if (this.CurrentState == GlobalStreamThreadStates.PENDING_SHUTDOWN
                    && newState == GlobalStreamThreadStates.PENDING_SHUTDOWN)
                {
                    // when the state is already in PENDING_SHUTDOWN, its transition to itself
                    // will be refused but we do not throw exception here
                    return false;
                }
                else if (this.CurrentState == GlobalStreamThreadStates.DEAD)
                {
                    // when the state is already in NOT_RUNNING, all its transitions
                    // will be refused but we do not throw exception here
                    return false;
                }
                else if (!this.isValidTransition(newState))
                {
                    this.logger.LogError("Unexpected state transition from {} to {}", oldState, newState);
                    throw new StreamsException(logPrefix + "Unexpected state transition from " + oldState + " to " + newState);
                }
                else
                {
                    this.logger.LogInformation("State transition from {} to {}", oldState, newState);
                }

                CurrentState = newState;
            }

            if (this.StateListener != null)
            {
                this.StateListener.onChange(this.Thread, this.CurrentState, oldState);
            }

            return true;
        }
    }
}