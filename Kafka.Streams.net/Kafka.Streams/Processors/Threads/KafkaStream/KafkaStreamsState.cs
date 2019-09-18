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
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams
{
    public class KafkaStreamsState : IStateMachine<KafkaStreamsStates>
    {
        private readonly ILogger<KafkaStreamsState> logger;
        private Dictionary<KafkaStreamsStates, StateTransition<KafkaStreamsStates>> validTransitions = new Dictionary<KafkaStreamsStates, StateTransition<KafkaStreamsStates>>();
        private readonly object stateLock = new object();

        public KafkaStreamsState(
            ILogger<KafkaStreamsState> logger)
        {
            this.logger = logger;
        }

        public KafkaStreamsStates CurrentState { get; private set; } = KafkaStreamsStates.UNKNOWN;
        public IStateListener StateListener { get; }
        public IThread<KafkaStreamsStates> Thread { get; }

        public bool isRunning()
        {
            return CurrentState.HasFlag(KafkaStreamsStates.RUNNING)
                || CurrentState.HasFlag(KafkaStreamsStates.REBALANCING);
        }

        public bool isValidTransition(KafkaStreamsStates newState)
            => this.validTransitions.ContainsKey(newState)
                ? this.validTransitions[newState].PossibleTransitions.Contains(newState)
                : false;

        public bool setState(KafkaStreamsStates newState)
        {
            KafkaStreamsStates oldState;

            lock (stateLock)
            {
                oldState = this.CurrentState;

                if (this.CurrentState == KafkaStreamsStates.PENDING_SHUTDOWN
                    && newState != KafkaStreamsStates.NOT_RUNNING)
                {
                    // when the state is already in PENDING_SHUTDOWN, all other transitions than NOT_RUNNING (due to thread dying) will be
                    // refused but we do not throw exception here, to allow appropriate error handling
                    return false;
                }
                else if (this.CurrentState == KafkaStreamsStates.NOT_RUNNING
                    && (newState == KafkaStreamsStates.PENDING_SHUTDOWN || newState == KafkaStreamsStates.NOT_RUNNING))
                {
                    // when the state is already in NOT_RUNNING, its transition to PENDING_SHUTDOWN or NOT_RUNNING (due to consecutive close calls)
                    // will be refused but we do not throw exception here, to allow idempotent close calls
                    return false;
                }
                else if (this.CurrentState == KafkaStreamsStates.REBALANCING
                    && newState == KafkaStreamsStates.REBALANCING)
                {
                    // when the state is already in REBALANCING, it should not transit to REBALANCING again
                    return false;
                }
                else if (this.CurrentState == KafkaStreamsStates.ERROR
                    && newState == KafkaStreamsStates.ERROR)
                {
                    // when the state is already in ERROR, it should not transit to ERROR again
                    return false;
                }
                else if (!this.isValidTransition(newState))
                {
                    //                    throw new IllegalStateException("Stream-client " + clientId + ": Unexpected state transition from " + oldState + " to " + newState);
                }
                else
                {
                    this.logger.LogInformation("State transition from {} to {}", oldState, newState);
                }

                this.CurrentState = newState;
                // stateLock.notifyAll();
            }

            // we need to call the user customized state listener outside the state lock to avoid potential deadlocks
            if (this.StateListener != null)
            {
                this.StateListener.onChange(this.Thread, newState, oldState);
            }

            return true;
        }

        public void setTransitions(IEnumerable<StateTransition<KafkaStreamsStates>> validTransitions)
        {
            this.validTransitions = validTransitions
                .ToDictionary(k => k.StartingState, v => v);
        }
    }
}
