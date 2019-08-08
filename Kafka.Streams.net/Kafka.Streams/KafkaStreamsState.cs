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

using System.Collections.Generic;

namespace Kafka.Streams
{
    // container states
    /**
     * Kafka Streams states are the possible state that a Kafka Streams instance can be in.
     * An instance must only be in one state at a time.
     * The expected state transition with the following defined states is:
     *
     * <pre>
     *                 +--------------+
     *         +&lt;----- | Created (0)  |
     *         |       +-----+--------+
     *         |             |
     *         |             v
     *         |       +----+--+------+
     *         |       | Re-          |
     *         +&lt;----- | Balancing (1)| --------&gt;+
     *         |       +-----+-+------+          |
     *         |             | ^                 |
     *         |             v |                 |
     *         |       +--------------+          v
     *         |       | Running (2)  | --------&gt;+
     *         |       +------+-------+          |
     *         |              |                  |
     *         |              v                  |
     *         |       +------+-------+     +----+-------+
     *         +-----&gt; | Pending      |&lt;--- | Error (5)  |
     *                 | Shutdown (3) |     +------------+
     *                 +------+-------+
     *                        |
     *                        v
     *                 +------+-------+
     *                 | Not          |
     *                 | Running (4)  |
     *                 +--------------+
     *
     *
     * </pre>
     * Note the following:
     * - RUNNING state will transit to REBALANCING if any of its threads is in PARTITION_REVOKED state
     * - REBALANCING state will transit to RUNNING if all of its threads are in RUNNING state
     * - Any state except NOT_RUNNING can go to PENDING_SHUTDOWN (whenever close is called)
     * - Of special importance: If the global stream thread dies, or all stream threads die (or both) then
     *   the instance will be in the ERROR state. The user will need to close it.
     */
    public enum KafkaStreamsStates
    {
        UNKNOWN = 0,
        CREATED, //(1, 3),
        REBALANCING, //(2, 3, 5),
        RUNNING, //(1, 3, 5),
        PENDING_SHUTDOWN, //(4),
        NOT_RUNNING,
        ERROR, //(3);
        DEAD,
    }

    public class KafkaStreamsState
    {
        private HashSet<KafkaStreamsStates> validTransitions = new HashSet<KafkaStreamsStates>();

        public KafkaStreamsState(params KafkaStreamsStates[] validTransitions)
        {
            this.validTransitions = new HashSet<KafkaStreamsStates>(validTransitions);
        }

        public KafkaStreamsStates CurrentState { get; set; } = KafkaStreamsStates.UNKNOWN;

        public bool isRunning()
        {
            return CurrentState.HasFlag(KafkaStreamsStates.RUNNING)
                || CurrentState.HasFlag(KafkaStreamsStates.REBALANCING);
        }

        public bool isValidTransition(KafkaStreamsStates newState)
        {
            return validTransitions.Contains(newState);
        }
    }
}
