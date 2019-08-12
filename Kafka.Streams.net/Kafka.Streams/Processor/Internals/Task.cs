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
using Confluent.Kafka;
using Kafka.Streams.Processor.Interfaces;
using System.Collections.Generic;

namespace Kafka.Streams.Processor.Internals
{
    public interface ITask
    {
        /**
         * Initialize the task and return {@code true} if the task is ready to run, i.e, it has not state stores
         * @return true if this task has no state stores that may need restoring.
         * @throws InvalidOperationException If store gets registered after initialized is already finished
         * @throws StreamsException if the store's change log does not contain the partition
         */
        bool initializeStateStores();

        bool commitNeeded();

        void initializeTopology();

        void commit();

        void suspend();

        void resume();

        void closeSuspended(bool clean,
                            bool isZombie,
                            RuntimeException e);

        void close(bool clean,
                   bool isZombie);

        IStateStore getStore(string name);

        string applicationId { get; }

        ProcessorTopology topology { get; }

        IProcessorContext<K, V> context;

        TaskId id();

        HashSet<TopicPartition> partitions();

        /**
         * @return any changelog partitions associated with this task
         */
        List<TopicPartition> changelogPartitions();

        bool hasStateStores();

        string ToString(string indent);
    }
}