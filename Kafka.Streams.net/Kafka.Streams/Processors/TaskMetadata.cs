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
using System.Collections.Generic;

namespace Kafka.Streams.Processor
{
    /**
     * Represents the state of a single task running within a {@link KafkaStreams} application.
     */
    public class TaskMetadata
    {
        private readonly string taskId;

        private readonly HashSet<TopicPartition> topicPartitions;

        public TaskMetadata(
            string taskId,
            HashSet<TopicPartition> topicPartitions)
        {
            this.taskId = taskId;
            this.topicPartitions = topicPartitions;
        }

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || GetType() != o.GetType())
            {
                return false;
            }

            TaskMetadata that = (TaskMetadata)o;
            return taskId.Equals(that.taskId)
                && topicPartitions.Equals(that.topicPartitions);
        }

        public override int GetHashCode()
        {
            return (taskId, topicPartitions).GetHashCode();
        }

        public override string ToString()
        {
            return "TaskMetadata{" +
                    "taskId=" + taskId +
                    ", topicPartitions=" + topicPartitions +
                    '}';
        }
    }
}