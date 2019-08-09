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
namespace Kafka.Streams.Processor
{


using Kafka.Common.TopicPartition;






/**
 * Represents the state of a single task running within a {@link KafkaStreams} application.
 */
public class TaskMetadata
{


    private string taskId;

    private HashSet<TopicPartition> topicPartitions;

    public TaskMetadata(string taskId,
                        HashSet<TopicPartition> topicPartitions)
    {
        this.taskId = taskId;
        this.topicPartitions = Collections.unmodifiableSet(topicPartitions);
    }

    public string taskId()
    {
        return taskId;
    }

    public HashSet<TopicPartition> topicPartitions()
    {
        return topicPartitions;
    }


    public bool Equals(object o)
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
        return Objects.Equals(taskId, that.taskId) &&
               Objects.Equals(topicPartitions, that.topicPartitions);
    }


    public int GetHashCode()
    {
        return Objects.hash(taskId, topicPartitions);
    }


    public string ToString()
    {
        return "TaskMetadata{" +
                "taskId=" + taskId +
                ", topicPartitions=" + topicPartitions +
                '}';
    }
}
