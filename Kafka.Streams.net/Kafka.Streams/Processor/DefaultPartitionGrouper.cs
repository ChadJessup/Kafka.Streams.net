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
namespace Kafka.Streams.Processor;

using Kafka.Common.Cluster;
using Kafka.Common.PartitionInfo;
using Kafka.Common.TopicPartition;
using Microsoft.Extensions.Logging;











/**
 * Default implementation of the {@link PartitionGrouper} interface that groups partitions by the partition id.
 *
 * Join operations requires that topics of the joining entities are copartitoned, i.e., being partitioned by the same key and having the same
 * number of partitions. Copartitioning is ensured by having the same number of partitions on
 * joined topics, and by using the serialization and Producer's default partitioner.
 */
public class DefaultPartitionGrouper : IPartitionGrouper
{


    private static ILogger log = new LoggerFactory().CreateLogger<DefaultPartitionGrouper>();
    /**
     * Generate tasks with the assigned topic partitions.
     *
     * @param topicGroups   group of topics that need to be joined together
     * @param metadata      metadata of the consuming cluster
     * @return The map from generated task ids to the assigned partitions
     */
    public Dictionary<TaskId, HashSet<TopicPartition>> partitionGroups(Dictionary<int, HashSet<string>> topicGroups, Cluster metadata)
{
        Dictionary<TaskId, HashSet<TopicPartition>> groups = new HashMap<>();

        foreach (KeyValuePair<int, HashSet<string>> entry in topicGroups.entrySet())
{
            int topicGroupId = entry.Key;
            HashSet<string> topicGroup = entry.Value;

            int maxNumPartitions = maxNumPartitions(metadata, topicGroup);

            for (int partitionId = 0; partitionId < maxNumPartitions; partitionId++)
{
                HashSet<TopicPartition> group = new HashSet<>(topicGroup.size());

                foreach (string topic in topicGroup)
{
                    List<PartitionInfo> partitions = metadata.partitionsForTopic(topic);
                    if (partitionId < partitions.size())
{
                        group.Add(new TopicPartition(topic, partitionId));
                    }
                }
                groups.Add(new TaskId(topicGroupId, partitionId), Collections.unmodifiableSet(group));
            }
        }

        return Collections.unmodifiableMap(groups);
    }

    /**
     * @throws StreamsException if no metadata can be received for a topic
     */
    protected int maxNumPartitions(Cluster metadata, HashSet<string> topics)
{
        int maxNumPartitions = 0;
        foreach (string topic in topics)
{
            List<PartitionInfo> partitions = metadata.partitionsForTopic(topic);
            if (partitions.isEmpty())
{
                log.LogError("Empty partitions for topic {}", topic);
                throw new RuntimeException("Empty partitions for topic " + topic);
            }

            int numPartitions = partitions.size();
            if (numPartitions > maxNumPartitions)
{
                maxNumPartitions = numPartitions;
            }
        }
        return maxNumPartitions;
    }

}



