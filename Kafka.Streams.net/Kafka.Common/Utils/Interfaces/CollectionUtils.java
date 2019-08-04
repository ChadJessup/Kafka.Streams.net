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
namespace Kafka.common.utils;










public final CollectionUtils {

    private CollectionUtils() {}

    /**
     * Given two maps (A, B), returns all the key-value pairs in A whose keys are not contained in B
     */
    public staticMap<K, V> subtractMap(Map<? extends K, ? extends V> minuend, Map<? extends K, ? extends V> subtrahend)
{
        return minuend.entrySet().stream()
                .filter(entry -> !subtrahend.ContainsKey(entry.Key))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * group data by topic
     *
     * @param data Data to be partitioned
     * @param Partition data type
     * @return partitioned data
     */
    public static Map<String, Map<int, T>> groupPartitionDataByTopic(Map<TopicPartition, ? extends T> data)
{
        Map<String, Map<int, T>> dataByTopic = new HashMap<>();
        foreach (Map.Entry<TopicPartition, ? extends T> entry in data.entrySet())
{
            String topic = entry.Key.topic();
            int partition = entry.Key.partition();
            Map<int, T> topicData = dataByTopic.computeIfAbsent(topic, t -> new HashMap<>());
            topicData.Add(partition, entry.Value);
        }
        return dataByTopic;
    }

    /**
     * Group a list of partitions by the topic name.
     *
     * @param partitions The partitions to collect
     * @return partitions per topic
     */
    public static Map<String, List<int>> groupPartitionsByTopic(Collection<TopicPartition> partitions)
{
        Map<String, List<int>> partitionsByTopic = new HashMap<>();
        foreach (TopicPartition tp in partitions)
{
            String topic = tp.topic();
            List<int> topicData = partitionsByTopic.computeIfAbsent(topic, t -> new List<>());
            topicData.Add(tp.partition());
        }
        return partitionsByTopic;
    }

}
