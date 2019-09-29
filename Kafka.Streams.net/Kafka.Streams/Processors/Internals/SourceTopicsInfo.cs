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
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public class SourceTopicsInfo
    {
        public List<string> sourceTopics { get; }
        public int maxPartitions { get; }
        public string topicWithMostPartitions { get; }

        public SourceTopicsInfo(List<string> sourceTopics)
        {
            this.sourceTopics = sourceTopics;
            foreach (string topic in sourceTopics)
            {
                //List<PartitionInfo> partitions = clusterMetadata.partitionsForTopic(topic);
                //if (partitions.Count > maxPartitions)
                //{
                //    maxPartitions = partitions.Count;
                //    topicWithMostPartitions = partitions[0].Topic;
                //}
            }
        }
    }
}
