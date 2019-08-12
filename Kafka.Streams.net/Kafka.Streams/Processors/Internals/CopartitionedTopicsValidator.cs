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
using System;
using Kafka.Common;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.Processor.Internals
{
    public partial class ClientMetadata
    {
        public class CopartitionedTopicsValidator
        {

            private string logPrefix;
            private ILogger log;

            public CopartitionedTopicsValidator(string logPrefix)
            {
                this.logPrefix = logPrefix;
                LogContext logContext = new LogContext(logPrefix);
                log = logContext.logger(GetType());
            }

            public void validate(HashSet<string> copartitionGroup,
                          Dictionary<string, InternalTopicConfig> allRepartitionTopicsNumPartitions,
                          Cluster metadata)
            {
                int numPartitions = UNKNOWN;

                foreach (string topic in copartitionGroup)
                {
                    if (!allRepartitionTopicsNumPartitions.ContainsKey(topic))
                    {
                        int partitions = metadata.partitionCountForTopic(topic);
                        if (partitions == null)
                        {
                            string str = string.Format("%sTopic not found: %s", logPrefix, topic);
                            log.LogError(str);
                            throw new InvalidOperationException(str);
                        }

                        if (numPartitions == UNKNOWN)
                        {
                            numPartitions = partitions;
                        }
                        else if (numPartitions != partitions)
                        {
                            string[] topics = copartitionGroup.ToArray(new string[0]);
                            Arrays.sort(topics);
                            throw new org.apache.kafka.streams.errors.TopologyException(string.Format("%sTopics not co-partitioned: [%s]", logPrefix, Utils.join(Arrays.asList(topics), ",")));
                        }
                    }
                }

                // if all topics for this co-partition group is repartition topics,
                // then set the number of partitions to be the maximum of the number of partitions.
                if (numPartitions == UNKNOWN)
                {
                    foreach (KeyValuePair<string, InternalTopicConfig> entry in allRepartitionTopicsNumPartitions)
                    {
                        if (copartitionGroup.Contains(entry.Key))
                        {
                            int partitions = entry.Value.numberOfPartitions;
                            if (partitions > numPartitions)
                            {
                                numPartitions = partitions;
                            }
                        }
                    }
                }
                // enforce co-partitioning restrictions to repartition topics by updating their number of partitions
                foreach (KeyValuePair<string, InternalTopicConfig> entry in allRepartitionTopicsNumPartitions)
                {
                    if (copartitionGroup.Contains(entry.Key))
                    {
                        entry.Value.setNumberOfPartitions(numPartitions);
                    }
                }

            }
        }
    }
}