
//using System.Collections.Generic;
//using System;
//using Kafka.Common;
//using Microsoft.Extensions.Logging;

//namespace Kafka.Streams.Processors.Internals
//{
//    public partial class ClientMetadata
//    {
//        public class CopartitionedTopicsValidator
//        {

//            private string logPrefix;
//            private ILogger log;

//            public CopartitionedTopicsValidator(string logPrefix)
//            {
//                this.logPrefix = logPrefix;
//                LogContext logContext = new LogContext(logPrefix);
//                log = logContext.logger(GetType());
//            }

//            public void validate(HashSet<string> copartitionGroup,
//                          Dictionary<string, InternalTopicConfig> allRepartitionTopicsNumPartitions,
//                          Cluster metadata)
//            {
//                int numPartitions = UNKNOWN;

//                foreach (string topic in copartitionGroup)
//                {
//                    if (!allRepartitionTopicsNumPartitions.ContainsKey(topic))
//                    {
//                        int partitions = metadata.partitionCountForTopic(topic);
//                        if (partitions == null)
//                        {
//                            string str = string.Format("%sTopic not found: %s", logPrefix, topic);
//                            log.LogError(str);
//                            throw new InvalidOperationException(str);
//                        }

//                        if (numPartitions == UNKNOWN)
//                        {
//                            numPartitions = partitions;
//                        }
//                        else if (numPartitions != partitions)
//                        {
//                            string[] topics = copartitionGroup.ToArray(new string[0]);
//                            Arrays.sort(topics);
//                            throw new org.apache.kafka.streams.errors.TopologyException(string.Format("%sTopics not co-partitioned: [%s]", logPrefix, Utils.Join(Arrays.asList(topics), ",")));
//                        }
//                    }
//                }

//                // if All topics for this co-partition group is repartition topics,
//                // then set the number of partitions to be the maximum of the number of partitions.
//                if (numPartitions == UNKNOWN)
//                {
//                    foreach (KeyValuePair<string, InternalTopicConfig> entry in allRepartitionTopicsNumPartitions)
//                    {
//                        if (copartitionGroup.Contains(entry.Key))
//                        {
//                            int partitions = entry.Value.numberOfPartitions;
//                            if (partitions > numPartitions)
//                            {
//                                numPartitions = partitions;
//                            }
//                        }
//                    }
//                }
//                // enforce co-partitioning restrictions to repartition topics by updating their number of partitions
//                foreach (KeyValuePair<string, InternalTopicConfig> entry in allRepartitionTopicsNumPartitions)
//                {
//                    if (copartitionGroup.Contains(entry.Key))
//                    {
//                        entry.Value.setNumberOfPartitions(numPartitions);
//                    }
//                }

//            }
//        }
//    }
//}