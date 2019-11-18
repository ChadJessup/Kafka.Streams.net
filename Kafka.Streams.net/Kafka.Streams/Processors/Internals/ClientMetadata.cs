
//        private void prepareTopic(Dictionary<string, InternalTopicConfig> topicPartitions)
//        {
//            log.LogDebug("Starting to validate internal topics {} in partition assignor.", topicPartitions);

//            // first construct the topics to make ready
//            Dictionary<string, InternalTopicConfig> topicsToMakeReady = new Dictionary<string, InternalTopicConfig>();

//            foreach (InternalTopicConfig topic in topicPartitions.Values)
//            {
//                int numPartitions = topic.numberOfPartitions;
//                if (numPartitions == UNKNOWN)
//                {
//                    throw new StreamsException(string.Format("%sTopic [%s] number of partitions not defined", logPrefix, topic.name));
//                }

//                topic.setNumberOfPartitions(numPartitions);
//                topicsToMakeReady.Add(topic.name, topic);
//            }

//            if (!topicsToMakeReady.isEmpty())
//            {
//                internalTopicManager.makeReady(topicsToMakeReady);
//            }

//            log.LogDebug("Completed validating internal topics {} in partition assignor.", topicPartitions);
//        }

//        private void ensureCopartitioning(List<HashSet<string>> copartitionGroups,
//                                          Dictionary<string, InternalTopicConfig> allRepartitionTopicsNumPartitions,
//                                          Cluster metadata)
//        {
//            foreach (HashSet<string> copartitionGroup in copartitionGroups)
//            {
//                copartitionedTopicsValidator.validate(copartitionGroup, allRepartitionTopicsNumPartitions, metadata);
//            }
//        }

//        // following functions are for test only
//        void setInternalTopicManager(InternalTopicManager internalTopicManager)
//        {
//            this.internalTopicManager = internalTopicManager;
//        }
//    }
//}