
//        public Dictionary<string, string> getProperties(Dictionary<string, string> defaultProperties, long additionalRetentionMs)
//        {
//            // internal topic config overridden rule: library overrides < global config overrides < per-topic config overrides
//            Dictionary<string, string> topicConfig = new Dictionary<>(REPARTITION_TOPIC_DEFAULT_OVERRIDES);

//            topicConfig.putAll(defaultProperties);

//            topicConfig.putAll(topicConfigs);

//            return topicConfig;
//        }


//        public bool Equals(object o)
//        {
//            if (this == o)
//            {
//                return true;
//            }
//            if (o == null || GetType() != o.GetType())
//            {
//                return false;
//            }
//            RepartitionTopicConfig that = (RepartitionTopicConfig)o;
//            return Objects.Equals(name, that.name) &&
//                   Objects.Equals(topicConfigs, that.topicConfigs);
//        }


//        public int GetHashCode()
//        {
//            return Objects.hash(name, topicConfigs);
//        }


//        public string ToString()
//        {
//            return "RepartitionTopicConfig(" +
//                    "name=" + name +
//                    ", topicConfigs=" + topicConfigs +
//                    ")";
//        }
//    }
//}