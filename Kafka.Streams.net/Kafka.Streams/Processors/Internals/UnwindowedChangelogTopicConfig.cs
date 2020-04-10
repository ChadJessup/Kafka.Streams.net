
//        public Dictionary<string, string> getProperties(Dictionary<string, string> defaultProperties, long additionalRetentionMs)
//        {
//            // internal topic config overridden rule: library overrides < global config overrides < per-topic config overrides
//            Dictionary<string, string> topicConfig = new Dictionary<string, string>(UNWINDOWED_STORE_CHANGELOG_TOPIC_DEFAULT_OVERRIDES);

//            topicConfig.putAll(defaultProperties);
//            topicConfig.putAll(topicConfigs);

//            return topicConfig;
//        }


//        public override bool Equals(object o)
//        {
//            if (this == o)
//            {
//                return true;
//            }
//            if (o == null || GetType() != o.GetType())
//            {
//                return false;
//            }
//            UnwindowedChangelogTopicConfig that = (UnwindowedChangelogTopicConfig)o;
//            return Name.Equals(that.Name) &&
//                   topicConfigs.Equals(that.topicConfigs);
//        }


//        public int GetHashCode()
//        {
//            return Objects.hash(Name, topicConfigs);
//        }


//        public string ToString()
//        {
//            return "UnwindowedChangelogTopicConfig(" +
//                    "Name=" + Name +
//                    ", topicConfigs=" + topicConfigs +
//                    ")";
//        }
//    }
//}
