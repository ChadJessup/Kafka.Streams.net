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

namespace Kafka.Streams.Processor.Internals
{
    /**
     * UnwindowedChangelogTopicConfig captures the properties required for configuring
     * the un-windowed store changelog topics.
     */
    public class UnwindowedChangelogTopicConfig : InternalTopicConfig
    {
        private static Dictionary<string, string> UNWINDOWED_STORE_CHANGELOG_TOPIC_DEFAULT_OVERRIDES;

        //Dictionary<string, string> tempTopicDefaultOverrides = new Dictionary<>();
        //tempTopicDefaultOverrides.Add(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        //UNWINDOWED_STORE_CHANGELOG_TOPIC_DEFAULT_OVERRIDES = Collections.unmodifiableMap(tempTopicDefaultOverrides);

        UnwindowedChangelogTopicConfig(string name, Dictionary<string, string> topicConfigs)
            : base(name, topicConfigs)
        {
        }

        /**
         * Get the configured properties for this topic. If retentionMs is set then
         * we.Add.AdditionalRetentionMs to work out the desired retention when cleanup.policy=compact,delete
         *
         * @param.AdditionalRetentionMs -.Added to retention to allow for clock drift etc
         * @return Properties to be used when creating the topic
         */
        public Dictionary<string, string> getProperties(Dictionary<string, string> defaultProperties, long additionalRetentionMs)
        {
            // internal topic config overridden rule: library overrides < global config overrides < per-topic config overrides
            Dictionary<string, string> topicConfig = new Dictionary<string, string>(UNWINDOWED_STORE_CHANGELOG_TOPIC_DEFAULT_OVERRIDES);

            topicConfig.putAll(defaultProperties);
            topicConfig.putAll(topicConfigs);

            return topicConfig;
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
            UnwindowedChangelogTopicConfig that = (UnwindowedChangelogTopicConfig)o;
            return name.Equals(that.name) &&
                   topicConfigs.Equals(that.topicConfigs);
        }


        public int GetHashCode()
        {
            return Objects.hash(name, topicConfigs);
        }


        public string ToString()
        {
            return "UnwindowedChangelogTopicConfig(" +
                    "name=" + name +
                    ", topicConfigs=" + topicConfigs +
                    ")";
        }
    }
}
