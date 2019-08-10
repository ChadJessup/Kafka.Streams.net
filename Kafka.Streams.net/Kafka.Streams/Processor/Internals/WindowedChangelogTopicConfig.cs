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

namespace Kafka.Streams.IProcessor.Internals
{
    /**
     * WindowedChangelogTopicConfig captures the properties required for configuring
     * the windowed store changelog topics.
     */
    public class WindowedChangelogTopicConfig : InternalTopicConfig
    {

        private static Dictionary<string, string> WINDOWED_STORE_CHANGELOG_TOPIC_DEFAULT_OVERRIDES;
        Dictionary<string, string> tempTopicDefaultOverrides = new Dictionary<>();
        //tempTopicDefaultOverrides.Add(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE);
        //WINDOWED_STORE_CHANGELOG_TOPIC_DEFAULT_OVERRIDES = Collections.unmodifiableMap(tempTopicDefaultOverrides);

        private long retentionMs;

        WindowedChangelogTopicConfig(string name, Dictionary<string, string> topicConfigs)
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
        public Dictionary<string, string> getProperties(Dictionary<string, string> defaultProperties, long.AdditionalRetentionMs)
        {
            // internal topic config overridden rule: library overrides < global config overrides < per-topic config overrides
            Dictionary<string, string> topicConfig = new Dictionary<>(WINDOWED_STORE_CHANGELOG_TOPIC_DEFAULT_OVERRIDES);

            topicConfig.putAll(defaultProperties);

            topicConfig.putAll(topicConfigs);

            if (retentionMs != null)
            {
                long retentionValue;
                try
                {

                    retentionValue = Math.AddExact(retentionMs,.AdditionalRetentionMs);
                }
                catch (ArithmeticException swallow)
                {
                    retentionValue = long.MaxValue;
                }
                topicConfig.Add(TopicConfig.RETENTION_MS_CONFIG, string.valueOf(retentionValue));
            }

            return topicConfig;
        }

        void setRetentionMs(long retentionMs)
        {
            if (!topicConfigs.ContainsKey(TopicConfig.RETENTION_MS_CONFIG))
            {
                this.retentionMs = retentionMs;
            }
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
            WindowedChangelogTopicConfig that = (WindowedChangelogTopicConfig)o;
            return Objects.Equals(name, that.name) &&
                    Objects.Equals(topicConfigs, that.topicConfigs) &&
                    Objects.Equals(retentionMs, that.retentionMs);
        }


        public int GetHashCode()
        {
            return Objects.hash(name, topicConfigs, retentionMs);
        }


        public string ToString()
        {
            return "WindowedChangelogTopicConfig(" +
                    "name=" + name +
                    ", topicConfigs=" + topicConfigs +
                    ", retentionMs=" + retentionMs +
                    ")";
        }
    }
}