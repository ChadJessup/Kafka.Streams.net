using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    /**
     * WindowedChangelogTopicConfig captures the properties required for configuring
     * the windowed store changelog topics.
     */
    public class WindowedChangelogTopicConfig : InternalTopicConfig
    {
        private static readonly Dictionary<string, string?> WINDOWED_STORE_CHANGELOG_TOPIC_DEFAULT_OVERRIDES;
        private readonly Dictionary<string, string> tempTopicDefaultOverrides = new Dictionary<string, string>();

        //tempTopicDefaultOverrides.Add(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE);
        //WINDOWED_STORE_CHANGELOG_TOPIC_DEFAULT_OVERRIDES = Collections.unmodifiableMap(tempTopicDefaultOverrides);

        private readonly long? retentionMs;

        public WindowedChangelogTopicConfig(string name, Dictionary<string, string?> topicConfigs)
            : base(name, topicConfigs)
        {
            name = name ?? throw new ArgumentNullException(nameof(name));
            // Topic.validate(Name);

            this.Name = name;
            this.TopicConfigs = topicConfigs;
        }

        /**
         * Get the configured properties for this topic. If retentionMs is set then
         * we.Add.AdditionalRetentionMs to work out the desired retention when cleanup.policy=compact,delete
         *
         * @param.AdditionalRetentionMs - added to retention to allow for clock drift etc
         * @return Properties to be used when creating the topic
         */
        public override Dictionary<string, string?> GetProperties(Dictionary<string, string?> defaultProperties, long? additionalRetentionMs)
        {
            // internal topic config overridden rule: library overrides < global config overrides < per-topic config overrides
            var topicConfig = new Dictionary<string, string?>(WINDOWED_STORE_CHANGELOG_TOPIC_DEFAULT_OVERRIDES);

            //topicConfig.putAll(defaultProperties);
            //topicConfig.putAll(topicConfigs);

            if (this.retentionMs != null)
            {
                long? retentionValue;

                try
                {
                    retentionValue = this.retentionMs + additionalRetentionMs;
                }
                catch (ArithmeticException)
                {
                    retentionValue = long.MaxValue;
                }

                //topicConfig.Add(TopicConfig.RETENTION_MS_CONFIG, retentionValue.ToString());
            }

            return topicConfig;
        }

        public void SetRetentionMs(long retentionMs)
        {
            //if (!topicConfigs.ContainsKey(TopicConfig.RETENTION_MS_CONFIG))
            {
                //  this.retentionMs = retentionMs;
            }
        }

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }

            if (o == null || this.GetType() != o.GetType())
            {
                return false;
            }

            var that = (WindowedChangelogTopicConfig)o;

            return this.Name.Equals(that.Name) &&
                    this.TopicConfigs.Equals(that.TopicConfigs) &&
                    this.retentionMs.Equals(that.retentionMs);
        }


        public override int GetHashCode()
        {
            return (this.Name, this.TopicConfigs, this.retentionMs).GetHashCode();
        }

        public override string ToString()
        {
            return "WindowedChangelogTopicConfig(" +
                    "Name=" + this.Name +
                    ", topicConfigs=" + this.TopicConfigs +
                    ", retentionMs=" + this.retentionMs +
                    ")";
        }
    }
}
