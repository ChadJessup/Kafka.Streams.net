
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    /**
     * InternalTopicConfig captures the properties required for configuring
     * the internal topics we create for change-logs and repartitioning etc.
     */
    public class InternalTopicConfig
    {
        string name;
        bool enforceNumberOfPartitions;

        public InternalTopicConfig(string name, Dictionary<string, string?> topicConfigs)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentException("message", nameof(name));
            }

            this.name = name;
            // Topic.validate(name);
            this.TopicConfigs = topicConfigs ?? throw new ArgumentNullException(nameof(topicConfigs));
            this.enforceNumberOfPartitions = false;
        }

        public string Name { get; protected set; }
        public Dictionary<string, string?> TopicConfigs { get; protected set; } = new Dictionary<string, string?>();

        public int numberOfPartitions { get; private set; } = StreamsPartitionAssignor.UNKNOWN;

        /**
         * Get the configured properties for this topic. If retentionMs is set then
         * we.Add.AdditionalRetentionMs to work out the desired retention when cleanup.policy=compact,delete
         *
         * @param.AdditionalRetentionMs -.Added to retention to allow for clock drift etc
         * @return Properties to be used when creating the topic
         */
        public virtual Dictionary<string, string?> GetProperties(Dictionary<string, string?> defaultProperties, long? additionalRetentionMs)
        {
            return defaultProperties;
        }

        public void SetNumberOfPartitions(int numberOfPartitions)
        {
            if (numberOfPartitions < 1)
            {
                throw new ArgumentException("Number of partitions must be at least 1.");
            }

            this.numberOfPartitions = numberOfPartitions;
        }

        public override string ToString()
        {
            return "InternalTopicConfig(" +
                    "Name=" + this.Name +
                    ", topicConfigs=" + this.TopicConfigs +
                    ")";
        }
    }
}
