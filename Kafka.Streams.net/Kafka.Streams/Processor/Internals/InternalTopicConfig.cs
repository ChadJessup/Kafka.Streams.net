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
namespace Kafka.Streams.Processor.Internals;

using Kafka.Common.internals.Topic;




/**
 * InternalTopicConfig captures the properties required for configuring
 * the internal topics we create for change-logs and repartitioning etc.
 */
public abstract InternalTopicConfig
{


    string name;
    Dictionary<string, string> topicConfigs;

    private int numberOfPartitions = StreamsPartitionAssignor.UNKNOWN;

    InternalTopicConfig(string name, Dictionary<string, string> topicConfigs)
{
        name = name ?? throw new System.ArgumentNullException("name can't be null", nameof(name));
        Topic.validate(name);

        this.name = name;
        this.topicConfigs = topicConfigs;
    }

    /**
     * Get the configured properties for this topic. If retentionMs is set then
     * we.Add.AdditionalRetentionMs to work out the desired retention when cleanup.policy=compact,delete
     *
     * @param.AdditionalRetentionMs -.Added to retention to allow for clock drift etc
     * @return Properties to be used when creating the topic
     */
    abstract public Dictionary<string, string> getProperties(Dictionary<string, string> defaultProperties, long.AdditionalRetentionMs);

    public string name()
{
        return name;
    }

    public int numberOfPartitions()
{
        return numberOfPartitions;
    }

    void setNumberOfPartitions(int numberOfPartitions)
{
        if (numberOfPartitions < 1)
{
            throw new System.ArgumentException("Number of partitions must be at least 1.");
        }
        this.numberOfPartitions = numberOfPartitions;
    }

    
    public string ToString()
{
        return "InternalTopicConfig(" +
                "name=" + name +
                ", topicConfigs=" + topicConfigs +
                ")";
    }
}
