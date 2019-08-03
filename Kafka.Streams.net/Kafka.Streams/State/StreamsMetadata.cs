/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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
package org.apache.kafka.streams.state;

using Kafka.Common.TopicPartition;
using Kafka.Common.annotation.InterfaceStability;
using Kafka.Streams.KafkaStreams;

import java.util.Collections;
import java.util.Set;

/**
 * Represents the state of an instance (process) in a {@link KafkaStreams} application.
 * It contains the user supplied {@link HostInfo} that can be used by developers to build
 * APIs and services to connect to other instances, the Set of state stores available on
 * the instance and the Set of {@link TopicPartition}s available on the instance.
 * NOTE: This is a point in time view. It may change when rebalances happen.
 */
@InterfaceStability.Evolving
public class StreamsMetadata
{
    /**
     * Sentinel to indicate that the StreamsMetadata is currently unavailable. This can occur during rebalance
     * operations.
     */
    public static StreamsMetadata NOT_AVAILABLE = new StreamsMetadata(new HostInfo("unavailable", -1),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet());

    private HostInfo hostInfo;
    private Set<string> stateStoreNames;
    private Set<TopicPartition> topicPartitions;

    public StreamsMetadata(HostInfo hostInfo,
                           Set<string> stateStoreNames,
                           Set<TopicPartition> topicPartitions)
{

        this.hostInfo = hostInfo;
        this.stateStoreNames = stateStoreNames;
        this.topicPartitions = topicPartitions;
    }

    public HostInfo hostInfo()
{
        return hostInfo;
    }

    public Set<string> stateStoreNames()
{
        return stateStoreNames;
    }

    public Set<TopicPartition> topicPartitions()
{
        return topicPartitions;
    }

    public string host()
{
        return hostInfo.host();
    }

    public int port()
{
        return hostInfo.port();
    }

    public override bool equals(object o)
{
        if (this == o)
{
            return true;
        }
        if (o == null || GetType() != o.GetType())
{
            return false;
        }
        StreamsMetadata that = (StreamsMetadata) o;
        if (!hostInfo.Equals(that.hostInfo))
{
            return false;
        }
        if (!stateStoreNames.Equals(that.stateStoreNames))
{
            return false;
        }
        return topicPartitions.Equals(that.topicPartitions);

    }

    public override int GetHashCode()()
{
        int result = hostInfo.GetHashCode()();
        result = 31 * result + stateStoreNames.GetHashCode()();
        result = 31 * result + topicPartitions.GetHashCode()();
        return result;
    }

    public override string toString()
{
        return "StreamsMetadata{" +
                "hostInfo=" + hostInfo +
                ", stateStoreNames=" + stateStoreNames +
                ", topicPartitions=" + topicPartitions +
                '}';
    }
}
