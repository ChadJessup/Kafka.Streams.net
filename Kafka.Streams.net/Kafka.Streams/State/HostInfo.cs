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
namespace Kafka.Streams.State;

using Kafka.Common.serialization.Serializer;
using Kafka.Streams.KafkaStreams;
using Kafka.Streams.Processor.IStreamPartitioner;
using Kafka.Streams.Processor.Internals.StreamsPartitionAssignor;

/**
 * Represents a user defined endpoint in a {@link org.apache.kafka.streams.KafkaStreams} application.
 * Instances of this can be obtained by calling one of:
 *  {@link KafkaStreams#allMetadata()}
 *  {@link KafkaStreams#allMetadataForStore(string)}
 *  {@link KafkaStreams#metadataForKey(string, object, StreamPartitioner)}
 *  {@link KafkaStreams#metadataForKey(string, object, Serializer)}
 *
 *  The HostInfo is constructed during Partition Assignment
 *  see {@link StreamsPartitionAssignor}
 *  It is extracted from the config {@link org.apache.kafka.streams.StreamsConfig#APPLICATION_SERVER_CONFIG}
 *
 *  If developers wish to expose an endpoint in their KafkaStreams applications they should provide the above
 *  config.
 */
public class HostInfo
{
    private string host;
    private int port;

    public HostInfo(string host,
                    int port)
{
        this.host = host;
        this.port = port;
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

        HostInfo hostInfo = (HostInfo) o;
        return port == hostInfo.port && host.Equals(hostInfo.host);
    }

    public override int GetHashCode()
{
        int result = host.GetHashCode();
        result = 31 * result + port;
        return result;
    }

    public string host()
{
        return host;
    }

    public int port()
{
        return port;
    }

    public override string ToString()
{
        return "HostInfo{" +
                "host=\'" + host + '\'' +
                ", port=" + port +
                '}';
    }
}
