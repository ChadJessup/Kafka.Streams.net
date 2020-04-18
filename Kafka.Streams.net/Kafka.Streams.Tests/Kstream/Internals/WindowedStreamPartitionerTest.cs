using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Temporary;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{

    public class WindowedStreamPartitionerTest
    {
        private string topicName = "topic";

        private ISerializer<int> intSerializer = Serdes.Int().Serializer;
        private ISerializer<string> stringSerializer = Serdes.String().Serializer;

        private List<PartitionInfo> infos = Array.AsReadOnly(
                new PartitionInfo(topicName, 0, Node.noNode(), Array.Empty<Node>(), Array.Empty<Node>()),
                new PartitionInfo(topicName, 1, Node.noNode(), Array.Empty<Node>(), Array.Empty<Node>()),
                new PartitionInfo(topicName, 2, Node.noNode(), Array.Empty<Node>(), Array.Empty<Node>()),
                new PartitionInfo(topicName, 3, Node.noNode(), Array.Empty<Node>(), Array.Empty<Node>()),
                new PartitionInfo(topicName, 4, Node.noNode(), Array.Empty<Node>(), Array.Empty<Node>()),
                new PartitionInfo(topicName, 5, Node.noNode(), Array.Empty<Node>(), Array.Empty<Node>())
        );

        private Cluster cluster = new Cluster("cluster", Collections.singletonList(Node.noNode()), infos,
                Collections.emptySet<string>(), Collections.emptySet<string>());

        [Fact]
        public void testCopartitioning()
        {
            var rand = new Random();
            var defaultPartitioner = new DefaultPartitioner();
            IWindowedSerializer<int> timeWindowedSerializer = new TimeWindowedSerializer<int>(intSerializer);
            WindowedStreamPartitioner<int, string> streamPartitioner = new WindowedStreamPartitioner<int, string>(timeWindowedSerializer);

            for (var k = 0; k < 10; k++)
            {
                int key = rand.Next();
                byte[] keyBytes = intSerializer.Serialize(topicName, key);

                var value = key.ToString();
                byte[] valueBytes = stringSerializer.Serialize(topicName, value);

                int expected = defaultPartitioner.Partition("topic", key, keyBytes, value, valueBytes, cluster);

                for (var w = 1; w < 10; w++)
                {
                    var window = new TimeWindow(10 * w, 20 * w);

                    IWindowed<int> windowedKey = new Windowed<string>(key, window);
                    int actual = streamPartitioner.Partition(topicName, windowedKey, value, infos.Count);

                    Assert.Equal(expected, actual);
                }
            }

            defaultPartitioner.Close();
        }
    }
}