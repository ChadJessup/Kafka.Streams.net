using Kafka.Common;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{

    public class WindowedStreamPartitionerTest
    {

        private string topicName = "topic";

        private IntegerSerializer intSerializer = Serdes.Int();
        private Serdes.String().Serializer stringSerializer = Serdes.String();

        private List<PartitionInfo> infos = Array.AsReadOnly(
                new PartitionInfo(topicName, 0, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
                new PartitionInfo(topicName, 1, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
                new PartitionInfo(topicName, 2, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
                new PartitionInfo(topicName, 3, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
                new PartitionInfo(topicName, 4, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
                new PartitionInfo(topicName, 5, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>())
        );

        private Cluster cluster = new Cluster("cluster", Collections.singletonList(Node.noNode()), infos,
                Collections.< string > emptySet(), Collections.< string > emptySet());

        [Fact]
        public void testCopartitioning()
        {
            var rand = new Random();
            var defaultPartitioner = new DefaultPartitioner();
            IWindowedSerializer<int> timeWindowedSerializer = new TimeWindowedSerializer<>(intSerializer);
            WindowedStreamPartitioner<int, string> streamPartitioner = new WindowedStreamPartitioner<>(timeWindowedSerializer);

            for (var k = 0; k < 10; k++)
            {
                int key = rand.nextInt();
                byte[] keyBytes = intSerializer.Serialize(topicName, key);

                var value = key.ToString();
                byte[] valueBytes = stringSerializer.Serialize(topicName, value);

                int expected = defaultPartitioner.partition("topic", key, keyBytes, value, valueBytes, cluster);

                for (var w = 1; w < 10; w++)
                {
                    var window = new TimeWindow(10 * w, 20 * w);

                    IWindowed<int> windowedKey = new Windowed<string>(key, window);
                    int actual = streamPartitioner.partition(topicName, windowedKey, value, infos.Count);

                    Assert.Equal(expected, actual);
                }
            }

            defaultPartitioner.Close();
        }
    }
}