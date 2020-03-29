//using Kafka.Common;
//using System;
//using System.Collections.Generic;

//namespace Kafka.Streams.KStream.Internals
//{

//    public class WindowedStreamPartitionerTest
//    {

//        private string topicName = "topic";

//        private IntegerSerializer intSerializer = Serdes.Int();
//        private StringSerializer stringSerializer = Serdes.String();

//        private List<PartitionInfo> infos = Array.AsReadOnly(
//                new PartitionInfo(topicName, 0, Node.noNode(), new Node[0], new Node[0]),
//                new PartitionInfo(topicName, 1, Node.noNode(), new Node[0], new Node[0]),
//                new PartitionInfo(topicName, 2, Node.noNode(), new Node[0], new Node[0]),
//                new PartitionInfo(topicName, 3, Node.noNode(), new Node[0], new Node[0]),
//                new PartitionInfo(topicName, 4, Node.noNode(), new Node[0], new Node[0]),
//                new PartitionInfo(topicName, 5, Node.noNode(), new Node[0], new Node[0])
//        );

//        private Cluster cluster = new Cluster("cluster", Collections.singletonList(Node.noNode()), infos,
//                Collections.< string > emptySet(), Collections.< string > emptySet());

//        [Fact]
//        public void testCopartitioning()
//        {
//            var rand = new Random();
//            var defaultPartitioner = new DefaultPartitioner();
//            WindowedSerializer<int> timeWindowedSerializer = new TimeWindowedSerializer<>(intSerializer);
//            WindowedStreamPartitioner<int, string> streamPartitioner = new WindowedStreamPartitioner<>(timeWindowedSerializer);

//            for (var k = 0; k < 10; k++)
//            {
//                int key = rand.nextInt();
//                byte[] keyBytes = intSerializer.serialize(topicName, key);

//                var value = key.ToString();
//                byte[] valueBytes = stringSerializer.serialize(topicName, value);

//                int expected = defaultPartitioner.partition("topic", key, keyBytes, value, valueBytes, cluster);

//                for (var w = 1; w < 10; w++)
//                {
//                    var window = new TimeWindow(10 * w, 20 * w);

//                    Windowed<int> windowedKey = new Windowed<>(key, window);
//                    int actual = streamPartitioner.partition(topicName, windowedKey, value, infos.Count);

//                    Assert.Equal(expected, actual);
//                }
//            }

//            defaultPartitioner.close();
//        }
//    }
//}