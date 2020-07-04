//using Confluent.Kafka;
//using Kafka.Common;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.Processors.Internals;
//using Kafka.Streams.State;
//using Kafka.Streams.State.KeyValues;
//using Kafka.Streams.Temporary;
//using Kafka.Streams.Tests.Integration;
//using System.Collections.Generic;
//using System.Collections.ObjectModel;
//using Xunit;

//namespace Kafka.Streams.Tests.Processor.Internals
//{
//    public class StreamsMetadataStateTest
//    {
//        private StreamsMetadataState metadataState;
//        private HostInfo hostOne;
//        private HostInfo hostTwo;
//        private HostInfo hostThree;
//        private TopicPartition topic1P0;
//        private TopicPartition topic2P0;
//        private TopicPartition topic3P0;
//        private Dictionary<HostInfo, HashSet<TopicPartition>> hostToPartitions;
//        private StreamsBuilder builder;
//        private TopicPartition topic1P1;
//        private TopicPartition topic2P1;
//        private TopicPartition topic4P0;
//        private Cluster cluster;
//        private readonly string globalTable = "global-table";
//        private IStreamPartitioner<string, object> partitioner;


//        public void Before()
//        {
//            builder = new StreamsBuilder();
//            IKStream<string, long> one = builder.Stream("topic-one");
//            one.GroupByKey().Count(Materialized.As<object, long, IKeyValueStore<Bytes, byte[]>>("table-one"));

//            IKStream<string, long> two = builder.Stream("topic-two");
//            two.GroupByKey().Count(Materialized.As<object, long, IKeyValueStore<Bytes, byte[]>>("table-two"));

//            builder.Stream("topic-three")
//                    .GroupByKey()
//                    .Count(Materialized.As<object, long, IKeyValueStore<Bytes, byte[]>>("table-three"));

//            one.merge(two).GroupByKey().Count(Materialized.As<object, long, IKeyValueStore<Bytes, byte[]>>("merged-table"));

//            builder.Stream("topic-four").MapValues(new ValueMapper<object, object>());
//            //    {
//            //
//            //
//            //    public object apply(object value)
//            //    {
//            //        return value;
//            //    }
//            //});
//            //
//            builder.globalTable("global-topic",
//                                Consumed.With(null, null),
//                                Materialized.As<object, object, IKeyValueStore<Bytes, byte[]>>(globalTable));

//            TopologyWrapper.getInternalTopologyBuilder(builder.Build()).SetApplicationId("appId");

//            topic1P0 = new TopicPartition("topic-one", 0);
//            topic1P1 = new TopicPartition("topic-one", 1);
//            topic2P0 = new TopicPartition("topic-two", 0);
//            topic2P1 = new TopicPartition("topic-two", 1);
//            topic3P0 = new TopicPartition("topic-three", 0);
//            topic4P0 = new TopicPartition("topic-four", 0);

//            hostOne = new HostInfo("host-one", 8080);
//            hostTwo = new HostInfo("host-two", 9090);
//            hostThree = new HostInfo("host-three", 7070);
//            hostToPartitions = new HashMap<>();
//            hostToPartitions.Put(hostOne, Utils.mkSet(topic1P0, topic2P1, topic4P0));
//            hostToPartitions.Put(hostTwo, Utils.mkSet(topic2P0, topic1P1));
//            hostToPartitions.Put(hostThree, Collections.singleton(topic3P0));

//            List<PartitionInfo> partitionInfos = Arrays.asList(
//                    new PartitionInfo("topic-one", 0, null, null, null),
//                    new PartitionInfo("topic-one", 1, null, null, null),
//                    new PartitionInfo("topic-two", 0, null, null, null),
//                    new PartitionInfo("topic-two", 1, null, null, null),
//                    new PartitionInfo("topic-three", 0, null, null, null),
//                    new PartitionInfo("topic-four", 0, null, null, null));

//            cluster = new Cluster(null, new List<Node>(), partitionInfos, Collections.emptySet<string>(), Collections.emptySet<string>());
//            metadataState = new StreamsMetadataState(TopologyWrapper.getInternalTopologyBuilder(builder.Build()), hostOne);
//            metadataState.OnChange(hostToPartitions, cluster);
//            //        partitioner = new StreamPartitioner<string, object>() {
//            //            
//            //            public int Partition(string topic, string key, object value, int numPartitions)
//            //        {
//            //            return 1;
//            //        }
//            //    };
//        }

//        [Fact]
//        public void ShouldNotThrowNPEWhenOnChangeNotCalled()
//        {
//            new StreamsMetadataState(TopologyWrapper.getInternalTopologyBuilder(builder.Build()), hostOne).GetAllMetadataForStore("store");
//        }

//        [Fact]
//        public void ShouldGetAllStreamInstances()
//        {
//            StreamsMetadata one = new StreamsMetadata(hostOne, Utils.mkSet(globalTable, "table-one", "table-two", "merged-table"),
//                    Utils.mkSet(topic1P0, topic2P1, topic4P0));
//            StreamsMetadata two = new StreamsMetadata(hostTwo, Utils.mkSet(globalTable, "table-two", "table-one", "merged-table"),
//                    Utils.mkSet(topic2P0, topic1P1));
//            StreamsMetadata three = new StreamsMetadata(hostThree, Utils.mkSet(globalTable, "table-three"),
//                    Collections.singleton(topic3P0));

//            Collection<StreamsMetadata> actual = metadataState.getAllMetadata();
//            Assert.Equal(3, actual.Count);
//            Assert.True("expected " + actual + " to contain " + one, actual.Contains(one));
//            Assert.True("expected " + actual + " to contain " + two, actual.Contains(two));
//            Assert.True("expected " + actual + " to contain " + three, actual.Contains(three));
//        }

//        [Fact]
//        public void ShouldGetAllStreamsInstancesWithNoStores()
//        {
//            builder.Stream<object, object>("topic-five")
//                .Filter((k, v) => true)
//                .To("some-other-topic");

//            TopicPartition tp5 = new TopicPartition("topic-five", 1);
//            HostInfo hostFour = new HostInfo("host-four", 8080);
//            hostToPartitions.Put(hostFour, Utils.mkSet(tp5));

//            metadataState.OnChange(hostToPartitions, cluster.WithPartitions(Collections.singletonMap(tp5, new PartitionInfo("topic-five", 1, null, null, null))));

//            StreamsMetadata expected = new StreamsMetadata(hostFour, Collections.singleton(globalTable),
//                    Collections.singleton(tp5));
//            List<StreamsMetadata> actual = metadataState.GetAllMetadata();
//            Assert.True("expected " + actual + " to contain " + expected, actual.Contains(expected));
//        }

//        [Fact]
//        public void ShouldGetInstancesForStoreName()
//        {
//            StreamsMetadata one = new StreamsMetadata(hostOne, Utils.mkSet(globalTable, "table-one", "table-two", "merged-table"),
//                    Utils.mkSet(topic1P0, topic2P1, topic4P0));
//            StreamsMetadata two = new StreamsMetadata(hostTwo, Utils.mkSet(globalTable, "table-two", "table-one", "merged-table"),
//                    Utils.mkSet(topic2P0, topic1P1));
//            List<StreamsMetadata> actual = metadataState.GetAllMetadataForStore("table-one");
//            Assert.Equal(2, actual.Count);
//            Assert.True("expected " + actual + " to contain " + one, actual.Contains(one));
//            Assert.True("expected " + actual + " to contain " + two, actual.Contains(two));
//        }

//        [Fact]// (expected = NullReferenceException)
//        public void ShouldThrowIfStoreNameIsNullOnGetAllInstancesWithStore()
//        {
//            metadataState.GetAllMetadataForStore(null);
//        }

//        [Fact]
//        public void ShouldReturnEmptyCollectionOnGetAllInstancesWithStoreWhenStoreDoesntExist()
//        {
//            List<StreamsMetadata> actual = metadataState.GetAllMetadataForStore("not-a-store");
//            Assert.True(actual.IsEmpty());
//        }

//        [Fact]
//        public void ShouldGetInstanceWithKey()
//        {
//            TopicPartition tp4 = new TopicPartition("topic-three", 1);
//            hostToPartitions.Put(hostTwo, Utils.mkSet(topic2P0, tp4));

//            metadataState.OnChange(hostToPartitions, cluster.withPartitions(Collections.singletonMap(tp4, new PartitionInfo("topic-three", 1, null, null, null))));

//            StreamsMetadata expected = new StreamsMetadata(hostThree, Utils.mkSet(globalTable, "table-three"),
//                    Collections.singleton(topic3P0));

//            var actual = metadataState.GetMetadataWithKey(
//                "table-three",
//                "the-key",
//                Serdes.String().Serializer);

//            Assert.Equal(expected, actual);
//        }

//        [Fact]
//        public void ShouldGetInstanceWithKeyAndCustomPartitioner()
//        {
//            TopicPartition tp4 = new TopicPartition("topic-three", 1);
//            hostToPartitions.Put(hostTwo, Utils.mkSet(topic2P0, tp4));

//            metadataState.OnChange(hostToPartitions, cluster.withPartitions(Collections.singletonMap(tp4, new PartitionInfo("topic-three", 1, null, null, null))));

//            StreamsMetadata expected = new StreamsMetadata(hostTwo, Utils.mkSet(globalTable, "table-two", "table-three", "merged-table"),
//                    Utils.mkSet(topic2P0, tp4));

//            StreamsMetadata actual = metadataState.GetMetadataWithKey("table-three", "the-key", partitioner);
//            Assert.Equal(expected, actual);
//        }

//        [Fact]
//        public void ShouldReturnNotAvailableWhenClusterIsEmpty()
//        {
//            metadataState.OnChange(Collections.emptyMap<HostInfo, HashSet<TopicPartition>>(), Cluster.empty());
//            StreamsMetadata result = metadataState.GetMetadataWithKey("table-one", "a", Serdes.String().Serializer);
//            Assert.Equal(StreamsMetadata.NOT_AVAILABLE, result);
//        }

//        [Fact]
//        public void ShouldGetInstanceWithKeyWithMergedStreams()
//        {
//            TopicPartition topic2P2 = new TopicPartition("topic-two", 2);
//            hostToPartitions.Put(hostTwo, Utils.mkSet(topic2P0, topic1P1, topic2P2));
//            metadataState.OnChange(hostToPartitions, cluster.withPartitions(Collections.singletonMap(topic2P2, new PartitionInfo("topic-two", 2, null, null, null))));

//            StreamsMetadata expected = new StreamsMetadata(hostTwo, Utils.mkSet("global-table", "table-two", "table-one", "merged-table"),
//                    Utils.mkSet(topic2P0, topic1P1, topic2P2));

//            StreamsMetadata actual = metadataState.GetMetadataWithKey("merged-table", "123", new StreamPartitioner<string, object>());
//            //    {
//            //
//            //
//            //    public int partition(string topic, string key, object value, int numPartitions)
//            //    {
//            //        return 2;
//            //    }
//            //});

//            Assert.Equal(expected, actual);

//        }

//        [Fact]
//        public void ShouldReturnNullOnGetWithKeyWhenStoreDoesntExist()
//        {
//            StreamsMetadata actual = metadataState.GetMetadataWithKey("not-a-store",
//                    "key",
//                    Serdes.String().Serializer);
//            Assert.Null(actual);
//        }

//        [Fact]// (expected = NullReferenceException)
//        public void ShouldThrowWhenKeyIsNull()
//        {
//            metadataState.GetMetadataWithKey("table-three", null, Serdes.String().Serializer);
//        }

//        [Fact]// (expected = NullReferenceException)
//        public void ShouldThrowWhenSerializerIsNull()
//        {
//            metadataState.GetMetadataWithKey("table-three", "key", (ISerializer<object>)null);
//        }

//        [Fact]// (expected = NullReferenceException)
//        public void ShouldThrowIfStoreNameIsNull()
//        {
//            metadataState.GetMetadataWithKey(null, "key", Serdes.String().Serializer);
//        }


//        [Fact]// (expected = NullReferenceException)
//        public void ShouldThrowIfStreamPartitionerIsNull()
//        {
//            metadataState.GetMetadataWithKey(null, "key", (StreamPartitioner)null);
//        }

//        [Fact]
//        public void ShouldHaveGlobalStoreInAllMetadata()
//        {
//            Collection<StreamsMetadata> metadata = metadataState.GetAllMetadataForStore(globalTable);
//            Assert.Equal(3, metadata.Count);
//            foreach (StreamsMetadata streamsMetadata in metadata)
//            {
//                Assert.True(streamsMetadata.stateStoreNames().Contains(globalTable));
//            }
//        }

//        [Fact]
//        public void ShouldGetMyMetadataForGlobalStoreWithKey()
//        {
//            StreamsMetadata metadata = metadataState.GetMetadataWithKey(globalTable, "key", Serdes.String().Serializer);
//            Assert.Equal(hostOne, metadata.hostInfo());
//        }

//        [Fact]
//        public void ShouldGetAnyHostForGlobalStoreByKeyIfMyHostUnknown()
//        {
//            StreamsMetadataState streamsMetadataState = new StreamsMetadataState(TopologyWrapper.getInternalTopologyBuilder(builder.Build()), StreamsMetadataState.UNKNOWN_HOST);
//            streamsMetadataState.onChange(hostToPartitions, cluster);
//            Assert.NotNull(streamsMetadataState.GetMetadataWithKey(globalTable, "key", Serdes.String().Serializer));
//        }

//        [Fact]
//        public void ShouldGetMyMetadataForGlobalStoreWithKeyAndPartitioner()
//        {
//            StreamsMetadata metadata = metadataState.GetMetadataWithKey(globalTable, "key", partitioner);
//            Assert.Equal(hostOne, metadata.hostInfo());
//        }

//        [Fact]
//        public void ShouldGetAnyHostForGlobalStoreByKeyAndPartitionerIfMyHostUnknown()
//        {
//            StreamsMetadataState streamsMetadataState = new StreamsMetadataState(TopologyWrapper.getInternalTopologyBuilder(builder.Build()), StreamsMetadataState.UNKNOWN_HOST);
//            streamsMetadataState.OnChange(hostToPartitions, cluster);
//            Assert.NotNull(streamsMetadataState.GetMetadataWithKey(globalTable, "key", partitioner));
//        }


//    }
//}
