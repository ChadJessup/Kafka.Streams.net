//using Confluent.Kafka;
//using Kafka.Common;
//using Kafka.Streams.Configs;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.State.KeyValues;
//using Kafka.Streams.Tasks;
//using Kafka.Streams.Temporary;
//using Kafka.Streams.Tests.Integration;
//using Kafka.Streams.Topologies;
//using System;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.Processor.Internals
//{
//    /*






//    *

//    *





//    */























































//    public class StreamsPartitionAssignorTest
//    {

//        private TopicPartition t1p0 = new TopicPartition("topic1", 0);
//        private TopicPartition t1p1 = new TopicPartition("topic1", 1);
//        private TopicPartition t1p2 = new TopicPartition("topic1", 2);
//        private TopicPartition t1p3 = new TopicPartition("topic1", 3);
//        private TopicPartition t2p0 = new TopicPartition("topic2", 0);
//        private TopicPartition t2p1 = new TopicPartition("topic2", 1);
//        private TopicPartition t2p2 = new TopicPartition("topic2", 2);
//        private TopicPartition t2p3 = new TopicPartition("topic2", 3);
//        private TopicPartition t3p0 = new TopicPartition("topic3", 0);
//        private TopicPartition t3p1 = new TopicPartition("topic3", 1);
//        private TopicPartition t3p2 = new TopicPartition("topic3", 2);
//        private TopicPartition t3p3 = new TopicPartition("topic3", 3);

//        private HashSet<string> allTopics = Utils.mkSet("topic1", "topic2");

//        private List<PartitionInfo> infos = Arrays.asList(
//                new PartitionInfo("topic1", 0, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
//                new PartitionInfo("topic1", 1, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
//                new PartitionInfo("topic1", 2, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
//                new PartitionInfo("topic2", 0, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
//                new PartitionInfo("topic2", 1, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
//                new PartitionInfo("topic2", 2, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
//                new PartitionInfo("topic3", 0, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
//                new PartitionInfo("topic3", 1, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
//                new PartitionInfo("topic3", 2, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
//                new PartitionInfo("topic3", 3, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>())
//        );

//        private HashSet<TaskId> emptyTasks = Collections.emptySet();

//        private Cluster metadata = new Cluster(
//            "cluster",
//            Collections.singletonList(Node.noNode()),
//            infos,
//            Collections.emptySet(),
//            Collections.emptySet());

//        private TaskId task0 = new TaskId(0, 0);
//        private TaskId task1 = new TaskId(0, 1);
//        private TaskId task2 = new TaskId(0, 2);
//        private TaskId task3 = new TaskId(0, 3);
//        private StreamsPartitionAssignor partitionAssignor = new StreamsPartitionAssignor();
//        private MockClientSupplier mockClientSupplier = new MockClientSupplier();
//        private InternalTopologyBuilder builder = new InternalTopologyBuilder();
//        private StreamsConfig streamsConfig = new StreamsConfig(configProps());
//        private readonly string userEndPoint = "localhost:8080";
//        private readonly string applicationId = "stream-partition-assignor-test";

//        private TaskManager taskManager = Mock.Of<TaskManager>();

//        private Dictionary<string, object> ConfigProps()
//        {
//            Dictionary<string, object> configurationMap = new HashMap<>();
//            configurationMap.Put(StreamsConfig.ApplicationIdConfig, applicationId);
//            configurationMap.Put(StreamsConfig.BootstrapServersConfig, userEndPoint);
//            configurationMap.Put(StreamsConfig.InternalConfig.TASK_MANAGER_FOR_PARTITION_ASSIGNOR, taskManager);
//            configurationMap.Put(StreamsConfig.InternalConfig.ASSIGNMENT_ERROR_CODE, new int());
//            return configurationMap;
//        }

//        private void ConfigurePartitionAssignor(Dictionary<string, object> props)
//        {
//            Dictionary<string, object> configurationMap = ConfigProps();
//            configurationMap.PutAll(props);
//            partitionAssignor.configure(configurationMap);
//        }

//        private void MockTaskManager(HashSet<TaskId> prevTasks,
//                                     HashSet<TaskId> cachedTasks,
//                                     UUID processId,
//                                     InternalTopologyBuilder builder)
//        {
//            EasyMock.expect(taskManager.builder()).andReturn(builder).anyTimes();
//            EasyMock.expect(taskManager.prevActiveTaskIds()).andReturn(prevTasks).anyTimes();
//            EasyMock.expect(taskManager.cachedTasksIds()).andReturn(cachedTasks).anyTimes();
//            EasyMock.expect(taskManager.processId()).andReturn(processId).anyTimes();
//            EasyMock.replay(taskManager);
//        }

//        private Dictionary<string, ConsumerPartitionAssignor.Subscription> subscriptions;


//        public void SetUp()
//        {
//            if (subscriptions != null)
//            {
//                subscriptions.Clear();
//            }
//            else
//            {
//                subscriptions = new HashMap<>();
//            }
//        }

//        [Fact]
//        public void ShouldInterleaveTasksByGroupId()
//        {
//            TaskId taskIdA0 = new TaskId(0, 0);
//            TaskId taskIdA1 = new TaskId(0, 1);
//            TaskId taskIdA2 = new TaskId(0, 2);
//            TaskId taskIdA3 = new TaskId(0, 3);

//            TaskId taskIdB0 = new TaskId(1, 0);
//            TaskId taskIdB1 = new TaskId(1, 1);
//            TaskId taskIdB2 = new TaskId(1, 2);

//            TaskId taskIdC0 = new TaskId(2, 0);
//            TaskId taskIdC1 = new TaskId(2, 1);

//            List<TaskId> expectedSubList1 = Arrays.asList(taskIdA0, taskIdA3, taskIdB2);
//            List<TaskId> expectedSubList2 = Arrays.asList(taskIdA1, taskIdB0, taskIdC0);
//            List<TaskId> expectedSubList3 = Arrays.asList(taskIdA2, taskIdB1, taskIdC1);
//            List<List<TaskId>> embeddedList = Arrays.asList(expectedSubList1, expectedSubList2, expectedSubList3);

//            List<TaskId> tasks = Arrays.asList(taskIdC0, taskIdC1, taskIdB0, taskIdB1, taskIdB2, taskIdA0, taskIdA1, taskIdA2, taskIdA3);
//            Collections.shuffle(tasks);

//            List<List<TaskId>> interleavedTaskIds = partitionAssignor.interleaveTasksByGroupId(tasks, 3);

//            Assert.Equal(interleavedTaskIds, embeddedList);
//        }

//        [Fact]
//        public void TestSubscription()
//        {
//            builder.AddSource(null, "source1", null, null, null, "topic1");
//            builder.AddSource(null, "source2", null, null, null, "topic2");
//            builder.AddProcessor("processor", new MockProcessorSupplier(), "source1", "source2");

//            HashSet<TaskId> prevTasks = Utils.mkSet(
//                    new TaskId(0, 1), new TaskId(1, 1), new TaskId(2, 1));
//            HashSet<TaskId> cachedTasks = Utils.mkSet(
//                    new TaskId(0, 1), new TaskId(1, 1), new TaskId(2, 1),
//                    new TaskId(0, 2), new TaskId(1, 2), new TaskId(2, 2));

//            UUID processId = UUID.randomUUID();
//            MockTaskManager(prevTasks, cachedTasks, processId, builder);

//            configurePartitionAssignor(Collections.emptyMap());

//            HashSet<string> topics = Utils.mkSet("topic1", "topic2");
//            ConsumerPartitionAssignor.Subscription subscription = new ConsumerPartitionAssignor.Subscription(new List<>(topics), partitionAssignor.subscriptionUserData(topics));

//            Collections.sort(subscription.topics());
//            Assert.Equal(asList("topic1", "topic2"), subscription.topics());

//            HashSet<TaskId> StandbyTasks = new HashSet<>(cachedTasks);
//            StandbyTasks.removeAll(prevTasks);

//            SubscriptionInfo info = new SubscriptionInfo(processId, prevTasks, StandbyTasks, null);
//            Assert.Equal(info.encode(), subscription.userData());
//        }

//        [Fact]
//        public void TestAssignBasic()
//        {
//            builder.AddSource(null, "source1", null, null, null, "topic1");
//            builder.AddSource(null, "source2", null, null, null, "topic2");
//            builder.AddProcessor("processor", new MockProcessorSupplier(), "source1", "source2");
//            List<string> topics = Arrays.asList("topic1", "topic2");
//            HashSet<TaskId> allTasks = Utils.mkSet(task0, task1, task2);

//            HashSet<TaskId> prevTasks10 = Utils.mkSet(task0);
//            HashSet<TaskId> prevTasks11 = Utils.mkSet(task1);
//            HashSet<TaskId> prevTasks20 = Utils.mkSet(task2);
//            HashSet<TaskId> standbyTasks10 = Utils.mkSet(task1);
//            HashSet<TaskId> standbyTasks11 = Utils.mkSet(task2);
//            HashSet<TaskId> standbyTasks20 = Utils.mkSet(task0);

//            UUID uuid1 = UUID.randomUUID();
//            UUID uuid2 = UUID.randomUUID();

//            MockTaskManager(prevTasks10, standbyTasks10, uuid1, builder);
//            configurePartitionAssignor(Collections.emptyMap());

//            partitionAssignor.setInternalTopicManager(new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer));

//            subscriptions.Put("consumer10",
//                    new ConsumerPartitionAssignor.Subscription(topics,
//                            new SubscriptionInfo(uuid1, prevTasks10, standbyTasks10, userEndPoint).encode()));
//            subscriptions.Put("consumer11",
//                    new ConsumerPartitionAssignor.Subscription(topics,
//                            new SubscriptionInfo(uuid1, prevTasks11, standbyTasks11, userEndPoint).encode()));
//            subscriptions.Put("consumer20",
//                    new ConsumerPartitionAssignor.Subscription(topics,
//                            new SubscriptionInfo(uuid2, prevTasks20, standbyTasks20, userEndPoint).encode()));

//            Dictionary<string, ConsumerPartitionAssignor.Assignment> assignments = partitionAssignor.Assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

//            // check assigned partitions
//            Assert.Equal(Utils.mkSet(Utils.mkSet(t1p0, t2p0), Utils.mkSet(t1p1, t2p1)),
//                    Utils.mkSet(new HashSet<>(assignments.Get("consumer10").partitions()), new HashSet<>(assignments.Get("consumer11").partitions())));
//            Assert.Equal(Utils.mkSet(t1p2, t2p2), new HashSet<>(assignments.Get("consumer20").partitions()));

//            // check assignment info

//            // the first consumer
//            AssignmentInfo info10 = checkAssignment(allTopics, assignments.Get("consumer10"));
//            HashSet<TaskId> allActiveTasks = new HashSet<>(info10.ActiveTasks());

//            // the second consumer
//            AssignmentInfo info11 = checkAssignment(allTopics, assignments.Get("consumer11"));
//            allActiveTasks.addAll(info11.ActiveTasks());

//            Assert.Equal(Utils.mkSet(task0, task1), allActiveTasks);

//            // the third consumer
//            AssignmentInfo info20 = checkAssignment(allTopics, assignments.Get("consumer20"));
//            allActiveTasks.addAll(info20.ActiveTasks());

//            Assert.Equal(3, allActiveTasks.Count);
//            Assert.Equal(allTasks, new HashSet<>(allActiveTasks));

//            Assert.Equal(3, allActiveTasks.Count);
//            Assert.Equal(allTasks, allActiveTasks);
//        }

//        [Fact]
//        public void ShouldAssignEvenlyAcrossConsumersOneClientMultipleThreads()
//        {
//            builder.AddSource(null, "source1", null, null, null, "topic1");
//            builder.AddSource(null, "source2", null, null, null, "topic2");
//            builder.AddProcessor("processor", new MockProcessorSupplier(), "source1");
//            builder.AddProcessor("processorII", new MockProcessorSupplier(), "source2");

//            List<PartitionInfo> localInfos = Arrays.asList(
//                new PartitionInfo("topic1", 0, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
//                new PartitionInfo("topic1", 1, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
//                new PartitionInfo("topic1", 2, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
//                new PartitionInfo("topic1", 3, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
//                new PartitionInfo("topic2", 0, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
//                new PartitionInfo("topic2", 1, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
//                new PartitionInfo("topic2", 2, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
//                new PartitionInfo("topic2", 3, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>())
//            );

//            Cluster localMetadata = new Cluster(
//                "cluster",
//                Collections.singletonList(Node.noNode()),
//                localInfos,
//                Collections.emptySet(),
//                Collections.emptySet());

//            List<string> topics = Arrays.asList("topic1", "topic2");

//            TaskId taskIdA0 = new TaskId(0, 0);
//            TaskId taskIdA1 = new TaskId(0, 1);
//            TaskId taskIdA2 = new TaskId(0, 2);
//            TaskId taskIdA3 = new TaskId(0, 3);

//            TaskId taskIdB0 = new TaskId(1, 0);
//            TaskId taskIdB1 = new TaskId(1, 1);
//            TaskId taskIdB2 = new TaskId(1, 2);
//            TaskId taskIdB3 = new TaskId(1, 3);

//            UUID uuid1 = UUID.randomUUID();

//            mockTaskManager(new HashSet<>(), new HashSet<>(), uuid1, builder);
//            configurePartitionAssignor(Collections.emptyMap());

//            partitionAssignor.setInternalTopicManager(new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer));

//            subscriptions.Put("consumer10",
//                              new ConsumerPartitionAssignor.Subscription(topics,
//                                      new SubscriptionInfo(uuid1, new HashSet<>(), new HashSet<>(), userEndPoint).encode()));
//            subscriptions.Put("consumer11",
//                              new ConsumerPartitionAssignor.Subscription(topics,
//                                      new SubscriptionInfo(uuid1, new HashSet<>(), new HashSet<>(), userEndPoint).encode()));

//            Dictionary<string, ConsumerPartitionAssignor.Assignment> assignments = partitionAssignor.Assign(localMetadata, new GroupSubscription(subscriptions)).groupAssignment();

//            // check assigned partitions
//            Assert.Equal(Utils.mkSet(Utils.mkSet(t2p2, t1p0, t1p2, t2p0), Utils.mkSet(t1p1, t2p1, t1p3, t2p3)),
//                         Utils.mkSet(new HashSet<>(assignments.Get("consumer10").partitions()), new HashSet<>(assignments.Get("consumer11").partitions())));

//            // the first consumer
//            AssignmentInfo info10 = AssignmentInfo.decode(assignments.Get("consumer10").userData());

//            List<TaskId> expectedInfo10TaskIds = Arrays.asList(taskIdA1, taskIdA3, taskIdB1, taskIdB3);
//            Assert.Equal(expectedInfo10TaskIds, info10.ActiveTasks());

//            // the second consumer
//            AssignmentInfo info11 = AssignmentInfo.decode(assignments.Get("consumer11").userData());
//            List<TaskId> expectedInfo11TaskIds = Arrays.asList(taskIdA0, taskIdA2, taskIdB0, taskIdB2);

//            Assert.Equal(expectedInfo11TaskIds, info11.ActiveTasks());
//        }

//        [Fact]
//        public void TestAssignWithPartialTopology()
//        {
//            builder.AddSource(null, "source1", null, null, null, "topic1");
//            builder.AddProcessor("processor1", new MockProcessorSupplier(), "source1");
//            builder.AddStateStore(new MockKeyValueStoreBuilder("store1", false), "processor1");
//            builder.AddSource(null, "source2", null, null, null, "topic2");
//            builder.AddProcessor("processor2", new MockProcessorSupplier(), "source2");
//            builder.AddStateStore(new MockKeyValueStoreBuilder("store2", false), "processor2");
//            List<string> topics = Arrays.asList("topic1", "topic2");
//            HashSet<TaskId> allTasks = Utils.mkSet(task0, task1, task2);

//            UUID uuid1 = UUID.randomUUID();

//            MockTaskManager(emptyTasks, emptyTasks, uuid1, builder);
//            configurePartitionAssignor(Collections.singletonMap(StreamsConfig.PARTITION_GROUPER_CLASS_CONFIG, SingleGroupPartitionGrouperStub));

//            partitionAssignor.setInternalTopicManager(new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer));

//            // will throw exception if it fails
//            subscriptions.Put("consumer10",
//                    new ConsumerPartitionAssignor.Subscription(topics,
//                            new SubscriptionInfo(uuid1, emptyTasks, emptyTasks, userEndPoint).encode()
//            ));
//            Dictionary<string, ConsumerPartitionAssignor.Assignment> assignments = partitionAssignor.Assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

//            // check assignment info
//            AssignmentInfo info10 = checkAssignment(Utils.mkSet("topic1"), assignments.Get("consumer10"));
//            HashSet<TaskId> allActiveTasks = new HashSet<>(info10.ActiveTasks());

//            Assert.Equal(3, allActiveTasks.Count);
//            Assert.Equal(allTasks, new HashSet<>(allActiveTasks));
//        }


//        [Fact]
//        public void TestAssignEmptyMetadata()
//        {
//            builder.AddSource(null, "source1", null, null, null, "topic1");
//            builder.AddSource(null, "source2", null, null, null, "topic2");
//            builder.AddProcessor("processor", new MockProcessorSupplier(), "source1", "source2");
//            List<string> topics = Arrays.asList("topic1", "topic2");
//            HashSet<TaskId> allTasks = Utils.mkSet(task0, task1, task2);

//            HashSet<TaskId> prevTasks10 = Utils.mkSet(task0);
//            HashSet<TaskId> standbyTasks10 = Utils.mkSet(task1);
//            Cluster emptyMetadata = new Cluster("cluster", Collections.singletonList(Node.noNode()),
//               Collections.emptySet(),
//               Collections.emptySet(),
//               Collections.emptySet());
//            UUID uuid1 = UUID.randomUUID();

//            MockTaskManager(prevTasks10, standbyTasks10, uuid1, builder);
//            configurePartitionAssignor(Collections.emptyMap());

//            subscriptions.Put("consumer10",
//                    new ConsumerPartitionAssignor.Subscription(topics,
//                            new SubscriptionInfo(uuid1, prevTasks10, standbyTasks10, userEndPoint).encode()
//                    ));

//            // initially metadata is empty
//            Dictionary<string, ConsumerPartitionAssignor.Assignment> assignments = partitionAssignor.Assign(emptyMetadata, new GroupSubscription(subscriptions)).groupAssignment();

//            // check assigned partitions
//            Assert.Equal(Collections.emptySet(),
//                new HashSet<>(assignments.Get("consumer10").partitions()));

//            // check assignment info
//            AssignmentInfo info10 = checkAssignment(Collections.emptySet(), assignments.Get("consumer10"));
//            HashSet<TaskId> allActiveTasks = new HashSet<>(info10.ActiveTasks());

//            Assert.Empty(allActiveTasks);

//            // then metadata gets populated
//            assignments = partitionAssignor.Assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();
//            // check assigned partitions
//            Assert.Equal(Utils.mkSet(Utils.mkSet(t1p0, t2p0, t1p0, t2p0, t1p1, t2p1, t1p2, t2p2)),
//                Utils.mkSet(new HashSet<>(assignments.Get("consumer10").partitions())));

//            // the first consumer
//            info10 = checkAssignment(allTopics, assignments.Get("consumer10"));
//            allActiveTasks.addAll(info10.ActiveTasks());

//            Assert.Equal(3, allActiveTasks.Count);
//            Assert.Equal(allTasks, new HashSet<>(allActiveTasks));

//            Assert.Equal(3, allActiveTasks.Count);
//            Assert.Equal(allTasks, allActiveTasks);
//        }

//        [Fact]
//        public void TestAssignWithNewTasks()
//        {
//            builder.AddSource(null, "source1", null, null, null, "topic1");
//            builder.AddSource(null, "source2", null, null, null, "topic2");
//            builder.AddSource(null, "source3", null, null, null, "topic3");
//            builder.AddProcessor("processor", new MockProcessorSupplier(), "source1", "source2", "source3");
//            List<string> topics = Arrays.asList("topic1", "topic2", "topic3");
//            HashSet<TaskId> allTasks = Utils.mkSet(task0, task1, task2, task3);

//            // assuming that previous tasks do not have topic3
//            HashSet<TaskId> prevTasks10 = Utils.mkSet(task0);
//            HashSet<TaskId> prevTasks11 = Utils.mkSet(task1);
//            HashSet<TaskId> prevTasks20 = Utils.mkSet(task2);

//            UUID uuid1 = UUID.randomUUID();
//            UUID uuid2 = UUID.randomUUID();
//            MockTaskManager(prevTasks10, emptyTasks, uuid1, builder);
//            configurePartitionAssignor(Collections.emptyMap());

//            partitionAssignor.setInternalTopicManager(new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer));

//            subscriptions.Put("consumer10",
//                    new ConsumerPartitionAssignor.Subscription(topics,
//                            new SubscriptionInfo(uuid1, prevTasks10, emptyTasks, userEndPoint).encode()));
//            subscriptions.Put("consumer11",
//                    new ConsumerPartitionAssignor.Subscription(topics,
//                            new SubscriptionInfo(uuid1, prevTasks11, emptyTasks, userEndPoint).encode()));
//            subscriptions.Put("consumer20",
//                    new ConsumerPartitionAssignor.Subscription(topics,
//                            new SubscriptionInfo(uuid2, prevTasks20, emptyTasks, userEndPoint).encode()));

//            Dictionary<string, ConsumerPartitionAssignor.Assignment> assignments = partitionAssignor.Assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

//            // check assigned partitions: since there is no previous task for topic 3 it will be assigned randomly so we cannot check exact match
//            // also note that previously assigned partitions / tasks may not stay on the previous host since we may assign the new task first and
//            // then later ones will be re-assigned to other hosts due to load balancing
//            AssignmentInfo info = AssignmentInfo.decode(assignments.Get("consumer10").userData());
//            HashSet<TaskId> allActiveTasks = new HashSet<>(info.ActiveTasks());
//            HashSet<TopicPartition> allPartitions = new HashSet<>(assignments.Get("consumer10").partitions());

//            info = AssignmentInfo.decode(assignments.Get("consumer11").userData());
//            allActiveTasks.addAll(info.ActiveTasks());
//            allPartitions.addAll(assignments.Get("consumer11").partitions());

//            info = AssignmentInfo.decode(assignments.Get("consumer20").userData());
//            allActiveTasks.addAll(info.ActiveTasks());
//            allPartitions.addAll(assignments.Get("consumer20").partitions());

//            Assert.Equal(allTasks, allActiveTasks);
//            Assert.Equal(Utils.mkSet(t1p0, t1p1, t1p2, t2p0, t2p1, t2p2, t3p0, t3p1, t3p2, t3p3), allPartitions);
//        }

//        [Fact]
//        public void TestAssignWithStates()
//        {
//            builder.SetApplicationId(applicationId);
//            builder.AddSource(null, "source1", null, null, null, "topic1");
//            builder.AddSource(null, "source2", null, null, null, "topic2");

//            builder.AddProcessor("processor-1", new MockProcessorSupplier(), "source1");
//            builder.AddStateStore(new MockKeyValueStoreBuilder("store1", false), "processor-1");

//            builder.AddProcessor("processor-2", new MockProcessorSupplier(), "source2");
//            builder.AddStateStore(new MockKeyValueStoreBuilder("store2", false), "processor-2");
//            builder.AddStateStore(new MockKeyValueStoreBuilder("store3", false), "processor-2");

//            List<string> topics = Arrays.asList("topic1", "topic2");

//            TaskId task00 = new TaskId(0, 0);
//            TaskId task01 = new TaskId(0, 1);
//            TaskId task02 = new TaskId(0, 2);
//            TaskId task10 = new TaskId(1, 0);
//            TaskId task11 = new TaskId(1, 1);
//            TaskId task12 = new TaskId(1, 2);
//            List<TaskId> tasks = Arrays.asList(task00, task01, task02, task10, task11, task12);

//            UUID uuid1 = UUID.randomUUID();
//            UUID uuid2 = UUID.randomUUID();

//            MockTaskManager(
//                emptyTasks,
//                emptyTasks,
//                uuid1,
//                builder);
//            configurePartitionAssignor(Collections.emptyMap());

//            partitionAssignor.setInternalTopicManager(new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer));

//            subscriptions.Put("consumer10",
//                    new ConsumerPartitionAssignor.Subscription(topics,
//                            new SubscriptionInfo(uuid1, emptyTasks, emptyTasks, userEndPoint).encode()));
//            subscriptions.Put("consumer11",
//                    new ConsumerPartitionAssignor.Subscription(topics,
//                            new SubscriptionInfo(uuid1, emptyTasks, emptyTasks, userEndPoint).encode()));
//            subscriptions.Put("consumer20",
//                    new ConsumerPartitionAssignor.Subscription(topics,
//                            new SubscriptionInfo(uuid2, emptyTasks, emptyTasks, userEndPoint).encode()));

//            Dictionary<string, ConsumerPartitionAssignor.Assignment> assignments = partitionAssignor.Assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

//            // check assigned partition size: since there is no previous task and there are two sub-topologies the assignment is random so we cannot check exact match
//            Assert.Equal(2, assignments.Get("consumer10").partitions().Count);
//            Assert.Equal(2, assignments.Get("consumer11").partitions().Count);
//            Assert.Equal(2, assignments.Get("consumer20").partitions().Count);

//            AssignmentInfo info10 = AssignmentInfo.decode(assignments.Get("consumer10").userData());
//            AssignmentInfo info11 = AssignmentInfo.decode(assignments.Get("consumer11").userData());
//            AssignmentInfo info20 = AssignmentInfo.decode(assignments.Get("consumer20").userData());

//            Assert.Equal(2, info10.ActiveTasks().Count);
//            Assert.Equal(2, info11.ActiveTasks().Count);
//            Assert.Equal(2, info20.ActiveTasks().Count);

//            HashSet<TaskId> allTasks = new HashSet<>();
//            allTasks.addAll(info10.ActiveTasks());
//            allTasks.addAll(info11.ActiveTasks());
//            allTasks.addAll(info20.ActiveTasks());
//            Assert.Equal(new HashSet<>(tasks), allTasks);

//            // check tasks for state topics
//            Dictionary<int, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.topicGroups();

//            Assert.Equal(Utils.mkSet(task00, task01, task02), TasksForState("store1", tasks, topicGroups));
//            Assert.Equal(Utils.mkSet(task10, task11, task12), TasksForState("store2", tasks, topicGroups));
//            Assert.Equal(Utils.mkSet(task10, task11, task12), TasksForState("store3", tasks, topicGroups));
//        }

//        private HashSet<TaskId> TasksForState(string storeName,
//                                          List<TaskId> tasks,
//                                          Dictionary<int, InternalTopologyBuilder.TopicsInfo> topicGroups)
//        {
//            string changelogTopic = ProcessorStateManager.StoreChangelogTopic(applicationId, storeName);

//            HashSet<TaskId> ids = new HashSet<>();
//            foreach (Map.Entry<int, InternalTopologyBuilder.TopicsInfo> entry in topicGroups)
//            {
//                HashSet<string> stateChangelogTopics = entry.Value.stateChangelogTopics.keySet();

//                if (stateChangelogTopics.Contains(changelogTopic))
//                {
//                    foreach (TaskId id in tasks)
//                    {
//                        if (id.topicGroupId == entry.Key)
//                        {
//                            ids.Add(id);
//                        }
//                    }
//                }
//            }
//            return ids;
//        }

//        [Fact]
//        public void TestAssignWithStandbyReplicas()
//        {
//            Dictionary<string, object> props = ConfigProps();
//            props.Put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, "1");
//            StreamsConfig streamsConfig = new StreamsConfig(props);

//            builder.AddSource(null, "source1", null, null, null, "topic1");
//            builder.AddSource(null, "source2", null, null, null, "topic2");
//            builder.AddProcessor("processor", new MockProcessorSupplier(), "source1", "source2");
//            List<string> topics = Arrays.asList("topic1", "topic2");
//            HashSet<TaskId> allTasks = Utils.mkSet(task0, task1, task2);


//            HashSet<TaskId> prevTasks00 = Utils.mkSet(task0);
//            HashSet<TaskId> prevTasks01 = Utils.mkSet(task1);
//            HashSet<TaskId> prevTasks02 = Utils.mkSet(task2);
//            HashSet<TaskId> standbyTasks01 = Utils.mkSet(task1);
//            HashSet<TaskId> standbyTasks02 = Utils.mkSet(task2);
//            HashSet<TaskId> standbyTasks00 = Utils.mkSet(task0);

//            UUID uuid1 = UUID.randomUUID();
//            UUID uuid2 = UUID.randomUUID();

//            MockTaskManager(prevTasks00, standbyTasks01, uuid1, builder);

//            configurePartitionAssignor(Collections.singletonMap(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1));

//            partitionAssignor.setInternalTopicManager(new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer));

//            subscriptions.Put("consumer10",
//                    new ConsumerPartitionAssignor.Subscription(topics,
//                            new SubscriptionInfo(uuid1, prevTasks00, standbyTasks01, userEndPoint).encode()));
//            subscriptions.Put("consumer11",
//                    new ConsumerPartitionAssignor.Subscription(topics,
//                            new SubscriptionInfo(uuid1, prevTasks01, standbyTasks02, userEndPoint).encode()));
//            subscriptions.Put("consumer20",
//                    new ConsumerPartitionAssignor.Subscription(topics,
//                            new SubscriptionInfo(uuid2, prevTasks02, standbyTasks00, "any:9097").encode()));

//            Dictionary<string, ConsumerPartitionAssignor.Assignment> assignments = partitionAssignor.Assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

//            // the first consumer
//            AssignmentInfo info10 = checkAssignment(allTopics, assignments.Get("consumer10"));
//            HashSet<TaskId> allActiveTasks = new HashSet<>(info10.ActiveTasks());
//            HashSet<TaskId> allStandbyTasks = new HashSet<>(info10.StandbyTasks().keySet());

//            // the second consumer
//            AssignmentInfo info11 = checkAssignment(allTopics, assignments.Get("consumer11"));
//            allActiveTasks.addAll(info11.ActiveTasks());
//            allStandbyTasks.addAll(info11.StandbyTasks().keySet());

//            Assert.NotEqual("same processId has same set of standby tasks", info11.StandbyTasks().keySet(), info10.StandbyTasks().keySet());

//            // check active tasks assigned to the first client
//            Assert.Equal(Utils.mkSet(task0, task1), new HashSet<>(allActiveTasks));
//            Assert.Equal(Utils.mkSet(task2), new HashSet<>(allStandbyTasks));

//            // the third consumer
//            AssignmentInfo info20 = checkAssignment(allTopics, assignments.Get("consumer20"));
//            allActiveTasks.addAll(info20.ActiveTasks());
//            allStandbyTasks.addAll(info20.StandbyTasks().keySet());

//            // All task ids are in the active tasks and also in the standby tasks

//            Assert.Equal(3, allActiveTasks.Count);
//            Assert.Equal(allTasks, allActiveTasks);

//            Assert.Equal(3, allStandbyTasks.Count);
//            Assert.Equal(allTasks, allStandbyTasks);
//        }

//        [Fact]
//        public void TestOnAssignment()
//        {
//            configurePartitionAssignor(Collections.emptyMap());

//            List<TaskId> activeTaskList = Arrays.asList(task0, task3);
//            Dictionary<TaskId, HashSet<TopicPartition>> ActiveTasks = new HashMap<>();
//            Dictionary<TaskId, HashSet<TopicPartition>> StandbyTasks = new HashMap<>();
//            Dictionary<HostInfo, HashSet<TopicPartition>> hostState = Collections.singletonMap(
//                    new HostInfo("localhost", 9090),
//                    Utils.mkSet(t3p0, t3p3));
//            ActiveTasks.Put(task0, Utils.mkSet(t3p0));
//            ActiveTasks.Put(task3, Utils.mkSet(t3p3));
//            StandbyTasks.Put(task1, Utils.mkSet(t3p1));
//            StandbyTasks.Put(task2, Utils.mkSet(t3p2));

//            AssignmentInfo info = new AssignmentInfo(activeTaskList, StandbyTasks, hostState);
//            ConsumerPartitionAssignor.Assignment assignment = new ConsumerPartitionAssignor.Assignment(asList(t3p0, t3p3), info.encode());

//            Capture<Cluster> capturedCluster = EasyMock.newCapture();
//            taskManager.setPartitionsByHostState(hostState);
//            EasyMock.expectLastCall();
//            taskManager.SetAssignmentMetadata(ActiveTasks, StandbyTasks);
//            EasyMock.expectLastCall();
//            taskManager.setClusterMetadata(EasyMock.capture(capturedCluster));
//            EasyMock.expectLastCall();
//            EasyMock.replay(taskManager);

//            partitionAssignor.onAssignment(assignment, null);

//            EasyMock.verify(taskManager);

//            Assert.Equal(Collections.singleton(t3p0.Topic), capturedCluster.Value.topics());
//            Assert.Equal(2, capturedCluster.Value.partitionsForTopic(t3p0.Topic).Count);
//        }

//        [Fact]
//        public void TestAssignWithInternalTopics()
//        {
//            builder.SetApplicationId(applicationId);
//            builder.AddInternalTopic("topicX");
//            builder.AddSource(null, "source1", null, null, null, "topic1");
//            builder.AddProcessor("processor1", new MockProcessorSupplier(), "source1");
//            builder.AddSink("sink1", "topicX", null, null, null, "processor1");
//            builder.AddSource(null, "source2", null, null, null, "topicX");
//            builder.AddProcessor("processor2", new MockProcessorSupplier(), "source2");
//            List<string> topics = Arrays.asList("topic1", applicationId + "-topicX");
//            HashSet<TaskId> allTasks = Utils.mkSet(task0, task1, task2);

//            UUID uuid1 = UUID.randomUUID();
//            MockTaskManager(emptyTasks, emptyTasks, uuid1, builder);
//            configurePartitionAssignor(Collections.emptyMap());
//            MockInternalTopicManager internalTopicManager = new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer);
//            partitionAssignor.setInternalTopicManager(internalTopicManager);

//            subscriptions.Put("consumer10",
//                    new ConsumerPartitionAssignor.Subscription(topics,
//                            new SubscriptionInfo(uuid1, emptyTasks, emptyTasks, userEndPoint).encode())
//            );
//            partitionAssignor.Assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

//            // check prepared internal topics
//            Assert.Equal(1, internalTopicManager.readyTopics.Count);
//            Assert.Equal(allTasks.Count, (long)internalTopicManager.readyTopics.Get(applicationId + "-topicX"));
//        }

//        [Fact]
//        public void TestAssignWithInternalTopicThatsSourceIsAnotherInternalTopic()
//        {
//            string applicationId = "test";
//            builder.SetApplicationId(applicationId);
//            builder.AddInternalTopic("topicX");
//            builder.AddSource(null, "source1", null, null, null, "topic1");
//            builder.AddProcessor("processor1", new MockProcessorSupplier(), "source1");
//            builder.AddSink("sink1", "topicX", null, null, null, "processor1");
//            builder.AddSource(null, "source2", null, null, null, "topicX");
//            builder.AddInternalTopic("topicZ");
//            builder.AddProcessor("processor2", new MockProcessorSupplier(), "source2");
//            builder.AddSink("sink2", "topicZ", null, null, null, "processor2");
//            builder.AddSource(null, "source3", null, null, null, "topicZ");
//            List<string> topics = Arrays.asList("topic1", "test-topicX", "test-topicZ");
//            HashSet<TaskId> allTasks = Utils.mkSet(task0, task1, task2);

//            UUID uuid1 = UUID.randomUUID();
//            MockTaskManager(emptyTasks, emptyTasks, uuid1, builder);

//            configurePartitionAssignor(Collections.emptyMap());
//            MockInternalTopicManager internalTopicManager = new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer);
//            partitionAssignor.setInternalTopicManager(internalTopicManager);

//            subscriptions.Put("consumer10",
//                    new ConsumerPartitionAssignor.Subscription(topics,
//                            new SubscriptionInfo(uuid1, emptyTasks, emptyTasks, userEndPoint).encode())
//            );
//            partitionAssignor.Assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

//            // check prepared internal topics
//            Assert.Equal(2, internalTopicManager.readyTopics.Count);
//            Assert.Equal(allTasks.Count, (long)internalTopicManager.readyTopics.Get("test-topicZ"));
//        }

//        [Fact]
//        public void ShouldGenerateTasksForAllCreatedPartitions()
//        {
//            StreamsBuilder builder = new StreamsBuilder();

//            // KStream with 3 partitions
//            IKStream<K, V> stream1 = builder
//                .Stream("topic1")
//                // force creation of internal repartition topic
//                .Map((KeyValueMapper<object, object, KeyValuePair<object, object>>)KeyValuePair);

//            // KTable with 4 partitions
//            KTable<object, long> table1 = builder
//                .table("topic3")
//                // force creation of internal repartition topic
//                .GroupBy(KeyValuePair)
//                .Count();

//            // joining the stream and the table
//            // this triggers the enforceCopartitioning() routine in the StreamsPartitionAssignor,
//            // forcing the stream.Map to get repartitioned to a topic with four partitions.
//            stream1.Join(
//                table1,
//                (value1, value2) => null);

//            UUID uuid = UUID.randomUUID();
//            string client = "client1";
//            InternalTopologyBuilder internalTopologyBuilder = TopologyWrapper.getInternalTopologyBuilder(builder.Build());
//            internalTopologyBuilder.SetApplicationId(applicationId);

//            mockTaskManager(
//                emptyTasks,
//                emptyTasks,
//                UUID.randomUUID(),
//                internalTopologyBuilder);
//            configurePartitionAssignor(Collections.emptyMap());

//            MockInternalTopicManager mockInternalTopicManager = new MockInternalTopicManager(
//                streamsConfig,
//                mockClientSupplier.restoreConsumer);
//            partitionAssignor.setInternalTopicManager(mockInternalTopicManager);

//            subscriptions.Put(client,
//                    new ConsumerPartitionAssignor.Subscription(
//                            Arrays.asList("topic1", "topic3"),
//                            new SubscriptionInfo(uuid, emptyTasks, emptyTasks, userEndPoint).encode())
//            );
//            Dictionary<string, ConsumerPartitionAssignor.Assignment> assignment = partitionAssignor.Assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

//            Dictionary<string, int> expectedCreatedInternalTopics = new HashMap<>();
//            expectedCreatedInternalTopics.Put(applicationId + "-KTABLE-AGGREGATE-STATE-STORE-0000000006-repartition", 4);
//            expectedCreatedInternalTopics.Put(applicationId + "-KTABLE-AGGREGATE-STATE-STORE-0000000006-changelog", 4);
//            expectedCreatedInternalTopics.Put(applicationId + "-topic3-STATE-STORE-0000000002-changelog", 4);
//            expectedCreatedInternalTopics.Put(applicationId + "-KSTREAM-MAP-0000000001-repartition", 4);

//            // check if All internal topics were created as expected
//            Assert.Equal(mockInternalTopicManager.readyTopics, expectedCreatedInternalTopics);

//            List<TopicPartition> expectedAssignment = Arrays.asList(
//                new TopicPartition("topic1", 0),
//                new TopicPartition("topic1", 1),
//                new TopicPartition("topic1", 2),
//                new TopicPartition("topic3", 0),
//                new TopicPartition("topic3", 1),
//                new TopicPartition("topic3", 2),
//                new TopicPartition("topic3", 3),
//                new TopicPartition(applicationId + "-KTABLE-AGGREGATE-STATE-STORE-0000000006-repartition", 0),
//                new TopicPartition(applicationId + "-KTABLE-AGGREGATE-STATE-STORE-0000000006-repartition", 1),
//                new TopicPartition(applicationId + "-KTABLE-AGGREGATE-STATE-STORE-0000000006-repartition", 2),
//                new TopicPartition(applicationId + "-KTABLE-AGGREGATE-STATE-STORE-0000000006-repartition", 3),
//                new TopicPartition(applicationId + "-KSTREAM-MAP-0000000001-repartition", 0),
//                new TopicPartition(applicationId + "-KSTREAM-MAP-0000000001-repartition", 1),
//                new TopicPartition(applicationId + "-KSTREAM-MAP-0000000001-repartition", 2),
//                new TopicPartition(applicationId + "-KSTREAM-MAP-0000000001-repartition", 3)
//            );

//            // check if we created a task for All expected topicPartitions.
//            Assert.Equal(new HashSet<>(assignment.Get(client).partitions()), new HashSet<>(expectedAssignment));
//        }

//        [Fact]
//        public void ShouldAddUserDefinedEndPointToSubscription()
//        {
//            builder.SetApplicationId(applicationId);
//            builder.AddSource(null, "source", null, null, null, "input");
//            builder.AddProcessor("processor", new MockProcessorSupplier(), "source");
//            builder.AddSink("sink", "output", null, null, null, "processor");

//            UUID uuid1 = UUID.randomUUID();
//            MockTaskManager(
//                emptyTasks,
//                emptyTasks,
//                uuid1,
//                builder);
//            configurePartitionAssignor(Collections.singletonMap(StreamsConfig.APPLICATION_SERVER_CONFIG, userEndPoint));
//            HashSet<string> topics = Utils.mkSet("input");
//            ConsumerPartitionAssignor.Subscription subscription = new ConsumerPartitionAssignor.Subscription(new List<>(topics), partitionAssignor.subscriptionUserData(topics));
//            SubscriptionInfo subscriptionInfo = SubscriptionInfo.decode(subscription.userData());
//            Assert.Equal("localhost:8080", subscriptionInfo.userEndPoint());
//        }

//        [Fact]
//        public void ShouldMapUserEndPointToTopicPartitions()
//        {
//            builder.SetApplicationId(applicationId);
//            builder.AddSource(null, "source", null, null, null, "topic1");
//            builder.AddProcessor("processor", new MockProcessorSupplier(), "source");
//            builder.AddSink("sink", "output", null, null, null, "processor");

//            List<string> topics = Collections.singletonList("topic1");

//            UUID uuid1 = UUID.randomUUID();

//            MockTaskManager(emptyTasks, emptyTasks, uuid1, builder);
//            configurePartitionAssignor(Collections.singletonMap(StreamsConfig.APPLICATION_SERVER_CONFIG, userEndPoint));

//            partitionAssignor.setInternalTopicManager(new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer));

//            subscriptions.Put("consumer1",
//                    new ConsumerPartitionAssignor.Subscription(topics,
//                            new SubscriptionInfo(uuid1, emptyTasks, emptyTasks, userEndPoint).encode())
//            );
//            Dictionary<string, ConsumerPartitionAssignor.Assignment> assignments = partitionAssignor.Assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();
//            ConsumerPartitionAssignor.Assignment consumerAssignment = assignments.Get("consumer1");
//            AssignmentInfo assignmentInfo = AssignmentInfo.decode(consumerAssignment.userData());
//            HashSet<TopicPartition> topicPartitions = assignmentInfo.partitionsByHost().Get(new HostInfo("localhost", 8080));
//            Assert.Equal(
//                Utils.mkSet(
//                    new TopicPartition("topic1", 0),
//                    new TopicPartition("topic1", 1),
//                    new TopicPartition("topic1", 2)),
//                topicPartitions);
//        }

//        [Fact]
//        public void ShouldThrowExceptionIfApplicationServerConfigIsNotHostPortPair()
//        {
//            builder.SetApplicationId(applicationId);

//            mockTaskManager(emptyTasks, emptyTasks, UUID.randomUUID(), builder);
//            partitionAssignor.setInternalTopicManager(new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer));

//            try
//            {
//                configurePartitionAssignor(Collections.singletonMap(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost"));
//                Assert.True(false, "expected to an exception due to invalid config");
//            }
//            catch (ConfigException e)
//            {
//                // pass
//            }
//        }

//        [Fact]
//        public void ShouldThrowExceptionIfApplicationServerConfigPortIsNotAnInteger()
//        {
//            builder.SetApplicationId(applicationId);

//            try
//            {
//                configurePartitionAssignor(Collections.singletonMap(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:j87yhk"));
//                Assert.True(false, "expected to an exception due to invalid config");
//            }
//            catch (ConfigException e)
//            {
//                // pass
//            }
//        }

//        [Fact]
//        public void ShouldNotLoopInfinitelyOnMissingMetadataAndShouldNotCreateRelatedTasks()
//        {
//            StreamsBuilder builder = new StreamsBuilder();

//            IKStream<string, string?> stream1 = builder

//                // Task 1 (should get created):
//                .Stream<string, string>("topic1")
//                // force repartitioning for aggregation
//                .SelectKey<string?>((key, value) => null)
//                .GroupByKey()

//                // Task 2 (should get created):
//                // Create repartioning and changelog topic as task 1 exists
//                .Count(Materialized.As<string?, long, IKeyValueStore<Bytes, byte[]>>("count"))

//                // force repartitioning for join, but second join input topic unknown
//                // => internal repartitioning topic should not get created
//                .ToStream()
//                .Map<string, string?>((key, value) => default);

//            builder
//                // Task 3 (should not get created because input topic unknown)
//                .Stream<string, string>("unknownTopic")

//                // force repartitioning for join, but input topic unknown
//                // => thus should not Create internal repartitioning topic
//                .SelectKey<string?>((key, value) => null)

//                // Task 4 (should not get created because input topics unknown)
//                // should not Create any of both input repartition topics or any of both changelog topics
//                .Join<string?, string?>(
//                    stream1,
//                    (value1, value2) => null,
//                    JoinWindows.Of(TimeSpan.FromMilliseconds(0))
//                );

//            var uuid = Guid.NewGuid();
//            string client = "client1";

//            InternalTopologyBuilder internalTopologyBuilder = TopologyWrapper.getInternalTopologyBuilder(builder.Build());
//            internalTopologyBuilder.SetApplicationId(applicationId);

//            mockTaskManager(
//                emptyTasks,
//                emptyTasks,
//                Guid.NewGuid(),
//                internalTopologyBuilder);
//            configurePartitionAssignor(Collections.emptyMap());

//            MockInternalTopicManager mockInternalTopicManager = new MockInternalTopicManager(
//                streamsConfig,
//                mockClientSupplier.RestoreConsumer);
//            partitionAssignor.setInternalTopicManager(mockInternalTopicManager);

//            subscriptions.Put(client,
//                    new ConsumerPartitionAssignor.Subscription(
//                            Collections.singletonList("unknownTopic"),
//                            new SubscriptionInfo(uuid, emptyTasks, emptyTasks, userEndPoint).encode())
//            );
//            Dictionary<string, ConsumerPartitionAssignor.Assignment> assignment = partitionAssignor.Assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

//            Assert.Equal(true, mockInternalTopicManager.readyTopics.IsEmpty());

//            Assert.Equal(true, assignment.Get(client).partitions().IsEmpty());
//        }

//        [Fact]
//        public void ShouldUpdateClusterMetadataAndHostInfoOnAssignment()
//        {
//            TopicPartition partitionOne = new TopicPartition("topic", 1);
//            TopicPartition partitionTwo = new TopicPartition("topic", 2);
//            Dictionary<HostInfo, HashSet<TopicPartition>> hostState = Collections.singletonMap(
//                    new HostInfo("localhost", 9090), Utils.mkSet(partitionOne, partitionTwo));

//            configurePartitionAssignor(Collections.emptyMap());

//            taskManager.setPartitionsByHostState(hostState);
//            EasyMock.expectLastCall();
//            EasyMock.replay(taskManager);

//            partitionAssignor.onAssignment(createAssignment(hostState), null);

//            EasyMock.verify(taskManager);
//        }

//        [Fact]
//        public void ShouldNotAddStandbyTaskPartitionsToPartitionsForHost()
//        {
//            StreamsBuilder builder = new StreamsBuilder();

//            builder.Stream("topic1").GroupByKey().Count();
//            InternalTopologyBuilder internalTopologyBuilder = TopologyWrapper.getInternalTopologyBuilder(builder.Build());
//            internalTopologyBuilder.SetApplicationId(applicationId);


//            UUID uuid = UUID.randomUUID();
//            MockTaskManager(
//                emptyTasks,
//                emptyTasks,
//                uuid,
//                internalTopologyBuilder);

//            Dictionary<string, object> props = new HashMap<>();
//            props.Put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
//            props.Put(StreamsConfig.APPLICATION_SERVER_CONFIG, userEndPoint);
//            ConfigurePartitionAssignor(props);
//            partitionAssignor.setInternalTopicManager(new MockInternalTopicManager(
//                streamsConfig,
//                mockClientSupplier.restoreConsumer));

//            subscriptions.Put("consumer1",
//                    new ConsumerPartitionAssignor.Subscription(
//                            Collections.singletonList("topic1"),
//                            new SubscriptionInfo(uuid, emptyTasks, emptyTasks, userEndPoint).encode())
//            );
//            subscriptions.Put("consumer2",
//                    new ConsumerPartitionAssignor.Subscription(
//                            Collections.singletonList("topic1"),
//                            new SubscriptionInfo(UUID.randomUUID(), emptyTasks, emptyTasks, "other:9090").encode())
//            );
//            HashSet<TopicPartition> allPartitions = Utils.mkSet(t1p0, t1p1, t1p2);
//            Dictionary<string, ConsumerPartitionAssignor.Assignment> assign = partitionAssignor.Assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();
//            ConsumerPartitionAssignor.Assignment consumer1Assignment = assign.Get("consumer1");
//            AssignmentInfo assignmentInfo = AssignmentInfo.decode(consumer1Assignment.userData());
//            HashSet<TopicPartition> consumer1partitions = assignmentInfo.partitionsByHost().Get(new HostInfo("localhost", 8080));
//            HashSet<TopicPartition> consumer2Partitions = assignmentInfo.partitionsByHost().Get(new HostInfo("other", 9090));
//            HashSet<TopicPartition> allAssignedPartitions = new HashSet<>(consumer1partitions);
//            allAssignedPartitions.addAll(consumer2Partitions);
//            Assert.Equal(consumer1partitions, not(allPartitions));
//            Assert.Equal(consumer2Partitions, not(allPartitions));
//            Assert.Equal(allAssignedPartitions, allPartitions);
//        }

//        [Fact]
//        public void ShouldThrowKafkaExceptionIfTaskMangerNotConfigured()
//        {
//            Dictionary<string, object> config = ConfigProps();
//            config.Remove(StreamsConfig.InternalConfig.TASK_MANAGER_FOR_PARTITION_ASSIGNOR);

//            try
//            {
//                partitionAssignor.configure(config);
//                Assert.True(false, "Should have thrown KafkaException");
//            }
//            catch (KafkaException expected)
//            {
//                Assert.Equal("TaskManager is not specified", expected.Message);
//            }
//        }

//        [Fact]
//        public void ShouldThrowKafkaExceptionIfTaskMangerConfigIsNotTaskManagerInstance()
//        {
//            Dictionary<string, object> config = ConfigProps();
//            config.Put(StreamsConfig.InternalConfig.TASK_MANAGER_FOR_PARTITION_ASSIGNOR, "i am not a task manager");

//            try
//            {
//                partitionAssignor.configure(config);
//                Assert.True(false, "Should have thrown KafkaException");
//            }
//            catch (KafkaException expected)
//            {
//                Assert.Equal(expected.Message,
//                    equalTo("java.lang.string is not an instance of org.apache.kafka.streams.processor.internals.TaskManager"));
//            }
//        }

//        [Fact]
//        public void ShouldThrowKafkaExceptionAssignmentErrorCodeNotConfigured()
//        {
//            Dictionary<string, object> config = ConfigProps();
//            config.remove(StreamsConfig.InternalConfig.ASSIGNMENT_ERROR_CODE);

//            try
//            {
//                partitionAssignor.configure(config);
//                Assert.True(false, "Should have thrown KafkaException");
//            }
//            catch (KafkaException expected)
//            {
//                Assert.Equal(expected.Message, "assignmentErrorCode is not specified");
//            }
//        }

//        [Fact]
//        public void ShouldThrowKafkaExceptionIfVersionProbingFlagConfigIsNotAtomicInteger()
//        {
//            Dictionary<string, object> config = ConfigProps();
//            config.Put(StreamsConfig.InternalConfig.ASSIGNMENT_ERROR_CODE, "i am not an int");

//            try
//            {
//                partitionAssignor.configure(config);
//                Assert.True(false, "Should have thrown KafkaException");
//            }
//            catch (KafkaException expected)
//            {
//                Assert.Equal(expected.Message,
//                    equalTo("java.lang.string is not an instance of java.util.concurrent.atomic.int"));
//            }
//        }

//        [Fact]
//        public void ShouldReturnLowestAssignmentVersionForDifferentSubscriptionVersionsV1V2()
//        {
//            ShouldReturnLowestAssignmentVersionForDifferentSubscriptionVersions(1, 2);
//        }

//        [Fact]
//        public void ShouldReturnLowestAssignmentVersionForDifferentSubscriptionVersionsV1V3()
//        {
//            ShouldReturnLowestAssignmentVersionForDifferentSubscriptionVersions(1, 3);
//        }

//        [Fact]
//        public void ShouldReturnLowestAssignmentVersionForDifferentSubscriptionVersionsV2V3()
//        {
//            ShouldReturnLowestAssignmentVersionForDifferentSubscriptionVersions(2, 3);
//        }

//        private void ShouldReturnLowestAssignmentVersionForDifferentSubscriptionVersions(int smallestVersion,
//                                                                                         int otherVersion)
//        {
//            subscriptions.Put("consumer1",
//                    new ConsumerPartitionAssignor.Subscription(
//                            Collections.singletonList("topic1"),
//                            new SubscriptionInfo(smallestVersion, UUID.randomUUID(), emptyTasks, emptyTasks, null).encode())
//            );
//            subscriptions.Put("consumer2",
//                    new ConsumerPartitionAssignor.Subscription(
//                            Collections.singletonList("topic1"),
//                            new SubscriptionInfo(otherVersion, UUID.randomUUID(), emptyTasks, emptyTasks, null).encode()
//                    )
//            );

//            mockTaskManager(
//                emptyTasks,
//                emptyTasks,
//                UUID.randomUUID(),
//                builder);
//            partitionAssignor.configure(ConfigProps());
//            Dictionary<string, ConsumerPartitionAssignor.Assignment> assignment = partitionAssignor.Assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

//            Assert.Equal(2, assignment.Count);
//            Assert.Equal(AssignmentInfo.decode(assignment.Get("consumer1").userData()).version(), smallestVersion);
//            Assert.Equal(AssignmentInfo.decode(assignment.Get("consumer2").userData()).version(), smallestVersion);
//        }

//        [Fact]
//        public void ShouldDownGradeSubscriptionToVersion1()
//        {
//            mockTaskManager(
//                emptyTasks,
//                emptyTasks,
//                UUID.randomUUID(),
//                builder);
//            configurePartitionAssignor(Collections.singletonMap(StreamsConfig.UPGRADE_FROM_CONFIG, StreamsConfig.UPGRADE_FROM_0100));

//            HashSet<string> topics = Utils.mkSet("topic1");
//            ConsumerPartitionAssignor.Subscription subscription = new ConsumerPartitionAssignor.Subscription(new List<>(topics), partitionAssignor.subscriptionUserData(topics));

//            Assert.Equal(SubscriptionInfo.decode(subscription.userData()).version(), 1);
//        }

//        [Fact]
//        public void ShouldDownGradeSubscriptionToVersion2For0101()
//        {
//            shouldDownGradeSubscriptionToVersion2(StreamsConfig.UPGRADE_FROM_0101);
//        }

//        [Fact]
//        public void ShouldDownGradeSubscriptionToVersion2For0102()
//        {
//            shouldDownGradeSubscriptionToVersion2(StreamsConfig.UPGRADE_FROM_0102);
//        }

//        [Fact]
//        public void ShouldDownGradeSubscriptionToVersion2For0110()
//        {
//            shouldDownGradeSubscriptionToVersion2(StreamsConfig.UPGRADE_FROM_0110);
//        }

//        [Fact]
//        public void ShouldDownGradeSubscriptionToVersion2For10()
//        {
//            shouldDownGradeSubscriptionToVersion2(StreamsConfig.UPGRADE_FROM_10);
//        }

//        [Fact]
//        public void ShouldDownGradeSubscriptionToVersion2For11()
//        {
//            shouldDownGradeSubscriptionToVersion2(StreamsConfig.UPGRADE_FROM_11);
//        }

//        private void ShouldDownGradeSubscriptionToVersion2(object upgradeFromValue)
//        {
//            mockTaskManager(
//                emptyTasks,
//                emptyTasks,
//                UUID.randomUUID(),
//                builder);
//            configurePartitionAssignor(Collections.singletonMap(StreamsConfig.UPGRADE_FROM_CONFIG, upgradeFromValue));

//            HashSet<string> topics = Utils.mkSet("topic1");
//            ConsumerPartitionAssignor.Subscription subscription = new ConsumerPartitionAssignor.Subscription(new List<>(topics), partitionAssignor.subscriptionUserData(topics));

//            Assert.Equal(SubscriptionInfo.decode(subscription.userData()).version(), 2);
//        }

//        [Fact]
//        public void ShouldReturnUnchangedAssignmentForOldInstancesAndEmptyAssignmentForFutureInstances()
//        {
//            builder.AddSource(null, "source1", null, null, null, "topic1");

//            HashSet<TaskId> allTasks = Utils.mkSet(task0, task1, task2);

//            HashSet<TaskId> ActiveTasks = Utils.mkSet(task0, task1);
//            HashSet<TaskId> StandbyTasks = Utils.mkSet(task2);
//            Dictionary<TaskId, HashSet<TopicPartition>> standbyTaskMap = new HashMap<TaskId, HashSet<TopicPartition>>();
//            //            {
//            //                Put(task2, Collections.singleton(t1p2));
//            //        }
//            //    };

//            subscriptions.Put("consumer1",
//                        new ConsumerPartitionAssignor.Subscription(
//                                Collections.singletonList("topic1"),
//                                new SubscriptionInfo(UUID.randomUUID(), ActiveTasks, StandbyTasks, null).encode())
//                );
//            subscriptions.Put("future-consumer",
//                    new ConsumerPartitionAssignor.Subscription(
//                            Collections.singletonList("topic1"),
//                            encodeFutureSubscription())
//            );

//            mockTaskManager(
//                allTasks,
//                allTasks,
//                UUID.randomUUID(),
//                builder);
//            partitionAssignor.configure(configProps());
//            Dictionary<string, ConsumerPartitionAssignor.Assignment> assignment = partitionAssignor.Assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

//            Assert.Equal(assignment.Count, (2));
//            Assert.Equal(
//                AssignmentInfo.decode(assignment.Get("consumer1").userData()),
//                equalTo(new AssignmentInfo(
//                    new List<>(ActiveTasks),
//                    standbyTaskMap,
//                    Collections.emptyMap()
//                )));
//            Assert.Equal(assignment.Get("consumer1").partitions(), (asList(t1p0, t1p1)));

//            Assert.Equal(AssignmentInfo.decode(assignment.Get("future-consumer").userData()), (new AssignmentInfo()));
//            Assert.Equal(assignment.Get("future-consumer").partitions().Count, (0));
//        }

//        [Fact]
//        public void ShouldThrowIfV1SubscriptionAndFutureSubscriptionIsMixed()
//        {
//            shouldThrowIfPreVersionProbingSubscriptionAndFutureSubscriptionIsMixed(1);
//        }

//        [Fact]
//        public void ShouldThrowIfV2SubscriptionAndFutureSubscriptionIsMixed()
//        {
//            shouldThrowIfPreVersionProbingSubscriptionAndFutureSubscriptionIsMixed(2);
//        }

//        private ByteBuffer EncodeFutureSubscription()
//        {
//            ByteBuffer buf = new ByteBuffer().Allocate(4 /* used version */
//                                                       + 4 /* supported version */);
//            buf.putInt(SubscriptionInfo.LATEST_SUPPORTED_VERSION + 1);
//            buf.putInt(SubscriptionInfo.LATEST_SUPPORTED_VERSION + 1);
//            return buf;
//        }

//        private void ShouldThrowIfPreVersionProbingSubscriptionAndFutureSubscriptionIsMixed(int oldVersion)
//        {
//            subscriptions.Put("consumer1",
//                    new ConsumerPartitionAssignor.Subscription(
//                            Collections.singletonList("topic1"),
//                            new SubscriptionInfo(oldVersion, UUID.randomUUID(), emptyTasks, emptyTasks, null).encode())
//            );
//            subscriptions.Put("future-consumer",
//                    new ConsumerPartitionAssignor.Subscription(
//                            Collections.singletonList("topic1"),
//                            encodeFutureSubscription())
//            );

//            mockTaskManager(
//                emptyTasks,
//                emptyTasks,
//                UUID.randomUUID(),
//                builder);
//            partitionAssignor.configure(configProps());

//            try
//            {
//                partitionAssignor.Assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();
//                Assert.True(false, "Should have thrown IllegalStateException");
//            }
//            catch (IllegalStateException expected)
//            {
//                // pass
//            }
//        }

//        private ConsumerPartitionAssignor.Assignment CreateAssignment(Dictionary<HostInfo, HashSet<TopicPartition>> firstHostState)
//        {
//            AssignmentInfo info = new AssignmentInfo(Collections.emptyList(),
//                                                           Collections.emptyMap(),
//                                                           firstHostState);

//            return new ConsumerPartitionAssignor.Assignment(
//                    Collections.emptyList(), info.encode());
//        }

//        private AssignmentInfo CheckAssignment(HashSet<string> expectedTopics,
//                                               ConsumerPartitionAssignor.Assignment assignment)
//        {

//            // This assumed 1) DefaultPartitionGrouper is used, and 2) there is an only one topic group.

//            AssignmentInfo info = AssignmentInfo.decode(assignment.userData());

//            // check if the number of assigned partitions == the size of active task id list
//            Assert.Equal(assignment.partitions().Count, info.ActiveTasks().Count);

//            // check if active tasks are consistent
//            List<TaskId> ActiveTasks = new List<TaskId>();
//            HashSet<string> activeTopics = new HashSet<>();
//            foreach (TopicPartition partition in assignment.partitions())
//            {
//                // since default grouper, taskid.partition == partition.Partition
//                ActiveTasks.Add(new TaskId(0, partition.Partition));
//                activeTopics.Add(partition.Topic);
//            }
//            Assert.Equal(ActiveTasks, info.ActiveTasks());

//            // check if active partitions cover All topics
//            Assert.Equal(expectedTopics, activeTopics);

//            // check if standby tasks are consistent
//            HashSet<string> standbyTopics = new HashSet<>();
//            foreach (Map.Entry<TaskId, HashSet<TopicPartition>> entry in info.StandbyTasks())
//            {
//                TaskId id = entry.Key;
//                HashSet<TopicPartition> partitions = entry.Value;
//                foreach (TopicPartition partition in partitions)
//                {
//                    // since default grouper, taskid.partition == partition.Partition
//                    Assert.Equal(id.partition, partition.Partition);

//                    standbyTopics.Add(partition.Topic);
//                }
//            }

//            if (info.StandbyTasks().Count > 0)
//            {
//                // check if standby partitions cover All topics
//                Assert.Equal(expectedTopics, standbyTopics);
//            }

//            return info;
//        }
//    }
//}
