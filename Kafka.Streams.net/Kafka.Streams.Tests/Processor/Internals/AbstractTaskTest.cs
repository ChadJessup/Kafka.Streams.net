using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.State;
using Kafka.Streams.Tasks;
using Kafka.Streams.Temporary;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Processor.Internals.Assignment;
using Moq;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class AbstractTaskTest
    {
        private TaskId id = new TaskId(0, 0);
        private StateDirectory stateDirectory = Mock.Of<StateDirectory>();
        private TopicPartition storeTopicPartition1 = new TopicPartition("t1", 0);
        private TopicPartition storeTopicPartition2 = new TopicPartition("t2", 0);
        private TopicPartition storeTopicPartition3 = new TopicPartition("t3", 0);
        private TopicPartition storeTopicPartition4 = new TopicPartition("t4", 0);
        private Collection<TopicPartition> storeTopicPartitions =
            Utils.mkSet(storeTopicPartition1, storeTopicPartition2, storeTopicPartition3, storeTopicPartition4);


        public void Before()
        {
            expect(stateDirectory.DirectoryForTask(id)).andReturn(TestUtils.GetTempDirectory());
        }

        [Fact]// (expected = ProcessorStateException)
        public void ShouldThrowProcessorStateExceptionOnInitializeOffsetsWhenAuthorizationException()
        {
            Consumer consumer = mockConsumer(new AuthorizationException("blah"));
            AbstractTask task = createTask(consumer, Collections.emptyMap<IStateStore, string>());
            task.updateOffsetLimits();
        }

        [Fact]// (expected = ProcessorStateException)
        public void ShouldThrowProcessorStateExceptionOnInitializeOffsetsWhenKafkaException()
        {
            Consumer consumer = mockConsumer(new KafkaException("blah"));
            AbstractTask task = createTask(consumer, Collections.emptyMap<IStateStore, string>());
            task.updateOffsetLimits();
        }

        [Fact]// (expected = WakeupException)
        public void ShouldThrowWakeupExceptionOnInitializeOffsetsWhenWakeupException()
        {
            Consumer consumer = mockConsumer(new WakeupException());
            AbstractTask task = createTask(consumer, Collections.emptyMap<IStateStore, string>());
            task.updateOffsetLimits();
        }

        [Fact]
        public void ShouldThrowLockExceptionIfFailedToLockStateDirectoryWhenTopologyHasStores()
        { //throws IOException
            Consumer consumer = Mock.Of<Consumer>();
            IStateStore store = Mock.Of<IStateStore>();
            expect(store.Name).andReturn("dummy-store-Name").anyTimes();
            EasyMock.replay(store);
            expect(stateDirectory.Lock(id)).andReturn(false);
            EasyMock.replay(stateDirectory);

            AbstractTask task = createTask(consumer, Collections.singletonMap(store, "dummy"));

            try
            {
                task.registerStateStores();
                Assert.True(false, "Should have thrown LockException");
            }
            catch (LockException e)
            {
                // ok
            }

        }

        [Fact]
        public void ShouldNotAttemptToLockIfNoStores()
        {
            var consumer = Mock.Of<Consumer>();
            EasyMock.replay(stateDirectory);

            AbstractTask task = createTask(consumer, Collections.emptyMap<IStateStore, string>());

            task.registerStateStores();

            // should fail if lock is called
            EasyMock.verify(stateDirectory);
        }

        [Fact]
        public void ShouldDeleteAndRecreateStoreDirectoryOnReinitialize()
        { //throws IOException
            StreamsConfig streamsConfig = new StreamsConfig
            {
                ApplicationId = "app-id",
                BootstrapServers = "localhost:9092",
                StateStoreDirectory = TestUtils.GetTempDirectory(),
            };

            var consumer = Mock.Of<Consumer>();

            IStateStore store1 = Mock.Of<IStateStore>();
            IStateStore store2 = Mock.Of<IStateStore>();
            IStateStore store3 = Mock.Of<IStateStore>();
            IStateStore store4 = Mock.Of<IStateStore>();
            string storeName1 = "storeName1";
            string storeName2 = "storeName2";
            string storeName3 = "storeName3";
            string storeName4 = "storeName4";

            expect(store1.Name).andReturn(storeName1).anyTimes();
            EasyMock.replay(store1);
            expect(store2.Name).andReturn(storeName2).anyTimes();
            EasyMock.replay(store2);
            expect(store3.Name).andReturn(storeName3).anyTimes();
            EasyMock.replay(store3);
            expect(store4.Name).andReturn(storeName4).anyTimes();
            EasyMock.replay(store4);

            StateDirectory stateDirectory = new StateDirectory(streamsConfig, new MockTime(), true);
            AbstractTask task = createTask(
                consumer,
            //        new Dictionary<IStateStore, string>() {
            //        {
            //            Put(store1, storeTopicPartition1.Topic);
            //    Put(store2, storeTopicPartition2.Topic);
            //    Put(store3, storeTopicPartition3.Topic);
            //    Put(store4, storeTopicPartition4.Topic);
            //}
            //}
            //,
            stateDirectory);

            string taskDir = stateDirectory.DirectoryForTask(task.id).FullName;
            FileInfo storeDirectory1 = new FileInfo(taskDir
                + Path.DirectorySeparatorChar + "rocksdb"
                + Path.DirectorySeparatorChar + storeName1);
            FileInfo storeDirectory2 = new FileInfo(taskDir
                + Path.DirectorySeparatorChar + "rocksdb"
                + Path.DirectorySeparatorChar + storeName2);
            FileInfo storeDirectory3 = new FileInfo(taskDir
                + Path.DirectorySeparatorChar + storeName3);
            FileInfo storeDirectory4 = new FileInfo(taskDir
                + Path.DirectorySeparatorChar + storeName4);
            FileInfo testFile1 = new FileInfo(storeDirectory1.FullName + Path.DirectorySeparatorChar + "testFile");
            FileInfo testFile2 = new FileInfo(storeDirectory2.FullName + Path.DirectorySeparatorChar + "testFile");
            FileInfo testFile3 = new FileInfo(storeDirectory3.FullName + Path.DirectorySeparatorChar + "testFile");
            FileInfo testFile4 = new FileInfo(storeDirectory4.FullName + Path.DirectorySeparatorChar + "testFile");

            storeDirectory1.mkdirs();
            storeDirectory2.mkdirs();
            storeDirectory3.mkdirs();
            storeDirectory4.mkdirs();

            testFile1.createNewFile();
            Assert.True(testFile1.Exists);
            testFile2.createNewFile();
            Assert.True(testFile2.Exists);
            testFile3.createNewFile();
            Assert.True(testFile3.Exists);
            testFile4.createNewFile();
            Assert.True(testFile4.Exists);

            task.processorContext = new InternalMockProcessorContext(stateDirectory.DirectoryForTask(task.id), streamsConfig);

            task.StateMgr.Register(store1, new MockRestoreCallback());
            task.StateMgr.Register(store2, new MockRestoreCallback());
            task.StateMgr.Register(store3, new MockRestoreCallback());
            task.StateMgr.Register(store4, new MockRestoreCallback());

            // only reinitialize store1 and store3 -- store2 and store4 should be untouched
            task.reinitializeStateStoresForPartitions(Utils.mkSet(storeTopicPartition1, storeTopicPartition3));

            Assert.False(testFile1.Exists);
            Assert.True(testFile2.Exists);
            Assert.False(testFile3.Exists);
            Assert.True(testFile4.Exists);
        }

        private AbstractTask CreateTask(Consumer consumer, Dictionary<IStateStore, string> stateStoresToChangelogTopics)
        {
            return createTask(consumer, stateStoresToChangelogTopics, stateDirectory);
        }


        private AbstractTask CreateTask(
            Consumer consumer,
            Dictionary<IStateStore, string> stateStoresToChangelogTopics,
            StateDirectory stateDirectory)
        {
            StreamsConfig properties = new StreamsConfig();
            properties.Set(StreamsConfig.ApplicationIdConfig, "app");
            properties.Put(StreamsConfig.BootstrapServersConfig, "dummyhost:9092");
            StreamsConfig config = new StreamsConfig(properties);

            var storeNamesToChangelogTopics = new Dictionary<string, string>(stateStoresToChangelogTopics.Count);
            foreach (var e in stateStoresToChangelogTopics)
            {
                storeNamesToChangelogTopics.Put(e.Key.Name, e.Value);
            }

            return null;
            //new AbstractTask(
            //    id,
            //                        storeTopicPartitions,
            //                        withLocalStores(new List<>(stateStoresToChangelogTopics.keySet()),
            //                                        storeNamesToChangelogTopics),
            //                        consumer,
            //                        new StoreChangelogReader(consumer,
            //                                                 TimeSpan.TimeSpan.Zero,
            //                                                 new MockStateRestoreListener(),
            //                                                 new LogContext("stream-task-test ")),
            //                        false,
            //                        stateDirectory,
            //                        config)
            //{
            //
            //
            //
            //        public void resume() { }
            //
            //
            //public void commit() { }
            //
            //
            //public void suspend() { }
            //
            //
            //public void Close(bool clean, bool isZombie) { }
            //
            //
            //public void closeSuspended(bool clean, bool isZombie, RuntimeException e) { }
            //
            //
            //public bool initializeStateStores()
            //{
            //    return false;
            //}
            //
            //
            //public void initializeTopology() { }
            //};  //
            //}

            //    private Consumer MockConsumer(RuntimeException toThrow)
            //{
            //    return new MockConsumer(OffsetResetStrategy.EARLIEST)
            //    {
            //
            //
            //            public OffsetAndMetadata committed(TopicPartition partition)
            //    {
            //        throw toThrow;
            //    }
            //};
            //    }
        }
    }
}
