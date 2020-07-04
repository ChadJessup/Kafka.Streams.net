//using Kafka.Streams.Configs;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.Processors;
//using Kafka.Streams.State;
//using Kafka.Streams.State.KeyValues;
//using Kafka.Streams.Tests.Helpers;
//using Kafka.Streams.Threads.KafkaStreams;
//using Kafka.Streams.Topologies;
//using System;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.Integration
//{
//    public class StandbyTaskCreationIntegrationTest
//    {

//        private const int NUM_BROKERS = 1;


//        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

//        private const string INPUT_TOPIC = "input-topic";

//        private IKafkaStreamsThread client1;
//        private IKafkaStreamsThread client2;
//        private readonly bool client1IsOk = false;
//        private readonly bool client2IsOk = false;


//        public static void CreateTopics()
//        {// throws InterruptedException
//            CLUSTER.CreateTopic(INPUT_TOPIC, 2, 1);
//        }


//        public void After()
//        {
//            client1.Close();
//            client2.Close();
//        }

//        private StreamsConfig StreamsConfiguration()
//        {
//            string applicationId = "testApp";
//            StreamsConfig streamsConfiguration = new StreamsConfig();
//            streamsConfiguration.Put(StreamsConfig.ApplicationIdConfig, applicationId);
//            streamsConfiguration.Put(StreamsConfig.BootstrapServersConfig, CLUSTER.bootstrapServers());
//            streamsConfiguration.Put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory(applicationId).getPath());
//            streamsConfiguration.Put(StreamsConfig.DefaultKeySerdeClassConfig, Serdes.Int().GetType());
//            streamsConfiguration.Put(StreamsConfig.DefaultValueSerdeClassConfig, Serdes.Int().GetType());
//            streamsConfiguration.Put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
//            return streamsConfiguration;
//        }

//        [Fact]
//        public void ShouldNotCreateAnyStandByTasksForStateStoreWithLoggingDisabled()
//        {// throws Exception
//            StreamsBuilder builder = new StreamsBuilder();
//            string stateStoreName = "myTransformState";
//            IStoreBuilder<IKeyValueStore<int, int>> KeyValueStoreBuilder =
//                Stores.KeyValueStoreBuilder(Stores.PersistentKeyValueStore(stateStoreName),
//                                            Serdes.Int(),
//                                            Serdes.Int()).WithLoggingDisabled();
//            builder.AddStateStore(KeyValueStoreBuilder);
//            builder.Stream(INPUT_TOPIC, Consumed.With(Serdes.Int(), Serdes.Int()))
//                .Transform(() => new Transformer<int, int, KeyValuePair<int, int>>());
//            //        {
//            //
//            //
//            //
//            //
//            //        public void Init(IProcessorContext context) { }
//            //
//            //
//            //    public KeyValuePair<int, int> transform(int key, int value)
//            //    {
//            //        return null;
//            //    }
//            //
//            //
//            //    //public void Close() { }
//            //}, stateStoreName);

//            Topology topology = builder.Build();
//            createClients(topology, streamsConfiguration(), topology, streamsConfiguration());

//            setStateListenersForVerification(thread => thread.StandbyTasks().IsEmpty() && !thread.ActiveTasks().IsEmpty());

//            startClients();

//            waitUntilBothClientAreOK(
//                "At least one client did not reach state RUNNING with active tasks but no stand-by tasks"
//            );
//        }

//        [Fact]
//        public void ShouldCreateStandByTasksForMaterializedAndOptimizedSourceTables()
//        {// throws Exception
//            StreamsConfig streamsConfiguration1 = streamsConfiguration();
//            streamsConfiguration1.Put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
//            StreamsConfig streamsConfiguration2 = streamsConfiguration();
//            streamsConfiguration2.Put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);

//            StreamsBuilder builder = new StreamsBuilder();
//            builder.table(INPUT_TOPIC, Consumed.With(Serdes.Int(), Serdes.Int()), Materialized.As("source-table"));

//            createClients(
//                builder.Build(streamsConfiguration1),
//                streamsConfiguration1,
//                builder.Build(streamsConfiguration2),
//                streamsConfiguration2
//            );

//            setStateListenersForVerification(thread => !thread.StandbyTasks().IsEmpty() && !thread.ActiveTasks().IsEmpty());

//            startClients();

//            waitUntilBothClientAreOK(
//                "At least one client did not reach state RUNNING with active tasks and stand-by tasks"
//            );
//        }

//        private void CreateClients(
//            Topology topology1,
//            StreamsConfig streamsConfiguration1,
//            Topology topology2,
//            StreamsConfig streamsConfiguration2)
//        {

//            client1 = new KafkaStreamsThread(topology1, streamsConfiguration1);
//            client2 = new KafkaStreamsThread(topology2, streamsConfiguration2);
//        }

//        private void SetStateListenersForVerification(Predicate<ThreadMetadata> taskCondition)
//        {
//            client1.SetStateListener((newState, oldState) =>
//            {
//                if (newState == State.RUNNING &&
//                    client1.localThreadsMetadata().Stream().allMatch(taskCondition))
//                {

//                    client1IsOk = true;
//                }
//            });
//            client2.SetStateListener((newState, oldState) =>
//            {
//                if (newState == State.RUNNING &&
//                    client2.localThreadsMetadata().Stream().allMatch(taskCondition))
//                {

//                    client2IsOk = true;
//                }
//            });
//        }

//        private void StartClients()
//        {
//            client1.Start();
//            client2.Start();
//        }

//        private void WaitUntilBothClientAreOK(string message)
//        {// throws Exception
//            TestUtils.WaitForCondition(
//                () => client1IsOk && client2IsOk,
//                30 * 1000,
//                message + ": "
//                    + "Client 1 is " + (!client1IsOk ? "NOT " : "") + "OK, "
//                    + "client 2 is " + (!client2IsOk ? "NOT " : "") + "OK."
//            );
//        }
//    }
//}
