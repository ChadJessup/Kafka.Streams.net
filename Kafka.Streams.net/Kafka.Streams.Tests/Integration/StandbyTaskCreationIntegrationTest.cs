//namespace Kafka.Streams.Tests.Integration
//{
//    /*






//    *

//    *





//    */






























//    public class StandbyTaskCreationIntegrationTest
//    {

//        private const int NUM_BROKERS = 1;


//        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

//        private const string INPUT_TOPIC = "input-topic";

//        private KafkaStreams client1;
//        private KafkaStreams client2;
//        private readonly bool client1IsOk = false;
//        private readonly bool client2IsOk = false;


//        public static void CreateTopics()
//        {// throws InterruptedException
//            CLUSTER.createTopic(INPUT_TOPIC, 2, 1);
//        }


//        public void After()
//        {
//            client1.close();
//            client2.close();
//        }

//        private StreamsConfig StreamsConfiguration()
//        {
//            string applicationId = "testApp";
//            StreamsConfig streamsConfiguration = new StreamsConfig();
//            streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
//            streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
//            streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory(applicationId).getPath());
//            streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Int().getClass());
//            streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Int().getClass());
//            streamsConfiguration.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
//            return streamsConfiguration;
//        }

//        [Xunit.Fact]
//        public void ShouldNotCreateAnyStandByTasksForStateStoreWithLoggingDisabled()
//        {// throws Exception
//            StreamsBuilder builder = new StreamsBuilder();
//            string stateStoreName = "myTransformState";
//            IStoreBuilder<IKeyValueStore<int, int>> KeyValueStoreBuilder =
//                Stores.KeyValueStoreBuilder(Stores.PersistentKeyValueStore(stateStoreName),
//                                            Serdes.Int(),
//                                            Serdes.Int()).withLoggingDisabled();
//            builder.addStateStore(KeyValueStoreBuilder);
//            builder.Stream(INPUT_TOPIC, Consumed.With(Serdes.Int(), Serdes.Int()))
//                .transform(() => new Transformer<int, int, KeyValuePair<int, int>>()
//                {
    


//                public void init(ProcessorContext context) { }


//            public KeyValuePair<int, int> transform(int key, int value)
//            {
//                return null;
//            }


//            public void close() { }
//        }, stateStoreName);

//        Topology topology = builder.Build();
//        createClients(topology, streamsConfiguration(), topology, streamsConfiguration());

//        setStateListenersForVerification(thread => thread.standbyTasks().isEmpty() && !thread.activeTasks().isEmpty());

//        startClients();

//        waitUntilBothClientAreOK(
//            "At least one client did not reach state RUNNING with active tasks but no stand-by tasks"
//        );
//    }

//    [Xunit.Fact]
//    public void ShouldCreateStandByTasksForMaterializedAndOptimizedSourceTables()
//    {// throws Exception
//        StreamsConfig streamsConfiguration1 = streamsConfiguration();
//        streamsConfiguration1.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
//        StreamsConfig streamsConfiguration2 = streamsConfiguration();
//        streamsConfiguration2.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);

//        StreamsBuilder builder = new StreamsBuilder();
//        builder.table(INPUT_TOPIC, Consumed.With(Serdes.Int(), Serdes.Int()), Materialized.As("source-table"));

//        createClients(
//            builder.Build(streamsConfiguration1),
//            streamsConfiguration1,
//            builder.Build(streamsConfiguration2),
//            streamsConfiguration2
//        );

//        setStateListenersForVerification(thread => !thread.standbyTasks().isEmpty() && !thread.activeTasks().isEmpty());

//        startClients();

//        waitUntilBothClientAreOK(
//            "At least one client did not reach state RUNNING with active tasks and stand-by tasks"
//        );
//    }

//    private void CreateClients(Topology topology1,
//                               StreamsConfig streamsConfiguration1,
//                               Topology topology2,
//                               StreamsConfig streamsConfiguration2)
//    {

//        client1 = new KafkaStreams(topology1, streamsConfiguration1);
//        client2 = new KafkaStreams(topology2, streamsConfiguration2);
//    }

//    private void SetStateListenersForVerification(Predicate<ThreadMetadata> taskCondition)
//    {
//        client1.setStateListener((newState, oldState) =>
//        {
//            if (newState == State.RUNNING &&
//                client1.localThreadsMetadata().Stream().allMatch(taskCondition))
//            {

//                client1IsOk = true;
//            }
//        });
//        client2.setStateListener((newState, oldState) =>
//        {
//            if (newState == State.RUNNING &&
//                client2.localThreadsMetadata().Stream().allMatch(taskCondition))
//            {

//                client2IsOk = true;
//            }
//        });
//    }

//    private void StartClients()
//    {
//        client1.start();
//        client2.start();
//    }

//    private void WaitUntilBothClientAreOK(string message)
//    {// throws Exception
//        TestUtils.WaitForCondition(
//            () => client1IsOk && client2IsOk,
//            30 * 1000,
//            message + ": "
//                + "Client 1 is " + (!client1IsOk ? "NOT " : "") + "OK, "
//                + "client 2 is " + (!client2IsOk ? "NOT " : "") + "OK."
//        );
//    }
//}
//}
