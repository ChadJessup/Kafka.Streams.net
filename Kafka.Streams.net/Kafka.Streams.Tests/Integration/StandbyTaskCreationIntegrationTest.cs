/*






 *

 *





 */






























public class StandbyTaskCreationIntegrationTest {

    private static int NUM_BROKERS = 1;

    
    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    private static string INPUT_TOPIC = "input-topic";

    private KafkaStreams client1;
    private KafkaStreams client2;
    private volatile bool client1IsOk = false;
    private volatile bool client2IsOk = false;

    
    public static void createTopics() {// throws InterruptedException
        CLUSTER.createTopic(INPUT_TOPIC, 2, 1);
    }

    
    public void after() {
        client1.close();
        client2.close();
    }

    private Properties streamsConfiguration() {
        string applicationId = "testApp";
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(applicationId).getPath());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Int().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Int().getClass());
        streamsConfiguration.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        return streamsConfiguration;
    }

    [Xunit.Fact]
    public void shouldNotCreateAnyStandByTasksForStateStoreWithLoggingDisabled() {// throws Exception
        StreamsBuilder builder = new StreamsBuilder();
        string stateStoreName = "myTransformState";
        StoreBuilder<KeyValueStore<int, int>> keyValueStoreBuilder =
            Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName),
                                        Serdes.Int(),
                                        Serdes.Int()).withLoggingDisabled();
        builder.addStateStore(keyValueStoreBuilder);
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.Int(), Serdes.Int()))
            .transform(() => new Transformer<int, int, KeyValuePair<int, int>>() {
                
                
                public void init(ProcessorContext context) {}

                
                public KeyValuePair<int, int> transform(int key, int value) {
                    return null;
                }

                
                public void close() {}
            }, stateStoreName);

        Topology topology = builder.build();
        createClients(topology, streamsConfiguration(), topology, streamsConfiguration());

        setStateListenersForVerification(thread => thread.standbyTasks().isEmpty() && !thread.activeTasks().isEmpty());

        startClients();

        waitUntilBothClientAreOK(
            "At least one client did not reach state RUNNING with active tasks but no stand-by tasks"
        );
    }

    [Xunit.Fact]
    public void shouldCreateStandByTasksForMaterializedAndOptimizedSourceTables() {// throws Exception
        Properties streamsConfiguration1 = streamsConfiguration();
        streamsConfiguration1.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
        Properties streamsConfiguration2 = streamsConfiguration();
        streamsConfiguration2.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);

        StreamsBuilder builder = new StreamsBuilder();
        builder.table(INPUT_TOPIC, Consumed.with(Serdes.Int(), Serdes.Int()), Materialized.As("source-table"));

        createClients(
            builder.build(streamsConfiguration1),
            streamsConfiguration1,
            builder.build(streamsConfiguration2),
            streamsConfiguration2
        );

        setStateListenersForVerification(thread => !thread.standbyTasks().isEmpty() && !thread.activeTasks().isEmpty());

        startClients();

        waitUntilBothClientAreOK(
            "At least one client did not reach state RUNNING with active tasks and stand-by tasks"
        );
    }

    private void createClients(Topology topology1,
                               Properties streamsConfiguration1,
                               Topology topology2,
                               Properties streamsConfiguration2) {

        client1 = new KafkaStreams(topology1, streamsConfiguration1);
        client2 = new KafkaStreams(topology2, streamsConfiguration2);
    }

    private void setStateListenersForVerification(Predicate<ThreadMetadata> taskCondition) {
        client1.setStateListener((newState, oldState) => {
            if (newState == State.RUNNING &&
                client1.localThreadsMetadata().stream().allMatch(taskCondition)) {

                client1IsOk = true;
            }
        });
        client2.setStateListener((newState, oldState) => {
            if (newState == State.RUNNING &&
                client2.localThreadsMetadata().stream().allMatch(taskCondition)) {

                client2IsOk = true;
            }
        });
    }

    private void startClients() {
        client1.start();
        client2.start();
    }

    private void waitUntilBothClientAreOK(string message) {// throws Exception
        TestUtils.waitForCondition(
            () => client1IsOk && client2IsOk,
            30 * 1000,
            message + ": "
                + "Client 1 is " + (!client1IsOk ? "NOT " : "") + "OK, "
                + "client 2 is " + (!client2IsOk ? "NOT " : "") + "OK."
        );
    }
}
