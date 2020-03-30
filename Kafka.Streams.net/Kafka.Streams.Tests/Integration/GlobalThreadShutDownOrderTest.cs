/*






 *

 *





 */







































public class GlobalThreadShutDownOrderTest
{

    private static readonly int NUM_BROKERS = 1;
    private static Properties BROKER_CONFIG;

    //static {
    //    BROKER_CONFIG = new Properties();
    //BROKER_CONFIG.put("transaction.state.log.replication.factor", (short) 1);
    //    BROKER_CONFIG.put("transaction.state.log.min.isr", 1);
    //}


public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS, BROKER_CONFIG);

private MockTime mockTime = CLUSTER.time;
private string globalStore = "globalStore";
private StreamsBuilder builder;
private Properties streamsConfiguration;
private KafkaStreams kafkaStreams;
private string globalStoreTopic;
private string streamTopic;
private List<long> retrievedValuesList = new ArrayList<>();
private bool firstRecordProcessed;


public void Before()
{// throws Exception
    builder = new StreamsBuilder();
    createTopics();
    streamsConfiguration = new Properties();
    string applicationId = "global-thread-shutdown-test";
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

    Consumed<string, long> stringLongConsumed = Consumed.with(Serdes.String(), Serdes.Long());

    KeyValueStoreBuilder<string, long> storeBuilder = new KeyValueStoreBuilder<>(
        Stores.persistentKeyValueStore(globalStore),
        Serdes.String(),
        Serdes.Long(),
        mockTime);

    builder.addGlobalStore(
        storeBuilder,
        globalStoreTopic,
        Consumed.with(Serdes.String(), Serdes.Long()),
        new MockProcessorSupplier());

    builder
        .stream(streamTopic, stringLongConsumed)
        .process(() => new GlobalStoreProcessor(globalStore));

}


public void WhenShuttingDown()
{// throws Exception
    if (kafkaStreams != null)
    {
        kafkaStreams.close();
    }
    IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
}


[Xunit.Fact]
public void ShouldFinishGlobalStoreOperationOnShutDown()
{// throws Exception
    kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
    populateTopics(globalStoreTopic);
    populateTopics(streamTopic);

    kafkaStreams.start();

    TestUtils.waitForCondition(
        () => firstRecordProcessed,
        30000,
        "Has not processed record within 30 seconds");

    kafkaStreams.close(Duration.ofSeconds(30));

    List<long> expectedRetrievedValues = Array.asList(1L, 2L, 3L, 4L);
    Assert.Equal(expectedRetrievedValues, retrievedValuesList);
}


private void CreateTopics()
{// throws Exception
    streamTopic = "stream-topic";
    globalStoreTopic = "global-store-topic";
    CLUSTER.createTopics(streamTopic);
    CLUSTER.createTopic(globalStoreTopic);
}


private void PopulateTopics(string topicName)
{// throws Exception
    IntegrationTestUtils.produceKeyValuesSynchronously(
        topicName,
        Array.asList(
            new KeyValuePair<>("A", 1L),
            new KeyValuePair<>("B", 2L),
            new KeyValuePair<>("C", 3L),
            new KeyValuePair<>("D", 4L)),
        TestUtils.producerConfig(
            CLUSTER.bootstrapServers(),
            StringSerializer,
            LongSerializer,
            new Properties()),
        mockTime);
}


private class GlobalStoreProcessor : AbstractProcessor<string, long>
{

    private KeyValueStore<string, long> store;
    private readonly string storeName;

    GlobalStoreProcessor(string storeName)
    {
        this.storeName = storeName;
    }



    public void Init(ProcessorContext context)
    {
        base.init(context);
        store = (KeyValueStore<string, long>)context.getStateStore(storeName);
    }


    public void Process(string key, long value)
    {
        firstRecordProcessed = true;
    }



    public void Close()
    {
        List<string> keys = Array.asList("A", "B", "C", "D");
        foreach (string key in keys)
        {
            // need to simulate thread slow in closing
            Utils.sleep(1000);
            retrievedValuesList.add(store.get(key));
        }
    }
}

}
