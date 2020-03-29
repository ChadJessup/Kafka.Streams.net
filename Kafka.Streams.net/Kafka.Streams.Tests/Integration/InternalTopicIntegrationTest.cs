/*






 *

 *





 */

















































/**
 * Tests related to internal topics in streams
 */

public class InternalTopicIntegrationTest {
    
    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    private static string APP_ID = "internal-topics-integration-test";
    private static string DEFAULT_INPUT_TOPIC = "inputTopic";

    private MockTime mockTime = CLUSTER.time;

    private Properties streamsProp;

    
    public static void startKafkaCluster() {// throws InterruptedException
        CLUSTER.createTopics(DEFAULT_INPUT_TOPIC);
    }

    
    public void before() {
        streamsProp = new Properties();
        streamsProp.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsProp.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProp.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProp.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsProp.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsProp.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsProp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    
    public void after(){ //throws IOException
        // Remove any state from previous test runs
        IntegrationTestUtils.purgeLocalStreamsState(streamsProp);
    }

    private void produceData(List<string> inputValues) {// throws Exception
        Properties producerProp = new Properties();
        producerProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerProp.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProp.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer);
        producerProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer);

        IntegrationTestUtils.produceValuesSynchronously(DEFAULT_INPUT_TOPIC, inputValues, producerProp, mockTime);
    }

    private Properties getTopicProperties(string changelog) {
        try { 
 (Admin adminClient = createAdminClient());
            ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, changelog);
            try {
                Config config = adminClient.describeConfigs(Collections.singletonList(configResource)).values().get(configResource).get();
                Properties properties = new Properties();
                foreach (ConfigEntry configEntry in config.entries()) {
                    if (configEntry.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG) {
                        properties.put(configEntry.name(), configEntry.Value);
                    }
                }
                return properties;
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Admin createAdminClient() {
        Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        return Admin.create(adminClientConfig);
    }

    [Xunit.Fact]
    public void shouldCompactTopicsForKeyValueStoreChangelogs() {// throws Exception
        string appID = APP_ID + "-compact";
        streamsProp.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        //
        // Step 1: Configure and start a simple word count topology
        //
        StreamsBuilder builder = new StreamsBuilder();
        KStream<string, string> textLines = builder.stream(DEFAULT_INPUT_TOPIC);

        textLines.flatMapValues(value => Array.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
            .groupBy(MockMapper.selectValueMapper())
            .count(Materialized.As("Counts"));

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsProp);
        streams.start();

        //
        // Step 2: Produce some input data to the input topic.
        //
        produceData(Array.asList("hello", "world", "world", "hello world"));

        //
        // Step 3: Verify the state changelog topics are compact
        //
        waitForCompletion(streams, 2, 30000);
        streams.close();

        Properties changelogProps = getTopicProperties(ProcessorStateManager.storeChangelogTopic(appID, "Counts"));
        Assert.Equal(LogConfig.Compact(), changelogProps.getProperty(LogConfig.CleanupPolicyProp()));

        Properties repartitionProps = getTopicProperties(appID + "-Counts-repartition");
        Assert.Equal(LogConfig.Delete(), repartitionProps.getProperty(LogConfig.CleanupPolicyProp()));
        Assert.Equal(3, repartitionProps.Count);
    }

    [Xunit.Fact]
    public void shouldCompactAndDeleteTopicsForWindowStoreChangelogs() {// throws Exception
        string appID = APP_ID + "-compact-delete";
        streamsProp.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        //
        // Step 1: Configure and start a simple word count topology
        //
        StreamsBuilder builder = new StreamsBuilder();
        KStream<string, string> textLines = builder.stream(DEFAULT_INPUT_TOPIC);

        int durationMs = 2000;

        textLines.flatMapValues(value => Array.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
            .groupBy(MockMapper.selectValueMapper())
            .windowedBy(TimeWindows.of(ofSeconds(1L)).grace(ofMillis(0L)))
            .count(Materialized<string, long, WindowStore<Bytes, byte[]>>.As("CountWindows").withRetention(ofSeconds(2L)));

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsProp);
        streams.start();

        //
        // Step 2: Produce some input data to the input topic.
        //
        produceData(Array.asList("hello", "world", "world", "hello world"));

        //
        // Step 3: Verify the state changelog topics are compact
        //
        waitForCompletion(streams, 2, 30000);
        streams.close();
        Properties properties = getTopicProperties(ProcessorStateManager.storeChangelogTopic(appID, "CountWindows"));
        List<string> policies = Array.asList(properties.getProperty(LogConfig.CleanupPolicyProp()).split(","));
        Assert.Equal(2, policies.Count);
        Assert.True(policies.Contains(LogConfig.Compact()));
        Assert.True(policies.Contains(LogConfig.Delete()));
        // retention should be 1 day + the window duration
        long retention = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS) + durationMs;
        Assert.Equal(retention, long.parseLong(properties.getProperty(LogConfig.RetentionMsProp())));

        Properties repartitionProps = getTopicProperties(appID + "-CountWindows-repartition");
        Assert.Equal(LogConfig.Delete(), repartitionProps.getProperty(LogConfig.CleanupPolicyProp()));
        Assert.Equal(3, repartitionProps.Count);
    }
}
