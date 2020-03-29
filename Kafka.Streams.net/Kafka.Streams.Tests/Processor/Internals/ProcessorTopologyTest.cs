/*






 *

 *





 */















































public class ProcessorTopologyTest {

    private static Serializer<string> STRING_SERIALIZER = new StringSerializer();
    private static Deserializer<string> STRING_DESERIALIZER = new StringDeserializer();

    private static string INPUT_TOPIC_1 = "input-topic-1";
    private static string INPUT_TOPIC_2 = "input-topic-2";
    private static string OUTPUT_TOPIC_1 = "output-topic-1";
    private static string OUTPUT_TOPIC_2 = "output-topic-2";
    private static string THROUGH_TOPIC_1 = "through-topic-1";

    private static Header HEADER = new RecordHeader("key", "value".getBytes());
    private static Headers HEADERS = new Headers(new Header[]{HEADER});

    private TopologyWrapper topology = new TopologyWrapper();
    private MockProcessorSupplier mockProcessorSupplier = new MockProcessorSupplier();
    private ConsumerRecordFactory<string, string> recordFactory = new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER, 0L);

    private TopologyTestDriver driver;
    private Properties props = new Properties();


    
    public void Setup() {
        // Create a new directory in which we'll put all of the state for this test, enabling running tests in parallel ...
        File localState = TestUtils.tempDirectory();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "processor-topology-test");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, localState.getAbsolutePath());
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, CustomTimestampExtractor.getName());
    }

    
    public void Cleanup() {
        props.Clear();
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    [Xunit.Fact]
    public void TestTopologyMetadata() {
        topology.addSource("source-1", "topic-1");
        topology.addSource("source-2", "topic-2", "topic-3");
        topology.addProcessor("processor-1", new MockProcessorSupplier<>(), "source-1");
        topology.addProcessor("processor-2", new MockProcessorSupplier<>(), "source-1", "source-2");
        topology.addSink("sink-1", "topic-3", "processor-1");
        topology.addSink("sink-2", "topic-4", "processor-1", "processor-2");

        ProcessorTopology processorTopology = topology.getInternalBuilder("X").build();

        Assert.Equal(6, processorTopology.processors().Count);

        Assert.Equal(2, processorTopology.sources().Count);

        Assert.Equal(3, processorTopology.sourceTopics().Count);

        assertNotNull(processorTopology.source("topic-1"));

        assertNotNull(processorTopology.source("topic-2"));

        assertNotNull(processorTopology.source("topic-3"));

        Assert.Equal(processorTopology.source("topic-2"), processorTopology.source("topic-3"));
    }

    [Xunit.Fact]
    public void TestDrivingSimpleTopology() {
        int partition = 10;
        driver = new TopologyTestDriver(CreateSimpleTopology(partition), props);
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key1", "value1"));
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1", partition);
        AssertNoOutputRecord(OUTPUT_TOPIC_2);

        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key2", "value2"));
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2", partition);
        AssertNoOutputRecord(OUTPUT_TOPIC_2);

        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key3", "value3"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key4", "value4"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key5", "value5"));
        AssertNoOutputRecord(OUTPUT_TOPIC_2);
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key3", "value3", partition);
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key4", "value4", partition);
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key5", "value5", partition);
    }


    [Xunit.Fact]
    public void TestDrivingMultiplexingTopology() {
        driver = new TopologyTestDriver(CreateMultiplexingTopology(), props);
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key1", "value1"));
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1(1)");
        AssertNextOutputRecord(OUTPUT_TOPIC_2, "key1", "value1(2)");

        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key2", "value2"));
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2(1)");
        AssertNextOutputRecord(OUTPUT_TOPIC_2, "key2", "value2(2)");

        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key3", "value3"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key4", "value4"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key5", "value5"));
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key3", "value3(1)");
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key4", "value4(1)");
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key5", "value5(1)");
        AssertNextOutputRecord(OUTPUT_TOPIC_2, "key3", "value3(2)");
        AssertNextOutputRecord(OUTPUT_TOPIC_2, "key4", "value4(2)");
        AssertNextOutputRecord(OUTPUT_TOPIC_2, "key5", "value5(2)");
    }

    [Xunit.Fact]
    public void TestDrivingMultiplexByNameTopology() {
        driver = new TopologyTestDriver(CreateMultiplexByNameTopology(), props);
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key1", "value1"));
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1(1)");
        AssertNextOutputRecord(OUTPUT_TOPIC_2, "key1", "value1(2)");

        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key2", "value2"));
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2(1)");
        AssertNextOutputRecord(OUTPUT_TOPIC_2, "key2", "value2(2)");

        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key3", "value3"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key4", "value4"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key5", "value5"));
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key3", "value3(1)");
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key4", "value4(1)");
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key5", "value5(1)");
        AssertNextOutputRecord(OUTPUT_TOPIC_2, "key3", "value3(2)");
        AssertNextOutputRecord(OUTPUT_TOPIC_2, "key4", "value4(2)");
        AssertNextOutputRecord(OUTPUT_TOPIC_2, "key5", "value5(2)");
    }

    [Xunit.Fact]
    public void TestDrivingStatefulTopology() {
        string storeName = "entries";
        driver = new TopologyTestDriver(CreateStatefulTopology(storeName), props);
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key1", "value1"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key2", "value2"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key3", "value3"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key1", "value4"));
        AssertNoOutputRecord(OUTPUT_TOPIC_1);

        KeyValueStore<string, string> store = driver.getKeyValueStore(storeName);
        Assert.Equal("value4", store.get("key1"));
        Assert.Equal("value2", store.get("key2"));
        Assert.Equal("value3", store.get("key3"));
        assertNull(store.get("key4"));
    }

    [Xunit.Fact]
    public void ShouldDriveGlobalStore() {
        string storeName = "my-store";
        string global = "global";
        string topic = "topic";

        topology.addGlobalStore(Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(storeName), Serdes.String(), Serdes.String()).withLoggingDisabled(),
                global, STRING_DESERIALIZER, STRING_DESERIALIZER, topic, "processor", define(new StatefulProcessor(storeName)));

        driver = new TopologyTestDriver(topology, props);
        KeyValueStore<string, string> globalStore = driver.getKeyValueStore(storeName);
        driver.pipeInput(recordFactory.create(topic, "key1", "value1"));
        driver.pipeInput(recordFactory.create(topic, "key2", "value2"));
        Assert.Equal("value1", globalStore.get("key1"));
        Assert.Equal("value2", globalStore.get("key2"));
    }

    [Xunit.Fact]
    public void TestDrivingSimpleMultiSourceTopology() {
        int partition = 10;
        driver = new TopologyTestDriver(CreateSimpleMultiSourceTopology(partition), props);

        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key1", "value1"));
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1", partition);
        AssertNoOutputRecord(OUTPUT_TOPIC_2);

        driver.pipeInput(recordFactory.create(INPUT_TOPIC_2, "key2", "value2"));
        AssertNextOutputRecord(OUTPUT_TOPIC_2, "key2", "value2", partition);
        AssertNoOutputRecord(OUTPUT_TOPIC_1);
    }

    [Xunit.Fact]
    public void TestDrivingForwardToSourceTopology() {
        driver = new TopologyTestDriver(CreateForwardToSourceTopology(), props);
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key1", "value1"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key2", "value2"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key3", "value3"));
        AssertNextOutputRecord(OUTPUT_TOPIC_2, "key1", "value1");
        AssertNextOutputRecord(OUTPUT_TOPIC_2, "key2", "value2");
        AssertNextOutputRecord(OUTPUT_TOPIC_2, "key3", "value3");
    }

    [Xunit.Fact]
    public void TestDrivingInternalRepartitioningTopology() {
        driver = new TopologyTestDriver(CreateInternalRepartitioningTopology(), props);
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key1", "value1"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key2", "value2"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key3", "value3"));
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1");
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2");
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key3", "value3");
    }

    [Xunit.Fact]
    public void TestDrivingInternalRepartitioningForwardingTimestampTopology() {
        driver = new TopologyTestDriver(CreateInternalRepartitioningWithValueTimestampTopology(), props);
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key1", "value1@1000"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key2", "value2@2000"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key3", "value3@3000"));
        Assert.Equal(driver.readOutput(OUTPUT_TOPIC_1, STRING_DESERIALIZER, STRING_DESERIALIZER),
                equalTo(new ProducerRecord<>(OUTPUT_TOPIC_1, null, 1000L, "key1", "value1")));
        Assert.Equal(driver.readOutput(OUTPUT_TOPIC_1, STRING_DESERIALIZER, STRING_DESERIALIZER),
                equalTo(new ProducerRecord<>(OUTPUT_TOPIC_1, null, 2000L, "key2", "value2")));
        Assert.Equal(driver.readOutput(OUTPUT_TOPIC_1, STRING_DESERIALIZER, STRING_DESERIALIZER),
                equalTo(new ProducerRecord<>(OUTPUT_TOPIC_1, null, 3000L, "key3", "value3")));
    }

    [Xunit.Fact]
    public void ShouldCreateStringWithSourceAndTopics() {
        topology.addSource("source", "topic1", "topic2");
        ProcessorTopology processorTopology = topology.getInternalBuilder().build();
        string result = processorTopology.toString();
        Assert.Equal(result, containsString("source:\n\t\ttopics:\t\t[topic1, topic2]\n"));
    }

    [Xunit.Fact]
    public void ShouldCreateStringWithMultipleSourcesAndTopics() {
        topology.addSource("source", "topic1", "topic2");
        topology.addSource("source2", "t", "t1", "t2");
        ProcessorTopology processorTopology = topology.getInternalBuilder().build();
        string result = processorTopology.toString();
        Assert.Equal(result, containsString("source:\n\t\ttopics:\t\t[topic1, topic2]\n"));
        Assert.Equal(result, containsString("source2:\n\t\ttopics:\t\t[t, t1, t2]\n"));
    }

    [Xunit.Fact]
    public void ShouldCreateStringWithProcessors() {
        topology.addSource("source", "t")
                .addProcessor("processor", mockProcessorSupplier, "source")
                .addProcessor("other", mockProcessorSupplier, "source");
        ProcessorTopology processorTopology = topology.getInternalBuilder().build();
        string result = processorTopology.toString();
        Assert.Equal(result, containsString("\t\tchildren:\t[processor, other]"));
        Assert.Equal(result, containsString("processor:\n"));
        Assert.Equal(result, containsString("other:\n"));
    }

    [Xunit.Fact]
    public void ShouldRecursivelyPrintChildren() {
        topology.addSource("source", "t")
                .addProcessor("processor", mockProcessorSupplier, "source")
                .addProcessor("child-one", mockProcessorSupplier, "processor")
                .addProcessor("child-one-one", mockProcessorSupplier, "child-one")
                .addProcessor("child-two", mockProcessorSupplier, "processor")
                .addProcessor("child-two-one", mockProcessorSupplier, "child-two");

        string result = topology.getInternalBuilder().build().toString();
        Assert.Equal(result, containsString("child-one:\n\t\tchildren:\t[child-one-one]"));
        Assert.Equal(result, containsString("child-two:\n\t\tchildren:\t[child-two-one]"));
    }

    [Xunit.Fact]
    public void ShouldConsiderTimeStamps() {
        int partition = 10;
        driver = new TopologyTestDriver(CreateSimpleTopology(partition), props);
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key1", "value1", 10L));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key2", "value2", 20L));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key3", "value3", 30L));
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1", partition, 10L);
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2", partition, 20L);
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key3", "value3", partition, 30L);
    }

    [Xunit.Fact]
    public void ShouldConsiderModifiedTimeStamps() {
        int partition = 10;
        driver = new TopologyTestDriver(CreateTimestampTopology(partition), props);
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key1", "value1", 10L));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key2", "value2", 20L));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key3", "value3", 30L));
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1", partition, 20L);
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2", partition, 30L);
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key3", "value3", partition, 40L);
    }

    [Xunit.Fact]
    public void ShouldConsiderModifiedTimeStampsForMultipleProcessors() {
        int partition = 10;
        driver = new TopologyTestDriver(CreateMultiProcessorTimestampTopology(partition), props);

        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key1", "value1", 10L));
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1", partition, 10L);
        AssertNextOutputRecord(OUTPUT_TOPIC_2, "key1", "value1", partition, 20L);
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1", partition, 15L);
        AssertNextOutputRecord(OUTPUT_TOPIC_2, "key1", "value1", partition, 20L);
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1", partition, 12L);
        AssertNextOutputRecord(OUTPUT_TOPIC_2, "key1", "value1", partition, 22L);
        AssertNoOutputRecord(OUTPUT_TOPIC_1);
        AssertNoOutputRecord(OUTPUT_TOPIC_2);

        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key2", "value2", 20L));
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2", partition, 20L);
        AssertNextOutputRecord(OUTPUT_TOPIC_2, "key2", "value2", partition, 30L);
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2", partition, 25L);
        AssertNextOutputRecord(OUTPUT_TOPIC_2, "key2", "value2", partition, 30L);
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2", partition, 22L);
        AssertNextOutputRecord(OUTPUT_TOPIC_2, "key2", "value2", partition, 32L);
        AssertNoOutputRecord(OUTPUT_TOPIC_1);
        AssertNoOutputRecord(OUTPUT_TOPIC_2);
    }

    [Xunit.Fact]
    public void ShouldConsiderHeaders() {
        int partition = 10;
        driver = new TopologyTestDriver(CreateSimpleTopology(partition), props);
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key1", "value1", HEADERS, 10L));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key2", "value2", HEADERS, 20L));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key3", "value3", HEADERS, 30L));
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1", HEADERS, partition, 10L);
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2", HEADERS, partition, 20L);
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key3", "value3", HEADERS, partition, 30L);
    }

    [Xunit.Fact]
    public void ShouldAddHeaders() {
        driver = new TopologyTestDriver(CreateAddHeaderTopology(), props);
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key1", "value1", 10L));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key2", "value2", 20L));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key3", "value3", 30L));
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1", HEADERS, 10L);
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2", HEADERS, 20L);
        AssertNextOutputRecord(OUTPUT_TOPIC_1, "key3", "value3", HEADERS, 30L);
    }

    [Xunit.Fact]
    public void StatelessTopologyShouldNotHavePersistentStore() {
        TopologyWrapper topology = new TopologyWrapper();
        ProcessorTopology processorTopology = topology.getInternalBuilder("anyAppId").build();
        Assert.False(processorTopology.hasPersistentLocalStore());
        Assert.False(processorTopology.hasPersistentGlobalStore());
    }

    [Xunit.Fact]
    public void InMemoryStoreShouldNotResultInPersistentLocalStore() {
        ProcessorTopology processorTopology = createLocalStoreTopology(Stores.inMemoryKeyValueStore("my-store"));
        Assert.False(processorTopology.hasPersistentLocalStore());
    }

    [Xunit.Fact]
    public void PersistentLocalStoreShouldBeDetected() {
        ProcessorTopology processorTopology = createLocalStoreTopology(Stores.persistentKeyValueStore("my-store"));
        Assert.True(processorTopology.hasPersistentLocalStore());
    }

    [Xunit.Fact]
    public void InMemoryStoreShouldNotResultInPersistentGlobalStore() {
        ProcessorTopology processorTopology = createGlobalStoreTopology(Stores.inMemoryKeyValueStore("my-store"));
        Assert.False(processorTopology.hasPersistentGlobalStore());
    }

    [Xunit.Fact]
    public void PersistentGlobalStoreShouldBeDetected() {
        ProcessorTopology processorTopology = createGlobalStoreTopology(Stores.persistentKeyValueStore("my-store"));
        Assert.True(processorTopology.hasPersistentGlobalStore());
    }

    private ProcessorTopology CreateLocalStoreTopology(KeyValueBytesStoreSupplier storeSupplier) {
        TopologyWrapper topology = new TopologyWrapper();
        string processor = "processor";
        StoreBuilder<KeyValueStore<string, string>> storeBuilder =
                Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.String());
        topology.addSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, "topic")
                .addProcessor(processor, () => new StatefulProcessor(storeSupplier.name()), "source")
                .addStateStore(storeBuilder, processor);
        return topology.getInternalBuilder("anyAppId").build();
    }

    private ProcessorTopology CreateGlobalStoreTopology(KeyValueBytesStoreSupplier storeSupplier) {
        TopologyWrapper topology = new TopologyWrapper();
        StoreBuilder<KeyValueStore<string, string>> storeBuilder =
                Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.String()).withLoggingDisabled();
        topology.addGlobalStore(storeBuilder, "global", STRING_DESERIALIZER, STRING_DESERIALIZER, "topic", "processor",
                define(new StatefulProcessor(storeSupplier.name())));
        return topology.getInternalBuilder("anyAppId").build();
    }

    private void AssertNextOutputRecord(string topic,
                                        string key,
                                        string value) {
        AssertNextOutputRecord(topic, key, value, (int) null, 0L);
    }

    private void AssertNextOutputRecord(string topic,
                                        string key,
                                        string value,
                                        int partition) {
        AssertNextOutputRecord(topic, key, value, partition, 0L);
    }

    private void AssertNextOutputRecord(string topic,
                                        string key,
                                        string value,
                                        Headers headers,
                                        long timestamp) {
        assertNextOutputRecord(topic, key, value, headers, null, timestamp);
    }

    private void AssertNextOutputRecord(string topic,
                                        string key,
                                        string value,
                                        int partition,
                                        long timestamp) {
        AssertNextOutputRecord(topic, key, value, new Headers(), partition, timestamp);
    }

    private void AssertNextOutputRecord(string topic,
                                        string key,
                                        string value,
                                        Headers headers,
                                        int partition,
                                        long timestamp) {
        ProducerRecord<string, string> record = driver.readOutput(topic, STRING_DESERIALIZER, STRING_DESERIALIZER);
        Assert.Equal(topic, record.topic());
        Assert.Equal(key, record.Key);
        Assert.Equal(value, record.Value);
        Assert.Equal(partition, record.partition());
        Assert.Equal(timestamp, record.Timestamp);
        Assert.Equal(headers, record.headers());
    }

    private void AssertNoOutputRecord(string topic) {
        assertNull(driver.readOutput(topic));
    }

    private StreamPartitioner<object, object> ConstantPartitioner(int partition) {
        return (topic, key, value, numPartitions) => partition;
    }

    private Topology CreateSimpleTopology(int partition) {
        return topology
            .addSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
            .addProcessor("processor", define(new ForwardingProcessor()), "source")
            .addSink("sink", OUTPUT_TOPIC_1, ConstantPartitioner(partition), "processor");
    }

    private Topology CreateTimestampTopology(int partition) {
        return topology
            .addSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
            .addProcessor("processor", define(new TimestampProcessor()), "source")
            .addSink("sink", OUTPUT_TOPIC_1, ConstantPartitioner(partition), "processor");
    }

    private Topology CreateMultiProcessorTimestampTopology(int partition) {
        return topology
            .addSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
            .addProcessor("processor", define(new FanOutTimestampProcessor("child1", "child2")), "source")
            .addProcessor("child1", define(new ForwardingProcessor()), "processor")
            .addProcessor("child2", define(new TimestampProcessor()), "processor")
            .addSink("sink1", OUTPUT_TOPIC_1, ConstantPartitioner(partition), "child1")
            .addSink("sink2", OUTPUT_TOPIC_2, ConstantPartitioner(partition), "child2");
    }

    private Topology CreateMultiplexingTopology() {
        return topology
            .addSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
            .addProcessor("processor", define(new MultiplexingProcessor(2)), "source")
            .addSink("sink1", OUTPUT_TOPIC_1, "processor")
            .addSink("sink2", OUTPUT_TOPIC_2, "processor");
    }

    private Topology CreateMultiplexByNameTopology() {
        return topology
            .addSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
            .addProcessor("processor", define(new MultiplexByNameProcessor(2)), "source")
            .addSink("sink0", OUTPUT_TOPIC_1, "processor")
            .addSink("sink1", OUTPUT_TOPIC_2, "processor");
    }

    private Topology CreateStatefulTopology(string storeName) {
        return topology
            .addSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
            .addProcessor("processor", define(new StatefulProcessor(storeName)), "source")
            .addStateStore(Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(storeName), Serdes.String(), Serdes.String()), "processor")
            .addSink("counts", OUTPUT_TOPIC_1, "processor");
    }

    private Topology CreateInternalRepartitioningTopology() {
        topology.addSource("source", INPUT_TOPIC_1)
            .addSink("sink0", THROUGH_TOPIC_1, "source")
            .addSource("source1", THROUGH_TOPIC_1)
            .addSink("sink1", OUTPUT_TOPIC_1, "source1");

        // use wrapper to get the internal topology builder to add internal topic
        InternalTopologyBuilder internalTopologyBuilder = TopologyWrapper.getInternalTopologyBuilder(topology);
        internalTopologyBuilder.addInternalTopic(THROUGH_TOPIC_1);

        return topology;
    }

    private Topology CreateInternalRepartitioningWithValueTimestampTopology() {
        topology.addSource("source", INPUT_TOPIC_1)
                .addProcessor("processor", define(new ValueTimestampProcessor()), "source")
                .addSink("sink0", THROUGH_TOPIC_1, "processor")
                .addSource("source1", THROUGH_TOPIC_1)
                .addSink("sink1", OUTPUT_TOPIC_1, "source1");

        // use wrapper to get the internal topology builder to add internal topic
        InternalTopologyBuilder internalTopologyBuilder = TopologyWrapper.getInternalTopologyBuilder(topology);
        internalTopologyBuilder.addInternalTopic(THROUGH_TOPIC_1);

        return topology;
    }

    private Topology CreateForwardToSourceTopology() {
        return topology.addSource("source-1", INPUT_TOPIC_1)
                .addSink("sink-1", OUTPUT_TOPIC_1, "source-1")
                .addSource("source-2", OUTPUT_TOPIC_1)
                .addSink("sink-2", OUTPUT_TOPIC_2, "source-2");
    }

    private Topology CreateSimpleMultiSourceTopology(int partition) {
        return topology.addSource("source-1", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
                .addProcessor("processor-1", define(new ForwardingProcessor()), "source-1")
                .addSink("sink-1", OUTPUT_TOPIC_1, ConstantPartitioner(partition), "processor-1")
                .addSource("source-2", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_2)
                .addProcessor("processor-2", define(new ForwardingProcessor()), "source-2")
                .addSink("sink-2", OUTPUT_TOPIC_2, ConstantPartitioner(partition), "processor-2");
    }

    private Topology CreateAddHeaderTopology() {
        return topology.addSource("source-1", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
                .addProcessor("processor-1", define(new AddHeaderProcessor()), "source-1")
                .addSink("sink-1", OUTPUT_TOPIC_1, "processor-1");
    }

    /**
     * A processor that simply forwards all messages to all children.
     */
    protected static class ForwardingProcessor : AbstractProcessor<string, string> {
        
        public void Process(string key, string value) {
            context().forward(key, value);
        }
    }

    /**
     * A processor that simply forwards all messages to all children with advanced timestamps.
     */
    protected static class TimestampProcessor : AbstractProcessor<string, string> {
        
        public void Process(string key, string value) {
            context().forward(key, value, To.all().withTimestamp(context().Timestamp + 10));
        }
    }

    protected static class FanOutTimestampProcessor : AbstractProcessor<string, string> {
        private string firstChild;
        private string secondChild;

        FanOutTimestampProcessor(string firstChild,
                                 string secondChild) {
            this.firstChild = firstChild;
            this.secondChild = secondChild;
        }

        
        public void Process(string key, string value) {
            context().forward(key, value);
            context().forward(key, value, To.child(firstChild).withTimestamp(context().Timestamp + 5));
            context().forward(key, value, To.child(secondChild));
            context().forward(key, value, To.all().withTimestamp(context().Timestamp + 2));
        }
    }

    protected static class AddHeaderProcessor : AbstractProcessor<string, string> {
        
        public void Process(string key, string value) {
            context().headers().add(HEADER);
            context().forward(key, value);
        }
    }

    /**
     * A processor that removes custom timestamp information from messages and forwards modified messages to each child.
     * A message contains custom timestamp information if the value is in ".*@[0-9]+" format.
     */
    protected static class ValueTimestampProcessor : AbstractProcessor<string, string> {
        
        public void Process(string key, string value) {
            context().forward(key, value.split("@")[0]);
        }
    }

    /**
     * A processor that forwards slightly-modified messages to each child.
     */
    protected static class MultiplexingProcessor : AbstractProcessor<string, string> {
        private int numChildren;

        MultiplexingProcessor(int numChildren) {
            this.numChildren = numChildren;
        }

         // need to test deprecated code until removed
        
        public void Process(string key, string value) {
            for (int i = 0; i != numChildren; ++i) {
                context().forward(key, value + "(" + (i + 1) + ")", i);
            }
        }
    }

    /**
     * A processor that forwards slightly-modified messages to each named child.
     * Note: the children are assumed to be named "sink{child number}", e.g., sink1, or sink2, etc.
     */
    protected static class MultiplexByNameProcessor : AbstractProcessor<string, string> {
        private int numChildren;

        MultiplexByNameProcessor(int numChildren) {
            this.numChildren = numChildren;
        }

         // need to test deprecated code until removed
        
        public void Process(string key, string value) {
            for (int i = 0; i != numChildren; ++i) {
                context().forward(key, value + "(" + (i + 1) + ")",  "sink" + i);
            }
        }
    }

    /**
     * A processor that stores each key-value pair in an in-memory key-value store registered with the context.
     */
    protected static class StatefulProcessor : AbstractProcessor<string, string> {
        private KeyValueStore<string, string> store;
        private string storeName;

        StatefulProcessor(string storeName) {
            this.storeName = storeName;
        }

        
        
        public void Init(ProcessorContext context) {
            base.init(context);
            store = (KeyValueStore<string, string>) context.getStateStore(storeName);
        }

        
        public void Process(string key, string value) {
            store.put(key, value);
        }
    }

    private ProcessorSupplier<K, V> Define<K, V>(Processor<K, V> processor) {
        return () => processor;
    }

    /**
     * A custom timestamp extractor that extracts the timestamp from the record's value if the value is in ".*@[0-9]+"
     * format. Otherwise, it returns the record's timestamp or the default timestamp if the record's timestamp is negative.
    */
    public static class CustomTimestampExtractor : TimestampExtractor {
        private static long DEFAULT_TIMESTAMP = 1000L;

        
        public long Extract(ConsumeResult<object, object> record, long partitionTime) {
            if (record.Value.toString().matches(".*@[0-9]+")) {
                return long.parseLong(record.Value.toString().split("@")[1]);
            }

            if (record.Timestamp >= 0L) {
                return record.Timestamp;
            }

            return DEFAULT_TIMESTAMP;
        }
    }
}
