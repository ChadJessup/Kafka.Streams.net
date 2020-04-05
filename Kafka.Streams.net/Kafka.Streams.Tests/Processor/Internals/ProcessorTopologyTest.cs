//using Confluent.Kafka;
//using Kafka.Streams.Configs;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.KStream;
//using Kafka.Streams.Processors;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.Processors.Internals;
//using Kafka.Streams.State;
//using Kafka.Streams.State.KeyValues;
//using Kafka.Streams.Tests.Helpers;
//using Kafka.Streams.Tests.Mocks;
//using Kafka.Streams.Topologies;
//using Xunit;

//namespace Kafka.Streams.Tests.Processor.Internals
//{
//    public class ProcessorTopologyTest
//    {
//        private static ISerializer<string> STRING_SERIALIZER = Serdes.String().Serializer;
//        private static IDeserializer<string> STRING_DESERIALIZER = Serdes.String().Deserializer;

//        private const string INPUT_TOPIC_1 = "input-topic-1";
//        private const string INPUT_TOPIC_2 = "input-topic-2";
//        private const string OUTPUT_TOPIC_1 = "output-topic-1";
//        private const string OUTPUT_TOPIC_2 = "output-topic-2";
//        private const string THROUGH_TOPIC_1 = "through-topic-1";

//        private static Header HEADER = new RecordHeader("key", "value".getBytes());
//        private static Headers HEADERS = new Headers(new Header[] { HEADER });

//        private TopologyWrapper topology = new TopologyWrapper();
//        private MockProcessorSupplier mockProcessorSupplier = new MockProcessorSupplier();
//        private ConsumerRecordFactory<string, string> recordFactory = new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER, 0L);

//        private TopologyTestDriver driver;
//        private StreamsConfig props = new StreamsConfig();



//        public void Setup()
//        {
//            // Create a new directory in which we'll put all of the state for this test, enabling running tests in parallel ...
//            File localState = TestUtils.GempDirectory();
//            props.Set(StreamsConfig.APPLICATION_ID_CONFIG, "processor-topology-test");
//            props.Set(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
//            props.Set(StreamsConfig.STATE_DIR_CONFIG, localState.FullName);
//            props.Set(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().GetType().FullName);
//            props.Set(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().GetType().FullName);
//            props.Set(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, CustomTimestampExtractor.getName());
//        }


//        public void Cleanup()
//        {
//            props.Clear();
//            if (driver != null)
//            {
//                driver.close();
//            }
//            driver = null;
//        }

//        [Xunit.Fact]
//        public void TestTopologyMetadata()
//        {
//            topology.AddSource("source-1", "topic-1");
//            topology.AddSource("source-2", "topic-2", "topic-3");
//            topology.AddProcessor("processor-1", new MockProcessorSupplier<>(), "source-1");
//            topology.AddProcessor("processor-2", new MockProcessorSupplier<>(), "source-1", "source-2");
//            topology.AddSink("sink-1", "topic-3", "processor-1");
//            topology.AddSink("sink-2", "topic-4", "processor-1", "processor-2");

//            ProcessorTopology processorTopology = topology.getInternalBuilder("X").Build();

//            Assert.Equal(6, processorTopology.processors().Count);

//            Assert.Equal(2, processorTopology.sources().Count);

//            Assert.Equal(3, processorTopology.sourceTopics().Count);

//            Assert.NotNull(processorTopology.source("topic-1"));

//            Assert.NotNull(processorTopology.source("topic-2"));

//            Assert.NotNull(processorTopology.source("topic-3"));

//            Assert.Equal(processorTopology.source("topic-2"), processorTopology.source("topic-3"));
//        }

//        [Xunit.Fact]
//        public void TestDrivingSimpleTopology()
//        {
//            int partition = 10;
//            driver = new TopologyTestDriver(CreateSimpleTopology(partition), props);
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key1", "value1"));
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1", partition);
//            AssertNoOutputRecord(OUTPUT_TOPIC_2);

//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key2", "value2"));
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2", partition);
//            AssertNoOutputRecord(OUTPUT_TOPIC_2);

//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key3", "value3"));
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key4", "value4"));
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key5", "value5"));
//            AssertNoOutputRecord(OUTPUT_TOPIC_2);
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key3", "value3", partition);
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key4", "value4", partition);
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key5", "value5", partition);
//        }


//        [Xunit.Fact]
//        public void TestDrivingMultiplexingTopology()
//        {
//            driver = new TopologyTestDriver(CreateMultiplexingTopology(), props);
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key1", "value1"));
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1(1)");
//            AssertNextOutputRecord(OUTPUT_TOPIC_2, "key1", "value1(2)");

//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key2", "value2"));
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2(1)");
//            AssertNextOutputRecord(OUTPUT_TOPIC_2, "key2", "value2(2)");

//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key3", "value3"));
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key4", "value4"));
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key5", "value5"));
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key3", "value3(1)");
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key4", "value4(1)");
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key5", "value5(1)");
//            AssertNextOutputRecord(OUTPUT_TOPIC_2, "key3", "value3(2)");
//            AssertNextOutputRecord(OUTPUT_TOPIC_2, "key4", "value4(2)");
//            AssertNextOutputRecord(OUTPUT_TOPIC_2, "key5", "value5(2)");
//        }

//        [Xunit.Fact]
//        public void TestDrivingMultiplexByNameTopology()
//        {
//            driver = new TopologyTestDriver(CreateMultiplexByNameTopology(), props);
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key1", "value1"));
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1(1)");
//            AssertNextOutputRecord(OUTPUT_TOPIC_2, "key1", "value1(2)");

//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key2", "value2"));
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2(1)");
//            AssertNextOutputRecord(OUTPUT_TOPIC_2, "key2", "value2(2)");

//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key3", "value3"));
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key4", "value4"));
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key5", "value5"));
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key3", "value3(1)");
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key4", "value4(1)");
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key5", "value5(1)");
//            AssertNextOutputRecord(OUTPUT_TOPIC_2, "key3", "value3(2)");
//            AssertNextOutputRecord(OUTPUT_TOPIC_2, "key4", "value4(2)");
//            AssertNextOutputRecord(OUTPUT_TOPIC_2, "key5", "value5(2)");
//        }

//        [Xunit.Fact]
//        public void TestDrivingStatefulTopology()
//        {
//            string storeName = "entries";
//            driver = new TopologyTestDriver(CreateStatefulTopology(storeName), props);
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key1", "value1"));
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key2", "value2"));
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key3", "value3"));
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key1", "value4"));
//            AssertNoOutputRecord(OUTPUT_TOPIC_1);

//            IKeyValueStore<string, string> store = driver.getKeyValueStore(storeName);
//            Assert.Equal("value4", store.Get("key1"));
//            Assert.Equal("value2", store.Get("key2"));
//            Assert.Equal("value3", store.Get("key3"));
//            Assert.Null(store.Get("key4"));
//        }

//        [Xunit.Fact]
//        public void ShouldDriveGlobalStore()
//        {
//            string storeName = "my-store";
//            string global = "global";
//            string topic = "topic";

//            topology.addGlobalStore(Stores.KeyValueStoreBuilder(Stores.InMemoryKeyValueStore(storeName), Serdes.String(), Serdes.String()).withLoggingDisabled(),
//                    global, STRING_DESERIALIZER, STRING_DESERIALIZER, topic, "processor", define(new StatefulProcessor(storeName)));

//            driver = new TopologyTestDriver(topology, props);
//            IKeyValueStore<string, string> globalStore = driver.getKeyValueStore(storeName);
//            driver.PipeInput(recordFactory.Create(topic, "key1", "value1"));
//            driver.PipeInput(recordFactory.Create(topic, "key2", "value2"));
//            Assert.Equal("value1", globalStore.Get("key1"));
//            Assert.Equal("value2", globalStore.Get("key2"));
//        }

//        [Xunit.Fact]
//        public void TestDrivingSimpleMultiSourceTopology()
//        {
//            int partition = 10;
//            driver = new TopologyTestDriver(CreateSimpleMultiSourceTopology(partition), props);

//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key1", "value1"));
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1", partition);
//            AssertNoOutputRecord(OUTPUT_TOPIC_2);

//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_2, "key2", "value2"));
//            AssertNextOutputRecord(OUTPUT_TOPIC_2, "key2", "value2", partition);
//            AssertNoOutputRecord(OUTPUT_TOPIC_1);
//        }

//        [Xunit.Fact]
//        public void TestDrivingForwardToSourceTopology()
//        {
//            driver = new TopologyTestDriver(CreateForwardToSourceTopology(), props);
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key1", "value1"));
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key2", "value2"));
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key3", "value3"));
//            AssertNextOutputRecord(OUTPUT_TOPIC_2, "key1", "value1");
//            AssertNextOutputRecord(OUTPUT_TOPIC_2, "key2", "value2");
//            AssertNextOutputRecord(OUTPUT_TOPIC_2, "key3", "value3");
//        }

//        [Xunit.Fact]
//        public void TestDrivingInternalRepartitioningTopology()
//        {
//            driver = new TopologyTestDriver(CreateInternalRepartitioningTopology(), props);
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key1", "value1"));
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key2", "value2"));
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key3", "value3"));
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1");
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2");
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key3", "value3");
//        }

//        [Xunit.Fact]
//        public void TestDrivingInternalRepartitioningForwardingTimestampTopology()
//        {
//            driver = new TopologyTestDriver(CreateInternalRepartitioningWithValueTimestampTopology(), props);
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key1", "value1@1000"));
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key2", "value2@2000"));
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key3", "value3@3000"));
//            Assert.Equal(driver.readOutput(OUTPUT_TOPIC_1, STRING_DESERIALIZER, STRING_DESERIALIZER),
//                    equalTo(new ProducerRecord<>(OUTPUT_TOPIC_1, null, 1000L, "key1", "value1")));
//            Assert.Equal(driver.readOutput(OUTPUT_TOPIC_1, STRING_DESERIALIZER, STRING_DESERIALIZER),
//                    equalTo(new ProducerRecord<>(OUTPUT_TOPIC_1, null, 2000L, "key2", "value2")));
//            Assert.Equal(driver.readOutput(OUTPUT_TOPIC_1, STRING_DESERIALIZER, STRING_DESERIALIZER),
//                    equalTo(new ProducerRecord<>(OUTPUT_TOPIC_1, null, 3000L, "key3", "value3")));
//        }

//        [Xunit.Fact]
//        public void ShouldCreateStringWithSourceAndTopics()
//        {
//            topology.AddSource("source", "topic1", "topic2");
//            ProcessorTopology processorTopology = topology.getInternalBuilder().Build();
//            string result = processorTopology.ToString();
//            Assert.Equal(result, containsString("source:\n\t\ttopics:\t\t[topic1, topic2]\n"));
//        }

//        [Xunit.Fact]
//        public void ShouldCreateStringWithMultipleSourcesAndTopics()
//        {
//            topology.AddSource("source", "topic1", "topic2");
//            topology.AddSource("source2", "t", "t1", "t2");
//            ProcessorTopology processorTopology = topology.getInternalBuilder().Build();
//            string result = processorTopology.ToString();
//            Assert.Equal(result, containsString("source:\n\t\ttopics:\t\t[topic1, topic2]\n"));
//            Assert.Equal(result, containsString("source2:\n\t\ttopics:\t\t[t, t1, t2]\n"));
//        }

//        [Xunit.Fact]
//        public void ShouldCreateStringWithProcessors()
//        {
//            topology.AddSource("source", "t")
//                    .AddProcessor("processor", mockProcessorSupplier, "source")
//                    .AddProcessor("other", mockProcessorSupplier, "source");
//            ProcessorTopology processorTopology = topology.getInternalBuilder().Build();
//            string result = processorTopology.ToString();
//            Assert.Equal(result, containsString("\t\tchildren:\t[processor, other]"));
//            Assert.Equal(result, containsString("processor:\n"));
//            Assert.Equal(result, containsString("other:\n"));
//        }

//        [Xunit.Fact]
//        public void ShouldRecursivelyPrintChildren()
//        {
//            topology.AddSource("source", "t")
//                    .AddProcessor("processor", mockProcessorSupplier, "source")
//                    .AddProcessor("child-one", mockProcessorSupplier, "processor")
//                    .AddProcessor("child-one-one", mockProcessorSupplier, "child-one")
//                    .AddProcessor("child-two", mockProcessorSupplier, "processor")
//                    .AddProcessor("child-two-one", mockProcessorSupplier, "child-two");

//            string result = topology.getInternalBuilder().Build().ToString();
//            Assert.Equal(result, containsString("child-one:\n\t\tchildren:\t[child-one-one]"));
//            Assert.Equal(result, containsString("child-two:\n\t\tchildren:\t[child-two-one]"));
//        }

//        [Xunit.Fact]
//        public void ShouldConsiderTimeStamps()
//        {
//            int partition = 10;
//            driver = new TopologyTestDriver(CreateSimpleTopology(partition), props);
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key1", "value1", 10L));
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key2", "value2", 20L));
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key3", "value3", 30L));
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1", partition, 10L);
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2", partition, 20L);
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key3", "value3", partition, 30L);
//        }

//        [Xunit.Fact]
//        public void ShouldConsiderModifiedTimeStamps()
//        {
//            int partition = 10;
//            driver = new TopologyTestDriver(CreateTimestampTopology(partition), props);
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key1", "value1", 10L));
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key2", "value2", 20L));
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key3", "value3", 30L));
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1", partition, 20L);
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2", partition, 30L);
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key3", "value3", partition, 40L);
//        }

//        [Xunit.Fact]
//        public void ShouldConsiderModifiedTimeStampsForMultipleProcessors()
//        {
//            int partition = 10;
//            driver = new TopologyTestDriver(CreateMultiProcessorTimestampTopology(partition), props);

//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key1", "value1", 10L));
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1", partition, 10L);
//            AssertNextOutputRecord(OUTPUT_TOPIC_2, "key1", "value1", partition, 20L);
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1", partition, 15L);
//            AssertNextOutputRecord(OUTPUT_TOPIC_2, "key1", "value1", partition, 20L);
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1", partition, 12L);
//            AssertNextOutputRecord(OUTPUT_TOPIC_2, "key1", "value1", partition, 22L);
//            AssertNoOutputRecord(OUTPUT_TOPIC_1);
//            AssertNoOutputRecord(OUTPUT_TOPIC_2);

//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key2", "value2", 20L));
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2", partition, 20L);
//            AssertNextOutputRecord(OUTPUT_TOPIC_2, "key2", "value2", partition, 30L);
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2", partition, 25L);
//            AssertNextOutputRecord(OUTPUT_TOPIC_2, "key2", "value2", partition, 30L);
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2", partition, 22L);
//            AssertNextOutputRecord(OUTPUT_TOPIC_2, "key2", "value2", partition, 32L);
//            AssertNoOutputRecord(OUTPUT_TOPIC_1);
//            AssertNoOutputRecord(OUTPUT_TOPIC_2);
//        }

//        [Xunit.Fact]
//        public void ShouldConsiderHeaders()
//        {
//            int partition = 10;
//            driver = new TopologyTestDriver(CreateSimpleTopology(partition), props);
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key1", "value1", HEADERS, 10L));
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key2", "value2", HEADERS, 20L));
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key3", "value3", HEADERS, 30L));
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1", HEADERS, partition, 10L);
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2", HEADERS, partition, 20L);
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key3", "value3", HEADERS, partition, 30L);
//        }

//        [Xunit.Fact]
//        public void ShouldAddHeaders()
//        {
//            driver = new TopologyTestDriver(CreateAddHeaderTopology(), props);
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key1", "value1", 10L));
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key2", "value2", 20L));
//            driver.PipeInput(recordFactory.Create(INPUT_TOPIC_1, "key3", "value3", 30L));
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1", HEADERS, 10L);
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2", HEADERS, 20L);
//            AssertNextOutputRecord(OUTPUT_TOPIC_1, "key3", "value3", HEADERS, 30L);
//        }

//        [Xunit.Fact]
//        public void StatelessTopologyShouldNotHavePersistentStore()
//        {
//            TopologyWrapper topology = new TopologyWrapper();
//            ProcessorTopology processorTopology = topology.getInternalBuilder("anyAppId").Build();
//            Assert.False(processorTopology.hasPersistentLocalStore());
//            Assert.False(processorTopology.hasPersistentGlobalStore());
//        }

//        [Xunit.Fact]
//        public void InMemoryStoreShouldNotResultInPersistentLocalStore()
//        {
//            ProcessorTopology processorTopology = createLocalStoreTopology(Stores.InMemoryKeyValueStore("my-store"));
//            Assert.False(processorTopology.hasPersistentLocalStore());
//        }

//        [Xunit.Fact]
//        public void PersistentLocalStoreShouldBeDetected()
//        {
//            ProcessorTopology processorTopology = createLocalStoreTopology(Stores.PersistentKeyValueStore("my-store"));
//            Assert.True(processorTopology.hasPersistentLocalStore());
//        }

//        [Xunit.Fact]
//        public void InMemoryStoreShouldNotResultInPersistentGlobalStore()
//        {
//            ProcessorTopology processorTopology = createGlobalStoreTopology(Stores.InMemoryKeyValueStore("my-store"));
//            Assert.False(processorTopology.hasPersistentGlobalStore());
//        }

//        [Xunit.Fact]
//        public void PersistentGlobalStoreShouldBeDetected()
//        {
//            ProcessorTopology processorTopology = createGlobalStoreTopology(Stores.PersistentKeyValueStore("my-store"));
//            Assert.True(processorTopology.hasPersistentGlobalStore());
//        }

//        private ProcessorTopology CreateLocalStoreTopology(IKeyValueBytesStoreSupplier storeSupplier)
//        {
//            TopologyWrapper topology = new TopologyWrapper();
//            string processor = "processor";
//            IStoreBuilder<IKeyValueStore<string, string>> storeBuilder =
//                    Stores.KeyValueStoreBuilder<string, string>(null, storeSupplier, Serdes.String(), Serdes.String());
//            topology.AddSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, "topic")
//                    .AddProcessor(processor, () => new StatefulProcessor(storeSupplier.name()), "source")
//                    .addStateStore(storeBuilder, processor);
//            return topology.getInternalBuilder("anyAppId").Build();
//        }

//        private ProcessorTopology CreateGlobalStoreTopology(IKeyValueBytesStoreSupplier storeSupplier)
//        {
//            TopologyWrapper topology = new TopologyWrapper();
//            IStoreBuilder<IKeyValueStore<string, string>> storeBuilder =
//                    Stores.KeyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.String()).withLoggingDisabled();
//            topology.addGlobalStore(storeBuilder, "global", STRING_DESERIALIZER, STRING_DESERIALIZER, "topic", "processor",
//                    define(new StatefulProcessor(storeSupplier.name())));
//            return topology.getInternalBuilder("anyAppId").Build();
//        }

//        private void AssertNextOutputRecord(string topic,
//                                            string key,
//                                            string value)
//        {
//            AssertNextOutputRecord(topic, key, value, (int)null, 0L);
//        }

//        private void AssertNextOutputRecord(string topic,
//                                            string key,
//                                            string value,
//                                            int partition)
//        {
//            AssertNextOutputRecord(topic, key, value, partition, 0L);
//        }

//        private void AssertNextOutputRecord(string topic,
//                                            string key,
//                                            string value,
//                                            Headers headers,
//                                            long timestamp)
//        {
//            assertNextOutputRecord(topic, key, value, headers, null, timestamp);
//        }

//        private void AssertNextOutputRecord(string topic,
//                                            string key,
//                                            string value,
//                                            int partition,
//                                            long timestamp)
//        {
//            AssertNextOutputRecord(topic, key, value, new Headers(), partition, timestamp);
//        }

//        private void AssertNextOutputRecord(string topic,
//                                            string key,
//                                            string value,
//                                            Headers headers,
//                                            int partition,
//                                            long timestamp)
//        {
//            ProducerRecord<string, string> record = driver.readOutput(topic, STRING_DESERIALIZER, STRING_DESERIALIZER);
//            Assert.Equal(topic, record.Topic);
//            Assert.Equal(key, record.Key);
//            Assert.Equal(value, record.Value);
//            Assert.Equal(partition, record.Partition);
//            Assert.Equal(timestamp, record.Timestamp);
//            Assert.Equal(headers, record.Headers);
//        }

//        private void AssertNoOutputRecord(string topic)
//        {
//            Assert.Null(driver.readOutput(topic));
//        }

//        private IStreamPartitioner<object, object> ConstantPartitioner(int partition)
//        {
//            return (topic, key, value, numPartitions) => partition;
//        }

//        private Topology CreateSimpleTopology(int partition)
//        {
//            return topology
//                .AddSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
//                .AddProcessor("processor", define(new ForwardingProcessor()), "source")
//                .AddSink("sink", OUTPUT_TOPIC_1, ConstantPartitioner(partition), "processor");
//        }

//        private Topology CreateTimestampTopology(int partition)
//        {
//            return topology
//                .AddSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
//                .AddProcessor("processor", define(new TimestampProcessor()), "source")
//                .AddSink("sink", OUTPUT_TOPIC_1, ConstantPartitioner(partition), "processor");
//        }

//        private Topology CreateMultiProcessorTimestampTopology(int partition)
//        {
//            return topology
//                .AddSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
//                .AddProcessor("processor", define(new FanOutTimestampProcessor("child1", "child2")), "source")
//                .AddProcessor("child1", define(new ForwardingProcessor()), "processor")
//                .AddProcessor("child2", define(new TimestampProcessor()), "processor")
//                .AddSink("sink1", OUTPUT_TOPIC_1, ConstantPartitioner(partition), "child1")
//                .AddSink("sink2", OUTPUT_TOPIC_2, ConstantPartitioner(partition), "child2");
//        }

//        private Topology CreateMultiplexingTopology()
//        {
//            return topology
//                .AddSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
//                .AddProcessor("processor", define(new MultiplexingProcessor(2)), "source")
//                .AddSink("sink1", OUTPUT_TOPIC_1, "processor")
//                .AddSink("sink2", OUTPUT_TOPIC_2, "processor");
//        }

//        private Topology CreateMultiplexByNameTopology()
//        {
//            return topology
//                .AddSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
//                .AddProcessor("processor", define(new MultiplexByNameProcessor(2)), "source")
//                .AddSink("sink0", OUTPUT_TOPIC_1, "processor")
//                .AddSink("sink1", OUTPUT_TOPIC_2, "processor");
//        }

//        private Topology CreateStatefulTopology(string storeName)
//        {
//            return topology
//                .AddSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
//                .AddProcessor("processor", new StatefulProcessor(storeName), "source")
//                .addStateStore(Stores.KeyValueStoreBuilder(Stores.InMemoryKeyValueStore(storeName), Serdes.String(), Serdes.String()), "processor")
//                .AddSink("counts", OUTPUT_TOPIC_1, "processor");
//        }

//        private Topology CreateInternalRepartitioningTopology()
//        {
//            topology.AddSource("source", INPUT_TOPIC_1)
//                .AddSink("sink0", THROUGH_TOPIC_1, "source")
//                .AddSource("source1", THROUGH_TOPIC_1)
//                .AddSink("sink1", OUTPUT_TOPIC_1, "source1");

//            // use wrapper to get the internal topology builder to add internal topic
//            InternalTopologyBuilder internalTopologyBuilder = TopologyWrapper.getInternalTopologyBuilder(topology);
//            internalTopologyBuilder.AddInternalTopic(THROUGH_TOPIC_1);

//            return topology;
//        }

//        private Topology CreateInternalRepartitioningWithValueTimestampTopology()
//        {
//            topology.AddSource("source", INPUT_TOPIC_1)
//                    .AddProcessor("processor", new ValueTimestampProcessor(), "source")
//                    .AddSink("sink0", THROUGH_TOPIC_1, "processor")
//                    .AddSource("source1", THROUGH_TOPIC_1)
//                    .AddSink("sink1", OUTPUT_TOPIC_1, "source1");

//            // use wrapper to get the internal topology builder to add internal topic
//            InternalTopologyBuilder internalTopologyBuilder = TopologyWrapper.getInternalTopologyBuilder(topology);
//            internalTopologyBuilder.AddInternalTopic(THROUGH_TOPIC_1);

//            return topology;
//        }

//        private Topology CreateForwardToSourceTopology()
//        {
//            return topology.AddSource("source-1", INPUT_TOPIC_1)
//                    .AddSink("sink-1", OUTPUT_TOPIC_1, "source-1")
//                    .AddSource("source-2", OUTPUT_TOPIC_1)
//                    .AddSink("sink-2", OUTPUT_TOPIC_2, "source-2");
//        }

//        private Topology CreateSimpleMultiSourceTopology(int partition)
//        {
//            return topology.AddSource("source-1", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
//                    .AddProcessor("processor-1", new ForwardingProcessor(), "source-1")
//                    .AddSink("sink-1", OUTPUT_TOPIC_1, ConstantPartitioner(partition), "processor-1")
//                    .AddSource("source-2", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_2)
//                    .AddProcessor("processor-2", new ForwardingProcessor(), "source-2")
//                    .AddSink("sink-2", OUTPUT_TOPIC_2, ConstantPartitioner(partition), "processor-2");
//        }

//        private Topology CreateAddHeaderTopology()
//        {
//            return topology.AddSource("source-1", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
//                    .AddProcessor("processor-1", new AddHeaderProcessor(), "source-1")
//                    .AddSink("sink-1", OUTPUT_TOPIC_1, "processor-1");
//        }

//        /**
//         * A processor that simply forwards all messages to all children.
//         */
//        protected class ForwardingProcessor : AbstractProcessor<string, string>
//        {
//            public override void Process(string key, string value)
//            {
//                context.Forward(key, value);
//            }
//        }

//        /**
//         * A processor that simply forwards all messages to all children with advanced timestamps.
//         */
//        protected class TimestampProcessor : AbstractProcessor<string, string>
//        {
//            public override void Process(string key, string value)
//            {
//                context.Forward(key, value, To.All().WithTimestamp(context.Timestamp + 10));
//            }
//        }

//        protected class FanOutTimestampProcessor : AbstractProcessor<string, string>
//        {
//            private readonly string firstChild;
//            private readonly string secondChild;

//            FanOutTimestampProcessor(string firstChild,
//                                     string secondChild)
//            {
//                this.firstChild = firstChild;
//                this.secondChild = secondChild;
//            }


//            public override void Process(string key, string value)
//            {
//                context.Forward(key, value);
//                context.Forward(key, value, To.Child(firstChild).WithTimestamp(context.Timestamp + 5));
//                context.Forward(key, value, To.Child(secondChild));
//                context.Forward(key, value, To.All().WithTimestamp(context.Timestamp + 2));
//            }
//        }

//        protected class AddHeaderProcessor : AbstractProcessor<string, string>
//        {
//            public override void Process(string key, string value)
//            {
//                context.headers.Add(HEADER);
//                context.Forward(key, value);
//            }
//        }

//        /**
//         * A processor that removes custom timestamp information from messages and forwards modified messages to each child.
//         * A message contains custom timestamp information if the value is in ".*@[0-9]+" format.
//         */
//        protected class ValueTimestampProcessor : AbstractProcessor<string, string>
//        {
//            public override void Process(string key, string value)
//            {
//                context.Forward(key, value.Split("@")[0]);
//            }
//        }

//        /**
//         * A processor that forwards slightly-modified messages to each child.
//         */
//        protected class MultiplexingProcessor : AbstractProcessor<string, string>
//        {
//            private readonly int numChildren;

//            MultiplexingProcessor(int numChildren)
//            {
//                this.numChildren = numChildren;
//            }

//            // need to test deprecated code until removed

//            public override void Process(string key, string value)
//            {
//                for (int i = 0; i != numChildren; ++i)
//                {
//                    context.Forward(key, value + "(" + (i + 1) + ")", i);
//                }
//            }
//        }

//        /**
//         * A processor that forwards slightly-modified messages to each named child.
//         * Note: the children are assumed to be named "sink{child number}", e.g., sink1, or sink2, etc.
//         */
//        protected class MultiplexByNameProcessor : AbstractProcessor<string, string>
//        {
//            private readonly int numChildren;

//            MultiplexByNameProcessor(int numChildren)
//            {
//                this.numChildren = numChildren;
//            }

//            // need to test deprecated code until removed

//            public override void Process(string key, string value)
//            {
//                for (int i = 0; i != numChildren; ++i)
//                {
//                    context.Forward(key, value + "(" + (i + 1) + ")", "sink" + i);
//                }
//            }
//        }

//        /**
//         * A processor that stores each key-value pair in an in-memory key-value store registered with the context.
//         */
//        protected class StatefulProcessor : AbstractProcessor<string, string>
//        {
//            private IKeyValueStore<string, string> store;
//            private readonly string storeName;

//            StatefulProcessor(string storeName)
//            {
//                this.storeName = storeName;
//            }

//            public override void Init(IProcessorContext context)
//            {
//                base.Init(context);
//                store = (IKeyValueStore<string, string>)context.GetStateStore(storeName);
//            }


//            public override void Process(string key, string value)
//            {
//                store.Add(key, value);
//            }
//        }

//        private IProcessorSupplier<K, V> Define<K, V>(IKeyValueProcessor<K, V> processor)
//        {
//            return () => processor;
//        }

//        /**
//         * A custom timestamp extractor that extracts the timestamp from the record's value if the value is in ".*@[0-9]+"
//         * format. Otherwise, it returns the record's timestamp or the default timestamp if the record's timestamp is negative.
//        */
//        public class CustomTimestampExtractor : ITimestampExtractor
//        {
//            private const long DEFAULT_TIMESTAMP = 1000L;


//            public long Extract<K, V>(ConsumeResult<K, V> record, long partitionTime)
//            {
//                if (record.Value.ToString().matches(".*@[0-9]+"))
//                {
//                    return long.parseLong(record.Value.ToString().Split("@")[1]);
//                }

//                if (record.Timestamp >= 0L)
//                {
//                    return record.Timestamp;
//                }

//                return DEFAULT_TIMESTAMP;
//            }
//        }
//    }
//}
