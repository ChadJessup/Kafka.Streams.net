//namespace Kafka.Streams.Tests.Integration
//{
//    /*






//    *

//    *





//    */




























































//    public class RestoreIntegrationTest
//    {
//        private const int NUM_BROKERS = 1;

//        private const string APPID = "restore-test";


//        public static EmbeddedKafkaCluster CLUSTER =
//                new EmbeddedKafkaCluster(NUM_BROKERS);
//        private const string INPUT_STREAM = "input-stream";
//        private const string INPUT_STREAM_2 = "input-stream-2";
//        private readonly int numberOfKeys = 10000;
//        private KafkaStreams kafkaStreams;


//        public static void CreateTopics()
//        {// throws InterruptedException
//            CLUSTER.createTopic(INPUT_STREAM, 2, 1);
//            CLUSTER.createTopic(INPUT_STREAM_2, 2, 1);
//            CLUSTER.createTopic(APPID + "-store-changelog", 2, 1);
//        }

//        private StreamsConfig Props(string applicationId)
//        {
//            StreamsConfig streamsConfiguration = new StreamsConfig();
//            streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
//            streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
//            streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
//            streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory(applicationId).getPath());
//            streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Int().getClass());
//            streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Int().getClass());
//            streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
//            streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//            return streamsConfiguration;
//        }


//        public void Shutdown()
//        {
//            if (kafkaStreams != null)
//            {
//                kafkaStreams.close(Duration.ofSeconds(30));
//            }
//        }

//        [Fact]
//        public void ShouldRestoreStateFromSourceTopic()
//        {// throws Exception
//            AtomicInteger numReceived = new AtomicInteger(0);
//            StreamsBuilder builder = new StreamsBuilder();

//            StreamsConfig props = props(APPID);
//            props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);

//            // restoring from 1000 to 4000 (committed), and then process from 4000 to 5000 on each of the two partitions
//            int offsetLimitDelta = 1000;
//            int offsetCheckpointed = 1000;
//            createStateForRestoration(INPUT_STREAM);
//            setCommittedOffset(INPUT_STREAM, offsetLimitDelta);

//            StateDirectory stateDirectory = new StateDirectory(new StreamsConfig(props), new MockTime(), true);
//            new OffsetCheckpoint(new File(stateDirectory.directoryForTask(new TaskId(0, 0)), ".checkpoint"))
//                    .write(Collections.singletonMap(new TopicPartition(INPUT_STREAM, 0), (long)offsetCheckpointed));
//            new OffsetCheckpoint(new File(stateDirectory.directoryForTask(new TaskId(0, 1)), ".checkpoint"))
//                    .write(Collections.singletonMap(new TopicPartition(INPUT_STREAM, 1), (long)offsetCheckpointed));

//            CountDownLatch startupLatch = new CountDownLatch(1);
//            CountDownLatch shutdownLatch = new CountDownLatch(1);

//            builder.table(INPUT_STREAM, Materialized<int, int, IKeyValueStore<Bytes, byte[]>>.As("store").WithKeySerde(Serdes.Int()).withValueSerde(Serdes.Int()))
//                    .toStream()
//                    .ForEach((key, value) =>
//                    {
//                        if (numReceived.incrementAndGet() == 2 * offsetLimitDelta)
//                        {
//                            shutdownLatch.countDown();
//                        }
//                    });

//            kafkaStreams = new KafkaStreams(builder.Build(props), props);
//            kafkaStreams.setStateListener((newState, oldState) =>
//            {
//                if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING)
//                {
//                    startupLatch.countDown();
//                }
//            });

//            AtomicLong restored = new AtomicLong(0);
//            kafkaStreams.setGlobalStateRestoreListener(new StateRestoreListener()
//            {


//            public void onRestoreStart(TopicPartition topicPartition, string storeName, long startingOffset, long endingOffset)
//            {

//            }


//            public void onBatchRestored(TopicPartition topicPartition, string storeName, long batchEndOffset, long numRestored)
//            {

//            }


//            public void onRestoreEnd(TopicPartition topicPartition, string storeName, long totalRestored)
//            {
//                restored.addAndGet(totalRestored);
//            }
//        });
//        kafkaStreams.start();

//        Assert.True(startupLatch.await(30, TimeUnit.SECONDS));
//        Assert.Equal(restored.Get(), ((long) numberOfKeys - offsetLimitDelta* 2 - offsetCheckpointed* 2));

//        Assert.True(shutdownLatch.await(30, TimeUnit.SECONDS));
//        Assert.Equal(numReceived.Get(), (offsetLimitDelta* 2));
//    }

//    [Fact]
//    public void ShouldRestoreStateFromChangelogTopic()
//    {// throws Exception
//        AtomicInteger numReceived = new AtomicInteger(0);
//        StreamsBuilder builder = new StreamsBuilder();

//        StreamsConfig props = props(APPID);

//        // restoring from 1000 to 5000, and then process from 5000 to 10000 on each of the two partitions
//        int offsetCheckpointed = 1000;
//        createStateForRestoration(APPID + "-store-changelog");
//        createStateForRestoration(INPUT_STREAM);

//        StateDirectory stateDirectory = new StateDirectory(new StreamsConfig(props), new MockTime(), true);
//        new OffsetCheckpoint(new File(stateDirectory.directoryForTask(new TaskId(0, 0)), ".checkpoint"))
//                .write(Collections.singletonMap(new TopicPartition(APPID + "-store-changelog", 0), (long)offsetCheckpointed));
//        new OffsetCheckpoint(new File(stateDirectory.directoryForTask(new TaskId(0, 1)), ".checkpoint"))
//                .write(Collections.singletonMap(new TopicPartition(APPID + "-store-changelog", 1), (long)offsetCheckpointed));

//        CountDownLatch startupLatch = new CountDownLatch(1);
//        CountDownLatch shutdownLatch = new CountDownLatch(1);

//        builder.table(INPUT_STREAM, Consumed.With(Serdes.Int(), Serdes.Int()), Materialized.As("store"))
//                .toStream()
//                .ForEach((key, value) =>
//                {
//                    if (numReceived.incrementAndGet() == numberOfKeys)
//                    {
//                        shutdownLatch.countDown();
//                    }
//                });

//        kafkaStreams = new KafkaStreams(builder.Build(), props);
//        kafkaStreams.setStateListener((newState, oldState) =>
//        {
//            if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING)
//            {
//                startupLatch.countDown();
//            }
//        });

//        AtomicLong restored = new AtomicLong(0);
//        kafkaStreams.setGlobalStateRestoreListener(new StateRestoreListener()
//        {


//            public void onRestoreStart(TopicPartition topicPartition, string storeName, long startingOffset, long endingOffset)
//        {

//        }


//        public void onBatchRestored(TopicPartition topicPartition, string storeName, long batchEndOffset, long numRestored)
//        {

//        }


//        public void onRestoreEnd(TopicPartition topicPartition, string storeName, long totalRestored)
//        {
//            restored.addAndGet(totalRestored);
//        }
//    });
//        kafkaStreams.start();

//        Assert.True(startupLatch.await(30, TimeUnit.SECONDS));
//        Assert.Equal(restored.Get(), ((long) numberOfKeys - 2 * offsetCheckpointed));

//        Assert.True(shutdownLatch.await(30, TimeUnit.SECONDS));
//        Assert.Equal(numReceived.Get(), (numberOfKeys));
//    }


//    [Fact]
//    public void ShouldSuccessfullyStartWhenLoggingDisabled()
//    {// throws InterruptedException
//        StreamsBuilder builder = new StreamsBuilder();

//        KStream<int, int> stream = builder.Stream(INPUT_STREAM);
//        stream.groupByKey()
//                .reduce(
//                    (value1, value2) => value1 + value2,
//                    Materialized<int, int, IKeyValueStore<Bytes, byte[]>>.As("reduce-store").withLoggingDisabled());

//        CountDownLatch startupLatch = new CountDownLatch(1);
//        kafkaStreams = new KafkaStreams(builder.Build(), props(APPID));
//        kafkaStreams.setStateListener((newState, oldState) =>
//        {
//            if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING)
//            {
//                startupLatch.countDown();
//            }
//        });

//        kafkaStreams.start();

//        Assert.True(startupLatch.await(30, TimeUnit.SECONDS));
//    }

//    [Fact]
//    public void ShouldProcessDataFromStoresWithLoggingDisabled()
//    {// throws InterruptedException, ExecutionException

//        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_STREAM_2,
//                                                           Array.asList(KeyValuePair.Create(1, 1),
//                                                                         KeyValuePair.Create(2, 2),
//                                                                         KeyValuePair.Create(3, 3)),
//                                                           TestUtils.producerConfig(CLUSTER.bootstrapServers(),
//                                                                                    IntegerSerializer,
//                                                                                    IntegerSerializer),
//                                                           CLUSTER.time);

//        IKeyValueBytesStoreSupplier lruMapSupplier = Stores.lruMap(INPUT_STREAM_2, 10);

//        IStoreBuilder<IKeyValueStore<int, int>> storeBuilder = new KeyValueStoreBuilder<>(lruMapSupplier,
//                                                                                                      Serdes.Int(),
//                                                                                                      Serdes.Int(),
//                                                                                                      CLUSTER.time)
//                .withLoggingDisabled();

//        StreamsBuilder streamsBuilder = new StreamsBuilder();

//        streamsBuilder.addStateStore(storeBuilder);

//        KStream<int, int> stream = streamsBuilder.Stream(INPUT_STREAM_2);
//        CountDownLatch processorLatch = new CountDownLatch(3);
//        stream.process(() => new KeyValueStoreProcessor(INPUT_STREAM_2, processorLatch), INPUT_STREAM_2);

//        Topology topology = streamsBuilder.Build();

//        kafkaStreams = new KafkaStreams(topology, props(APPID + "-logging-disabled"));

//        CountDownLatch latch = new CountDownLatch(1);
//        kafkaStreams.setStateListener((newState, oldState) =>
//        {
//            if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING)
//            {
//                latch.countDown();
//            }
//        });
//        kafkaStreams.start();

//        latch.await(30, TimeUnit.SECONDS);

//        Assert.True(processorLatch.await(30, TimeUnit.SECONDS));

//    }


//    public static class KeyValueStoreProcessor : Processor<int, int>
//    {

//        private readonly string topic;
//        private CountDownLatch processorLatch;

//        private IKeyValueStore<int, int> store;

//        KeyValueStoreProcessor(string topic, CountDownLatch processorLatch)
//        {
//            this.topic = topic;
//            this.processorLatch = processorLatch;
//        }



//        public void Init(ProcessorContext context)
//        {
//            this.store = (IKeyValueStore<int, int>)context.getStateStore(topic);
//        }


//        public void Process(int key, int value)
//        {
//            if (key != null)
//            {
//                store.put(key, value);
//                processorLatch.countDown();
//            }
//        }


//        public void Close() { }
//    }

//    private void CreateStateForRestoration(string changelogTopic)
//    {
//        StreamsConfig producerConfig = new StreamsConfig();
//        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());

//        try (KafkaProducer<int, int> producer =
//                     new KafkaProducer<>(producerConfig, new IntegerSerializer(), new IntegerSerializer())) {

//            for (int i = 0; i < numberOfKeys; i++)
//            {
//                producer.send(new ProducerRecord<>(changelogTopic, i, i));
//            }
//        }
//        }

//    private void setCommittedOffset(string topic, int limitDelta)
//        {
//            StreamsConfig consumerConfig = new StreamsConfig();
//            consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
//            consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, APPID);
//            consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, "commit-consumer");
//            consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer);
//            consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer);

//            Consumer<int, int> consumer = new KafkaConsumer<>(consumerConfig);
//            List<TopicPartition> partitions = Array.asList(
//                new TopicPartition(topic, 0),
//                new TopicPartition(topic, 1));

//            consumer.assign(partitions);
//            consumer.seekToEnd(partitions);

//            foreach (TopicPartition partition in partitions)
//            {
//                long position = consumer.position(partition);
//                consumer.seek(partition, position - limitDelta);
//            }

//            consumer.commitSync();
//            consumer.close();
//        }

//    }
//}
///*
