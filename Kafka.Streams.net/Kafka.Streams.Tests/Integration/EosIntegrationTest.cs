//using Kafka.Streams.Configs;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.Tests.Helpers;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.Integration
//{
//    public class EosIntegrationTest
//    {
//        private static readonly int NUM_BROKERS = 3;
//        private static readonly int MAX_POLL_INTERVAL_MS = 5 * 1000;
//        private static readonly int MAX_WAIT_TIME_MS = 60 * 1000;


//        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(
//            NUM_BROKERS,
//            Utils.mkProperties(Collections.singletonMap("auto.Create.topics.enable", "false"))
//        );

//        private static string applicationId;
//        private static readonly int NUM_TOPIC_PARTITIONS = 2;
//        private static readonly string CONSUMER_GROUP_ID = "readCommitted";
//        private static readonly string SINGLE_PARTITION_INPUT_TOPIC = "singlePartitionInputTopic";
//        private static readonly string SINGLE_PARTITION_THROUGH_TOPIC = "singlePartitionThroughTopic";
//        private static readonly string SINGLE_PARTITION_OUTPUT_TOPIC = "singlePartitionOutputTopic";
//        private static readonly string MULTI_PARTITION_INPUT_TOPIC = "multiPartitionInputTopic";
//        private static readonly string MULTI_PARTITION_THROUGH_TOPIC = "multiPartitionThroughTopic";
//        private static readonly string MULTI_PARTITION_OUTPUT_TOPIC = "multiPartitionOutputTopic";
//        private readonly string storeName = "store";

//        private bool errorInjected;
//        private bool gcInjected;
//        private readonly bool doGC = true;
//        private int commitRequested;
//        private Throwable uncaughtException;

//        private int testNumber = 0;


//        public void CreateTopics()
//        {// throws Exception
//            applicationId = "appId-" + ++testNumber;
//            CLUSTER.deleteTopicsAndWait(
//                SINGLE_PARTITION_INPUT_TOPIC, MULTI_PARTITION_INPUT_TOPIC,
//                SINGLE_PARTITION_THROUGH_TOPIC, MULTI_PARTITION_THROUGH_TOPIC,
//                SINGLE_PARTITION_OUTPUT_TOPIC, MULTI_PARTITION_OUTPUT_TOPIC);

//            CLUSTER.CreateTopics(SINGLE_PARTITION_INPUT_TOPIC, SINGLE_PARTITION_THROUGH_TOPIC, SINGLE_PARTITION_OUTPUT_TOPIC);
//            CLUSTER.CreateTopic(MULTI_PARTITION_INPUT_TOPIC, NUM_TOPIC_PARTITIONS, 1);
//            CLUSTER.CreateTopic(MULTI_PARTITION_THROUGH_TOPIC, NUM_TOPIC_PARTITIONS, 1);
//            CLUSTER.CreateTopic(MULTI_PARTITION_OUTPUT_TOPIC, NUM_TOPIC_PARTITIONS, 1);
//        }

//        [Fact]
//        public void ShouldBeAbleToRunWithEosEnabled()
//        {// throws Exception
//            RunSimpleCopyTest(1, SINGLE_PARTITION_INPUT_TOPIC, null, SINGLE_PARTITION_OUTPUT_TOPIC);
//        }

//        [Fact]
//        public void ShouldBeAbleToRestartAfterClose()
//        {// throws Exception
//            RunSimpleCopyTest(2, SINGLE_PARTITION_INPUT_TOPIC, null, SINGLE_PARTITION_OUTPUT_TOPIC);
//        }

//        [Fact]
//        public void ShouldBeAbleToCommitToMultiplePartitions()
//        {// throws Exception
//            RunSimpleCopyTest(1, SINGLE_PARTITION_INPUT_TOPIC, null, MULTI_PARTITION_OUTPUT_TOPIC);
//        }

//        [Fact]
//        public void ShouldBeAbleToCommitMultiplePartitionOffsets()
//        {// throws Exception
//            RunSimpleCopyTest(1, MULTI_PARTITION_INPUT_TOPIC, null, SINGLE_PARTITION_OUTPUT_TOPIC);
//        }

//        [Fact]
//        public void ShouldBeAbleToRunWithTwoSubtopologies()
//        {// throws Exception
//            RunSimpleCopyTest(1, SINGLE_PARTITION_INPUT_TOPIC, SINGLE_PARTITION_THROUGH_TOPIC, SINGLE_PARTITION_OUTPUT_TOPIC);
//        }

//        [Fact]
//        public void ShouldBeAbleToRunWithTwoSubtopologiesAndMultiplePartitions()
//        {// throws Exception
//            RunSimpleCopyTest(1, MULTI_PARTITION_INPUT_TOPIC, MULTI_PARTITION_THROUGH_TOPIC, MULTI_PARTITION_OUTPUT_TOPIC);
//        }

//        private void RunSimpleCopyTest(int numberOfRestarts,
//                                       string inputTopic,
//                                       string throughTopic,
//                                       string outputTopic)
//        {// throws Exception
//            StreamsBuilder builder = new StreamsBuilder();
//            IIIKStream<K, V> input = builder.Stream(inputTopic);
//            IIIKStream<K, V> output = input;
//            if (throughTopic != null)
//            {
//                output = input.through(throughTopic);
//            }
//            output.To(outputTopic);

//            StreamsConfig properties = new StreamsConfig();
//            properties.Put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.StreamsConfig.ExactlyOnceConfig);
//            properties.Put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
//            properties.Put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
//            properties.Put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), 1);
//            properties.Put(StreamsConfig.consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), "1000");
//            properties.Put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");

//            for (int i = 0; i < numberOfRestarts; ++i)
//            {
//                StreamsConfig config = StreamsTestUtils.getStreamsConfig(
//                    applicationId,
//                    CLUSTER.bootstrapServers(),
//                    Serdes.LongSerde.getName(),
//                    Serdes.LongSerde.getName(),
//                    properties);

//                KafkaStreamsThread streams = new KafkaStreamsThread(builder.Build(), config);
//                streams.Start();

//                List<KeyValuePair<long, long>> inputData = prepareData(i * 100, i * 100 + 10L, 0L, 1L);

//                IntegrationTestUtils.ProduceKeyValuesSynchronously(
//                    inputTopic,
//                    inputData,
//                    TestUtils.ProducerConfig(CLUSTER.bootstrapServers(), Serdes.Long().Serializer, Serdes.Long().Serializer),
//                    CLUSTER.time
//                );

//                List<KeyValuePair<long, long>> committedRecords =
//                    IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(
//                        TestUtils.consumerConfig(
//                            CLUSTER.bootstrapServers(),
//                            CONSUMER_GROUP_ID,
//                            LongDeserializer,
//                            LongDeserializer,
//                            Utils.mkProperties(Collections.singletonMap(
//                                ConsumerConfig.ISOLATION_LEVEL_CONFIG,
//                                IsolationLevel.READ_COMMITTED.Name().toLowerCase(Locale.ROOT)))
//                            ),
//                        outputTopic,
//                        inputData.Count
//                    );

//                checkResultPerKey(committedRecords, inputData);
//            }
//        }

//        private void CheckResultPerKey(List<KeyValuePair<long, long>> result,
//                                       List<KeyValuePair<long, long>> expectedResult)
//        {
//            HashSet<long> allKeys = new HashSet<>();
//            addAllKeys(allKeys, result);
//            addAllKeys(allKeys, expectedResult);

//            foreach (long key in allKeys)
//            {
//                Assert.Equal(getAllRecordPerKey(key, result), getAllRecordPerKey(key, expectedResult));
//            }

//        }

//        private void AddAllKeys(HashSet<long> allKeys,
//                                List<KeyValuePair<long, long>> records)
//        {
//            foreach (KeyValuePair<long, long> record in records)
//            {
//                allKeys.Add(record.Key);
//            }
//        }

//        private List<KeyValuePair<long, long>> GetAllRecordPerKey(long key,
//                                                              List<KeyValuePair<long, long>> records)
//        {
//            List<KeyValuePair<long, long>> recordsPerKey = new List<>(records.Count);

//            foreach (KeyValuePair<long, long> record in records)
//            {
//                if (record.Key.Equals(key))
//                {
//                    recordsPerKey.Add(record);
//                }
//            }

//            return recordsPerKey;
//        }

//        [Fact]
//        public void ShouldBeAbleToPerformMultipleTransactions()
//        {// throws Exception
//            StreamsBuilder builder = new StreamsBuilder();
//            builder.Stream(SINGLE_PARTITION_INPUT_TOPIC).To(SINGLE_PARTITION_OUTPUT_TOPIC);

//            StreamsConfig properties = new StreamsConfig();
//            properties.Put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.StreamsConfig.ExactlyOnceConfig);
//            properties.Put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
//            properties.Put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
//            properties.Put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000");
//            properties.Put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

//            StreamsConfig config = StreamsTestUtils.getStreamsConfig(
//                applicationId,
//                CLUSTER.bootstrapServers(),
//                Serdes.LongSerde.getName(),
//                Serdes.LongSerde.getName(),
//                properties);

//            KafkaStreamsThread streams = new KafkaStreamsThread(builder.Build(), config);
//            streams.Start();

//            List<KeyValuePair<long, long>> firstBurstOfData = prepareData(0L, 5L, 0L);
//            List<KeyValuePair<long, long>> secondBurstOfData = prepareData(5L, 8L, 0L);

//            IntegrationTestUtils.ProduceKeyValuesSynchronously(
//                SINGLE_PARTITION_INPUT_TOPIC,
//                firstBurstOfData,
//                TestUtils.ProducerConfig(CLUSTER.bootstrapServers(), Serdes.Long().Serializer, Serdes.Long().Serializer),
//                CLUSTER.time
//            );

//            List<KeyValuePair<long, long>> firstCommittedRecords =
//                IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(
//                    TestUtils.consumerConfig(
//                        CLUSTER.bootstrapServers(),
//                        CONSUMER_GROUP_ID,
//                        LongDeserializer,
//                        LongDeserializer,
//                        Utils.mkProperties(Collections.singletonMap(
//                            ConsumerConfig.ISOLATION_LEVEL_CONFIG,
//                            IsolationLevel.READ_COMMITTED.Name().toLowerCase(Locale.ROOT)))
//                        ),
//                    SINGLE_PARTITION_OUTPUT_TOPIC,
//                    firstBurstOfData.Count
//                );

//            Assert.Equal(firstCommittedRecords, firstBurstOfData);

//            IntegrationTestUtils.ProduceKeyValuesSynchronously(
//                SINGLE_PARTITION_INPUT_TOPIC,
//                secondBurstOfData,
//                TestUtils.ProducerConfig(CLUSTER.bootstrapServers(), Serdes.Long().Serializer, Serdes.Long().Serializer),
//                CLUSTER.time
//            );

//            List<KeyValuePair<long, long>> secondCommittedRecords =
//                IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(
//                    TestUtils.consumerConfig(
//                        CLUSTER.bootstrapServers(),
//                        CONSUMER_GROUP_ID,
//                        LongDeserializer,
//                        LongDeserializer,
//                        Utils.mkProperties(Collections.singletonMap(
//                            ConsumerConfig.ISOLATION_LEVEL_CONFIG,
//                            IsolationLevel.READ_COMMITTED.Name().toLowerCase(Locale.ROOT)))
//                        ),
//                    SINGLE_PARTITION_OUTPUT_TOPIC,
//                    secondBurstOfData.Count
//                );

//            Assert.Equal(secondCommittedRecords, secondBurstOfData);
//        }

//        [Fact]
//        public void ShouldNotViolateEosIfOneTaskFails()
//        {// throws Exception
//         // this test writes 10 + 5 + 5 records per partition (running with 2 partitions)
//         // the app is supposed to copy All 40 records into the output topic
//         // the app commits after each 10 records per partition, and thus will have 2*5 uncommitted writes
//         //
//         // the failure gets inject after 20 committed and 30 uncommitted records got received
//         // => the failure only kills one thread
//         // after fail over, we should read 40 committed records (even if 50 record got written)

//            KafkaStreamsThread streams = getKafkaStreams(false, "appDir", 2);
//            streams.Start();

//            List<KeyValuePair<long, long>> committedDataBeforeFailure = prepareData(0L, 10L, 0L, 1L);
//            List<KeyValuePair<long, long>> uncommittedDataBeforeFailure = prepareData(10L, 15L, 0L, 1L);

//            List<KeyValuePair<long, long>> dataBeforeFailure = new List<KeyValuePair<long, long>>();
//            dataBeforeFailure.addAll(committedDataBeforeFailure);
//            dataBeforeFailure.addAll(uncommittedDataBeforeFailure);

//            List<KeyValuePair<long, long>> dataAfterFailure = prepareData(15L, 20L, 0L, 1L);

//            writeInputData(committedDataBeforeFailure);

//            TestUtils.WaitForCondition(
//                () => commitRequested.Get() == 2, MAX_WAIT_TIME_MS,
//                "SteamsTasks did not request commit.");

//            writeInputData(uncommittedDataBeforeFailure);

//            List<KeyValuePair<long, long>> uncommittedRecords = readResult(dataBeforeFailure.Count, null);
//            List<KeyValuePair<long, long>> committedRecords = readResult(committedDataBeforeFailure.Count, CONSUMER_GROUP_ID);

//            checkResultPerKey(committedRecords, committedDataBeforeFailure);
//            checkResultPerKey(uncommittedRecords, dataBeforeFailure);

//            errorInjected.set(true);
//            writeInputData(dataAfterFailure);

//            TestUtils.WaitForCondition(
//                () => uncaughtException != null, MAX_WAIT_TIME_MS,
//                "Should receive uncaught exception from one StreamThread.");

//            List<KeyValuePair<long, long>> allCommittedRecords = readResult(
//                committedDataBeforeFailure.Count + uncommittedDataBeforeFailure.Count + dataAfterFailure.Count,
//                CONSUMER_GROUP_ID + "_ALL");

//            List<KeyValuePair<long, long>> committedRecordsAfterFailure = readResult(
//                uncommittedDataBeforeFailure.Count + dataAfterFailure.Count,
//                CONSUMER_GROUP_ID);

//            List<KeyValuePair<long, long>> allExpectedCommittedRecordsAfterRecovery = new List<KeyValuePair<long, long>>();
//            allExpectedCommittedRecordsAfterRecovery.addAll(committedDataBeforeFailure);
//            allExpectedCommittedRecordsAfterRecovery.addAll(uncommittedDataBeforeFailure);
//            allExpectedCommittedRecordsAfterRecovery.addAll(dataAfterFailure);

//            List<KeyValuePair<long, long>> expectedCommittedRecordsAfterRecovery = new List<KeyValuePair<long, long>>();
//            expectedCommittedRecordsAfterRecovery.addAll(uncommittedDataBeforeFailure);
//            expectedCommittedRecordsAfterRecovery.addAll(dataAfterFailure);

//            checkResultPerKey(allCommittedRecords, allExpectedCommittedRecordsAfterRecovery);
//            checkResultPerKey(committedRecordsAfterFailure, expectedCommittedRecordsAfterRecovery);
//        }
//    }

//    [Fact]
//    public void ShouldNotViolateEosIfOneTaskFailsWithState()
//    {// throws Exception
//     // this test updates a store with 10 + 5 + 5 records per partition (running with 2 partitions)
//     // the app is supposed to emit All 40 update records into the output topic
//     // the app commits after each 10 records per partition, and thus will have 2*5 uncommitted writes
//     // and store updates (ie, another 5 uncommitted writes to a changelog topic per partition)
//     // in the uncommitted batch sending some data for the new key to validate that upon resuming they will not be shown up in the store
//     //
//     // the failure gets inject after 20 committed and 10 uncommitted records got received
//     // => the failure only kills one thread
//     // after fail over, we should read 40 committed records and the state stores should contain the correct sums
//     // per key (even if some records got processed twice)

//        KafkaStreamsThread streams = getKafkaStreams(true, "appDir", 2);
//        streams.Start();

//        List<KeyValuePair<long, long>> committedDataBeforeFailure = prepareData(0L, 10L, 0L, 1L);
//        List<KeyValuePair<long, long>> uncommittedDataBeforeFailure = prepareData(10L, 15L, 0L, 1L, 2L, 3L);

//        List<KeyValuePair<long, long>> dataBeforeFailure = new List<KeyValuePair<long, long>>();
//        dataBeforeFailure.addAll(committedDataBeforeFailure);
//        dataBeforeFailure.addAll(uncommittedDataBeforeFailure);

//        List<KeyValuePair<long, long>> dataAfterFailure = prepareData(15L, 20L, 0L, 1L);

//        writeInputData(committedDataBeforeFailure);

//        TestUtils.WaitForCondition(
//            () => commitRequested.Get() == 2, MAX_WAIT_TIME_MS,
//            "SteamsTasks did not request commit.");

//        writeInputData(uncommittedDataBeforeFailure);

//        List<KeyValuePair<long, long>> uncommittedRecords = readResult(dataBeforeFailure.Count, null);
//        List<KeyValuePair<long, long>> committedRecords = readResult(committedDataBeforeFailure.Count, CONSUMER_GROUP_ID);

//        List<KeyValuePair<long, long>> expectedResultBeforeFailure = computeExpectedResult(dataBeforeFailure);
//        checkResultPerKey(committedRecords, computeExpectedResult(committedDataBeforeFailure));
//        checkResultPerKey(uncommittedRecords, expectedResultBeforeFailure);
//        verifyStateStore(streams, getMaxPerKey(expectedResultBeforeFailure));

//        errorInjected.set(true);
//        writeInputData(dataAfterFailure);

//        TestUtils.WaitForCondition(
//            () => uncaughtException != null, MAX_WAIT_TIME_MS,
//            "Should receive uncaught exception from one StreamThread.");

//        List<KeyValuePair<long, long>> allCommittedRecords = readResult(
//            committedDataBeforeFailure.Count + uncommittedDataBeforeFailure.Count + dataAfterFailure.Count,
//            CONSUMER_GROUP_ID + "_ALL");

//        List<KeyValuePair<long, long>> committedRecordsAfterFailure = readResult(
//            uncommittedDataBeforeFailure.Count + dataAfterFailure.Count,
//            CONSUMER_GROUP_ID);

//        List<KeyValuePair<long, long>> allExpectedCommittedRecordsAfterRecovery = new List<KeyValuePair<long, long>>();
//        allExpectedCommittedRecordsAfterRecovery.addAll(committedDataBeforeFailure);
//        allExpectedCommittedRecordsAfterRecovery.addAll(uncommittedDataBeforeFailure);
//        allExpectedCommittedRecordsAfterRecovery.addAll(dataAfterFailure);

//        List<KeyValuePair<long, long>> expectedResult = computeExpectedResult(allExpectedCommittedRecordsAfterRecovery);

//        checkResultPerKey(allCommittedRecords, expectedResult);
//        checkResultPerKey(
//            committedRecordsAfterFailure,
//            expectedResult.subList(committedDataBeforeFailure.Count, expectedResult.Count));

//        verifyStateStore(streams, getMaxPerKey(expectedResult));
//    }
//}

//[Fact]
//public void ShouldNotViolateEosIfOneTaskGetsFencedUsingIsolatedAppInstances()
//{// throws Exception
// // this test writes 10 + 5 + 5 + 10 records per partition (running with 2 partitions)
// // the app is supposed to copy All 60 records into the output topic
// // the app commits after each 10 records per partition, and thus will have 2*5 uncommitted writes
// //
// // a GC pause gets inject after 20 committed and 30 uncommitted records got received
// // => the GC pause only affects one thread and should trigger a rebalance
// // after rebalancing, we should read 40 committed records (even if 50 record got written)
// //
// // afterwards, the "stalling" thread resumes, and another rebalance should get triggered
// // we write the remaining 20 records and verify to read 60 result records

//    KafkaStreamsThread streams1 = getKafkaStreams(false, "appDir1", 1);
//    KafkaStreamsThread streams2 = getKafkaStreams(false, "appDir2", 1);
//    streams1.Start();
//    streams2.Start();

//    List<KeyValuePair<long, long>> committedDataBeforeGC = prepareData(0L, 10L, 0L, 1L);
//    List<KeyValuePair<long, long>> uncommittedDataBeforeGC = prepareData(10L, 15L, 0L, 1L);

//    List<KeyValuePair<long, long>> dataBeforeGC = new List<KeyValuePair<long, long>>();
//    dataBeforeGC.addAll(committedDataBeforeGC);
//    dataBeforeGC.addAll(uncommittedDataBeforeGC);

//    List<KeyValuePair<long, long>> dataToTriggerFirstRebalance = prepareData(15L, 20L, 0L, 1L);

//    List<KeyValuePair<long, long>> dataAfterSecondRebalance = prepareData(20L, 30L, 0L, 1L);

//    writeInputData(committedDataBeforeGC);

//    TestUtils.WaitForCondition(
//        () => commitRequested.Get() == 2, MAX_WAIT_TIME_MS,
//        "SteamsTasks did not request commit.");

//    writeInputData(uncommittedDataBeforeGC);

//    List<KeyValuePair<long, long>> uncommittedRecords = readResult(dataBeforeGC.Count, null);
//    List<KeyValuePair<long, long>> committedRecords = readResult(committedDataBeforeGC.Count, CONSUMER_GROUP_ID);

//    checkResultPerKey(committedRecords, committedDataBeforeGC);
//    checkResultPerKey(uncommittedRecords, dataBeforeGC);

//    gcInjected.set(true);
//    writeInputData(dataToTriggerFirstRebalance);

//    TestUtils.WaitForCondition(
//        () => streams1.allMetadata().Count == 1
//            && streams2.allMetadata().Count == 1
//            && (streams1.allMetadata().iterator().MoveNext().topicPartitions().Count == 2
//                || streams2.allMetadata().iterator().MoveNext().topicPartitions().Count == 2),
//        MAX_WAIT_TIME_MS, "Should have rebalanced.");

//    List<KeyValuePair<long, long>> committedRecordsAfterRebalance = readResult(
//        uncommittedDataBeforeGC.Count + dataToTriggerFirstRebalance.Count,
//        CONSUMER_GROUP_ID);

//    List<KeyValuePair<long, long>> expectedCommittedRecordsAfterRebalance = new List<KeyValuePair<long, long>>();
//    expectedCommittedRecordsAfterRebalance.addAll(uncommittedDataBeforeGC);
//    expectedCommittedRecordsAfterRebalance.addAll(dataToTriggerFirstRebalance);

//    checkResultPerKey(committedRecordsAfterRebalance, expectedCommittedRecordsAfterRebalance);

//    doGC = false;
//    TestUtils.WaitForCondition(
//        () => streams1.allMetadata().Count == 1
//            && streams2.allMetadata().Count == 1
//            && streams1.allMetadata().iterator().MoveNext().topicPartitions().Count == 1
//            && streams2.allMetadata().iterator().MoveNext().topicPartitions().Count == 1,
//        MAX_WAIT_TIME_MS,
//        "Should have rebalanced.");

//    writeInputData(dataAfterSecondRebalance);

//    List<KeyValuePair<long, long>> allCommittedRecords = readResult(
//        committedDataBeforeGC.Count + uncommittedDataBeforeGC.Count
//        + dataToTriggerFirstRebalance.Count + dataAfterSecondRebalance.Count,
//        CONSUMER_GROUP_ID + "_ALL");

//    List<KeyValuePair<long, long>> allExpectedCommittedRecordsAfterRecovery = new List<KeyValuePair<long, long>>();
//    allExpectedCommittedRecordsAfterRecovery.addAll(committedDataBeforeGC);
//    allExpectedCommittedRecordsAfterRecovery.addAll(uncommittedDataBeforeGC);
//    allExpectedCommittedRecordsAfterRecovery.addAll(dataToTriggerFirstRebalance);
//    allExpectedCommittedRecordsAfterRecovery.addAll(dataAfterSecondRebalance);

//    checkResultPerKey(allCommittedRecords, allExpectedCommittedRecordsAfterRecovery);
//}

//private List<KeyValuePair<long, long>> prepareData(long fromInclusive,
//                                               long toExclusive,
//                                               params long[] keys)
//{
//    List<KeyValuePair<long, long>> data = new List<KeyValuePair<long, long>>();

//    foreach (long k in keys)
//    {
//        for (long v = fromInclusive; v < toExclusive; ++v)
//        {
//            data.Add(KeyValuePair.Create(k, v));
//        }
//    }

//    return data;
//}

//private KafkaStreamsThread getKafkaStreams(bool withState,
//                                     string appDir,
//                                     int numberOfStreamsThreads)
//{
//    commitRequested = new int(0);
//    errorInjected = new bool(false);
//    gcInjected = new bool(false);
//    StreamsBuilder builder = new StreamsBuilder();

//    string[] storeNames = null;
//    if (withState)
//    {
//        storeNames = new string[] { storeName };
//        IStoreBuilder<IKeyValueStore<long, long>> storeBuilder = Stores
//            .KeyValueStoreBuilder(Stores.PersistentKeyValueStore(storeName), Serdes.Long(), Serdes.Long())
//            .withCachingEnabled();

//        builder.AddStateStore(storeBuilder);
//    }

//    IIIKStream<K, V> input = builder.Stream(MULTI_PARTITION_INPUT_TOPIC);
//    input.transform(new TransformerSupplier<long, long, KeyValuePair<long, long>>()
//    {



//            public Transformer<long, long, KeyValuePair<long, long>> get()
//    {
//        return new Transformer<long, long, KeyValuePair<long, long>>() {
//                    ProcessorContext context;
//        IKeyValueStore<long, long> state = null;


//        public void Init(IProcessorContext context)
//        {
//            this.context = context;

//            if (withState)
//            {
//                state = (IKeyValueStore<long, long>)context.getStateStore(storeName);
//            }
//        }


//        public KeyValuePair<long, long> transform(long key,
//                                              long value)
//        {
//            if (gcInjected.compareAndSet(true, false))
//            {
//                while (doGC)
//                {
//                    try
//                    {
//                        Thread.Sleep(100);
//                    }
//                    catch (InterruptedException e)
//                    {
//                        throw new RuntimeException(e);
//                    }
//                }
//            }

//            if ((value + 1) % 10 == 0)
//            {
//                context.Commit();
//                commitRequested.incrementAndGet();
//            }

//            if (state != null)
//            {
//                long sum = state.Get(key);

//                if (sum == null)
//                {
//                    sum = value;
//                }
//                else
//                {
//                    sum += value;
//                }
//                state.Put(key, sum);
//                state.Flush();
//            }


//            if (errorInjected.compareAndSet(true, false))
//            {
//                // only tries to fail once on one of the task
//                throw new RuntimeException("Injected test exception.");
//            }

//            if (state != null)
//            {
//                return KeyValuePair.Create(key, state.Get(key));
//            }
//            else
//            {
//                return KeyValuePair.Create(key, value);
//            }
//        }


//        public void Close() { }
//    };
//}
//}, storeNames)
//            .To(SINGLE_PARTITION_OUTPUT_TOPIC);

//StreamsConfig properties = new StreamsConfig();
//properties.Put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.StreamsConfig.ExactlyOnceConfig);
//    properties.Put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numberOfStreamsThreads);
//    properties.Put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, long.MaxValue);
//    properties.Put(StreamsConfig.consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), "1000");
//    properties.Put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
//    properties.Put(StreamsConfig.consumerPrefix(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG), 5 * 1000);
//    properties.Put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 5 * 1000 - 1);
//    properties.Put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG), MAX_POLL_INTERVAL_MS);
//    properties.Put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
//    properties.Put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory().getPath() + Path.DirectorySeparatorChar + appDir);
//    properties.Put(StreamsConfig.APPLICATION_SERVER_CONFIG, "dummy:2142");

//    StreamsConfig config = StreamsTestUtils.getStreamsConfig(
//        applicationId,
//        CLUSTER.bootstrapServers(),
//        Serdes.LongSerde.getName(),
//        Serdes.LongSerde.getName(),
//        properties);

//KafkaStreamsThread streams = new KafkaStreamsThread(builder.Build(), config);

//streams.setUncaughtExceptionHandler((t, e) =>
//    {
//        if (uncaughtException != null)
//        {
//            e.printStackTrace(System.Console.Error);
//            Assert.True(false, "Should only get one uncaught exception from Streams.");
//        }
//        uncaughtException = e;
//    });

//    return streams;
//}

//private void WriteInputData(List<KeyValuePair<long, long>> records)
//{// throws Exception
//    IntegrationTestUtils.ProduceKeyValuesSynchronously(
//        MULTI_PARTITION_INPUT_TOPIC,
//        records,
//        TestUtils.ProducerConfig(CLUSTER.bootstrapServers(), Serdes.Long().Serializer, Serdes.Long().Serializer),
//        CLUSTER.time
//    );
//}

//private List<KeyValuePair<long, long>> ReadResult(int numberOfRecords,
//                                              string groupId)
//{// throws Exception
//    if (groupId != null)
//    {
//        return IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(
//            TestUtils.consumerConfig(
//                CLUSTER.bootstrapServers(),
//                groupId,
//                LongDeserializer,
//                LongDeserializer,
//                Utils.mkProperties(Collections.singletonMap(
//                    ConsumerConfig.ISOLATION_LEVEL_CONFIG,
//                    IsolationLevel.READ_COMMITTED.Name().toLowerCase(Locale.ROOT)))),
//            SINGLE_PARTITION_OUTPUT_TOPIC,
//            numberOfRecords
//        );
//    }

//    // read uncommitted
//    return IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(
//        TestUtils.consumerConfig(CLUSTER.bootstrapServers(), LongDeserializer, LongDeserializer),
//        SINGLE_PARTITION_OUTPUT_TOPIC,
//        numberOfRecords
//    );
//}

//private List<KeyValuePair<long, long>> ComputeExpectedResult(List<KeyValuePair<long, long>> input)
//{
//    List<KeyValuePair<long, long>> expectedResult = new List<>(input.Count);

//    Dictionary<long, long> sums = new HashMap<>();

//    foreach (KeyValuePair<long, long> record in input)
//    {
//        long sum = sums.Get(record.Key);
//        if (sum == null)
//        {
//            sum = record.Value;
//        }
//        else
//        {
//            sum += record.Value;
//        }
//        sums.Put(record.Key, sum);
//        expectedResult.Add(KeyValuePair.Create(record.Key, sum));
//    }

//    return expectedResult;
//}

//private HashSet<KeyValuePair<long, long>> GetMaxPerKey(List<KeyValuePair<long, long>> input)
//{
//    HashSet<KeyValuePair<long, long>> expectedResult = new HashSet<>(input.Count);

//    Dictionary<long, long> maxPerKey = new HashMap<>();

//    foreach (KeyValuePair<long, long> record in input)
//    {
//        long max = maxPerKey.Get(record.Key);
//        if (max == null || record.Value > max)
//        {
//            maxPerKey.Put(record.Key, record.Value);
//        }

//    }

//    foreach (Map.Entry<long, long> max in maxPerKey)
//    {
//        expectedResult.Add(KeyValuePair.Create(max.Key, max.Value));
//    }

//    return expectedResult;
//}

//private void VerifyStateStore(KafkaStreamsThread streams,
//                              HashSet<KeyValuePair<long, long>> expectedStoreContent)
//{
//    IReadOnlyKeyValueStore<long, long> store = null;

//    long maxWaitingTime = System.currentTimeMillis() + 300000L;
//    while (System.currentTimeMillis() < maxWaitingTime)
//    {
//        try
//        {
//            store = streams.Store(storeName, QueryableStoreTypes.KeyValueStore);
//            break;
//        }
//        catch (InvalidStateStoreException okJustRetry)
//        {
//            try
//            {
//                Thread.Sleep(5000L);
//            }
//            catch (Exception ignore) { }
//        }
//    }

//    Assert.NotNull(store);

//    IKeyValueIterator<long, long> it = store.All();
//    while (it.MoveNext())
//    {
//        Assert.True(expectedStoreContent.remove(it.MoveNext()));
//    }

//    Assert.True(expectedStoreContent.IsEmpty());
//}

//}
