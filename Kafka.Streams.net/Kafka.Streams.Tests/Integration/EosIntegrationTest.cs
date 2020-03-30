using Kafka.Streams.Configs;
using Kafka.Streams.KStream;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Integration
{
    public class EosIntegrationTest
    {
        private static readonly int NUM_BROKERS = 3;
        private static readonly int MAX_POLL_INTERVAL_MS = 5 * 1000;
        private static readonly int MAX_WAIT_TIME_MS = 60 * 1000;


        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(
            NUM_BROKERS,
            Utils.mkProperties(Collections.singletonMap("auto.create.topics.enable", "false"))
        );

        private static string applicationId;
        private static readonly int NUM_TOPIC_PARTITIONS = 2;
        private static readonly string CONSUMER_GROUP_ID = "readCommitted";
        private static readonly string SINGLE_PARTITION_INPUT_TOPIC = "singlePartitionInputTopic";
        private static readonly string SINGLE_PARTITION_THROUGH_TOPIC = "singlePartitionThroughTopic";
        private static readonly string SINGLE_PARTITION_OUTPUT_TOPIC = "singlePartitionOutputTopic";
        private static readonly string MULTI_PARTITION_INPUT_TOPIC = "multiPartitionInputTopic";
        private static readonly string MULTI_PARTITION_THROUGH_TOPIC = "multiPartitionThroughTopic";
        private static readonly string MULTI_PARTITION_OUTPUT_TOPIC = "multiPartitionOutputTopic";
        private readonly string storeName = "store";

        private AtomicBoolean errorInjected;
        private AtomicBoolean gcInjected;
        private readonly bool doGC = true;
        private AtomicInteger commitRequested;
        private Throwable uncaughtException;

        private int testNumber = 0;


        public void CreateTopics()
        {// throws Exception
            applicationId = "appId-" + ++testNumber;
            CLUSTER.deleteTopicsAndWait(
                SINGLE_PARTITION_INPUT_TOPIC, MULTI_PARTITION_INPUT_TOPIC,
                SINGLE_PARTITION_THROUGH_TOPIC, MULTI_PARTITION_THROUGH_TOPIC,
                SINGLE_PARTITION_OUTPUT_TOPIC, MULTI_PARTITION_OUTPUT_TOPIC);

            CLUSTER.createTopics(SINGLE_PARTITION_INPUT_TOPIC, SINGLE_PARTITION_THROUGH_TOPIC, SINGLE_PARTITION_OUTPUT_TOPIC);
            CLUSTER.createTopic(MULTI_PARTITION_INPUT_TOPIC, NUM_TOPIC_PARTITIONS, 1);
            CLUSTER.createTopic(MULTI_PARTITION_THROUGH_TOPIC, NUM_TOPIC_PARTITIONS, 1);
            CLUSTER.createTopic(MULTI_PARTITION_OUTPUT_TOPIC, NUM_TOPIC_PARTITIONS, 1);
        }

        [Xunit.Fact]
        public void ShouldBeAbleToRunWithEosEnabled()
        {// throws Exception
            RunSimpleCopyTest(1, SINGLE_PARTITION_INPUT_TOPIC, null, SINGLE_PARTITION_OUTPUT_TOPIC);
        }

        [Xunit.Fact]
        public void ShouldBeAbleToRestartAfterClose()
        {// throws Exception
            RunSimpleCopyTest(2, SINGLE_PARTITION_INPUT_TOPIC, null, SINGLE_PARTITION_OUTPUT_TOPIC);
        }

        [Xunit.Fact]
        public void ShouldBeAbleToCommitToMultiplePartitions()
        {// throws Exception
            RunSimpleCopyTest(1, SINGLE_PARTITION_INPUT_TOPIC, null, MULTI_PARTITION_OUTPUT_TOPIC);
        }

        [Xunit.Fact]
        public void ShouldBeAbleToCommitMultiplePartitionOffsets()
        {// throws Exception
            RunSimpleCopyTest(1, MULTI_PARTITION_INPUT_TOPIC, null, SINGLE_PARTITION_OUTPUT_TOPIC);
        }

        [Xunit.Fact]
        public void ShouldBeAbleToRunWithTwoSubtopologies()
        {// throws Exception
            RunSimpleCopyTest(1, SINGLE_PARTITION_INPUT_TOPIC, SINGLE_PARTITION_THROUGH_TOPIC, SINGLE_PARTITION_OUTPUT_TOPIC);
        }

        [Xunit.Fact]
        public void ShouldBeAbleToRunWithTwoSubtopologiesAndMultiplePartitions()
        {// throws Exception
            RunSimpleCopyTest(1, MULTI_PARTITION_INPUT_TOPIC, MULTI_PARTITION_THROUGH_TOPIC, MULTI_PARTITION_OUTPUT_TOPIC);
        }

        private void RunSimpleCopyTest(int numberOfRestarts,
                                       string inputTopic,
                                       string throughTopic,
                                       string outputTopic)
        {// throws Exception
            StreamsBuilder builder = new StreamsBuilder();
            KStream<long, long> input = builder.stream(inputTopic);
            KStream<long, long> output = input;
            if (throughTopic != null)
            {
                output = input.through(throughTopic);
            }
            output.to(outputTopic);

            Properties properties = new Properties();
            properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
            properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
            properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
            properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), 1);
            properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), "1000");
            properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");

            for (int i = 0; i < numberOfRestarts; ++i)
            {
                Properties config = StreamsTestUtils.getStreamsConfig(
                    applicationId,
                    CLUSTER.bootstrapServers(),
                    Serdes.LongSerde.getName(),
                    Serdes.LongSerde.getName(),
                    properties);

                try
                {
                    (KafkaStreams streams = new KafkaStreams(builder.build(), config));
            streams.start();

            List<KeyValuePair<long, long>> inputData = prepareData(i * 100, i * 100 + 10L, 0L, 1L);

            IntegrationTestUtils.produceKeyValuesSynchronously(
                inputTopic,
                inputData,
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer, LongSerializer),
                CLUSTER.time
            );

            List<KeyValuePair<long, long>> committedRecords =
                IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                    TestUtils.consumerConfig(
                        CLUSTER.bootstrapServers(),
                        CONSUMER_GROUP_ID,
                        LongDeserializer,
                        LongDeserializer,
                        Utils.mkProperties(Collections.singletonMap(
                            ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                            IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT)))
                        ),
                    outputTopic,
                    inputData.Count
                );

            checkResultPerKey(committedRecords, inputData);
        }
    }
}

private void CheckResultPerKey(List<KeyValuePair<long, long>> result,
                               List<KeyValuePair<long, long>> expectedResult)
{
    HashSet<long> allKeys = new HashSet<>();
    addAllKeys(allKeys, result);
    addAllKeys(allKeys, expectedResult);

    foreach (long key in allKeys)
    {
        Assert.Equal(getAllRecordPerKey(key, result), (getAllRecordPerKey(key, expectedResult)));
    }

}

private void AddAllKeys(HashSet<long> allKeys,
                        List<KeyValuePair<long, long>> records)
{
    foreach (KeyValuePair<long, long> record in records)
    {
        allKeys.add(record.Key);
    }
}

private List<KeyValuePair<long, long>> GetAllRecordPerKey(long key,
                                                      List<KeyValuePair<long, long>> records)
{
    List<KeyValuePair<long, long>> recordsPerKey = new ArrayList<>(records.Count);

    foreach (KeyValuePair<long, long> record in records)
    {
        if (record.key.equals(key))
        {
            recordsPerKey.add(record);
        }
    }

    return recordsPerKey;
}

[Xunit.Fact]
public void ShouldBeAbleToPerformMultipleTransactions()
{// throws Exception
    StreamsBuilder builder = new StreamsBuilder();
    builder.stream(SINGLE_PARTITION_INPUT_TOPIC).to(SINGLE_PARTITION_OUTPUT_TOPIC);

    Properties properties = new Properties();
    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
    properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    properties.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    Properties config = StreamsTestUtils.getStreamsConfig(
        applicationId,
        CLUSTER.bootstrapServers(),
        Serdes.LongSerde.getName(),
        Serdes.LongSerde.getName(),
        properties);

    try
    {
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        List<KeyValuePair<long, long>> firstBurstOfData = prepareData(0L, 5L, 0L);
        List<KeyValuePair<long, long>> secondBurstOfData = prepareData(5L, 8L, 0L);

        IntegrationTestUtils.produceKeyValuesSynchronously(
            SINGLE_PARTITION_INPUT_TOPIC,
            firstBurstOfData,
            TestUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer, LongSerializer),
            CLUSTER.time
        );

        List<KeyValuePair<long, long>> firstCommittedRecords =
            IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                TestUtils.consumerConfig(
                    CLUSTER.bootstrapServers(),
                    CONSUMER_GROUP_ID,
                    LongDeserializer,
                    LongDeserializer,
                    Utils.mkProperties(Collections.singletonMap(
                        ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                        IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT)))
                    ),
                SINGLE_PARTITION_OUTPUT_TOPIC,
                firstBurstOfData.Count
            );

        Assert.Equal(firstCommittedRecords, (firstBurstOfData));

        IntegrationTestUtils.produceKeyValuesSynchronously(
            SINGLE_PARTITION_INPUT_TOPIC,
            secondBurstOfData,
            TestUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer, LongSerializer),
            CLUSTER.time
        );

        List<KeyValuePair<long, long>> secondCommittedRecords =
            IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                TestUtils.consumerConfig(
                    CLUSTER.bootstrapServers(),
                    CONSUMER_GROUP_ID,
                    LongDeserializer,
                    LongDeserializer,
                    Utils.mkProperties(Collections.singletonMap(
                        ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                        IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT)))
                    ),
                SINGLE_PARTITION_OUTPUT_TOPIC,
                secondBurstOfData.Count
            );

        Assert.Equal(secondCommittedRecords, (secondBurstOfData));
    }
    }

[Xunit.Fact]
public void ShouldNotViolateEosIfOneTaskFails()
{// throws Exception
 // this test writes 10 + 5 + 5 records per partition (running with 2 partitions)
 // the app is supposed to copy all 40 records into the output topic
 // the app commits after each 10 records per partition, and thus will have 2*5 uncommitted writes
 //
 // the failure gets inject after 20 committed and 30 uncommitted records got received
 // => the failure only kills one thread
 // after fail over, we should read 40 committed records (even if 50 record got written)

    try
    {
        (KafkaStreams streams = getKafkaStreams(false, "appDir", 2));
        streams.start();

        List<KeyValuePair<long, long>> committedDataBeforeFailure = prepareData(0L, 10L, 0L, 1L);
        List<KeyValuePair<long, long>> uncommittedDataBeforeFailure = prepareData(10L, 15L, 0L, 1L);

        List<KeyValuePair<long, long>> dataBeforeFailure = new ArrayList<>();
        dataBeforeFailure.addAll(committedDataBeforeFailure);
        dataBeforeFailure.addAll(uncommittedDataBeforeFailure);

        List<KeyValuePair<long, long>> dataAfterFailure = prepareData(15L, 20L, 0L, 1L);

        writeInputData(committedDataBeforeFailure);

        TestUtils.waitForCondition(
            () => commitRequested.get() == 2, MAX_WAIT_TIME_MS,
            "SteamsTasks did not request commit.");

        writeInputData(uncommittedDataBeforeFailure);

        List<KeyValuePair<long, long>> uncommittedRecords = readResult(dataBeforeFailure.Count, null);
        List<KeyValuePair<long, long>> committedRecords = readResult(committedDataBeforeFailure.Count, CONSUMER_GROUP_ID);

        checkResultPerKey(committedRecords, committedDataBeforeFailure);
        checkResultPerKey(uncommittedRecords, dataBeforeFailure);

        errorInjected.set(true);
        writeInputData(dataAfterFailure);

        TestUtils.waitForCondition(
            () => uncaughtException != null, MAX_WAIT_TIME_MS,
            "Should receive uncaught exception from one StreamThread.");

        List<KeyValuePair<long, long>> allCommittedRecords = readResult(
            committedDataBeforeFailure.Count + uncommittedDataBeforeFailure.Count + dataAfterFailure.Count,
            CONSUMER_GROUP_ID + "_ALL");

        List<KeyValuePair<long, long>> committedRecordsAfterFailure = readResult(
            uncommittedDataBeforeFailure.Count + dataAfterFailure.Count,
            CONSUMER_GROUP_ID);

        List<KeyValuePair<long, long>> allExpectedCommittedRecordsAfterRecovery = new ArrayList<>();
        allExpectedCommittedRecordsAfterRecovery.addAll(committedDataBeforeFailure);
        allExpectedCommittedRecordsAfterRecovery.addAll(uncommittedDataBeforeFailure);
        allExpectedCommittedRecordsAfterRecovery.addAll(dataAfterFailure);

        List<KeyValuePair<long, long>> expectedCommittedRecordsAfterRecovery = new ArrayList<>();
        expectedCommittedRecordsAfterRecovery.addAll(uncommittedDataBeforeFailure);
        expectedCommittedRecordsAfterRecovery.addAll(dataAfterFailure);

        checkResultPerKey(allCommittedRecords, allExpectedCommittedRecordsAfterRecovery);
        checkResultPerKey(committedRecordsAfterFailure, expectedCommittedRecordsAfterRecovery);
    }
    }

[Xunit.Fact]
public void ShouldNotViolateEosIfOneTaskFailsWithState()
{// throws Exception
 // this test updates a store with 10 + 5 + 5 records per partition (running with 2 partitions)
 // the app is supposed to emit all 40 update records into the output topic
 // the app commits after each 10 records per partition, and thus will have 2*5 uncommitted writes
 // and store updates (ie, another 5 uncommitted writes to a changelog topic per partition)
 // in the uncommitted batch sending some data for the new key to validate that upon resuming they will not be shown up in the store
 //
 // the failure gets inject after 20 committed and 10 uncommitted records got received
 // => the failure only kills one thread
 // after fail over, we should read 40 committed records and the state stores should contain the correct sums
 // per key (even if some records got processed twice)

    try
    {
        (KafkaStreams streams = getKafkaStreams(true, "appDir", 2));
        streams.start();

        List<KeyValuePair<long, long>> committedDataBeforeFailure = prepareData(0L, 10L, 0L, 1L);
        List<KeyValuePair<long, long>> uncommittedDataBeforeFailure = prepareData(10L, 15L, 0L, 1L, 2L, 3L);

        List<KeyValuePair<long, long>> dataBeforeFailure = new ArrayList<>();
        dataBeforeFailure.addAll(committedDataBeforeFailure);
        dataBeforeFailure.addAll(uncommittedDataBeforeFailure);

        List<KeyValuePair<long, long>> dataAfterFailure = prepareData(15L, 20L, 0L, 1L);

        writeInputData(committedDataBeforeFailure);

        TestUtils.waitForCondition(
            () => commitRequested.get() == 2, MAX_WAIT_TIME_MS,
            "SteamsTasks did not request commit.");

        writeInputData(uncommittedDataBeforeFailure);

        List<KeyValuePair<long, long>> uncommittedRecords = readResult(dataBeforeFailure.Count, null);
        List<KeyValuePair<long, long>> committedRecords = readResult(committedDataBeforeFailure.Count, CONSUMER_GROUP_ID);

        List<KeyValuePair<long, long>> expectedResultBeforeFailure = computeExpectedResult(dataBeforeFailure);
        checkResultPerKey(committedRecords, computeExpectedResult(committedDataBeforeFailure));
        checkResultPerKey(uncommittedRecords, expectedResultBeforeFailure);
        verifyStateStore(streams, getMaxPerKey(expectedResultBeforeFailure));

        errorInjected.set(true);
        writeInputData(dataAfterFailure);

        TestUtils.waitForCondition(
            () => uncaughtException != null, MAX_WAIT_TIME_MS,
            "Should receive uncaught exception from one StreamThread.");

        List<KeyValuePair<long, long>> allCommittedRecords = readResult(
            committedDataBeforeFailure.Count + uncommittedDataBeforeFailure.Count + dataAfterFailure.Count,
            CONSUMER_GROUP_ID + "_ALL");

        List<KeyValuePair<long, long>> committedRecordsAfterFailure = readResult(
            uncommittedDataBeforeFailure.Count + dataAfterFailure.Count,
            CONSUMER_GROUP_ID);

        List<KeyValuePair<long, long>> allExpectedCommittedRecordsAfterRecovery = new ArrayList<>();
        allExpectedCommittedRecordsAfterRecovery.addAll(committedDataBeforeFailure);
        allExpectedCommittedRecordsAfterRecovery.addAll(uncommittedDataBeforeFailure);
        allExpectedCommittedRecordsAfterRecovery.addAll(dataAfterFailure);

        List<KeyValuePair<long, long>> expectedResult = computeExpectedResult(allExpectedCommittedRecordsAfterRecovery);

        checkResultPerKey(allCommittedRecords, expectedResult);
        checkResultPerKey(
            committedRecordsAfterFailure,
            expectedResult.subList(committedDataBeforeFailure.Count, expectedResult.Count));

        verifyStateStore(streams, getMaxPerKey(expectedResult));
    }
    }

[Xunit.Fact]
public void ShouldNotViolateEosIfOneTaskGetsFencedUsingIsolatedAppInstances()
{// throws Exception
 // this test writes 10 + 5 + 5 + 10 records per partition (running with 2 partitions)
 // the app is supposed to copy all 60 records into the output topic
 // the app commits after each 10 records per partition, and thus will have 2*5 uncommitted writes
 //
 // a GC pause gets inject after 20 committed and 30 uncommitted records got received
 // => the GC pause only affects one thread and should trigger a rebalance
 // after rebalancing, we should read 40 committed records (even if 50 record got written)
 //
 // afterwards, the "stalling" thread resumes, and another rebalance should get triggered
 // we write the remaining 20 records and verify to read 60 result records

    try (
        KafkaStreams streams1 = getKafkaStreams(false, "appDir1", 1);
    KafkaStreams streams2 = getKafkaStreams(false, "appDir2", 1)
        ) {
        streams1.start();
        streams2.start();

        List<KeyValuePair<long, long>> committedDataBeforeGC = prepareData(0L, 10L, 0L, 1L);
        List<KeyValuePair<long, long>> uncommittedDataBeforeGC = prepareData(10L, 15L, 0L, 1L);

        List<KeyValuePair<long, long>> dataBeforeGC = new ArrayList<>();
        dataBeforeGC.addAll(committedDataBeforeGC);
        dataBeforeGC.addAll(uncommittedDataBeforeGC);

        List<KeyValuePair<long, long>> dataToTriggerFirstRebalance = prepareData(15L, 20L, 0L, 1L);

        List<KeyValuePair<long, long>> dataAfterSecondRebalance = prepareData(20L, 30L, 0L, 1L);

        writeInputData(committedDataBeforeGC);

        TestUtils.waitForCondition(
            () => commitRequested.get() == 2, MAX_WAIT_TIME_MS,
            "SteamsTasks did not request commit.");

        writeInputData(uncommittedDataBeforeGC);

        List<KeyValuePair<long, long>> uncommittedRecords = readResult(dataBeforeGC.Count, null);
        List<KeyValuePair<long, long>> committedRecords = readResult(committedDataBeforeGC.Count, CONSUMER_GROUP_ID);

        checkResultPerKey(committedRecords, committedDataBeforeGC);
        checkResultPerKey(uncommittedRecords, dataBeforeGC);

        gcInjected.set(true);
        writeInputData(dataToTriggerFirstRebalance);

        TestUtils.waitForCondition(
            () => streams1.allMetadata().Count == 1
                && streams2.allMetadata().Count == 1
                && (streams1.allMetadata().iterator().next().topicPartitions().Count == 2
                    || streams2.allMetadata().iterator().next().topicPartitions().Count == 2),
            MAX_WAIT_TIME_MS, "Should have rebalanced.");

        List<KeyValuePair<long, long>> committedRecordsAfterRebalance = readResult(
            uncommittedDataBeforeGC.Count + dataToTriggerFirstRebalance.Count,
            CONSUMER_GROUP_ID);

        List<KeyValuePair<long, long>> expectedCommittedRecordsAfterRebalance = new ArrayList<>();
        expectedCommittedRecordsAfterRebalance.addAll(uncommittedDataBeforeGC);
        expectedCommittedRecordsAfterRebalance.addAll(dataToTriggerFirstRebalance);

        checkResultPerKey(committedRecordsAfterRebalance, expectedCommittedRecordsAfterRebalance);

        doGC = false;
        TestUtils.waitForCondition(
            () => streams1.allMetadata().Count == 1
                && streams2.allMetadata().Count == 1
                && streams1.allMetadata().iterator().next().topicPartitions().Count == 1
                && streams2.allMetadata().iterator().next().topicPartitions().Count == 1,
            MAX_WAIT_TIME_MS,
            "Should have rebalanced.");

        writeInputData(dataAfterSecondRebalance);

        List<KeyValuePair<long, long>> allCommittedRecords = readResult(
            committedDataBeforeGC.Count + uncommittedDataBeforeGC.Count
            + dataToTriggerFirstRebalance.Count + dataAfterSecondRebalance.Count,
            CONSUMER_GROUP_ID + "_ALL");

        List<KeyValuePair<long, long>> allExpectedCommittedRecordsAfterRecovery = new ArrayList<>();
        allExpectedCommittedRecordsAfterRecovery.addAll(committedDataBeforeGC);
        allExpectedCommittedRecordsAfterRecovery.addAll(uncommittedDataBeforeGC);
        allExpectedCommittedRecordsAfterRecovery.addAll(dataToTriggerFirstRebalance);
        allExpectedCommittedRecordsAfterRecovery.addAll(dataAfterSecondRebalance);

        checkResultPerKey(allCommittedRecords, allExpectedCommittedRecordsAfterRecovery);
    }
    }

    private List<KeyValuePair<long, long>> prepareData(long fromInclusive,
                                                   long toExclusive,
                                                   long... keys) {
        List<KeyValuePair<long, long>> data = new ArrayList<>();

        foreach (long k in keys)
        {
            for (long v = fromInclusive; v < toExclusive; ++v)
            {
                data.add(new KeyValuePair<>(k, v));
            }
        }

        return data;
    }

    private KafkaStreams getKafkaStreams(bool withState,
                                         string appDir,
                                         int numberOfStreamsThreads)
    {
        commitRequested = new AtomicInteger(0);
        errorInjected = new AtomicBoolean(false);
        gcInjected = new AtomicBoolean(false);
        StreamsBuilder builder = new StreamsBuilder();

        string[] storeNames = null;
        if (withState)
        {
            storeNames = new string[] { storeName };
            StoreBuilder<KeyValueStore<long, long>> storeBuilder = Stores
                .keyValueStoreBuilder(Stores.persistentKeyValueStore(storeName), Serdes.Long(), Serdes.Long())
                .withCachingEnabled();

            builder.addStateStore(storeBuilder);
        }

        KStream<long, long> input = builder.stream(MULTI_PARTITION_INPUT_TOPIC);
        input.transform(new TransformerSupplier<long, long, KeyValuePair<long, long>>()
        {



            public Transformer<long, long, KeyValuePair<long, long>> get()
        {
            return new Transformer<long, long, KeyValuePair<long, long>>() {
                    ProcessorContext context;
            KeyValueStore<long, long> state = null;


            public void init(ProcessorContext context)
            {
                this.context = context;

                if (withState)
                {
                    state = (KeyValueStore<long, long>)context.getStateStore(storeName);
                }
            }


            public KeyValuePair<long, long> transform(long key,
                                                  long value)
            {
                if (gcInjected.compareAndSet(true, false))
                {
                    while (doGC)
                    {
                        try
                        {
                            Thread.sleep(100);
                        }
                        catch (InterruptedException e)
                        {
                            throw new RuntimeException(e);
                        }
                    }
                }

                if ((value + 1) % 10 == 0)
                {
                    context.commit();
                    commitRequested.incrementAndGet();
                }

                if (state != null)
                {
                    long sum = state.get(key);

                    if (sum == null)
                    {
                        sum = value;
                    }
                    else
                    {
                        sum += value;
                    }
                    state.put(key, sum);
                    state.flush();
                }


                if (errorInjected.compareAndSet(true, false))
                {
                    // only tries to fail once on one of the task
                    throw new RuntimeException("Injected test exception.");
                }

                if (state != null)
                {
                    return new KeyValuePair<>(key, state.get(key));
                }
                else
                {
                    return new KeyValuePair<>(key, value);
                }
            }


            public void close() { }
        };
    }
}, storeNames)
            .to(SINGLE_PARTITION_OUTPUT_TOPIC);

Properties properties = new Properties();
properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numberOfStreamsThreads);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, long.MaxValue);
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), "1000");
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG), 5 * 1000);
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 5 * 1000 - 1);
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG), MAX_POLL_INTERVAL_MS);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        properties.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath() + File.separator + appDir);
        properties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "dummy:2142");

        Properties config = StreamsTestUtils.getStreamsConfig(
            applicationId,
            CLUSTER.bootstrapServers(),
            Serdes.LongSerde.getName(),
            Serdes.LongSerde.getName(),
            properties);

KafkaStreams streams = new KafkaStreams(builder.build(), config);

streams.setUncaughtExceptionHandler((t, e) => {
            if (uncaughtException != null) {
                e.printStackTrace(System.Console.Error);
                Assert.True(false, "Should only get one uncaught exception from Streams.");
            }
            uncaughtException = e;
        });

        return streams;
    }

    private void WriteInputData(List<KeyValuePair<long, long>> records)
{// throws Exception
    IntegrationTestUtils.produceKeyValuesSynchronously(
        MULTI_PARTITION_INPUT_TOPIC,
        records,
        TestUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer, LongSerializer),
        CLUSTER.time
    );
}

private List<KeyValuePair<long, long>> ReadResult(int numberOfRecords,
                                              string groupId)
{// throws Exception
    if (groupId != null)
    {
        return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            TestUtils.consumerConfig(
                CLUSTER.bootstrapServers(),
                groupId,
                LongDeserializer,
                LongDeserializer,
                Utils.mkProperties(Collections.singletonMap(
                    ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                    IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT)))),
            SINGLE_PARTITION_OUTPUT_TOPIC,
            numberOfRecords
        );
    }

    // read uncommitted
    return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
        TestUtils.consumerConfig(CLUSTER.bootstrapServers(), LongDeserializer, LongDeserializer),
        SINGLE_PARTITION_OUTPUT_TOPIC,
        numberOfRecords
    );
}

private List<KeyValuePair<long, long>> ComputeExpectedResult(List<KeyValuePair<long, long>> input)
{
    List<KeyValuePair<long, long>> expectedResult = new ArrayList<>(input.Count);

    HashDictionary<long, long> sums = new HashMap<>();

    foreach (KeyValuePair<long, long> record in input)
    {
        long sum = sums.get(record.key);
        if (sum == null)
        {
            sum = record.value;
        }
        else
        {
            sum += record.value;
        }
        sums.put(record.key, sum);
        expectedResult.add(new KeyValuePair<>(record.key, sum));
    }

    return expectedResult;
}

private HashSet<KeyValuePair<long, long>> GetMaxPerKey(List<KeyValuePair<long, long>> input)
{
    HashSet<KeyValuePair<long, long>> expectedResult = new HashSet<>(input.Count);

    HashDictionary<long, long> maxPerKey = new HashMap<>();

    foreach (KeyValuePair<long, long> record in input)
    {
        long max = maxPerKey.get(record.key);
        if (max == null || record.value > max)
        {
            maxPerKey.put(record.key, record.value);
        }

    }

    foreach (Map.Entry<long, long> max in maxPerKey.entrySet())
    {
        expectedResult.add(new KeyValuePair<>(max.getKey(), max.getValue()));
    }

    return expectedResult;
}

private void VerifyStateStore(KafkaStreams streams,
                              HashSet<KeyValuePair<long, long>> expectedStoreContent)
{
    ReadOnlyKeyValueStore<long, long> store = null;

    long maxWaitingTime = System.currentTimeMillis() + 300000L;
    while (System.currentTimeMillis() < maxWaitingTime)
    {
        try
        {
            store = streams.store(storeName, QueryableStoreTypes.keyValueStore());
            break;
        }
        catch (InvalidStateStoreException okJustRetry)
        {
            try
            {
                Thread.sleep(5000L);
            }
            catch (Exception ignore) { }
        }
    }

    assertNotNull(store);

    KeyValueIterator<long, long> it = store.all();
    while (it.hasNext())
    {
        Assert.True(expectedStoreContent.remove(it.next()));
    }

    Assert.True(expectedStoreContent.isEmpty());
}

}
