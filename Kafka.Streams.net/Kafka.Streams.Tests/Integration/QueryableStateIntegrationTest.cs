using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Mappers;
using Kafka.Streams.State;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Queryable;
using Kafka.Streams.State.ReadOnly;
using Kafka.Streams.State.Windowed;
using Kafka.Streams.Temporary;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Mocks;
using Kafka.Streams.Threads;
using Kafka.Streams.Threads.KafkaStreams;
using Kafka.Streams.Threads.KafkaStreamsThread;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using Xunit;

namespace Kafka.Streams.Tests.Integration
{
    public class QueryableStateIntegrationTest
    {
        private static ILogger<QueryableStateIntegrationTest> log = null;

        private const int NUM_BROKERS = 1;


        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
        private const int STREAM_THREE_PARTITIONS = 4;
        private MockTime mockTime = CLUSTER.time;
        private string streamOne = "stream-one";
        private string streamTwo = "stream-two";
        private string streamThree = "stream-three";
        private string streamConcurrent = "stream-concurrent";
        private string outputTopic = "output";
        private string outputTopicConcurrent = "output-concurrent";
        private string outputTopicConcurrentWindowed = "output-concurrent-windowed";
        private string outputTopicThree = "output-three";
        // sufficiently large window size such that everything falls into 1 window
        private static readonly long WINDOW_SIZE = TimeUnit.MILLISECONDS.Convert(2, TimeUnit.DAYS);
        private const int STREAM_TWO_PARTITIONS = 2;
        private const int NUM_REPLICAS = NUM_BROKERS;
        private StreamsConfig streamsConfiguration;
        private List<string> inputValues;
        private int numberOfWordsPerIteration = 0;
        private HashSet<string> inputValuesKeys;
        private KafkaStreamsThread kafkaStreams;
        private Comparator<KeyValuePair<string, string>> stringComparator;
        private Comparator<KeyValuePair<string, long>> stringLongComparator;
        private static volatile int testNo = 0;

        private void CreateTopics()
        {// throws Exception
            streamOne = streamOne + "-" + testNo;
            streamConcurrent = streamConcurrent + "-" + testNo;
            streamThree = streamThree + "-" + testNo;
            outputTopic = outputTopic + "-" + testNo;
            outputTopicConcurrent = outputTopicConcurrent + "-" + testNo;
            outputTopicConcurrentWindowed = outputTopicConcurrentWindowed + "-" + testNo;
            outputTopicThree = outputTopicThree + "-" + testNo;
            streamTwo = streamTwo + "-" + testNo;
            CLUSTER.createTopics(streamOne, streamConcurrent);
            CLUSTER.CreateTopic(streamTwo, STREAM_TWO_PARTITIONS, NUM_REPLICAS);
            CLUSTER.CreateTopic(streamThree, STREAM_THREE_PARTITIONS, 1);
            CLUSTER.createTopics(outputTopic, outputTopicConcurrent, outputTopicConcurrentWindowed, outputTopicThree);
        }

        /**
         * Try to read inputValues from {@code resources/QueryableStateIntegrationTest/inputValues.txt}, which might be useful
         * for larger scale testing. In case of exception, for instance if no such file can be read, return a small list
         * which satisfies All the prerequisites of the tests.
         */
        private List<string> GetInputValues()
        {
            List<string> input = new List<string>();
            ClassLoader classLoader = getClass().getClassLoader();
            string fileName = "QueryableStateIntegrationTest" + Path.DirectorySeparatorChar + "inputValues.txt";
            BufferedReader reader = new BufferedReader(
                new FileReader(Objects.requireNonNull(classLoader.getResource(fileName)).getFile()));

            for (string line = reader.readLine(); line != null; line = reader.readLine())
            {
                input.Add(line);
            }

            log.LogWarning("Unable to read '{}{}{}'. Using default inputValues list", "resources", Path.DirectorySeparatorChar, fileName);
            input = Arrays.asList(
                        "hello world",
                        "All streams lead to kafka",
                        "streams",
                        "kafka streams",
                        "the cat in the hat",
                        "green eggs and ham",
                        "that Sam i am",
                        "up the creek without a paddle",
                        "run forest run",
                        "a tank full of gas",
                        "eat sleep rave repeat",
                        "one jolly sailor",
                        "king of the world");

            return input;
        }


        public QueryableStateIntegrationTest()
        {// throws Exception
            CreateTopics();
            streamsConfiguration = new StreamsConfig();
            string applicationId = "queryable-state-" + ++testNo;

            streamsConfiguration.Set(StreamsConfig.ApplicationId, applicationId);
            streamsConfiguration.Set(StreamsConfig.BootstrapServers, CLUSTER.bootstrapServers());
            //streamsConfiguration.Set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            streamsConfiguration.Set(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory("qs-test").getPath());
            streamsConfiguration.Set(StreamsConfig.DefaultKeySerdeClass, Serdes.String().GetType());
            streamsConfiguration.Set(StreamsConfig.DefaultValueSerdeClass, Serdes.String().GetType());
            streamsConfiguration.Set(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

            stringComparator = Comparator.comparing((KeyValuePair<string, string> o) => o.Key).thenComparing(o => o.Value);
            stringLongComparator = Comparator.comparing((KeyValuePair<string, long> o) => o.Key).thenComparingLong(o => o.Value);
            inputValues = GetInputValues();
            inputValuesKeys = new HashSet<string>();
            foreach (string sentence in inputValues)
            {
                string[] words = sentence.Split("\\W+");
                numberOfWordsPerIteration += words.Length;
                Collections.addAll(inputValuesKeys, words);
            }
        }


        public void Shutdown()
        {// throws Exception
            if (kafkaStreams != null)
            {
                kafkaStreams.Close(TimeSpan.FromSeconds(30));
            }
            IntegrationTestUtils.PurgeLocalStreamsState(streamsConfiguration);
        }

        /**
         * Creates a typical word count topology
         */
        private KafkaStreamsThread CreateCountStream(
            string inputTopic,
            string outputTopic,
            string windowOutputTopic,
            string storeName,
            string windowStoreName,
            StreamsConfig streamsConfiguration)
        {
            StreamsBuilder builder = new StreamsBuilder();
            ISerde<string> stringSerde = Serdes.String();
            IKStream<string, string> textLines = builder.Stream(inputTopic, Consumed.With(stringSerde, stringSerde));

            IKGroupedStream<string, string> groupedByWord = textLines
                .FlatMapValues(value => Arrays.asList(value.Split("\\W+")))
                .GroupBy(MockMapper.GetSelectValueMapper<string, string>());

            // Create a State Store for the All time word count
            groupedByWords
                .Count(Materialized.As(storeName + "-" + inputTopic))
                .ToStream()
                .To(outputTopic, Produced.With(Serdes.String(), Serdes.Long()));

            // Create a Windowed State Store that contains the word count for every 1 minute
            groupedByWord
                .WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(WINDOW_SIZE)))
                .Count(Materialized.As(windowStoreName + "-" + inputTopic))
                .toStream((key, value) => key.Key)
                .To(windowOutputTopic, Produced.With(Serdes.String(), Serdes.Long()));

            return new KafkaStreamsThread(builder.Build(), streamsConfiguration);
        }

        private class StreamRunnable //: Runnable
        {
            private KafkaStreamsThread myStream;
            private bool closed = false;
            private StateListenerStub stateListener = new StateListenerStub();

            public StreamRunnable(
                string inputTopic,
                string outputTopic,
                string outputTopicWindowed,
                string storeName,
                string windowStoreName,
                int queryPort)
            {
                StreamsConfig props = (StreamsConfig)streamsConfiguration.clone();
                props.Put(StreamsConfig.ApplicationIdConfig, "localhost:" + queryPort);
                myStream = CreateCountStream(inputTopic, outputTopic, outputTopicWindowed, storeName, windowStoreName, props);
                myStream.SetStateListener(stateListener);
            }


            public void Run()
            {
                myStream.Start();
            }

            public void Close()
            {
                if (!closed)
                {
                    myStream.Close();
                    closed = true;
                }
            }

            public bool IsClosed()
            {
                return closed;
            }

            public KafkaStreamsThread GetStream()
            {
                return myStream;
            }

            public StateListenerStub GetStateListener()
            {
                return stateListener;
            }
        }

        private void VerifyAllKVKeys(
            StreamRunnable[] streamRunnables,
            KafkaStreamsThread streams,
            StateListenerStub stateListenerStub,
            HashSet<string> keys,
            string storeName)
        {// throws Exception
            foreach (string key in keys)
            {
                TestUtils.WaitForCondition(
                    () =>
                    {
                        try
                        {
                            StreamsMetadata metadata = streams.MetadataForKey(storeName, key, Serdes.Int().Serializer);

                            if (metadata == null || metadata.Equals(StreamsMetadata.NOT_AVAILABLE))
                            {
                                return false;
                            }
                            int index = metadata.HostInfo().port();
                            KafkaStreamsThread streamsWithKey = streamRunnables[index].GetStream();
                            IReadOnlyKeyValueStore<string, long> store =
                                streamsWithKey.Store(storeName, QueryableStoreTypes.KeyValueStore);

                            return store != null && store.Get(key) != null;
                        }
                        catch (Exception e)
                        {
                            // Kafka Streams instance may have closed but rebalance hasn't happened
                            return false;
                        }
                        catch (Exception e)
                        {
                            // there must have been at least one rebalance state
                            Assert.True(stateListenerStub.mapStates[KafkaStreamsThreadStates.REBALANCING] >= 1);
                            return false;
                        }
                    },
                    120000,
                    "waiting for metadata, store and value to be non null");
            }
        }

        private void VerifyAllWindowedKeys(
            StreamRunnable[] streamRunnables,
            IKafkaStreamsThread streams,
            StateListenerStub stateListenerStub,
            HashSet<string> keys,
            string storeName,
            long from,
            long to)
        {// throws Exception
            foreach (string key in keys)
            {
                TestUtils.WaitForCondition(
                    () =>
                    {
                        try
                        {
                            StreamsMetadata metadata = streams.metadataForKey(storeName, key, new Serdes.String().Serializer());
                            if (metadata == null || metadata.Equals(StreamsMetadata.NOT_AVAILABLE))
                            {
                                return false;
                            }
                            int index = metadata.hostInfo().port();
                            IKafkaStreamsThread streamsWithKey = streamRunnables[index].GetStream();
                            IReadOnlyWindowStore<string, long> store =
                                streamsWithKey.store(storeName, QueryableStoreTypes.windowStore());
                            return store != null && store.Fetch(key, ofEpochMilli(from), ofEpochMilli(to)) != null;
                        }
                        catch (IllegalStateException e)
                        {
                            // Kafka Streams instance may have closed but rebalance hasn't happened
                            return false;
                        }
                        catch (InvalidStateStoreException e)
                        {
                            // there must have been at least one rebalance state
                            Assert.True(stateListenerStub.mapStates[KafkaStreamsThreadStates.REBALANCING] >= 1);
                            return false;
                        }
                    },
                    120000,
                    "waiting for metadata, store and value to be non null");
            }
        }

        [Fact]
        public void QueryOnRebalance()
        {// throws Exception
            int numThreads = STREAM_TWO_PARTITIONS;
            StreamRunnable[] streamRunnables = new StreamRunnable[numThreads];
            IKafkaStreamsThread[] streamThreads = new KafkaStreamsThread[numThreads];

            ProducerRunnable producerRunnable = new ProducerRunnable(streamThree, inputValues, 1);
            producerRunnable.Run();

            // Create stream threads
            string storeName = "word-count-store";
            string windowStoreName = "windowed-word-count-store";
            for (int i = 0; i < numThreads; i++)
            {
                streamRunnables[i] = new StreamRunnable(
                    streamThree,
                    outputTopicThree,
                    outputTopicConcurrentWindowed,
                    storeName,
                    windowStoreName,
                    i);
                streamThreads[i] = new KafkaStreamsThread(streamRunnables[i]);
                streamThreads[i].Start();
            }

            try
            {
                WaitUntilAtLeastNumRecordProcessed(outputTopicThree, 1);

                for (int i = 0; i < numThreads; i++)
                {
                    VerifyAllKVKeys(
                        streamRunnables,
                        streamRunnables[i].GetStream(),
                        streamRunnables[i].GetStateListener(),
                        inputValuesKeys,
                        storeName + "-" + streamThree);
                    VerifyAllWindowedKeys(
                        streamRunnables,
                        streamRunnables[i].GetStream(),
                        streamRunnables[i].GetStateListener(),
                        inputValuesKeys,
                        windowStoreName + "-" + streamThree,
                        0L,
                        WINDOW_SIZE);

                    Assert.Equal(
                        KafkaStreamsThreadStates.RUNNING,
                        streamRunnables[i].GetStream().State.CurrentState);
                }

                // kill N-1 threads
                for (int i = 1; i < numThreads; i++)
                {
                    streamRunnables[i].Close();
                    // streamThreads[i].Interrupt();
                    streamThreads[i].Join();
                }

                // query from the remaining thread
                VerifyAllKVKeys(
                    streamRunnables,
                    streamRunnables[0].GetStream(),
                    streamRunnables[0].GetStateListener(),
                    inputValuesKeys,
                    storeName + "-" + streamThree);

                VerifyAllWindowedKeys(
                    streamRunnables,
                    streamRunnables[0].GetStream(),
                    streamRunnables[0].GetStateListener(),
                    inputValuesKeys,
                    windowStoreName + "-" + streamThree,
                    0L,
                    WINDOW_SIZE);
                Assert.Equal(KafkaStreamsThreadStates.RUNNING, streamRunnables[0].GetStream().state());
            }
            finally
            {
                for (int i = 0; i < numThreads; i++)
                {
                    if (!streamRunnables[i].IsClosed())
                    {
                        streamRunnables[i].Close();
                        // streamThreads[i].interrupt();
                        streamThreads[i].Join();
                    }
                }
            }
        }

        [Fact]
        public void ConcurrentAccesses()
        {// throws Exception
            int numIterations = 500000;
            string storeName = "word-count-store";
            string windowStoreName = "windowed-word-count-store";

            ProducerRunnable producerRunnable = new ProducerRunnable(streamConcurrent, inputValues, numIterations);
            var producerThread = new KafkaStreamsThread(producerRunnable);
            kafkaStreams = CreateCountStream(
                streamConcurrent,
                outputTopicConcurrent,
                outputTopicConcurrentWindowed,
                storeName,
                windowStoreName,
                streamsConfiguration);

            kafkaStreams.Start();
            producerThread.Start();

            try
            {
                WaitUntilAtLeastNumRecordProcessed(outputTopicConcurrent, numberOfWordsPerIteration);
                WaitUntilAtLeastNumRecordProcessed(outputTopicConcurrentWindowed, numberOfWordsPerIteration);

                IReadOnlyKeyValueStore<string, long> KeyValueStore =
                    kafkaStreams.Store(storeName + "-" + streamConcurrent, QueryableStoreTypes.KeyValueStore);

                IReadOnlyWindowStore<string, long> windowStore =
                    kafkaStreams.Store(windowStoreName + "-" + streamConcurrent, QueryableStoreTypes.windowStore());

                Dictionary<string, long> expectedWindowState = new Dictionary<string, long>();
                Dictionary<string, long> expectedCount = new Dictionary<string, long>();
                while (producerRunnable.GetCurrIteration() < numIterations)
                {
                    VerifyGreaterOrEqual(inputValuesKeys.ToArray(), expectedWindowState,
                        expectedCount, windowStore, KeyValueStore, true);
                }
            }
            finally
            {
                producerRunnable.Shutdown();
                // producerThread.interrupt();
                producerThread.Join();
            }
        }

        [Fact]
        public void ShouldBeAbleToQueryStateWithZeroSizedCache()
        {// throws Exception
            VerifyCanQueryState(0);
        }

        [Fact]
        public void ShouldBeAbleToQueryStateWithNonZeroSizedCache()
        {// throws Exception
            VerifyCanQueryState(10 * 1024 * 1024);
        }

        [Fact]
        public void ShouldBeAbleToQueryFilterState()
        {// throws Exception
            streamsConfiguration.Put(StreamsConfig.DefaultKeySerdeClassConfig, Serdes.String().GetType());
            streamsConfiguration.Put(StreamsConfig.DefaultValueSerdeClassConfig, Serdes.Long().GetType());
            StreamsBuilder builder = new StreamsBuilder();
            string[] keys = { "hello", "goodbye", "welcome", "go", "kafka" };
            HashSet<KeyValuePair<string, long>> batch1 = new HashSet<KeyValuePair<string, long>>(
                Arrays.asList(
                    KeyValuePair.Create(keys[0], 1L),
                    KeyValuePair.Create(keys[1], 1L),
                    KeyValuePair.Create(keys[2], 3L),
                    KeyValuePair.Create(keys[3], 5L),
                    KeyValuePair.Create(keys[4], 2L))
            );

            HashSet<KeyValuePair<string, long>> expectedBatch1 =
                new HashSet<KeyValuePair<string, long>>(Collections.singleton(KeyValuePair.Create(keys[4], 2L)));

            IntegrationTestUtils.ProduceKeyValuesSynchronously(
                streamOne,
                batch1,
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    Serdes.String().Serializer,
                    Serdes.Long().Serializer,
                    new StreamsConfig()),
                mockTime);

            Func<string, long, bool> filterPredicate = (key, value) => key.Contains("kafka");
            IKTable<string, long> t1 = builder.Table<string, long>(streamOne);
            IKTable<string, long> t2 = t1.Filter(filterPredicate, Materialized.As("queryFilter"));
            t1.FilterNot(filterPredicate, Materialized.As("queryFilterNot"));
            t2.ToStream().To(outputTopic);

            kafkaStreams = new KafkaStreamsThread(builder.Build(), streamsConfiguration);
            kafkaStreams.Start();

            WaitUntilAtLeastNumRecordProcessed(outputTopic, 1);

            IReadOnlyKeyValueStore<string, long>
                myFilterStore = kafkaStreams.Store("queryFilter", QueryableStoreTypes.KeyValueStore);
            IReadOnlyKeyValueStore<string, long>
                myFilterNotStore = kafkaStreams.Store("queryFilterNot", QueryableStoreTypes.KeyValueStore);

            foreach (KeyValuePair<string, long> expectedEntry in expectedBatch1)
            {
                TestUtils.WaitForCondition(() => expectedEntry.Value.Equals(myFilterStore.Get(expectedEntry.Key)),
                        "Cannot get expected result");
            }

            foreach (KeyValuePair<string, long> batchEntry in batch1)
            {
                if (!expectedBatch1.Contains(batchEntry))
                {
                    TestUtils.WaitForCondition(() => myFilterStore.Get(batchEntry.Key) == null,
                            "Cannot get null result");
                }
            }

            foreach (KeyValuePair<string, long> expectedEntry in expectedBatch1)
            {
                TestUtils.WaitForCondition(() => myFilterNotStore.Get(expectedEntry.Key) == null,
                        "Cannot get null result");
            }

            foreach (KeyValuePair<string, long> batchEntry in batch1)
            {
                if (!expectedBatch1.Contains(batchEntry))
                {
                    TestUtils.WaitForCondition(() => batchEntry.Value.Equals(myFilterNotStore.Get(batchEntry.Key)),
                            "Cannot get expected result");
                }
            }
        }

        [Fact]
        public void ShouldBeAbleToQueryMapValuesState()
        {// throws Exception
            streamsConfiguration.DefaultKeySerdeType = Serdes.String().GetType();
            streamsConfiguration.DefaultValueSerdeType = Serdes.String().GetType();

            StreamsBuilder builder = new StreamsBuilder();
            string[] keys = { "hello", "goodbye", "welcome", "go", "kafka" };
            HashSet<KeyValuePair<string, string>> batch1 = new HashSet<KeyValuePair<string, string>>(
                Arrays.asList(
                    KeyValuePair.Create(keys[0], "1"),
                    KeyValuePair.Create(keys[1], "1"),
                    KeyValuePair.Create(keys[2], "3"),
                    KeyValuePair.Create(keys[3], "5"),
                    KeyValuePair.Create(keys[4], "2")));

            IntegrationTestUtils.ProduceKeyValuesSynchronously(
                streamOne,
                batch1,
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    Serdes.String().Serializer,
                    Serdes.String().Serializer,
                    new StreamsConfig()),
                mockTime);

            IKTable<string, string> t1 = builder.Table<string, string>(streamOne);
            t1.MapValues(v => long.Parse(v), Materialized.As<string, long, IKeyValueStore<Bytes, byte[]>>("queryMapValues")
                .WithValueSerde(Serdes.Long()))
                .ToStream()
                .To(outputTopic, Produced.With(Serdes.String(), Serdes.Long()));

            kafkaStreams = new KafkaStreamsThread(builder.Build(), streamsConfiguration);
            kafkaStreams.Start();

            WaitUntilAtLeastNumRecordProcessed(outputTopic, 5);

            IReadOnlyKeyValueStore<string, long> myMapStore =
                kafkaStreams.Store("queryMapValues", QueryableStoreTypes.KeyValueStore);

            foreach (KeyValuePair<string, string> batchEntry in batch1)
            {
                Assert.Equal(long.Parse(batchEntry.Value), myMapStore.Get(batchEntry.Key));
            }
        }

        [Fact]
        public void ShouldBeAbleToQueryMapValuesAfterFilterState()
        {// throws Exception
            streamsConfiguration.DefaultKeySerdeType = Serdes.String().GetType();
            streamsConfiguration.DefaultValueSerdeType = Serdes.String().GetType().GetType();

            StreamsBuilder builder = new StreamsBuilder();
            string[] keys = { "hello", "goodbye", "welcome", "go", "kafka" };
            HashSet<KeyValuePair<string, string>> batch1 = new HashSet<KeyValuePair<string, string>>(
                Arrays.asList(
                    KeyValuePair.Create(keys[0], "1"),
                    KeyValuePair.Create(keys[1], "1"),
                    KeyValuePair.Create(keys[2], "3"),
                    KeyValuePair.Create(keys[3], "5"),
                    KeyValuePair.Create(keys[4], "2")));

            HashSet<KeyValuePair<string, long>> expectedBatch1 =
                new HashSet<KeyValuePair<string, long>>(Collections.singleton(KeyValuePair.Create(keys[4], 2L)));

            IntegrationTestUtils.ProduceKeyValuesSynchronously(
                streamOne,
                batch1,
                TestUtils.ProducerConfig(
                    CLUSTER.bootstrapServers(),
                    Serdes.String().Serializer,
                    Serdes.String().Serializer,
                    new StreamsConfig()),
                mockTime);

            Func<string, string, bool> filterPredicate = (key, value) => key.Contains("kafka");
            IKTable<string, string> t1 = builder.Table(streamOne);
            IKTable<string, string> t2 = t1.Filter(filterPredicate, Materialized.As("queryFilter"));
            IKTable<string, long> t3 = t2
                .MapValues(
                    new ValueMapper<string, long>(v => long.Parse(v)),
                    Materialized.As<string, long, IKeyValueStore<Bytes, byte[]>>("queryMapValues")
                    .WithValueSerde(Serdes.Long()));

            t3.ToStream().To(outputTopic, Produced.With(Serdes.String(), Serdes.Long()));

            kafkaStreams = new KafkaStreamsThread(builder.Build(), streamsConfiguration);
            kafkaStreams.Start();

            WaitUntilAtLeastNumRecordProcessed(outputTopic, 1);

            IReadOnlyKeyValueStore<string, long>
                myMapStore = kafkaStreams.Store("queryMapValues",
                QueryableStoreTypes.KeyValueStore);

            foreach (KeyValuePair<string, long> expectedEntry in expectedBatch1)
            {
                Assert.Equal(myMapStore.Get(expectedEntry.Key), expectedEntry.Value);
            }

            foreach (KeyValuePair<string, string> batchEntry in batch1)
            {
                var batchEntryMapValue = KeyValuePair.Create(batchEntry.Key, long.Parse(batchEntry.Value));

                if (!expectedBatch1.Contains(batchEntryMapValue))
                {
                    Assert.Null(myMapStore.Get(batchEntry.Key));
                }
            }
        }

        private void VerifyCanQueryState(int cacheSizeBytes)
        {// throws Exception
            streamsConfiguration.CacheMaxBytesBuffering = cacheSizeBytes;

            StreamsBuilder builder = new StreamsBuilder();
            string[] keys = { "hello", "goodbye", "welcome", "go", "kafka" };

            HashSet<KeyValuePair<string, string>> batch1 = new HashSet<KeyValuePair<string, string>>(stringComparator);
            batch1.AddRange(Arrays.asList(
                KeyValuePair.Create(keys[0], "hello"),
                KeyValuePair.Create(keys[1], "goodbye"),
                KeyValuePair.Create(keys[2], "welcome"),
                KeyValuePair.Create(keys[3], "go"),
                KeyValuePair.Create(keys[4], "kafka")));

            HashSet<KeyValuePair<string, long>> expectedCount = new TreeSet<>(stringLongComparator);
            foreach (string key in keys)
            {
                expectedCount.Add(KeyValuePair.Create(key, 1L));
            }

            IntegrationTestUtils.ProduceKeyValuesSynchronously(
                    streamOne,
                    batch1,
                    TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    Serdes.String().Serializer,
                    Serdes.String().Serializer,
                    new StreamsConfig()),
                    mockTime);

            IKStream<K, V> s1 = builder.Stream(streamOne);

            // Non Windowed
            string storeName = "my-count";
            s1.GroupByKey()
                .Count(Materialized.As(storeName))
                .ToStream()
                .To(outputTopic, Produced.With(Serdes.String(), Serdes.Long()));

            string windowStoreName = "windowed-count";
            s1.GroupByKey()
                .WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(WINDOW_SIZE)))
                .Count(Materialized.As(windowStoreName));
            kafkaStreams = new KafkaStreamsThread(builder.Build(), streamsConfiguration);
            kafkaStreams.Start();

            WaitUntilAtLeastNumRecordProcessed(outputTopic, 1);

            IReadOnlyKeyValueStore<string, long>
                myCount = kafkaStreams.store(storeName, QueryableStoreTypes.KeyValueStore);

            IReadOnlyWindowStore<string, long> windowStore =
                kafkaStreams.store(windowStoreName, QueryableStoreTypes.windowStore());
            VerifyCanGetByKey(keys,
                expectedCount,
                expectedCount,
                windowStore,
                myCount);

            VerifyRangeAndAll(expectedCount, myCount);
        }

        [Fact]
        public void ShouldNotMakeStoreAvailableUntilAllStoresAvailable()
        {// throws Exception
            StreamsBuilder builder = new StreamsBuilder();
            IKStream<K, V> stream = builder.Stream(streamThree);

            string storeName = "count-by-key";
            stream.GroupByKey().Count(Materialized.As(storeName));
            kafkaStreams = new KafkaStreamsThread(builder.Build(), streamsConfiguration);
            kafkaStreams.Start();

            KeyValuePair<string, string> hello = KeyValuePair.Create("hello", "hello");
            IntegrationTestUtils.ProduceKeyValuesSynchronously(
                    streamThree,
                    Arrays.asList(hello, hello, hello, hello, hello, hello, hello, hello),
                    TestUtils.producerConfig(
                            CLUSTER.bootstrapServers(),
                            Serdes.String().Serializer,
                            Serdes.String().Serializer,
                            new StreamsConfig()),
                    mockTime);

            int maxWaitMs = 30000;

            TestUtils.WaitForCondition(
                new WaitForStore(storeName),
                maxWaitMs,
                "waiting for store " + storeName);

            IReadOnlyKeyValueStore<string, long> store =
                kafkaStreams.store(storeName, QueryableStoreTypes.KeyValueStore);

            TestUtils.WaitForCondition(
                () => new long(8).Equals(store.Get("hello")),
                maxWaitMs,
                "wait for count to be 8");

            // Close stream
            kafkaStreams.Close();

            // start again
            kafkaStreams = new KafkaStreamsThread(builder.Build(), streamsConfiguration);
            kafkaStreams.Start();

            // make sure we never get any value other than 8 for hello
            TestUtils.WaitForCondition(
                () =>
                {
                    try
                    {
                        Assert.Equal(
                            8L.ToString(),
                            kafkaStreams.store(storeName, QueryableStoreTypes.KeyValueStore<string, long>).Get("hello"));
                        return true;
                    }
                    catch (InvalidStateStoreException ise)
                    {
                        return false;
                    }
                },
                maxWaitMs,
                "waiting for store " + storeName);

        }

        private class WaitForStore // : TestCondition
        {
            private readonly string storeName;

            public WaitForStore(string storeName)
            {
                this.storeName = storeName;
            }


            public bool ConditionMet()
            {
                try
                {
                    kafkaStreams.store(storeName, QueryableStoreTypes.KeyValueStore<string, long>());
                    return true;
                }
                catch (InvalidStateStoreException ise)
                {
                    return false;
                }
            }
        }

        [Fact]
        public void ShouldAllowToQueryAfterThreadDied()
        {// throws Exception
            bool beforeFailure = true;
            bool failed = false;
            string storeName = "store";

            StreamsBuilder builder = new StreamsBuilder();
            IKStream<string, string> input = builder.Stream<string, string>(streamOne);

            input
                .GroupByKey()
                .Reduce((value1, value2) =>
                {
                    if (value1.Length() > 1)
                    {
                        if (beforeFailure.CompareAndSet(true, false))
                        {
                            throw new RuntimeException("Injected test exception");
                        }
                    }
                    return value1 + value2;
                }, Materialized.As(storeName))
                .ToStream()
                .To(outputTopic);

            streamsConfiguration.Put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
            kafkaStreams = new KafkaStreamsThread(builder.Build(), streamsConfiguration);
            kafkaStreams.setUncaughtExceptionHandler((t, e) => failed.set(true));
            kafkaStreams.Start();

            IntegrationTestUtils.ProduceKeyValuesSynchronously(
                streamOne,
                Arrays.asList(
                    KeyValuePair.Create("a", "1"),
                    KeyValuePair.Create("a", "2"),
                    KeyValuePair.Create("b", "3"),
                    KeyValuePair.Create("b", "4")),
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    Serdes.String().Serializer,
                    Serdes.String().Serializer,
                    new StreamsConfig()),
                mockTime);

            int maxWaitMs = 30000;

            TestUtils.WaitForCondition(
                new WaitForStore(storeName),
                maxWaitMs,
                "waiting for store " + storeName);

            IReadOnlyKeyValueStore<string, string> store =
                kafkaStreams.store(storeName, QueryableStoreTypes.KeyValueStore);

            TestUtils.WaitForCondition(
                () => "12".Equals(store.Get("a")) && "34".Equals(store.Get("b")),
                maxWaitMs,
                "wait for agg to be <a,12> and <b,34>");

            IntegrationTestUtils.ProduceKeyValuesSynchronously(
                streamOne,
                Collections.singleton(KeyValuePair.Create("a", "5")),
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    Serdes.String().Serializer,
                    Serdes.String().Serializer,
                    new StreamsConfig()),
                mockTime);

            TestUtils.WaitForCondition(
                failed,//::get,
                maxWaitMs,
                "wait for thread to fail");
            TestUtils.WaitForCondition(
                new WaitForStore(storeName),
                maxWaitMs,
                "waiting for store " + storeName);

            IReadOnlyKeyValueStore<string, string> store2 =
                kafkaStreams.store(storeName, QueryableStoreTypes.KeyValueStore);

            try
            {
                TestUtils.WaitForCondition(
                    () => ("125".Equals(store2.Get("a"))
                        || "1225".Equals(store2.Get("a"))
                        || "12125".Equals(store2.Get("a")))
                        &&
                        ("34".Equals(store2.Get("b"))
                        || "344".Equals(store2.Get("b"))
                        || "3434".Equals(store2.Get("b"))),
                    maxWaitMs,
                    "wait for agg to be <a,125>||<a,1225>||<a,12125> and <b,34>||<b,344>||<b,3434>");
            }
            catch (Exception t)
            {
                throw new RuntimeException("Store content is a: " + store2.Get("a") + "; b: " + store2.Get("b"), t);
            }
        }

        private void VerifyRangeAndAll(HashSet<KeyValuePair<string, long>> expectedCount,
                                       IReadOnlyKeyValueStore<string, long> myCount)
        {
            HashSet<KeyValuePair<string, long>> countRangeResults = new HashSet<KeyValuePair<string, long>>(stringLongComparator);
            HashSet<KeyValuePair<string, long>> countAllResults = new HashSet<KeyValuePair<string, long>>(stringLongComparator);
            HashSet<KeyValuePair<string, long>> expectedRangeResults = new HashSet<KeyValuePair<string, long>>(stringLongComparator);

            expectedRangeResults.AddRange(
                Arrays.asList(
                    KeyValuePair.Create("hello", 1L),
                    KeyValuePair.Create("go", 1L),
                    KeyValuePair.Create("goodbye", 1L),
                    KeyValuePair.Create("kafka", 1L)));

            IKeyValueIterator<string, long> range = myCount.Range("go", "kafka");
            while (range.MoveNext())
            {
                countRangeResults.Add(range.Current);
            }

            IKeyValueIterator<string, long> All = myCount.All();
            while (All.MoveNext())
            {
                countAllResults.Add(All.Current);
            }

            Assert.Equal(countRangeResults, expectedRangeResults);
            Assert.Equal(countAllResults, expectedCount);
        }

        private void VerifyCanGetByKey(string[] keys,
                                       HashSet<KeyValuePair<string, long>> expectedWindowState,
                                       HashSet<KeyValuePair<string, long>> expectedCount,
                                       IReadOnlyWindowStore<string, long> windowStore,
                                       IReadOnlyKeyValueStore<string, long> myCount)
        {// throws Exception
            HashSet<KeyValuePair<string, long>> windowState = new TreeSet<>(stringLongComparator);
            HashSet<KeyValuePair<string, long>> countState = new TreeSet<>(stringLongComparator);

            long timeout = System.currentTimeMillis() + 30000;
            while ((windowState.Count < keys.Length ||
                countState.Count < keys.Length) &&
                System.currentTimeMillis() < timeout)
            {
                Thread.Sleep(10);
                foreach (string key in keys)
                {
                    windowState.addAll(Fetch(windowStore, key));
                    long value = myCount.Get(key);
                    if (value != null)
                    {
                        countState.Add(KeyValuePair.Create(key, value));
                    }
                }
            }
            Assert.Equal(windowState, expectedWindowState);
            Assert.Equal(countState, expectedCount);
        }

        /**
         * Verify that the new count is greater than or equal to the previous count.
         * Note: this method changes the values in expectedWindowState and expectedCount
         *
         * @param keys                  All the keys we ever expect to find
         * @param expectedWindowedCount Expected windowed count
         * @param expectedCount         Expected count
         * @param windowStore           Window Store
         * @param KeyValueStore         Key-value store
         * @param failIfKeyNotFound     if true, tests fails if an expected key is not found in store. If false,
         *                              the method merely inserts the new found key into the list of
         *                              expected keys.
         */
        private void VerifyGreaterOrEqual(
            string[] keys,
            Dictionary<string, long> expectedWindowedCount,
            Dictionary<string, long> expectedCount,
            IReadOnlyWindowStore<string, long> windowStore,
            IReadOnlyKeyValueStore<string, long> KeyValueStore,
            bool failIfKeyNotFound)
        {
            Dictionary<string, long> windowState = new Dictionary<string, long>();
            Dictionary<string, long> countState = new Dictionary<string, long>();

            foreach (string key in keys)
            {
                Dictionary<string, long> map = FetchMap(windowStore, key);
                if (map.Equals(Collections.emptyMap<string, long>()) && failIfKeyNotFound)
                {
                    Assert.True(false, "Key in windowed-store not found " + key);
                }

                windowState.AddRange(map);
                long value = KeyValueStore.Get(key);
                if (value != null)
                {
                    countState.Put(key, value);
                }
                else if (failIfKeyNotFound)
                {
                    Assert.True(false, "Key in key-value-store not found " + key);
                }
            }

            foreach (var actualWindowStateEntry in windowState)
            {
                if (expectedWindowedCount.ContainsKey(actualWindowStateEntry.Key))
                {
                    long expectedValue = expectedWindowedCount[actualWindowStateEntry.Key];
                    Assert.True(actualWindowStateEntry.Value >= expectedValue);
                }
                // return this for next round of comparisons
                expectedWindowedCount.Put(actualWindowStateEntry.Key, actualWindowStateEntry.Value);
            }

            foreach (var actualCountStateEntry in countState)
            {
                if (expectedCount.ContainsKey(actualCountStateEntry.Key))
                {
                    long expectedValue = expectedCount[actualCountStateEntry.Key];
                    Assert.True(actualCountStateEntry.Value >= expectedValue);
                }
                // return this for next round of comparisons
                expectedCount.Put(actualCountStateEntry.Key, actualCountStateEntry.Value);
            }

        }

        private void WaitUntilAtLeastNumRecordProcessed(string topic,
                                                        int numRecs)
        {// throws Exception
            StreamsConfig config = new StreamsConfig();
            config.Set(ConsumerConfig.BootstrapServersConfig, CLUSTER.bootstrapServers());
            config.Set(ConsumerConfig.GROUP_ID_CONFIG, "queryable-state-consumer");
            config.Set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            config.Set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().Deserializer.getName());
            config.Set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.getName());
            IntegrationTestUtils.waitUntilMinValuesRecordsReceived(
                config,
                topic,
                numRecs,
                120 * 1000);
        }

        private HashSet<KeyValuePair<string, long>> Fetch(IReadOnlyWindowStore<string, long> store,
                                                  string key)
        {
            IWindowStoreIterator<long> Fetch =
                store.Fetch(key, ofEpochMilli(0), ofEpochMilli(System.currentTimeMillis()));
            if (Fetch.MoveNext())
            {
                KeyValuePair<long, long> next = Fetch.Current;
                return Collections.singleton(KeyValuePair.Create(key, next.Value));
            }

            return Collections.emptySet();
        }

        private Dictionary<string, long> FetchMap(IReadOnlyWindowStore<string, long> store,
                                           string key)
        {
            IWindowStoreIterator<long> Fetch =
                store.Fetch(key, ofEpochMilli(0), ofEpochMilli(System.currentTimeMillis()));
            if (Fetch.HasNext())
            {
                KeyValuePair<long, long> next = Fetch.MoveNext();
                return Collections.singletonMap(key, next.Value);
            }
            return Collections.emptyMap();
        }

        /**
         * A class that periodically produces records in a separate thread
         */
        private class ProducerRunnable //: Runnable
        {
            private readonly string topic;
            private List<string> inputValues;
            private readonly int numIterations;
            private int currIteration = 0;
            private bool shutdown = false;

            public ProducerRunnable(
                string topic,
                List<string> inputValues,
                int numIterations)
            {
                this.topic = topic;
                this.inputValues = inputValues;
                this.numIterations = numIterations;
            }

            [MethodImpl(MethodImplOptions.Synchronized)]
            private void IncrementIteration()
            {
                currIteration++;
            }

            [MethodImpl(MethodImplOptions.Synchronized)]
            int GetCurrIteration()
            {
                return currIteration;
            }

            [MethodImpl(MethodImplOptions.Synchronized)]
            void Shutdown()
            {
                this.shutdown = true;
            }


            public void Run()
            {
                StreamsConfig producerConfig = new StreamsConfig();
                producerConfig.Set(ProducerConfig.BootstrapServersConfig, CLUSTER.bootstrapServers());
                producerConfig.Set(ProducerConfig.ACKS_CONFIG, "All");
                producerConfig.Set(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().Serializer);
                producerConfig.Set(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().Serializer);

                KafkaProducer<string, string> producer =
                         new KafkaProducer<>(producerConfig, Serdes.String().Serializer, Serdes.String().Serializer);

                while (GetCurrIteration() < numIterations && !this.shutdown)
                {
                    foreach (string value in inputValues)
                    {
                        producer.send(new ProducerRecord<>(topic, value));
                    }
                    IncrementIteration();
                }
            }
        }
    }
}
