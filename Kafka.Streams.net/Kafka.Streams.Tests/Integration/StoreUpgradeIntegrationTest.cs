using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.ReadOnly;
using Kafka.Streams.State.TimeStamped;
using Kafka.Streams.Temporary;
using Kafka.Streams.Tests.Helpers;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Integration
{
    public class StoreUpgradeIntegrationTest
    {
        private static string inputStream;
        private static string STORE_NAME = "store";

        private KafkaStreamsThread kafkaStreams;
        private static int testCounter = 0;


        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);


        public void createTopics()
        {// throws Exception
            inputStream = "input-stream-" + testCounter;
            CLUSTER.CreateTopic(inputStream);
        }

        private StreamsConfig props()
        {
            StreamsConfig streamsConfiguration = new StreamsConfig();
            streamsConfiguration.Set(StreamsConfig.ApplicationIdConfig, "addId-" + testCounter++);
            streamsConfiguration.Set(StreamsConfig.BootstrapServersConfig, CLUSTER.bootstrapServers());
            streamsConfiguration.Set(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
            streamsConfiguration.Set(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory().getPath());
            streamsConfiguration.Set(StreamsConfig.DefaultKeySerdeClassConfig, Serdes.Int().GetType());
            streamsConfiguration.Set(StreamsConfig.DefaultValueSerdeClassConfig, Serdes.Int().GetType());
            streamsConfiguration.Set(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
            streamsConfiguration.Set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            return streamsConfiguration;
        }


        public void Shutdown()
        {
            if (kafkaStreams != null)
            {
                kafkaStreams.Close(TimeSpan.FromSeconds(30L));
                kafkaStreams.cleanUp();
            }
        }

        [Fact]
        public void shouldMigrateInMemoryKeyValueStoreToTimestampedKeyValueStoreUsingPapi()
        {// throws Exception
            shouldMigrateKeyValueStoreToTimestampedKeyValueStoreUsingPapi(false);
        }

        [Fact]
        public void shouldMigratePersistentKeyValueStoreToTimestampedKeyValueStoreUsingPapi()
        {// throws Exception
            shouldMigrateKeyValueStoreToTimestampedKeyValueStoreUsingPapi(true);
        }

        private void shouldMigrateKeyValueStoreToTimestampedKeyValueStoreUsingPapi(bool persistentStore)
        {// throws Exception
            StreamsBuilder streamsBuilderForOldStore = new StreamsBuilder();

            //streamsBuilderForOldStore.AddStateStore(
            //    Stores.KeyValueStoreBuilder(
            //        persistentStore ? Stores.PersistentKeyValueStore(STORE_NAME) : Stores.InMemoryKeyValueStore(STORE_NAME),
            //        Serdes.Int(),
            //        Serdes.Long())).< int, int> stream(inputStream)
            //      .Process(KeyValueProcessor, STORE_NAME);

            StreamsConfig props = props();
            kafkaStreams = new KafkaStreamsThread(streamsBuilderForOldStore.Build(), props);
            kafkaStreams.Start();

            processKeyValueAndVerifyPlainCount(1, Collections.singletonList(KeyValuePair.Create(1, 1L)));

            processKeyValueAndVerifyPlainCount(1, Collections.singletonList(KeyValuePair.Create(1, 2L)));
            long lastUpdateKeyOne = persistentStore ? -1L : CLUSTER.time.NowAsEpochMilliseconds; //-1L;

            processKeyValueAndVerifyPlainCount(2, Arrays.asList(
                KeyValuePair.Create(1, 2L),
                KeyValuePair.Create(2, 1L)));
            long lastUpdateKeyTwo = persistentStore ? -1L : CLUSTER.time.NowAsEpochMilliseconds; //-1L;

            processKeyValueAndVerifyPlainCount(3, Arrays.asList(
                KeyValuePair.Create(1, 2L),
                KeyValuePair.Create(2, 1L),
                KeyValuePair.Create(3, 1L)));
            long lastUpdateKeyThree = persistentStore ? -1L : CLUSTER.time.NowAsEpochMilliseconds; //-1L;

            processKeyValueAndVerifyPlainCount(4, Arrays.asList(
                KeyValuePair.Create(1, 2L),
                KeyValuePair.Create(2, 1L),
                KeyValuePair.Create(3, 1L),
                KeyValuePair.Create(4, 1L)));

            processKeyValueAndVerifyPlainCount(4, Arrays.asList(
                KeyValuePair.Create(1, 2L),
                KeyValuePair.Create(2, 1L),
                KeyValuePair.Create(3, 1L),
                KeyValuePair.Create(4, 2L)));

            processKeyValueAndVerifyPlainCount(4, Arrays.asList(
                KeyValuePair.Create(1, 2L),
                KeyValuePair.Create(2, 1L),
                KeyValuePair.Create(3, 1L),
                KeyValuePair.Create(4, 3L)));
            long lastUpdateKeyFour = persistentStore ? -1L : CLUSTER.time.NowAsEpochMilliseconds; //-1L;

            kafkaStreams.Close();
            kafkaStreams = null;

            StreamsBuilder streamsBuilderForNewStore = new StreamsBuilder();

            //streamsBuilderForNewStore.AddStateStore(
            //    Stores.TimestampedKeyValueStoreBuilder(
            //        persistentStore ? Stores.PersistentTimestampedKeyValueStore(STORE_NAME) : Stores.InMemoryKeyValueStore(STORE_NAME),
            //        Serdes.Int(),
            //        Serdes.Long()))
            //    .< int, int> stream(inputStream)
            //     .Process(TimestampedKeyValueProcessor, STORE_NAME);

            kafkaStreams = new KafkaStreamsThread(streamsBuilderForNewStore.Build(), props);
            kafkaStreams.Start();

            verifyCountWithTimestamp(1, 2L, lastUpdateKeyOne);
            verifyCountWithTimestamp(2, 1L, lastUpdateKeyTwo);
            verifyCountWithTimestamp(3, 1L, lastUpdateKeyThree);
            verifyCountWithTimestamp(4, 3L, lastUpdateKeyFour);

            long currentTime = CLUSTER.time.NowAsEpochMilliseconds;
            processKeyValueAndVerifyCountWithTimestamp(1, currentTime + 42L, Arrays.asList(
                KeyValuePair.Create(1, ValueAndTimestamp.Make(3L, currentTime + 42L)),
                KeyValuePair.Create(2, ValueAndTimestamp.Make(1L, lastUpdateKeyTwo)),
                KeyValuePair.Create(3, ValueAndTimestamp.Make(1L, lastUpdateKeyThree)),
                KeyValuePair.Create(4, ValueAndTimestamp.Make(3L, lastUpdateKeyFour))));

            processKeyValueAndVerifyCountWithTimestamp(2, currentTime + 45L, Arrays.asList(
                KeyValuePair.Create(1, ValueAndTimestamp.Make(3L, currentTime + 42L)),
                KeyValuePair.Create(2, ValueAndTimestamp.Make(2L, currentTime + 45L)),
                KeyValuePair.Create(3, ValueAndTimestamp.Make(1L, lastUpdateKeyThree)),
                KeyValuePair.Create(4, ValueAndTimestamp.Make(3L, lastUpdateKeyFour))));

            // can process "out of order" record for different key
            processKeyValueAndVerifyCountWithTimestamp(4, currentTime + 21L, Arrays.asList(
                KeyValuePair.Create(1, ValueAndTimestamp.Make(3L, currentTime + 42L)),
                KeyValuePair.Create(2, ValueAndTimestamp.Make(2L, currentTime + 45L)),
                KeyValuePair.Create(3, ValueAndTimestamp.Make(1L, lastUpdateKeyThree)),
                KeyValuePair.Create(4, ValueAndTimestamp.Make(4L, currentTime + 21L))));

            processKeyValueAndVerifyCountWithTimestamp(4, currentTime + 42L, Arrays.asList(
                KeyValuePair.Create(1, ValueAndTimestamp.Make(3L, currentTime + 42L)),
                KeyValuePair.Create(2, ValueAndTimestamp.Make(2L, currentTime + 45L)),
                KeyValuePair.Create(3, ValueAndTimestamp.Make(1L, lastUpdateKeyThree)),
                KeyValuePair.Create(4, ValueAndTimestamp.Make(5L, currentTime + 42L))));

            // out of order (same key) record should not reduce result timestamp
            processKeyValueAndVerifyCountWithTimestamp(4, currentTime + 10L, Arrays.asList(
                KeyValuePair.Create(1, ValueAndTimestamp.Make(3L, currentTime + 42L)),
                KeyValuePair.Create(2, ValueAndTimestamp.Make(2L, currentTime + 45L)),
                KeyValuePair.Create(3, ValueAndTimestamp.Make(1L, lastUpdateKeyThree)),
                KeyValuePair.Create(4, ValueAndTimestamp.Make(6L, currentTime + 42L))));

            kafkaStreams.Close();
        }

        [Fact]
        public void ShouldProxyKeyValueStoreToTimestampedKeyValueStoreUsingPapi()
        {// throws Exception
            StreamsBuilder streamsBuilderForOldStore = new StreamsBuilder();

            //streamsBuilderForOldStore.AddStateStore(
            //    Stores.KeyValueStoreBuilder(
            //        Stores.PersistentKeyValueStore(STORE_NAME),
            //        Serdes.Int(),
            //        Serdes.Long()))
            //    .< int, int> stream(inputStream)
            //     .Process(KeyValueProcessor, STORE_NAME);

            StreamsConfig props = props();
            kafkaStreams = new KafkaStreamsThread(streamsBuilderForOldStore.Build(), props);
            kafkaStreams.Start();

            processKeyValueAndVerifyPlainCount(1, Collections.singletonList(KeyValuePair.Create(1, 1L)));

            processKeyValueAndVerifyPlainCount(1, Collections.singletonList(KeyValuePair.Create(1, 2L)));

            processKeyValueAndVerifyPlainCount(2, Arrays.asList(
                KeyValuePair.Create(1, 2L),
                KeyValuePair.Create(2, 1L)));

            processKeyValueAndVerifyPlainCount(3, Arrays.asList(
                KeyValuePair.Create(1, 2L),
                KeyValuePair.Create(2, 1L),
                KeyValuePair.Create(3, 1L)));

            processKeyValueAndVerifyPlainCount(4, Arrays.asList(
                KeyValuePair.Create(1, 2L),
                KeyValuePair.Create(2, 1L),
                KeyValuePair.Create(3, 1L),
                KeyValuePair.Create(4, 1L)));

            processKeyValueAndVerifyPlainCount(4, Arrays.asList(
                KeyValuePair.Create(1, 2L),
                KeyValuePair.Create(2, 1L),
                KeyValuePair.Create(3, 1L),
                KeyValuePair.Create(4, 2L)));

            processKeyValueAndVerifyPlainCount(4, Arrays.asList(
                KeyValuePair.Create(1, 2L),
                KeyValuePair.Create(2, 1L),
                KeyValuePair.Create(3, 1L),
                KeyValuePair.Create(4, 3L)));

            kafkaStreams.Close();
            kafkaStreams = null;

            StreamsBuilder streamsBuilderForNewStore = new StreamsBuilder();

            //streamsBuilderForNewStore.AddStateStore(
            //    Stores.TimestampedKeyValueStoreBuilder(
            //        Stores.PersistentKeyValueStore(STORE_NAME),
            //        Serdes.Int(),
            //        Serdes.Long()))
            //    .< int, int> stream(inputStream)
            //     .Process(TimestampedKeyValueProcessor, STORE_NAME);

            kafkaStreams = new KafkaStreamsThread(streamsBuilderForNewStore.Build(), props);
            kafkaStreams.Start();

            verifyCountWithSurrogateTimestamp(1, 2L);
            verifyCountWithSurrogateTimestamp(2, 1L);
            verifyCountWithSurrogateTimestamp(3, 1L);
            verifyCountWithSurrogateTimestamp(4, 3L);

            processKeyValueAndVerifyCount(1, 42L, Arrays.asList(
                KeyValuePair.Create(1, ValueAndTimestamp.Make(3L, -1L)),
                KeyValuePair.Create(2, ValueAndTimestamp.Make(1L, -1L)),
                KeyValuePair.Create(3, ValueAndTimestamp.Make(1L, -1L)),
                KeyValuePair.Create(4, ValueAndTimestamp.Make(3L, -1L))));

            processKeyValueAndVerifyCount(2, 45L, Arrays.asList(
                KeyValuePair.Create(1, ValueAndTimestamp.Make(3L, -1L)),
                KeyValuePair.Create(2, ValueAndTimestamp.Make(2L, -1L)),
                KeyValuePair.Create(3, ValueAndTimestamp.Make(1L, -1L)),
                KeyValuePair.Create(4, ValueAndTimestamp.Make(3L, -1L))));

            // can process "out of order" record for different key
            processKeyValueAndVerifyCount(4, 21L, Arrays.asList(
                KeyValuePair.Create(1, ValueAndTimestamp.Make(3L, -1L)),
                KeyValuePair.Create(2, ValueAndTimestamp.Make(2L, -1L)),
                KeyValuePair.Create(3, ValueAndTimestamp.Make(1L, -1L)),
                KeyValuePair.Create(4, ValueAndTimestamp.Make(4L, -1L))));

            processKeyValueAndVerifyCount(4, 42L, Arrays.asList(
                KeyValuePair.Create(1, ValueAndTimestamp.Make(3L, -1L)),
                KeyValuePair.Create(2, ValueAndTimestamp.Make(2L, -1L)),
                KeyValuePair.Create(3, ValueAndTimestamp.Make(1L, -1L)),
                KeyValuePair.Create(4, ValueAndTimestamp.Make(5L, -1L))));

            // out of order (same key) record should not reduce result timestamp
            processKeyValueAndVerifyCount(4, 10L, Arrays.asList(
                KeyValuePair.Create(1, ValueAndTimestamp.Make(3L, -1L)),
                KeyValuePair.Create(2, ValueAndTimestamp.Make(2L, -1L)),
                KeyValuePair.Create(3, ValueAndTimestamp.Make(1L, -1L)),
                KeyValuePair.Create(4, ValueAndTimestamp.Make(6L, -1L))));

            kafkaStreams.Close();
        }

        private void processKeyValueAndVerifyPlainCount<K, V>(
            K key,
            List<KeyValuePair<int, object>> expectedStoreContent)
        //throws Exception {
        {
            IntegrationTestUtils.ProduceKeyValuesSynchronously(
                inputStream,
                Collections.singletonList(KeyValuePair.Create(key, 0)),
                TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                    Serdes.Int().Serializer,
                    Serdes.Int().Serializer),
                CLUSTER.time);

            //TestUtils.WaitForCondition(
            //    () => {
            //        try {
            //            IReadOnlyKeyValueStore<K, V> store =
            //                kafkaStreams.store(STORE_NAME, QueryableStoreTypes.KeyValueStore);

            //IKeyValueIterator<K, V> All = store.All();
            //List<KeyValuePair<K, V>> storeContent = new LinkedList<>();
            //
            //                while (All.HasNext()) {
            //                    storeContent.Add(All.MoveNext());
            //                }
            //                return storeContent.Equals(expectedStoreContent);
            //            }
            //        } catch (Exception swallow) {
            //            //swallow.printStackTrace();
            //            System.Console.Error.WriteLine(swallow.Message);
            //            return false;
            //        }
            //    },
            //    60_000L,
            //    "Could not get expected result in time.");
        }

        private void verifyCountWithTimestamp<K>(K key,
            long value,
            long timestamp)
        {// throws Exception
            TestUtils.WaitForCondition(
                () =>
                {
                    try
                    {
                        IReadOnlyKeyValueStore<K, IValueAndTimestamp<long>> store =
                            kafkaStreams.store(STORE_NAME, QueryableStoreTypes.TimestampedKeyValueStore());
                        IValueAndTimestamp<long> count = store.Get(key);
                        return count.Value == value && count.Timestamp == timestamp;
                    }
                    catch (Exception swallow)
                    {
                        //swallow.printStackTrace();
                        System.Console.Error.WriteLine(swallow.Message);
                        return false;
                    }
                },
                60_000L,
                "Could not get expected result in time.");
        }

        //private void verifyCountWithSurrogateTimestamp<K>(K key, long value)
        //{// throws Exception
        //    TestUtils.WaitForCondition(
        //        () =>
        //        {
        //            try
        //            {
        //                IReadOnlyKeyValueStore<K, IValueAndTimestamp<long>> store =
        //                    kafkaStreams.store(STORE_NAME, QueryableStoreTypes.TimestampedKeyValueStore());
        //                IValueAndTimestamp<long> count = store.Get(key);
        //                return count.Value == value && count.Timestamp == -1L;
        //            }
        //            catch (Exception swallow)
        //            {
        //                Console.Error.WriteLine(swallow.StackTrace);
        //                Console.Error.WriteLine(swallow.Message);
        //                return false;
        //            }
        //        },
        //        60_000L,
        //        "Could not get expected result in time.");
        //}

        private void processKeyValueAndVerifyCount<K, V>(
            K key,
            long timestamp,
            List<KeyValuePair<int, object>> expectedStoreContent)
        //throws Exception {
        {
            IntegrationTestUtils.ProduceKeyValuesSynchronouslyWithTimestamp(
                inputStream,
                Collections.singletonList(KeyValuePair.Create(key, 0)),
                TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                    Serdes.Int().Serializer,
                    Serdes.Int().Serializer),
                timestamp);

            //        TestUtils.WaitForCondition(
            //            () =>
            //    {
            //                    IReadOnlyKeyValueStore<K, IValueAndTimestamp<V>> store =
            //                        kafkaStreams.store(STORE_NAME, QueryableStoreTypes.TimestampedKeyValueStore());
            //IKeyValueIterator<K, IValueAndTimestamp<V>> All = store.All();
            //var storeContent = new List<KeyValuePair<K, IValueAndTimestamp<V>>>();
            //                        while (All.HasNext()) {
            //                            storeContent.Add(All.MoveNext());
            //                        }
            //                        return storeContent.Equals(expectedStoreContent);
            //                    }
            //                }
            //    catch (Exception swallow)
            //    {
            //                    swallow.printStackTrace();
            //                    System.Console.Error.WriteLine(swallow.getMessage());
            //                    return false;
            //                }
            //            },
            //            60_000L,
            //            "Could not get expected result in time.");
        }

        private void processKeyValueAndVerifyCountWithTimestamp<K, V>(
            K key,
            long timestamp,
            List<KeyValuePair<int, object>> expectedStoreContent)
        {
            IntegrationTestUtils.ProduceKeyValuesSynchronouslyWithTimestamp(
                inputStream,
                Collections.singletonList(KeyValuePair.Create(key, 0)),
                TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                    Serdes.Int().Serializer,
                    Serdes.Int().Serializer),
                timestamp);

            //            TestUtils.WaitForCondition(
            //                () =>
            //                {
            //                    IReadOnlyKeyValueStore<K, IValueAndTimestamp<V>> store =
            //                    kafkaStreams.store(STORE_NAME, QueryableStoreTypes.TimestampedKeyValueStore());
            //                    IKeyValueIterator<K, IValueAndTimestamp<V>> All = store.All();
            //                    var storeContent = new List<KeyValuePair<K, IValueAndTimestamp<V>>>();
            //                    while (All.HasNext())
            //                    {
            //                        storeContent.Add(All.MoveNext());
            //                    }
            //                    return storeContent.Equals(expectedStoreContent);
            //                }
            //                } catch (Exception swallow) {
            //                    swallow.printStackTrace();
            //                    System.Console.Error.WriteLine(swallow.getMessage());
            //                    return false;
            //                }
            //},
            //            60_000L,
            //            "Could not get expected result in time.");
        }

        [Fact]
        public void shouldMigrateInMemoryWindowStoreToTimestampedWindowStoreUsingPapi()
        {// throws Exception
            StreamsBuilder streamsBuilderForOldStore = new StreamsBuilder();
            //  streamsBuilderForOldStore
            //      .AddStateStore(
            //          Stores.windowStoreBuilder(
            //              Stores.inMemoryWindowStore(
            //                  STORE_NAME,
            //                  TimeSpan.FromMilliseconds(1000L),
            //                  TimeSpan.FromMilliseconds(1000L),
            //                  false),
            //          Serdes.Int(),
            //          Serdes.Long()))
            //      .< int, int> stream(inputStream)
            //       .Process(WindowedProcessor, STORE_NAME);

            StreamsBuilder streamsBuilderForNewStore = new StreamsBuilder();
            //streamsBuilderForNewStore
            //    .AddStateStore(
            //        Stores.timestampedWindowStoreBuilder(
            //            Stores.inMemoryWindowStore(
            //                STORE_NAME,
            //                TimeSpan.FromMilliseconds(1000L),
            //                TimeSpan.FromMilliseconds(1000L),
            //                false),
            //    Serdes.Int(),
            //    Serdes.Long()))
            //    .< int, int> stream(inputStream)
            //     .Process(TimestampedWindowedProcessor, STORE_NAME);


            shouldMigrateWindowStoreToTimestampedWindowStoreUsingPapi(
                new KafkaStreamsThread(streamsBuilderForOldStore.Build(), props()),
                new KafkaStreamsThread(streamsBuilderForNewStore.Build(), props()),
                false);
        }

        [Fact]
        public void shouldMigratePersistentWindowStoreToTimestampedWindowStoreUsingPapi()
        {// throws Exception
            StreamsBuilder streamsBuilderForOldStore = new StreamsBuilder();

            // streamsBuilderForOldStore
            //     .AddStateStore(
            //         Stores.windowStoreBuilder(
            //             Stores.PersistentWindowStore(
            //                 STORE_NAME,
            //                 TimeSpan.FromMilliseconds(1000L),
            //                 TimeSpan.FromMilliseconds(1000L),
            //                 false),
            //             Serdes.Int(),
            //             Serdes.Long()))
            //     .< int, int> stream(inputStream)
            //      .Process(WindowedProcessor, STORE_NAME);

            StreamsBuilder streamsBuilderForNewStore = new StreamsBuilder();
            //streamsBuilderForNewStore
            //    .AddStateStore(
            //        Stores.timestampedWindowStoreBuilder(
            //            Stores.PersistentTimestampedWindowStore(
            //                STORE_NAME,
            //                TimeSpan.FromMilliseconds(1000L),
            //                TimeSpan.FromMilliseconds(1000L),
            //                false),
            //            Serdes.Int(),
            //            Serdes.Long()))
            //    .< int, int> stream(inputStream)
            //     .Process(TimestampedWindowedProcessor, STORE_NAME);

            StreamsConfig props = props();
            shouldMigrateWindowStoreToTimestampedWindowStoreUsingPapi(
                new KafkaStreamsThread(streamsBuilderForOldStore.Build(), props),
                new KafkaStreamsThread(streamsBuilderForNewStore.Build(), props),
                true);
        }

        private void shouldMigrateWindowStoreToTimestampedWindowStoreUsingPapi(
            KafkaStreamsThread kafkaStreamsOld,
            KafkaStreamsThread kafkaStreamsNew,
            bool persistentStore)
        {// throws Exception
            kafkaStreams = kafkaStreamsOld;
            kafkaStreams.Start();

            processWindowedKeyValueAndVerifyPlainCount(1, Collections.singletonList(
                KeyValuePair.Create(new Windowed<string>(1, new TimeWindow(0L, 1000L)), 1L)));

            processWindowedKeyValueAndVerifyPlainCount(1, Collections.singletonList(
                KeyValuePair.Create(new Windowed<string>(1, new TimeWindow(0L, 1000L)), 2L)));
            long lastUpdateKeyOne = persistentStore ? -1L : CLUSTER.time.NowAsEpochMilliseconds; //-1L;

            processWindowedKeyValueAndVerifyPlainCount(2, Arrays.asList(
                KeyValuePair.Create(new Windowed<string>(1, new TimeWindow(0L, 1000L)), 2L),
                KeyValuePair.Create(new Windowed<string>(2, new TimeWindow(0L, 1000L)), 1L)));
            long lastUpdateKeyTwo = persistentStore ? -1L : CLUSTER.time.NowAsEpochMilliseconds; //-1L;

            processWindowedKeyValueAndVerifyPlainCount(3, Arrays.asList(
                KeyValuePair.Create(new Windowed<string>(1, new TimeWindow(0L, 1000L)), 2L),
                KeyValuePair.Create(new Windowed<string>(2, new TimeWindow(0L, 1000L)), 1L),
                KeyValuePair.Create(new Windowed<string>(3, new TimeWindow(0L, 1000L)), 1L)));
            long lastUpdateKeyThree = persistentStore ? -1L : CLUSTER.time.NowAsEpochMilliseconds; //-1L;

            processWindowedKeyValueAndVerifyPlainCount(4, Arrays.asList(
                KeyValuePair.Create(new Windowed<string>(1, new TimeWindow(0L, 1000L)), 2L),
                KeyValuePair.Create(new Windowed<string>(2, new TimeWindow(0L, 1000L)), 1L),
                KeyValuePair.Create(new Windowed<string>(3, new TimeWindow(0L, 1000L)), 1L),
                KeyValuePair.Create(new Windowed<string>(4, new TimeWindow(0L, 1000L)), 1L)));

            processWindowedKeyValueAndVerifyPlainCount(4, Arrays.asList(
                KeyValuePair.Create(new Windowed<string>(1, new TimeWindow(0L, 1000L)), 2L),
                KeyValuePair.Create(new Windowed<string>(2, new TimeWindow(0L, 1000L)), 1L),
                KeyValuePair.Create(new Windowed<string>(3, new TimeWindow(0L, 1000L)), 1L),
                KeyValuePair.Create(new Windowed<string>(4, new TimeWindow(0L, 1000L)), 2L)));

            processWindowedKeyValueAndVerifyPlainCount(4, Arrays.asList(
                KeyValuePair.Create(new Windowed<string>(1, new TimeWindow(0L, 1000L)), 2L),
                KeyValuePair.Create(new Windowed<string>(2, new TimeWindow(0L, 1000L)), 1L),
                KeyValuePair.Create(new Windowed<string>(3, new TimeWindow(0L, 1000L)), 1L),
                KeyValuePair.Create(new Windowed<string>(4, new TimeWindow(0L, 1000L)), 3L)));

            long lastUpdateKeyFour = persistentStore ? -1L : CLUSTER.time.NowAsEpochMilliseconds;// -1L;

            kafkaStreams.Close();
            kafkaStreams = null;


            kafkaStreams = kafkaStreamsNew;
            kafkaStreams.Start();

            verifyWindowedCountWithTimestamp(new Windowed<string>(1, new TimeWindow(0L, 1000L)), 2L, lastUpdateKeyOne);
            verifyWindowedCountWithTimestamp(new Windowed<string>(2, new TimeWindow(0L, 1000L)), 1L, lastUpdateKeyTwo);
            verifyWindowedCountWithTimestamp(new Windowed<string>(3, new TimeWindow(0L, 1000L)), 1L, lastUpdateKeyThree);
            verifyWindowedCountWithTimestamp(new Windowed<string>(4, new TimeWindow(0L, 1000L)), 3L, lastUpdateKeyFour);

            long currentTime = CLUSTER.time.NowAsEpochMilliseconds; ;
            processKeyValueAndVerifyWindowedCountWithTimestamp(1, currentTime + 42L, Arrays.asList(
                KeyValuePair.Create(
                    new Windowed<string>(1, new TimeWindow(0L, 1000L)),
                    ValueAndTimestamp.Make(3L, currentTime + 42L)),
                KeyValuePair.Create(
                    new Windowed<string>(2, new TimeWindow(0L, 1000L)),
                    ValueAndTimestamp.Make(1L, lastUpdateKeyTwo)),
                KeyValuePair.Create(
                    new Windowed<string>(3, new TimeWindow(0L, 1000L)),
                    ValueAndTimestamp.Make(1L, lastUpdateKeyThree)),
                KeyValuePair.Create(
                    new Windowed<string>(4, new TimeWindow(0L, 1000L)),
                    ValueAndTimestamp.Make(3L, lastUpdateKeyFour))));

            processKeyValueAndVerifyWindowedCountWithTimestamp(2, currentTime + 45L, Arrays.asList(
                KeyValuePair.Create(
                    new Windowed<string>(1, new TimeWindow(0L, 1000L)),
                    ValueAndTimestamp.Make(3L, currentTime + 42L)),
                KeyValuePair.Create(
                    new Windowed<string>(2, new TimeWindow(0L, 1000L)),
                    ValueAndTimestamp.Make(2L, currentTime + 45L)),
                KeyValuePair.Create(
                    new Windowed<string>(3, new TimeWindow(0L, 1000L)),
                    ValueAndTimestamp.Make(1L, lastUpdateKeyThree)),
                KeyValuePair.Create(
                    new Windowed<string>(4, new TimeWindow(0L, 1000L)),
                    ValueAndTimestamp.Make(3L, lastUpdateKeyFour))));

            // can process "out of order" record for different key
            processKeyValueAndVerifyWindowedCountWithTimestamp(4, currentTime + 21L, Arrays.asList(
                KeyValuePair.Create(
                    new Windowed<string>(1, new TimeWindow(0L, 1000L)),
                    ValueAndTimestamp.Make(3L, currentTime + 42L)),
                KeyValuePair.Create(
                    new Windowed<string>(2, new TimeWindow(0L, 1000L)),
                    ValueAndTimestamp.Make(2L, currentTime + 45L)),
                KeyValuePair.Create(
                    new Windowed<string>(3, new TimeWindow(0L, 1000L)),
                    ValueAndTimestamp.Make(1L, lastUpdateKeyThree)),
                KeyValuePair.Create(
                    new Windowed<string>(4, new TimeWindow(0L, 1000L)),
                    ValueAndTimestamp.Make(4L, currentTime + 21L))));

            processKeyValueAndVerifyWindowedCountWithTimestamp(4, currentTime + 42L, Arrays.asList(
                KeyValuePair.Create(
                    new Windowed<string>(1, new TimeWindow(0L, 1000L)),
                    ValueAndTimestamp.Make(3L, currentTime + 42L)),
                KeyValuePair.Create(
                    new Windowed<string>(2, new TimeWindow(0L, 1000L)),
                    ValueAndTimestamp.Make(2L, currentTime + 45L)),
                KeyValuePair.Create(
                    new Windowed<string>(3, new TimeWindow(0L, 1000L)),
                    ValueAndTimestamp.Make(1L, lastUpdateKeyThree)),
                KeyValuePair.Create(
                    new Windowed<string>(4, new TimeWindow(0L, 1000L)),
                    ValueAndTimestamp.Make(5L, currentTime + 42L))));

            // out of order (same key) record should not reduce result timestamp
            processKeyValueAndVerifyWindowedCountWithTimestamp(4, currentTime + 10L, Arrays.asList(
                KeyValuePair.Create(
                    new Windowed<string>(1, new TimeWindow(0L, 1000L)),
                    ValueAndTimestamp.Make(3L, currentTime + 42L)),
                KeyValuePair.Create(
                    new Windowed<string>(2, new TimeWindow(0L, 1000L)),
                    ValueAndTimestamp.Make(2L, currentTime + 45L)),
                KeyValuePair.Create(
                    new Windowed<string>(3, new TimeWindow(0L, 1000L)),
                    ValueAndTimestamp.Make(1L, lastUpdateKeyThree)),
                KeyValuePair.Create(
                    new Windowed<string>(4, new TimeWindow(0L, 1000L)),
                    ValueAndTimestamp.Make(6L, currentTime + 42L))));

            // test new segment
            processKeyValueAndVerifyWindowedCountWithTimestamp(10, currentTime + 100001L, Collections.singletonList(
                KeyValuePair.Create(
                    new Windowed<string>(10, new TimeWindow(100000L, 101000L)), ValueAndTimestamp.Make(1L, currentTime + 100001L))));


            kafkaStreams.Close();
        }

        [Fact]
        public void shouldProxyWindowStoreToTimestampedWindowStoreUsingPapi()
        {// throws Exception
            StreamsBuilder streamsBuilderForOldStore = new StreamsBuilder();

            // streamsBuilderForOldStore.AddStateStore(
            //     Stores.windowStoreBuilder(
            //         Stores.PersistentWindowStore(
            //             STORE_NAME,
            //             TimeSpan.FromMilliseconds(1000L),
            //             TimeSpan.FromMilliseconds(1000L),
            //             false),
            //         Serdes.Int(),
            //         Serdes.Long()))
            //     .< int, int> stream(inputStream)
            //      .Process(WindowedProcessor, STORE_NAME);

            StreamsConfig props = props();
            kafkaStreams = new KafkaStreamsThread(streamsBuilderForOldStore.Build(), props);
            kafkaStreams.Start();

            processWindowedKeyValueAndVerifyPlainCount(1, Collections.singletonList(
                KeyValuePair.Create(new Windowed<string>(1, new TimeWindow(0L, 1000L)), 1L)));

            processWindowedKeyValueAndVerifyPlainCount(1, Collections.singletonList(
                KeyValuePair.Create(new Windowed<string>(1, new TimeWindow(0L, 1000L)), 2L)));

            processWindowedKeyValueAndVerifyPlainCount(2, Arrays.asList(
                KeyValuePair.Create(new Windowed<string>(1, new TimeWindow(0L, 1000L)), 2L),
                KeyValuePair.Create(new Windowed<string>(2, new TimeWindow(0L, 1000L)), 1L)));

            processWindowedKeyValueAndVerifyPlainCount(3, Arrays.asList(
                KeyValuePair.Create(new Windowed<string>(1, new TimeWindow(0L, 1000L)), 2L),
                KeyValuePair.Create(new Windowed<string>(2, new TimeWindow(0L, 1000L)), 1L),
                KeyValuePair.Create(new Windowed<string>(3, new TimeWindow(0L, 1000L)), 1L)));

            processWindowedKeyValueAndVerifyPlainCount(4, Arrays.asList(
                KeyValuePair.Create(new Windowed<string>(1, new TimeWindow(0L, 1000L)), 2L),
                KeyValuePair.Create(new Windowed<string>(2, new TimeWindow(0L, 1000L)), 1L),
                KeyValuePair.Create(new Windowed<string>(3, new TimeWindow(0L, 1000L)), 1L),
                KeyValuePair.Create(new Windowed<string>(4, new TimeWindow(0L, 1000L)), 1L)));

            processWindowedKeyValueAndVerifyPlainCount(4, Arrays.asList(
                KeyValuePair.Create(new Windowed<string>(1, new TimeWindow(0L, 1000L)), 2L),
                KeyValuePair.Create(new Windowed<string>(2, new TimeWindow(0L, 1000L)), 1L),
                KeyValuePair.Create(new Windowed<string>(3, new TimeWindow(0L, 1000L)), 1L),
                KeyValuePair.Create(new Windowed<string>(4, new TimeWindow(0L, 1000L)), 2L)));

            processWindowedKeyValueAndVerifyPlainCount(4, Arrays.asList(
                KeyValuePair.Create(new Windowed<string>(1, new TimeWindow(0L, 1000L)), 2L),
                KeyValuePair.Create(new Windowed<string>(2, new TimeWindow(0L, 1000L)), 1L),
                KeyValuePair.Create(new Windowed<string>(3, new TimeWindow(0L, 1000L)), 1L),
                KeyValuePair.Create(new Windowed<string>(4, new TimeWindow(0L, 1000L)), 3L)));

            kafkaStreams.Close();
            kafkaStreams = null;

            StreamsBuilder streamsBuilderForNewStore = new StreamsBuilder();

            //streamsBuilderForNewStore.AddStateStore(
            //    Stores.timestampedWindowStoreBuilder(
            //        Stores.PersistentWindowStore(
            //            STORE_NAME,
            //            TimeSpan.FromMilliseconds(1000L),
            //            TimeSpan.FromMilliseconds(1000L),
            //            false),
            //        Serdes.Int(),
            //        Serdes.Long()))
            //    .< int, int> stream(inputStream)
            //     .Process(TimestampedWindowedProcessor, STORE_NAME);

            kafkaStreams = new KafkaStreamsThread(streamsBuilderForNewStore.Build(), props);
            kafkaStreams.Start();

            verifyWindowedCountWithSurrogateTimestamp(new Windowed<string>(1, new TimeWindow(0L, 1000L)), 2L);
            verifyWindowedCountWithSurrogateTimestamp(new Windowed<string>(2, new TimeWindow(0L, 1000L)), 1L);
            verifyWindowedCountWithSurrogateTimestamp(new Windowed<string>(3, new TimeWindow(0L, 1000L)), 1L);
            verifyWindowedCountWithSurrogateTimestamp(new Windowed<string>(4, new TimeWindow(0L, 1000L)), 3L);

            processKeyValueAndVerifyWindowedCountWithTimestamp(1, 42L, Arrays.asList(
                KeyValuePair.Create(new Windowed<string>(1, new TimeWindow(0L, 1000L)), ValueAndTimestamp.Make(3L, -1L)),
                KeyValuePair.Create(new Windowed<string>(2, new TimeWindow(0L, 1000L)), ValueAndTimestamp.Make(1L, -1L)),
                KeyValuePair.Create(new Windowed<string>(3, new TimeWindow(0L, 1000L)), ValueAndTimestamp.Make(1L, -1L)),
                KeyValuePair.Create(new Windowed<string>(4, new TimeWindow(0L, 1000L)), ValueAndTimestamp.Make(3L, -1L))));

            processKeyValueAndVerifyWindowedCountWithTimestamp(2, 45L, Arrays.asList(
                KeyValuePair.Create(new Windowed<string>(1, new TimeWindow(0L, 1000L)), ValueAndTimestamp.Make(3L, -1L)),
                KeyValuePair.Create(new Windowed<string>(2, new TimeWindow(0L, 1000L)), ValueAndTimestamp.Make(2L, -1L)),
                KeyValuePair.Create(new Windowed<string>(3, new TimeWindow(0L, 1000L)), ValueAndTimestamp.Make(1L, -1L)),
                KeyValuePair.Create(new Windowed<string>(4, new TimeWindow(0L, 1000L)), ValueAndTimestamp.Make(3L, -1L))));

            // can process "out of order" record for different key
            processKeyValueAndVerifyWindowedCountWithTimestamp(4, 21L, Arrays.asList(
                KeyValuePair.Create(new Windowed<string>(1, new TimeWindow(0L, 1000L)), ValueAndTimestamp.Make(3L, -1L)),
                KeyValuePair.Create(new Windowed<string>(2, new TimeWindow(0L, 1000L)), ValueAndTimestamp.Make(2L, -1L)),
                KeyValuePair.Create(new Windowed<string>(3, new TimeWindow(0L, 1000L)), ValueAndTimestamp.Make(1L, -1L)),
                KeyValuePair.Create(new Windowed<string>(4, new TimeWindow(0L, 1000L)), ValueAndTimestamp.Make(4L, -1L))));

            processKeyValueAndVerifyWindowedCountWithTimestamp(4, 42L, Arrays.asList(
                KeyValuePair.Create(new Windowed<string>(1, new TimeWindow(0L, 1000L)), ValueAndTimestamp.Make(3L, -1L)),
                KeyValuePair.Create(new Windowed<string>(2, new TimeWindow(0L, 1000L)), ValueAndTimestamp.Make(2L, -1L)),
                KeyValuePair.Create(new Windowed<string>(3, new TimeWindow(0L, 1000L)), ValueAndTimestamp.Make(1L, -1L)),
                KeyValuePair.Create(new Windowed<string>(4, new TimeWindow(0L, 1000L)), ValueAndTimestamp.Make(5L, -1L))));

            // out of order (same key) record should not reduce result timestamp
            processKeyValueAndVerifyWindowedCountWithTimestamp(4, 10L, Arrays.asList(
                KeyValuePair.Create(new Windowed<string>(1, new TimeWindow(0L, 1000L)), ValueAndTimestamp.Make(3L, -1L)),
                KeyValuePair.Create(new Windowed<string>(2, new TimeWindow(0L, 1000L)), ValueAndTimestamp.Make(2L, -1L)),
                KeyValuePair.Create(new Windowed<string>(3, new TimeWindow(0L, 1000L)), ValueAndTimestamp.Make(1L, -1L)),
                KeyValuePair.Create(new Windowed<string>(4, new TimeWindow(0L, 1000L)), ValueAndTimestamp.Make(6L, -1L))));

            // test new segment
            processKeyValueAndVerifyWindowedCountWithTimestamp(10, 100001L, Collections.singletonList(
                KeyValuePair.Create(new Windowed<string>(10, new TimeWindow(100000L, 101000L)), ValueAndTimestamp.Make(1L, -1L))));


            kafkaStreams.Close();
        }

        private void processWindowedKeyValueAndVerifyPlainCount<K, V>(K key,
                                                                       List<KeyValuePair<IWindowed<int>, object>> expectedStoreContent)
        //throws Exception {
        {
            IntegrationTestUtils.ProduceKeyValuesSynchronously(
                inputStream,
                Collections.singletonList(KeyValuePair.Create(key, 0)),
                TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                    Serdes.Int().Serializer,
                    Serdes.Int().Serializer),
                CLUSTER.time);

            //        TestUtils.WaitForCondition(
            //            () => {
            //                try {
            //                    IReadOnlyWindowStore<K, V> store =
            //                        kafkaStreams.store(STORE_NAME, QueryableStoreTypes.windowStore());
            //                    try { 
            // (IKeyValueIterator<IWindowed<K>, V> All = store.All());
            //                        List<KeyValuePair<IWindowed<K>, V>> storeContent = new LinkedList<>();
            //                        while (All.HasNext()) {
            //                            storeContent.Add(All.MoveNext());
            //                        }
            //                        return storeContent.Equals(expectedStoreContent);
            //                    }
            //                } catch (Exception swallow) {
            //                    swallow.printStackTrace();
            //                    System.Console.Error.WriteLine(swallow.getMessage());
            //                    return false;
            //                }
            //            },
            //            60_000L,
            //            "Could not get expected result in time.");
        }

        private void verifyWindowedCountWithSurrogateTimestamp<K>(IWindowed<K> key,
                                                                   long value)
        {// throws Exception
            TestUtils.WaitForCondition(
                () =>
                {
                    try
                    {
                        IReadOnlyWindowStore<K, IValueAndTimestamp<long>> store =
                            kafkaStreams.store(STORE_NAME, QueryableStoreTypes.timestampedWindowStore());
                        IValueAndTimestamp<long> count = store.Fetch(key.Key, key.window().Start());
                        return count.Value == value && count.Timestamp == -1L;
                    }
                    catch (Exception swallow)
                    {
                        swallow.printStackTrace();
                        System.Console.Error.WriteLine(swallow.getMessage());
                        return false;
                    }
                },
                60_000L,
                "Could not get expected result in time.");
        }

        private void verifyWindowedCountWithTimestamp<K>(IWindowed<K> key,
                                                          long value,
                                                          long timestamp)
        {// throws Exception
         //    TestUtils.WaitForCondition(
         //        () =>
         //        {
         //            try
         //            {
         //                IReadOnlyWindowStore<K, IValueAndTimestamp<long>> store =
         //                    kafkaStreams.store(STORE_NAME, QueryableStoreTypes.timestampedWindowStore());
         //                IValueAndTimestamp<long> count = store.Fetch(key.Key, key.window().Start());
         //                return count.Value == value && count.Timestamp == timestamp;
         //            }
         //            catch (Exception swallow)
         //            {
         //                swallow.printStackTrace();
         //                System.Console.Error.WriteLine(swallow.getMessage());
         //                return false;
         //            }
         //        },
         //        60_000L,
         //        "Could not get expected result in time.");
        }

        private void processKeyValueAndVerifyWindowedCountWithTimestamp<K, V>(K key,
                                                                               long timestamp,
                                                                               List<KeyValuePair<IWindowed<int>, object>> expectedStoreContent)
        {
            IntegrationTestUtils.ProduceKeyValuesSynchronouslyWithTimestamp(
                inputStream,
                Collections.singletonList(KeyValuePair.Create(key, 0)),
                TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                    Serdes.Int().Serializer,
                    Serdes.Int().Serializer),
                timestamp);

            //        TestUtils.WaitForCondition(
            //            () => {
            //                    IReadOnlyWindowStore<K, IValueAndTimestamp<V>> store =
            //                        kafkaStreams.store(STORE_NAME, QueryableStoreTypes.timestampedWindowStore());
            //        IKeyValueIterator<IWindowed<K>, IValueAndTimestamp<V>> All = store.All();
            //        var storeContent = new List<KeyValuePair<IWindowed<K>, IValueAndTimestamp<V>>>();
            //                        while (All.HasNext()) {
            //                            storeContent.Add(All.MoveNext());
            //                        }
            //                        return storeContent.Equals(expectedStoreContent);
            //                    }
            //                } catch (Exception swallow) {
            //                    //swallow.printStackTrace();
            //                    Console.Error.WriteLine(swallow.Message);
            //                    return false;
            //                }
            //            },
            //            60_000L,
            //            "Could not get expected result in time.");
        }

        private class KeyValueProcessor : Processor<int, int>
        {
            private IKeyValueStore<int, long> store;



            public void Init(IProcessorContext context)
            {
                store = (IKeyValueStore<int, long>)context.getStateStore(STORE_NAME);
            }


            public void process(int key, int value)
            {
                long newCount;

                long oldCount = store.Get(key);
                if (oldCount != null)
                {
                    newCount = oldCount + 1L;
                }
                else
                {
                    newCount = 1L;
                }

                store.Put(key, newCount);
            }


            public void Close() { }
        }

        private class TimestampedKeyValueProcessor : Processor<int, int>
        {
            private IProcessorContext context;
            private ITimestampedKeyValueStore<int, long> store;



            public void Init(IProcessorContext context)
            {
                this.context = context;
                store = (ITimestampedKeyValueStore<int, long>)context.getStateStore(STORE_NAME);
            }


            public void process(int key, int value)
            {
                long newCount;

                IValueAndTimestamp<long> oldCountWithTimestamp = store.Get(key);
                long newTimestamp;

                if (oldCountWithTimestamp == null)
                {
                    newCount = 1L;
                    newTimestamp = context.Timestamp;
                }
                else
                {
                    newCount = oldCountWithTimestamp.Value + 1L;
                    newTimestamp = Math.Max(oldCountWithTimestamp.Timestamp, context.Timestamp);
                }

                store.Put(key, ValueAndTimestamp.Make(newCount, newTimestamp));
            }


            public void Close() { }
        }

        private class WindowedProcessor : Processor<int, int>
        {
            private IWindowStore<int, long> store;



            public void Init(IProcessorContext context)
            {
                store = (IWindowStore<int, long>)context.getStateStore(STORE_NAME);
            }


            public void process(int key, int value)
            {
                long newCount;

                long oldCount = store.Fetch(key, key < 10 ? 0L : 100000L);
                if (oldCount != null)
                {
                    newCount = oldCount + 1L;
                }
                else
                {
                    newCount = 1L;
                }

                store.Put(key, newCount, key < 10 ? 0L : 100000L);
            }


            public void Close() { }
        }

        private class TimestampedWindowedProcessor : Processor<int, int>
        {
            private IProcessorContext context;
            private ITimestampedWindowStore<int, long> store;



            public void Init(IProcessorContext context)
            {
                this.context = context;
                store = (ITimestampedWindowStore<int, long>)context.getStateStore(STORE_NAME);
            }


            public void process(int key, int value)
            {
                long newCount;

                IValueAndTimestamp<long> oldCountWithTimestamp = store.Fetch(key, key < 10 ? 0L : 100000L);
                long newTimestamp;

                if (oldCountWithTimestamp == null)
                {
                    newCount = 1L;
                    newTimestamp = context.Timestamp;
                }
                else
                {
                    newCount = oldCountWithTimestamp.Value + 1L;
                    newTimestamp = Math.Max(oldCountWithTimestamp.Timestamp, context.Timestamp);
                }

                store.Put(key, ValueAndTimestamp.Make(newCount, newTimestamp), key < 10 ? 0L : 100000L);
            }


            public void Close() { }
        }
    }
}
