using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.Tasks;
using Kafka.Streams.Temporary;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Threads.KafkaStreams;
using Kafka.Streams.Topologies;
using System;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace Kafka.Streams.Tests.Integration
{
    /*






    *

    *





    */




























































    public class RestoreIntegrationTest
    {
        private const int NUM_BROKERS = 1;

        private const string APPID = "restore-test";


        public static EmbeddedKafkaCluster CLUSTER =
                new EmbeddedKafkaCluster(NUM_BROKERS);
        private const string INPUT_STREAM = "input-stream";
        private const string INPUT_STREAM_2 = "input-stream-2";
        private readonly int numberOfKeys = 10000;
        private KafkaStreamsThread kafkaStreams;


        internal static void CreateTopics()
        {// throws InterruptedException
            CLUSTER.CreateTopic(INPUT_STREAM, 2, 1);
            CLUSTER.CreateTopic(INPUT_STREAM_2, 2, 1);
            CLUSTER.CreateTopic(APPID + "-store-changelog", 2, 1);
        }

        private StreamsConfig Props(string applicationId)
        {
            StreamsConfig streamsConfiguration = new StreamsConfig
            {
                ApplicationId = applicationId,
                BootstrapServers = CLUSTER.bootstrapServers(),
                CacheMaxBytesBuffering = 0,
                StateStoreDirectory = TestUtils.GetTempDirectory(applicationId),
                DefaultKeySerdeType = Serdes.Int().GetType(),
                DefaultValueSerdeType = Serdes.Int().GetType(),
                CommitIntervalMs = 1000,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            return streamsConfiguration;
        }


        public void Shutdown()
        {
            if (kafkaStreams != null)
            {
                kafkaStreams.Close(TimeSpan.FromSeconds(30));
            }
        }

        [Fact]
        public void ShouldRestoreStateFromSourceTopic()
        {// throws Exception
            int numReceived = 0;
            StreamsBuilder builder = new StreamsBuilder();

            StreamsConfig props = props(APPID);
            props.Set(StreamsConfig.TOPOLOGY_OPTIMIZATIONConfig, StreamsConfig.OPTIMIZEConfig);

            // restoring from 1000 to 4000 (committed), and then process from 4000 to 5000 on each of the two partitions
            int offsetLimitDelta = 1000;
            long offsetCheckpointed = 1000;
            CreateStateForRestoration(INPUT_STREAM);
            SetCommittedOffset(INPUT_STREAM, offsetLimitDelta);

            StateDirectory stateDirectory = new StateDirectory(null, new StreamsConfig(props), new MockTime(), true);
            new OffsetCheckpoint(new FileInfo(Path.Combine(stateDirectory.DirectoryForTask(new TaskId(0, 0)).FullName, ".checkpoint")))
                    .Write(new Dictionary<TopicPartition, long?>
                    {
                        { new TopicPartition(INPUT_STREAM, 0), offsetCheckpointed },
                    });

            new OffsetCheckpoint(new FileInfo(Path.Combine(stateDirectory.DirectoryForTask(new TaskId(0, 1)).FullName, ".checkpoint")))
                    .Write(new Dictionary<TopicPartition, long?>
                    {
                        { new TopicPartition(INPUT_STREAM, 1), offsetCheckpointed },
                    });
            //CountDownLatch startupLatch = new CountDownLatch(1);
            //CountDownLatch shutdownLatch = new CountDownLatch(1);

            builder.Table(INPUT_STREAM, Materialized.As<int, int, IKeyValueStore<Bytes, byte[]>>("store").WithKeySerde(Serdes.Int()).WithValueSerde(Serdes.Int()))
                    .ToStream();
            //                    .ForEach((key, value) =>
            //                    {
            //                        if (numReceived.incrementAndGet() == 2 * offsetLimitDelta)
            //                        {
            //                            shutdownLatch.countDown();
            //                        }
            //                    });
            //
            //            kafkaStreams = new KafkaStreamsThread(builder.Build(props), props);
            //            kafkaStreams.SetStateListener((newState, oldState) =>
            //            {
            //                if (newState == KafkaStreamsThreadStates.RUNNING && oldState == KafkaStreamsThreadStates.REBALANCING)
            //                {
            //                    startupLatch.countDown();
            //                }
            //            });
            //
            //            AtomicLong restored = new AtomicLong(0);
            //            kafkaStreams.setGlobalStateRestoreListener(new StateRestoreListener()
            //            {
            //
            //
            //            public void onRestoreStart(TopicPartition topicPartition, string storeName, long startingOffset, long endingOffset)
            //            {
            //
            //            }
            //
            //
            //            public void onBatchRestored(TopicPartition topicPartition, string storeName, long batchEndOffset, long numRestored)
            //            {
            //
            //            }
            //
            //
            //            public void onRestoreEnd(TopicPartition topicPartition, string storeName, long totalRestored)
            //            {
            //                restored.addAndGet(totalRestored);
            //            }
            //        });
            kafkaStreams.Start();

            // Assert.True(startupLatch.wait(30, TimeUnit.SECONDS));
            Assert.Equal(restored, ((long)numberOfKeys - offsetLimitDelta * 2 - offsetCheckpointed * 2));

            //Assert.True(shutdownLatch.wait(30, TimeUnit.SECONDS));
            Assert.Equal(numReceived, (offsetLimitDelta * 2));
        }

        [Fact]
        public void ShouldRestoreStateFromChangelogTopic()
        {// throws Exception
            int numReceived = 0;
            StreamsBuilder builder = new StreamsBuilder();

            StreamsConfig props = props(APPID);

            // restoring from 1000 to 5000, and then process from 5000 to 10000 on each of the two partitions
            int offsetCheckpointed = 1000;
            CreateStateForRestoration(APPID + "-store-changelog");
            CreateStateForRestoration(INPUT_STREAM);

            StateDirectory stateDirectory = new StateDirectory(new StreamsConfig(props), new MockTime(), true);
            new OffsetCheckpoint(new FileInfo(Path.Combine(stateDirectory.DirectoryForTask(new TaskId(0, 0)).FullName, ".checkpoint")))
                    .Write(Collections.singletonMap(new TopicPartition(APPID + "-store-changelog", 0), offsetCheckpointed));
            new OffsetCheckpoint(new FileInfo(Path.Combine(stateDirectory.DirectoryForTask(new TaskId(0, 1)).FullName, ".checkpoint")))
                    .Write(Collections.singletonMap(new TopicPartition(APPID + "-store-changelog", 1), offsetCheckpointed));

            CountDownLatch startupLatch = new CountDownLatch(1);
            CountDownLatch shutdownLatch = new CountDownLatch(1);

            builder.Table(INPUT_STREAM, Consumed.With(Serdes.Int(), Serdes.Int()), Materialized.As<int, int, IKeyValueStore<Bytes, byte[]>>("store"))
                .ToStream()
                .ForEach((key, value) =>
                {
                    if (++numReceived == numberOfKeys)
                    {
                        shutdownLatch.countDown();
                    }
                });

            kafkaStreams = new KafkaStreamsThread(builder.Build(), props);
            kafkaStreams.SetStateListener((newState, oldState) =>
            {
                if (newState == KafkaStreamsThreadStates.RUNNING
                    && oldState == KafkaStreamsThreadStates.REBALANCING)
                {
                    startupLatch.countDown();
                }
            });

            long restored = 0;
            kafkaStreams.SetGlobalStateRestoreListener(new StateRestoreListener());
            //        {
            //
            //
            //            public void onRestoreStart(TopicPartition topicPartition, string storeName, long startingOffset, long endingOffset)
            //        {
            //
            //        }
            //
            //
            //        public void onBatchRestored(TopicPartition topicPartition, string storeName, long batchEndOffset, long numRestored)
            //        {
            //
            //        }
            //
            //
            //        public void onRestoreEnd(TopicPartition topicPartition, string storeName, long totalRestored)
            //        {
            //            restored.addAndGet(totalRestored);
            //        }
            //    });
            kafkaStreams.Start();

            Assert.True(startupLatch.wait(30, TimeUnit.SECONDS));
            Assert.Equal(restored, ((long)numberOfKeys - 2 * offsetCheckpointed));

            Assert.True(shutdownLatch.wait(30, TimeUnit.SECONDS));
            Assert.Equal(numReceived, (numberOfKeys));
        }


        [Fact]
        public void ShouldSuccessfullyStartWhenLoggingDisabled()
        {// throws InterruptedException
            StreamsBuilder builder = new StreamsBuilder();

            IKStream<int, int> stream = builder.Stream<int, int>(INPUT_STREAM);
            stream.GroupByKey()
                    .Reduce((value1, value2) => value1 + value2,
                        Materialized.As<int, int, IKeyValueStore<Bytes, byte[]>>("reduce-store").WithLoggingDisabled());

            CountDownLatch startupLatch = new CountDownLatch(1);
            kafkaStreams = new KafkaStreamsThread(builder.Build(), props(APPID));
            kafkaStreams.SetStateListener((newState, oldState) =>
            {
                if (newState == KafkaStreamsThreadStates.RUNNING && oldState == KafkaStreamsThreadStates.REBALANCING)
                {
                    startupLatch.countDown();
                }
            });

            kafkaStreams.Start();

            Assert.True(startupLatch.wait(30, TimeUnit.SECONDS));
        }

        [Fact]
        public void ShouldProcessDataFromStoresWithLoggingDisabled()
        {// throws InterruptedException, ExecutionException

            IntegrationTestUtils.ProduceKeyValuesSynchronously(
                INPUT_STREAM_2,
                Arrays.asList(KeyValuePair.Create(1, 1),
                              KeyValuePair.Create(2, 2),
                              KeyValuePair.Create(3, 3)),
                TestUtils.ProducerConfig(CLUSTER.bootstrapServers(),
                Serdes.Int().Serializer,
                Serdes.Int().Serializer),
                CLUSTER.time);

            IKeyValueBytesStoreSupplier lruMapSupplier = Stores.lruMap(INPUT_STREAM_2, 10);

            IStoreBuilder<IKeyValueStore<int, int>> storeBuilder = new KeyValueStoreBuilder<int, int>(
                null,
                lruMapSupplier,
                Serdes.Int(),
                Serdes.Int())
                    .WithLoggingDisabled();

            StreamsBuilder streamsBuilder = new StreamsBuilder();

            streamsBuilder.AddStateStore<string, string, IKeyValueStore<int, int>>(storeBuilder);

            IKStream<string, string> stream = streamsBuilder.Stream<string, string>(INPUT_STREAM_2);
            //CountDownLatch processorLatch = new CountDownLatch(3);
            stream.Process(() => new KeyValueStoreProcessor(INPUT_STREAM_2/*, processorLatch*/), INPUT_STREAM_2);

            Topology topology = streamsBuilder.Build();

            kafkaStreams = new KafkaStreamsThread(topology, props(APPID + "-logging-disabled"));

            // CountDownLatch latch = new CountDownLatch(1);

            kafkaStreams.SetStateListener((newState, oldState) =>
            {
                if (newState == KafkaStreamsThreadStates.RUNNING && oldState == KafkaStreamsThreadStates.REBALANCING)
                {
                    //latch.countDown();
                }
            });

            kafkaStreams.Start();

            //latch.await(30, TimeUnit.SECONDS);

            Assert.True(processorLatch.await(30, TimeUnit.SECONDS));

        }


        public class KeyValueStoreProcessor : IKeyValueProcessor<int, int>
        {

            private readonly string topic;
            //private CountDownLatch processorLatch;

            private IKeyValueStore<int, int> store;

            public KeyValueStoreProcessor(string topic)//, CountDownLatch processorLatch)
            {
                this.topic = topic;
                // this.processorLatch = processorLatch;
            }

            public void Init(IProcessorContext context)
            {
                this.store = (IKeyValueStore<int, int>)context.GetStateStore(topic);
            }

            public void Process(int? key, int value)
            {
                if (key == null)
                {
                    return;
                    // processorLatch.countDown();
                }

                store.Add(key.Value, value);
            }

            public void Close()
            {
            }

            public void Process(int key, int value)
            {
            }

            public void Process<K1, V1>(K1 key, V1 value)
            {
            }
        }

        private void CreateStateForRestoration(string changelogTopic)
        {
            StreamsConfig producerConfig = new StreamsConfig
            {
                BootstrapServers = CLUSTER.bootstrapServers()
            };

            KafkaProducer<int, int> producer =
                         new KafkaProducer<>(producerConfig, Serdes.Int().Serializer, Serdes.Int().Serializer);

            for (int i = 0; i < numberOfKeys; i++)
            {
                producer.send(new ProducerRecord<>(changelogTopic, i, i));
            }
        }

        private void SetCommittedOffset(string topic, int limitDelta)
        {
            StreamsConfig consumerConfig = new StreamsConfig();
            consumerConfig.Put(ConsumerConfig.BootstrapServersConfig, CLUSTER.bootstrapServers());
            consumerConfig.Put(ConsumerConfig.GROUP_ID_CONFIG, APPID);
            consumerConfig.Put(ConsumerConfig.CLIENT_ID_CONFIG, "commit-consumer");
            consumerConfig.Put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.Int().Deserializer);
            consumerConfig.Put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.Int().Deserializer);

            IConsumer<int, int> consumer = new KafkaConsumer<int, int>(consumerConfig);
            List<TopicPartition> partitions = Arrays.asList(
                new TopicPartition(topic, 0),
                new TopicPartition(topic, 1));

            consumer.Assign(partitions);
            consumer.SeekToEnd(partitions);

            foreach (TopicPartition partition in partitions)
            {
                long position = consumer.Position(partition);
                consumer.Seek(new TopicPartitionOffset(partition, position - limitDelta));
            }

            consumer.Commit();
            consumer.Close();
        }
    }
}
