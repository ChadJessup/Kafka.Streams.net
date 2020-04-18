namespace Kafka.Streams.Tests.Tests
{
}
//using Confluent.Kafka;
//using Xunit;

//namespace Kafka.Streams.Tests.Tools
//{
//    public class StreamsStandByReplicaTest {

//    public static void main(string[] args){ //throws IOException
//        if (args.Length < 2) {
//            System.Console.Error.WriteLine("StreamsStandByReplicaTest are expecting two parameters: " +
//                "propFile, additionalConfigs; but only see " + args.Length + " parameter");
//            System.exit(1);
//        }

//        System.Console.Out.WriteLine("StreamsTest instance started");

//        string propFileName = args[0];
//        string additionalConfigs = args[1];

//        StreamsConfig streamsProperties = Utils.loadProps(propFileName);
//        string kafka = streamsProperties.getProperty(StreamsConfig.BootstrapServersConfig);

//        if (kafka == null) {
//            System.Console.Error.WriteLine("No bootstrap kafka servers specified in " + StreamsConfig.BootstrapServersConfig);
//            System.exit(1);
//        }

//        streamsProperties.Put(StreamsConfig.ApplicationIdConfig, "kafka-streams-standby-tasks");
//        streamsProperties.Put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
//        streamsProperties.Put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
//        streamsProperties.Put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
//        streamsProperties.Put(StreamsConfig.DefaultKeySerdeClassConfig, Serdes.String().GetType());
//        streamsProperties.Put(StreamsConfig.DefaultValueSerdeClassConfig, Serdes.String().GetType());
//        streamsProperties.Put(StreamsConfig.producerPrefix(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG), true);

//        if (additionalConfigs == null) {
//            System.Console.Error.WriteLine("additional configs are not provided");
//            System.Console.Error.Flush();
//            System.exit(1);
//        }

//        Dictionary<string, string> updated = SystemTestUtil.parseConfigs(additionalConfigs);
//        System.Console.Out.WriteLine("Updating configs with " + updated);

//        string sourceTopic = updated.remove("sourceTopic");
//        string sinkTopic1 = updated.remove("sinkTopic1");
//        string sinkTopic2 = updated.remove("sinkTopic2");

//        if (sourceTopic == null || sinkTopic1 == null || sinkTopic2 == null) {
//            System.Console.Error.WriteLine(string.format(
//                "one or more required topics null sourceTopic[%s], sinkTopic1[%s], sinkTopic2[%s]",
//                sourceTopic,
//                sinkTopic1,
//                sinkTopic2));
//            System.Console.Error.Flush();
//            System.exit(1);
//        }

//        streamsProperties.PutAll(updated);

//        if (!confirmCorrectConfigs(streamsProperties)) {
//            System.Console.Error.WriteLine(string.format("ERROR: Did not have All required configs expected  to contain %s, %s,  %s,  %s",
//                                             StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG),
//                                             StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG),
//                                             StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG),
//                                             StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG)));

//            System.exit(1);
//        }

//        StreamsBuilder builder = new StreamsBuilder();

//        string inMemoryStoreName = "in-memory-store";
//        string persistentMemoryStoreName = "Persistent-memory-store";

//        IKeyValueBytesStoreSupplier inMemoryStoreSupplier = Stores.InMemoryKeyValueStore(inMemoryStoreName);
//        IKeyValueBytesStoreSupplier persistentStoreSupplier = Stores.PersistentKeyValueStore(persistentMemoryStoreName);

//        Serde<string> stringSerde = Serdes.String();
//        ValueMapper<long, string> countMapper = object::ToString;

//        IKStream<K, V> inputStream = builder.Stream(sourceTopic, Consumed.With(stringSerde, stringSerde));

//        inputStream.GroupByKey().Count(Materialized.As(inMemoryStoreSupplier)).ToStream().MapValues(countMapper)
//            .To(sinkTopic1, Produced.With(stringSerde, stringSerde));

//        inputStream.GroupByKey().Count(Materialized.As(persistentStoreSupplier)).ToStream().MapValues(countMapper)
//            .To(sinkTopic2, Produced.With(stringSerde, stringSerde));

//        KafkaStreamsThread streams = new KafkaStreamsThread(builder.Build(), streamsProperties);

//        streams.setUncaughtExceptionHandler((t, e) => {
//            System.Console.Error.WriteLine("FATAL: An unexpected exception " + e);
//            e.printStackTrace(System.Console.Error);
//            System.Console.Error.Flush();
//            Shutdown(streams);
//        });

//        streams.SetStateListener((newState, oldState) => {
//            if (newState == KafkaStreamsThreadStates.RUNNING && oldState == KafkaStreamsThreadStates.REBALANCING) {
//                HashSet<ThreadMetadata> threadMetadata = streams.localThreadsMetadata();
//                foreach (ThreadMetadata threadMetadatum in threadMetadata) {
//                    System.Console.Out.WriteLine(
//                        "ACTIVE_TASKS:" + threadMetadatum.activeTasks().Count
//                        + " STANDBY_TASKS:" + threadMetadatum.standbyTasks().Count);
//                }
//            }
//        });

//        System.Console.Out.WriteLine("Start Kafka Streams");
//        streams.Start();

//        Runtime.getRuntime().addShutdownHook(new Thread(() => {
//            Shutdown(streams);
//            System.Console.Out.WriteLine("Shut down streams now");
//        }));
//    }

//    private static void Shutdown(KafkaStreamsThread streams) {
//        streams.Close(TimeSpan.FromSeconds(10));
//    }

//    private static bool confirmCorrectConfigs(StreamsConfig properties) {
//        return properties.ContainsKey(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)) &&
//               properties.ContainsKey(StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG)) &&
//               properties.ContainsKey(StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)) &&
//               properties.ContainsKey(StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG));
//    }

//}
