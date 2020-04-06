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
//            System.Console.Error.println("StreamsStandByReplicaTest are expecting two parameters: " +
//                "propFile, additionalConfigs; but only see " + args.Length + " parameter");
//            System.exit(1);
//        }

//        System.Console.Out.WriteLine("StreamsTest instance started");

//        string propFileName = args[0];
//        string additionalConfigs = args[1];

//        StreamsConfig streamsProperties = Utils.loadProps(propFileName);
//        string kafka = streamsProperties.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);

//        if (kafka == null) {
//            System.Console.Error.println("No bootstrap kafka servers specified in " + StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
//            System.exit(1);
//        }

//        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-standby-tasks");
//        streamsProperties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
//        streamsProperties.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
//        streamsProperties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
//        streamsProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        streamsProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        streamsProperties.put(StreamsConfig.producerPrefix(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG), true);

//        if (additionalConfigs == null) {
//            System.Console.Error.println("additional configs are not provided");
//            System.Console.Error.flush();
//            System.exit(1);
//        }

//        Dictionary<string, string> updated = SystemTestUtil.parseConfigs(additionalConfigs);
//        System.Console.Out.WriteLine("Updating configs with " + updated);

//        string sourceTopic = updated.remove("sourceTopic");
//        string sinkTopic1 = updated.remove("sinkTopic1");
//        string sinkTopic2 = updated.remove("sinkTopic2");

//        if (sourceTopic == null || sinkTopic1 == null || sinkTopic2 == null) {
//            System.Console.Error.println(string.format(
//                "one or more required topics null sourceTopic[%s], sinkTopic1[%s], sinkTopic2[%s]",
//                sourceTopic,
//                sinkTopic1,
//                sinkTopic2));
//            System.Console.Error.flush();
//            System.exit(1);
//        }

//        streamsProperties.putAll(updated);

//        if (!confirmCorrectConfigs(streamsProperties)) {
//            System.Console.Error.println(string.format("ERROR: Did not have all required configs expected  to contain %s, %s,  %s,  %s",
//                                             StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG),
//                                             StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG),
//                                             StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG),
//                                             StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG)));

//            System.exit(1);
//        }

//        StreamsBuilder builder = new StreamsBuilder();

//        string inMemoryStoreName = "in-memory-store";
//        string persistentMemoryStoreName = "persistent-memory-store";

//        IKeyValueBytesStoreSupplier inMemoryStoreSupplier = Stores.InMemoryKeyValueStore(inMemoryStoreName);
//        IKeyValueBytesStoreSupplier persistentStoreSupplier = Stores.PersistentKeyValueStore(persistentMemoryStoreName);

//        Serde<string> stringSerde = Serdes.String();
//        ValueMapper<long, string> countMapper = object::toString;

//        KStream<string, string> inputStream = builder.Stream(sourceTopic, Consumed.With(stringSerde, stringSerde));

//        inputStream.groupByKey().count(Materialized.As(inMemoryStoreSupplier)).toStream().mapValues(countMapper)
//            .To(sinkTopic1, Produced.With(stringSerde, stringSerde));

//        inputStream.groupByKey().count(Materialized.As(persistentStoreSupplier)).toStream().mapValues(countMapper)
//            .To(sinkTopic2, Produced.With(stringSerde, stringSerde));

//        KafkaStreams streams = new KafkaStreams(builder.Build(), streamsProperties);

//        streams.setUncaughtExceptionHandler((t, e) => {
//            System.Console.Error.println("FATAL: An unexpected exception " + e);
//            e.printStackTrace(System.Console.Error);
//            System.Console.Error.flush();
//            shutdown(streams);
//        });

//        streams.setStateListener((newState, oldState) => {
//            if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
//                HashSet<ThreadMetadata> threadMetadata = streams.localThreadsMetadata();
//                foreach (ThreadMetadata threadMetadatum in threadMetadata) {
//                    System.Console.Out.WriteLine(
//                        "ACTIVE_TASKS:" + threadMetadatum.activeTasks().Count
//                        + " STANDBY_TASKS:" + threadMetadatum.standbyTasks().Count);
//                }
//            }
//        });

//        System.Console.Out.WriteLine("Start Kafka Streams");
//        streams.start();

//        Runtime.getRuntime().addShutdownHook(new Thread(() => {
//            shutdown(streams);
//            System.Console.Out.WriteLine("Shut down streams now");
//        }));
//    }

//    private static void shutdown(KafkaStreams streams) {
//        streams.close(TimeSpan.ofSeconds(10));
//    }

//    private static bool confirmCorrectConfigs(StreamsConfig properties) {
//        return properties.containsKey(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)) &&
//               properties.containsKey(StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG)) &&
//               properties.containsKey(StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)) &&
//               properties.containsKey(StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG));
//    }

//}
