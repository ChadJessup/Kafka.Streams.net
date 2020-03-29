//using Confluent.Kafka;
//using Xunit;

//namespace Kafka.Streams.Tests.Tools
//{
//    public class StreamsBrokerDownResilienceTest
//    {

//        private static int KEY = 0;
//        private static int VALUE = 1;

//        private static string SOURCE_TOPIC_1 = "streamsResilienceSource";

//        private static string SINK_TOPIC = "streamsResilienceSink";

//        public static void main(string[] args){ //throws IOException
//        if (args.Length< 2) {
//            System.Console.Error.println("StreamsBrokerDownResilienceTest are expecting two parameters: propFile, additionalConfigs; but only see " + args.Length + " parameter");
//            System.exit(1);
//        }

//    System.Console.Out.WriteLine("StreamsTest instance started");

//        string propFileName = args[0];
//    string additionalConfigs = args[1];

//    Properties streamsProperties = Utils.loadProps(propFileName);
//    string kafka = streamsProperties.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);

//        if (kafka == null) {
//            System.Console.Error.println("No bootstrap kafka servers specified in " + StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
//            System.exit(1);
//        }

//streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-resilience");
//        streamsProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        streamsProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        streamsProperties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);


//        // it is expected that max.poll.interval, retries, request.timeout and max.block.ms set
//        // streams_broker_down_resilience_test and passed as args
//        if (additionalConfigs != null && !additionalConfigs.equalsIgnoreCase("none")) {
//            Dictionary<string, string> updated = updatedConfigs(additionalConfigs);
//System.Console.Out.WriteLine("Updating configs with " + updated);
//            streamsProperties.putAll(updated);
//        }

//        if (!confirmCorrectConfigs(streamsProperties)) {
//            System.Console.Error.println(string.format("ERROR: Did not have all required configs expected  to contain %s %s %s %s",
//                                             StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG),
//                                             StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG),
//                                             StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG),
//                                             StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG)));

//            System.exit(1);
//        }

//        StreamsBuilder builder = new StreamsBuilder();
//Serde<string> stringSerde = Serdes.String();

//builder.stream(Collections.singletonList(SOURCE_TOPIC_1), Consumed.with(stringSerde, stringSerde))
//            .peek(new ForeachAction<string, string>() {
//                int messagesProcessed = 0;

//public void apply(string key, string value)
//{
//    System.Console.Out.WriteLine("received key " + key + " and value " + value);
//    messagesProcessed++;
//    System.Console.Out.WriteLine("processed" + messagesProcessed + "messages");
//    System.Console.Out.flush();
//}
//            }).to(SINK_TOPIC);

//KafkaStreams streams = new KafkaStreams(builder.build(), streamsProperties);

//streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            
//            public void uncaughtException(Thread t, Throwable e)
//{
//    System.Console.Error.println("FATAL: An unexpected exception " + e);
//    System.Console.Error.flush();
//    streams.close(Duration.ofSeconds(30));
//}
//        });
//        System.Console.Out.WriteLine("Start Kafka Streams");
//        streams.start();

//        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
//{

//    public void run()
//    {
//        streams.close(Duration.ofSeconds(30));
//        System.Console.Out.WriteLine("Complete shutdown of streams resilience test app now");
//        System.Console.Out.flush();
//    }
//}));


//    }

//    private static bool confirmCorrectConfigs(Properties properties)
//{
//    return properties.containsKey(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)) &&
//           properties.containsKey(StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG)) &&
//           properties.containsKey(StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)) &&
//           properties.containsKey(StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG));
//}

///**
// * Takes a string with keys and values separated by '=' and each key value pair
// * separated by ',' for example max.block.ms=5000,retries=6,request.timeout.ms=6000
// *
// * @param formattedConfigs the formatted config string
// * @return HashMap with keys and values inserted
// */
//private static Dictionary<string, string> updatedConfigs(string formattedConfigs)
//{
//    string[] parts = formattedConfigs.split(",");
//    Dictionary<string, string> updatedConfigs = new HashMap<>();
//    foreach (string part in parts)
//    {
//        string[] keyValue = part.split("=");
//        updatedConfigs.put(keyValue[KEY], keyValue[VALUE]);
//    }
//    return updatedConfigs;
//}

//}
