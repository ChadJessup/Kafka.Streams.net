namespace Kafka.Streams.Tests.Tests
{
}
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
//            System.Console.Error.WriteLine("StreamsBrokerDownResilienceTest are expecting two parameters: propFile, additionalConfigs; but only see " + args.Length + " parameter");
//            System.exit(1);
//        }

//    System.Console.Out.WriteLine("StreamsTest instance started");

//        string propFileName = args[0];
//    string additionalConfigs = args[1];

//    StreamsConfig streamsProperties = Utils.loadProps(propFileName);
//    string kafka = streamsProperties.getProperty(StreamsConfig.BootstrapServersConfig);

//        if (kafka == null) {
//            System.Console.Error.WriteLine("No bootstrap kafka servers specified in " + StreamsConfig.BootstrapServersConfig);
//            System.exit(1);
//        }

//streamsProperties.Put(StreamsConfig.ApplicationIdConfig, "kafka-streams-resilience");
//        streamsProperties.Put(StreamsConfig.DefaultKeySerdeClassConfig, Serdes.String().GetType());
//        streamsProperties.Put(StreamsConfig.DefaultValueSerdeClassConfig, Serdes.String().GetType());
//        streamsProperties.Put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);


//        // it is expected that max.poll.interval, retries, request.timeout and max.block.ms set
//        // streams_broker_down_resilience_test and passed as args
//        if (additionalConfigs != null && !additionalConfigs.EqualsIgnoreCase("none")) {
//            Dictionary<string, string> updated = updatedConfigs(additionalConfigs);
//System.Console.Out.WriteLine("Updating configs with " + updated);
//            streamsProperties.PutAll(updated);
//        }

//        if (!confirmCorrectConfigs(streamsProperties)) {
//            System.Console.Error.WriteLine(string.format("ERROR: Did not have All required configs expected  to contain %s %s %s %s",
//                                             StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG),
//                                             StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG),
//                                             StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG),
//                                             StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG)));

//            System.exit(1);
//        }

//        StreamsBuilder builder = new StreamsBuilder();
//Serde<string> stringSerde = Serdes.String();

//builder.Stream(Collections.singletonList(SOURCE_TOPIC_1), Consumed.With(stringSerde, stringSerde))
//            .peek(new ForeachAction<string, string>() {
//                int messagesProcessed = 0;

//public void apply(string key, string value)
//{
//    System.Console.Out.WriteLine("received key " + key + " and value " + value);
//    messagesProcessed++;
//    System.Console.Out.WriteLine("processed" + messagesProcessed + "messages");
//    System.Console.Out.Flush();
//}
//            }).To(SINK_TOPIC);

//KafkaStreamsThread streams = new KafkaStreamsThread(builder.Build(), streamsProperties);

//streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {

//            public void uncaughtException(Thread t, Throwable e)
//{
//    System.Console.Error.WriteLine("FATAL: An unexpected exception " + e);
//    System.Console.Error.Flush();
//    streams.Close(TimeSpan.FromSeconds(30));
//}
//        });
//        System.Console.Out.WriteLine("Start Kafka Streams");
//        streams.Start();

//        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
//{

//    public void run()
//    {
//        streams.Close(TimeSpan.FromSeconds(30));
//        System.Console.Out.WriteLine("Complete Shutdown of streams resilience test app now");
//        System.Console.Out.Flush();
//    }
//}));


//    }

//    private static bool confirmCorrectConfigs(StreamsConfig properties)
//{
//    return properties.ContainsKey(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)) &&
//           properties.ContainsKey(StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG)) &&
//           properties.ContainsKey(StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)) &&
//           properties.ContainsKey(StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG));
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
//    string[] parts = formattedConfigs.Split(",");
//    Dictionary<string, string> updatedConfigs = new HashMap<>();
//    foreach (string part in parts)
//    {
//        string[] keyValue = part.Split("=");
//        updatedConfigs.Put(keyValue[KEY], keyValue[VALUE]);
//    }
//    return updatedConfigs;
//}

//}
