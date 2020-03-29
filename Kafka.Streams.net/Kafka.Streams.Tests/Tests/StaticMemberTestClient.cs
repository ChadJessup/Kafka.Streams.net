//using Confluent.Kafka;
//using Xunit;

//namespace Kafka.Streams.Tests.Tools
//{
//    public class StaticMemberTestClient {

//    private static string testName = "StaticMemberTestClient";

    
//    public static void main(string[] args) {// throws Exception
//        if (args.Length < 1) {
//            System.Console.Error.println(testName + " requires one argument (properties-file) but none provided: ");
//        }

//        System.Console.Out.WriteLine("StreamsTest instance started");

//        string propFileName = args[0];

//        Properties streamsProperties = Utils.loadProps(propFileName);

//        string groupInstanceId = Objects.requireNonNull(streamsProperties.getProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG));

//        System.Console.Out.WriteLine(testName + " instance started with group.instance.id " + groupInstanceId);
//        System.Console.Out.WriteLine("props=" + streamsProperties);
//        System.Console.Out.flush();

//        StreamsBuilder builder = new StreamsBuilder();
//        string inputTopic = (string) (Objects.requireNonNull(streamsProperties.remove("input.topic")));

//        KStream dataStream = builder.stream(inputTopic);
//        dataStream.peek((k, v) =>  System.Console.Out.WriteLine(string.format("PROCESSED key=%s value=%s", k, v)));

//        Properties config = new Properties();
//        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, testName);
//        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

//        config.putAll(streamsProperties);

//        KafkaStreams streams = new KafkaStreams(builder.build(), config);
//        streams.setStateListener((newState, oldState) => {
//            if (oldState == KafkaStreams.State.REBALANCING && newState == KafkaStreams.State.RUNNING) {
//                System.Console.Out.WriteLine("REBALANCING => RUNNING");
//                System.Console.Out.flush();
//            }
//        });

//        streams.start();

//        Runtime.getRuntime().addShutdownHook(new Thread() {
            
//            public void run() {
//                System.Console.Out.WriteLine("closing Kafka Streams instance");
//                System.Console.Out.flush();
//                streams.close();
//                System.Console.Out.WriteLine("Static membership test closed");
//                System.Console.Out.flush();
//            }
//        });
//    }
//}
