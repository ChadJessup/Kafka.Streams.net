namespace Kafka.Streams.Tests.Tests
{
}
//using Confluent.Kafka;
//using Xunit;

//namespace Kafka.Streams.Tests.Tools
//{
//    public class StreamsSmokeTest {

//    /**
//     *  args ::= kafka propFileName command disableAutoTerminate
//     *  command := "run" | "process"
//     *
//     * @param args
//     */
//    public static void main(string[] args) {// throws InterruptedException, IOException
//        if (args.Length < 2) {
//            System.Console.Error.WriteLine("StreamsSmokeTest are expecting two parameters: propFile, command; but only see " + args.Length + " parameter");
//            System.exit(1);
//        }

//        string propFileName = args[0];
//        string command = args[1];
//        bool disableAutoTerminate = args.Length > 2;

//        StreamsConfig streamsProperties = Utils.loadProps(propFileName);
//        string kafka = streamsProperties.Get(StreamsConfig.BootstrapServersConfig);

//        if (kafka == null) {
//            System.Console.Error.WriteLine("No bootstrap kafka servers specified in " + StreamsConfig.BootstrapServersConfig);
//            System.exit(1);
//        }

//        System.Console.Out.WriteLine("StreamsTest instance started (StreamsSmokeTest)");
//        System.Console.Out.WriteLine("command=" + command);
//        System.Console.Out.WriteLine("props=" + streamsProperties);
//        System.Console.Out.WriteLine("disableAutoTerminate=" + disableAutoTerminate);

//        switch (command) {
//            case "run":
//                // this starts the driver (data generation and result verification)
//                int numKeys = 10;
//                int maxRecordsPerKey = 500;
//                if (disableAutoTerminate) {
//                    generatePerpetually(kafka, numKeys, maxRecordsPerKey);
//                } else {
//                    // slow down data production to span 30 seconds so that system tests have time to
//                    // do their bounces, etc.
//                    Dictionary<string, HashSet<int>> allData =
//                        generate(kafka, numKeys, maxRecordsPerKey, TimeSpan.FromSeconds(30));
//                    SmokeTestDriver.verify(kafka, allData, maxRecordsPerKey);
//                }
//                break;
//            case "process":
//                // this starts the stream processing app
//                new SmokeTestClient(UUID.randomUUID().ToString()).start(streamsProperties);
//                break;
//            case "process-eos":
//                // this starts the stream processing app with EOS
//                streamsProperties.Set(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.StreamsConfig.ExactlyOnceConfig);
//                new SmokeTestClient(UUID.randomUUID().ToString()).start(streamsProperties);
//                break;
//            case "Close-deadlock-test":
//                ShutdownDeadlockTest test = new ShutdownDeadlockTest(kafka);
//                test.Start();
//                break;
//            default:
//                System.Console.Out.WriteLine("unknown command: " + command);
//        }
//    }

//}
