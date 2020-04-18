//using Confluent.Kafka;
//using Xunit;

//namespace Kafka.Streams.Tests.Tools
//{
//    public class StreamsEosTest
//    {

//        /**
//         *  args ::= kafka propFileName command
//         *  command := "run" | "process" | "verify"
//         */
//        public static void Main(string[] args)
//        {// throws IOException
//            if (args.Length < 2)
//            {
//                System.Console.Error.WriteLine("StreamsEosTest are expecting two parameters: propFile, command; but only see " + args.Length + " parameter");
//                System.exit(1);
//            }

//            string propFileName = args[0];
//            string command = args[1];

//            StreamsConfig streamsProperties = Utils.loadProps(propFileName);
//            string kafka = streamsProperties.getProperty(StreamsConfig.BootstrapServersConfig);

//            if (kafka == null)
//            {
//                System.Console.Error.WriteLine("No bootstrap kafka servers specified in " + StreamsConfig.BootstrapServersConfig);
//                System.exit(1);
//            }

//            System.Console.Out.WriteLine("StreamsTest instance started");
//            System.Console.Out.WriteLine("kafka=" + kafka);
//            System.Console.Out.WriteLine("props=" + streamsProperties);
//            System.Console.Out.WriteLine("command=" + command);
//            System.Console.Out.Flush();

//            if (command == null || propFileName == null)
//            {
//                System.exit(-1);
//            }

//            switch (command)
//            {
//                case "run":
//                    EosTestDriver.generate(kafka);
//                    break;
//                case "process":
//                    new EosTestClient(streamsProperties, false).Start();
//                    break;
//                case "process-complex":
//                    new EosTestClient(streamsProperties, true).Start();
//                    break;
//                case "verify":
//                    EosTestDriver.verify(kafka, false);
//                    break;
//                case "verify-complex":
//                    EosTestDriver.verify(kafka, true);
//                    break;
//                default:
//                    System.Console.Out.WriteLine("unknown command: " + command);
//                    System.Console.Out.Flush();
//                    System.exit(-1);
//            }
//        }

//    }
//}
