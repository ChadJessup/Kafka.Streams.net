using Confluent.Kafka;
using Xunit;

namespace Kafka.Streams.Tests.Tools
{
    public class StreamsEosTest
    {

        /**
         *  args ::= kafka propFileName command
         *  command := "run" | "process" | "verify"
         */
        public static void main(string[] args)
        {// throws IOException
            if (args.Length < 2)
            {
                System.Console.Error.println("StreamsEosTest are expecting two parameters: propFile, command; but only see " + args.Length + " parameter");
                System.exit(1);
            }

            string propFileName = args[0];
            string command = args[1];

            Properties streamsProperties = Utils.loadProps(propFileName);
            string kafka = streamsProperties.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);

            if (kafka == null)
            {
                System.Console.Error.println("No bootstrap kafka servers specified in " + StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
                System.exit(1);
            }

            System.Console.Out.WriteLine("StreamsTest instance started");
            System.Console.Out.WriteLine("kafka=" + kafka);
            System.Console.Out.WriteLine("props=" + streamsProperties);
            System.Console.Out.WriteLine("command=" + command);
            System.Console.Out.flush();

            if (command == null || propFileName == null)
            {
                System.exit(-1);
            }

            switch (command)
            {
                case "run":
                    EosTestDriver.generate(kafka);
                    break;
                case "process":
                    new EosTestClient(streamsProperties, false).start();
                    break;
                case "process-complex":
                    new EosTestClient(streamsProperties, true).start();
                    break;
                case "verify":
                    EosTestDriver.verify(kafka, false);
                    break;
                case "verify-complex":
                    EosTestDriver.verify(kafka, true);
                    break;
                default:
                    System.Console.Out.WriteLine("unknown command: " + command);
                    System.Console.Out.flush();
                    System.exit(-1);
            }
        }

    }
}
