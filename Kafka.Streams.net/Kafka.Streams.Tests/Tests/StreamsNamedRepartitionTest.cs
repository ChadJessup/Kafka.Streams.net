//using Confluent.Kafka;
//using Kafka.Streams.Configs;
//using Kafka.Streams.KStream;
//using Kafka.Streams.State.KeyValues;
//using Kafka.Streams.Topologies;
//using System.Threading;
//using Xunit;

//namespace Kafka.Streams.Tests.Tools
//{
//    public class StreamsNamedRepartitionTest
//    {

//        public static void Main(string[] args)
//        {// throws Exception
//            if (args.Length < 1)
//            {
//                System.Console.Error.WriteLine("StreamsNamedRepartitionTest requires one argument (properties-file) but none provided: ");
//            }
//            string propFileName = args[0];

//            StreamsConfig streamsProperties = Utils.loadProps(propFileName);

//            System.Console.Out.WriteLine("StreamsTest instance started NAMED_REPARTITION_TEST");
//            System.Console.Out.WriteLine("props=" + streamsProperties);

//            string inputTopic = (string)(Objects.requireNonNull(streamsProperties.remove("input.topic")));
//            string aggregationTopic = (string)(Objects.requireNonNull(streamsProperties.remove("aggregation.topic")));
//            bool addOperators = Boolean.valueOf(Objects.requireNonNull((string)streamsProperties.remove("add.operations")));


//            Initializer<int> initializer = () => 0;
//            Aggregator<string, string, int> aggregator = (k, v, agg) => agg + int.parseInt(v);

//            Function<string, string> keyFunction = s => int.ToString(int.parseInt(s) % 9);

//            StreamsBuilder builder = new StreamsBuilder();

//            IKStream<K, V> sourceStream = builder.Stream(inputTopic, Consumed.With(Serdes.String(), Serdes.String()));
//            sourceStream.peek((k, v) => System.Console.Out.WriteLine(string.format("input data key=%s, value=%s", k, v)));

//            IKStream<K, V> keyFunction.apply(v));

//            IKStream<K, V> maybeUpdatedStream;

//            if (addOperators)
//            {
//                maybeUpdatedStream = mappedStream.filter((k, v) => true).MapValues(v => int.ToString(int.parseInt(v) + 1));
//            }
//            else
//            {
//                maybeUpdatedStream = mappedStream;
//            }

//            maybeUpdatedStream.GroupByKey(Grouped.With("grouped-stream", Serdes.String(), Serdes.String()))
//                .Aggregate(initializer, aggregator, Materialized<string, int, IKeyValueStore<Bytes, byte[]>>.As("count-store").WithKeySerde(Serdes.String()).withValueSerde(Serdes.Int()))
//            .ToStream()
//            .peek((k, v) => System.Console.Out.WriteLine(string.format("AGGREGATED key=%s value=%s", k, v)))
//            .To(aggregationTopic, Produced.With(Serdes.String(), Serdes.Int()));

//            StreamsConfig config = new StreamsConfig();

//            config.Set(StreamsConfig.APPLICATION_ID_CONFIG, "StreamsNamedRepartitionTest");
//            config.Set(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
//            config.Set(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().GetType().FullName);
//            config.Set(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().GetType().FullName);


//            config.PutAll(streamsProperties);

//            Topology topology = builder.Build(config);
//            KafkaStreams streams = new KafkaStreams(topology, config);


//            streams.setStateListener((newState, oldState) =>
//            {
//                if (oldState == State.REBALANCING && newState == State.RUNNING)
//                {
//                    if (addOperators)
//                    {
//                        System.Console.Out.WriteLine("UPDATED Topology");
//                    }
//                    else
//                    {
//                        System.Console.Out.WriteLine("REBALANCING => RUNNING");
//                    }
//                    System.Console.Out.Flush();
//                }
//            });

//            streams.start();

//            Runtime.getRuntime().addShutdownHook(new Thread(() =>
//            {
//                System.Console.Out.WriteLine("closing Kafka Streams instance");
//                System.Console.Out.Flush();
//                streams.Close(TimeSpan.FromMilliseconds(5000));
//                System.Console.Out.WriteLine("NAMED_REPARTITION_TEST Streams Stopped");
//                System.Console.Out.Flush();
//            }));
//        }
//    }
//}
