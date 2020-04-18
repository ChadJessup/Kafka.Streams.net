//using Confluent.Kafka;
//using Xunit;

//namespace Kafka.Streams.Tests.Tools
//{
//    public class StreamsOptimizedTest
//    {


//        public static void Main(string[] args)
//        {// throws Exception
//            if (args.Length < 1)
//            {
//                System.Console.Error.WriteLine("StreamsOptimizedTest requires one argument (properties-file) but no provided: ");
//            }
//            string propFileName = args[0];

//            StreamsConfig streamsProperties = Utils.loadProps(propFileName);

//            System.Console.Out.WriteLine("StreamsTest instance started StreamsOptimizedTest");
//            System.Console.Out.WriteLine("props=" + streamsProperties);

//            string inputTopic = (string)Objects.requireNonNull(streamsProperties.remove("input.topic"));
//            string aggregationTopic = (string)Objects.requireNonNull(streamsProperties.remove("aggregation.topic"));
//            string reduceTopic = (string)Objects.requireNonNull(streamsProperties.remove("reduce.topic"));
//            string joinTopic = (string)Objects.requireNonNull(streamsProperties.remove("join.topic"));


//            Pattern repartitionTopicPattern = new Regex("Sink: .*-repartition", RegexOptions.Compiled);
//            Initializer<int> initializer = () => 0;
//            Aggregator<string, string, int> aggregator = (k, v, agg) => agg + v.Length();

//            Reducer<string> reducer = (v1, v2) => int.ToString(int.parseInt(v1) + int.parseInt(v2));

//            Function<string, string> keyFunction = s => int.ToString(int.parseInt(s) % 9);

//            StreamsBuilder builder = new StreamsBuilder();

//            IKStream<K, V> sourceStream = builder.Stream(inputTopic, Consumed.With(Serdes.String(), Serdes.String()));

//            IKStream<K, V> keyFunction.apply(v));

//            IKStream<K, V> countStream = mappedStream.GroupByKey()
//                                                                   .Count(Materialized.With(Serdes.String(),
//                                                                                            Serdes.Long())).ToStream();

//            mappedStream.GroupByKey().Aggregate(
//                initializer,
//                aggregator,
//                Materialized.With(Serdes.String(), Serdes.Int()))
//                .ToStream()
//                .peek((k, v) => System.Console.Out.WriteLine(string.format("AGGREGATED key=%s value=%s", k, v)))
//                .To(aggregationTopic, Produced.With(Serdes.String(), Serdes.Int()));


//            mappedStream.GroupByKey()
//                .Reduce(reducer, Materialized.With(Serdes.String(), Serdes.String()))
//                .ToStream()
//                .peek((k, v) => System.Console.Out.WriteLine(string.format("REDUCED key=%s value=%s", k, v)))
//                .To(reduceTopic, Produced.With(Serdes.String(), Serdes.String()));

//            mappedStream.Join(countStream, (v1, v2) => v1 + ":" + v2.ToString(),
//                JoinWindows.Of(TimeSpan.FromMilliseconds(500)),
//                Joined.With(Serdes.String(), Serdes.String(), Serdes.Long()))
//                .peek((k, v) => System.Console.Out.WriteLine(string.format("JOINED key=%s value=%s", k, v)))
//                .To(joinTopic, Produced.With(Serdes.String(), Serdes.String()));

//            StreamsConfig config = new StreamsConfig();


//            config.Set(StreamsConfig.ApplicationIdConfig, "StreamsOptimizedTest");
//            config.Set(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
//            config.Set(StreamsConfig.DefaultKeySerdeClassConfig, Serdes.String().GetType().FullName);
//            config.Set(StreamsConfig.DefaultValueSerdeClassConfig, Serdes.String().GetType().FullName);
//            config.Set(StreamsConfig.adminClientPrefix(AdminClientConfig.RETRIES_CONFIG), "100");


//            config.PutAll(streamsProperties);

//            Topology topology = builder.Build(config);
//            KafkaStreamsThread streams = new KafkaStreamsThread(topology, config);


//            streams.SetStateListener((newState, oldState) =>
//            {
//                if (oldState == State.REBALANCING && newState == State.RUNNING)
//                {
//                    int repartitionTopicCount = getCountOfRepartitionTopicsFound(topology.describe().ToString(), repartitionTopicPattern);
//                    System.Console.Out.WriteLine(string.format("REBALANCING => RUNNING with REPARTITION TOPIC COUNT=%d", repartitionTopicCount));
//                    System.Console.Out.Flush();
//                }
//            });

//            streams.Start();

//            Runtime.getRuntime().addShutdownHook(new Thread()
//            {


//            public void run()
//            {
//                System.Console.Out.WriteLine("closing Kafka Streams instance");
//                System.Console.Out.Flush();
//                streams.Close(TimeSpan.FromMilliseconds(5000));
//                System.Console.Out.WriteLine("OPTIMIZE_TEST Streams Stopped");
//                System.Console.Out.Flush();
//            }
//        });

//    }

//    private static int GetCountOfRepartitionTopicsFound(string topologyString,
//                                                        Pattern repartitionTopicPattern)
//    {
//        Matcher matcher = repartitionTopicPattern.matcher(topologyString);
//        List<string> repartitionTopicsFound = new List<string>();
//        while (matcher.find())
//        {
//            string repartitionTopic = matcher.group();
//            System.Console.Out.WriteLine(string.format("REPARTITION TOPIC found => %s", repartitionTopic));
//            repartitionTopicsFound.Add(repartitionTopic);
//        }
//        return repartitionTopicsFound.Count;
//    }
//}
