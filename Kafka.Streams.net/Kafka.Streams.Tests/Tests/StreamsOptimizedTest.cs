using Confluent.Kafka;
using Xunit;

namespace Kafka.Streams.Tests.Tools
{
    public class StreamsOptimizedTest
    {


        public static void main(string[] args)
        {// throws Exception
            if (args.Length < 1)
            {
                System.Console.Error.println("StreamsOptimizedTest requires one argument (properties-file) but no provided: ");
            }
            string propFileName = args[0];

            Properties streamsProperties = Utils.loadProps(propFileName);

            System.Console.Out.WriteLine("StreamsTest instance started StreamsOptimizedTest");
            System.Console.Out.WriteLine("props=" + streamsProperties);

            string inputTopic = (string)Objects.requireNonNull(streamsProperties.remove("input.topic"));
            string aggregationTopic = (string)Objects.requireNonNull(streamsProperties.remove("aggregation.topic"));
            string reduceTopic = (string)Objects.requireNonNull(streamsProperties.remove("reduce.topic"));
            string joinTopic = (string)Objects.requireNonNull(streamsProperties.remove("join.topic"));


            Pattern repartitionTopicPattern = new Regex("Sink: .*-repartition", RegexOptions.Compiled);
            Initializer<int> initializer = () => 0;
            Aggregator<string, string, int> aggregator = (k, v, agg) => agg + v.Length();

            Reducer<string> reducer = (v1, v2) => int.toString(int.parseInt(v1) + int.parseInt(v2));

            Function<string, string> keyFunction = s => int.toString(int.parseInt(s) % 9);

            StreamsBuilder builder = new StreamsBuilder();

            KStream<string, string> sourceStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

            KStream<string, string> mappedStream = sourceStream.selectKey((k, v) => keyFunction.apply(v));

            KStream<string, long> countStream = mappedStream.groupByKey()
                                                                   .count(Materialized.with(Serdes.String(),
                                                                                            Serdes.Long())).toStream();

            mappedStream.groupByKey().aggregate(
                initializer,
                aggregator,
                Materialized.with(Serdes.String(), Serdes.Int()))
                .toStream()
                .peek((k, v) => System.Console.Out.WriteLine(string.format("AGGREGATED key=%s value=%s", k, v)))
                .to(aggregationTopic, Produced.with(Serdes.String(), Serdes.Int()));


            mappedStream.groupByKey()
                .reduce(reducer, Materialized.with(Serdes.String(), Serdes.String()))
                .toStream()
                .peek((k, v) => System.Console.Out.WriteLine(string.format("REDUCED key=%s value=%s", k, v)))
                .to(reduceTopic, Produced.with(Serdes.String(), Serdes.String()));

            mappedStream.join(countStream, (v1, v2) => v1 + ":" + v2.toString(),
                JoinWindows.of(ofMillis(500)),
                Joined.with(Serdes.String(), Serdes.String(), Serdes.Long()))
                .peek((k, v) => System.Console.Out.WriteLine(string.format("JOINED key=%s value=%s", k, v)))
                .to(joinTopic, Produced.with(Serdes.String(), Serdes.String()));

            Properties config = new Properties();


            config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "StreamsOptimizedTest");
            config.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
            config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            config.setProperty(StreamsConfig.adminClientPrefix(AdminClientConfig.RETRIES_CONFIG), "100");


            config.putAll(streamsProperties);

            Topology topology = builder.build(config);
            KafkaStreams streams = new KafkaStreams(topology, config);


            streams.setStateListener((newState, oldState) =>
            {
                if (oldState == State.REBALANCING && newState == State.RUNNING)
                {
                    int repartitionTopicCount = getCountOfRepartitionTopicsFound(topology.describe().toString(), repartitionTopicPattern);
                    System.Console.Out.WriteLine(string.format("REBALANCING => RUNNING with REPARTITION TOPIC COUNT=%d", repartitionTopicCount));
                    System.Console.Out.flush();
                }
            });

            streams.start();

            Runtime.getRuntime().addShutdownHook(new Thread()
            {


            public void run()
            {
                System.Console.Out.WriteLine("closing Kafka Streams instance");
                System.Console.Out.flush();
                streams.close(Duration.ofMillis(5000));
                System.Console.Out.WriteLine("OPTIMIZE_TEST Streams Stopped");
                System.Console.Out.flush();
            }
        });

    }

    private static int getCountOfRepartitionTopicsFound(string topologyString,
                                                        Pattern repartitionTopicPattern)
    {
        Matcher matcher = repartitionTopicPattern.matcher(topologyString);
        List<string> repartitionTopicsFound = new ArrayList<>();
        while (matcher.find())
        {
            string repartitionTopic = matcher.group();
            System.Console.Out.WriteLine(string.format("REPARTITION TOPIC found => %s", repartitionTopic));
            repartitionTopicsFound.add(repartitionTopic);
        }
        return repartitionTopicsFound.Count;
    }
}
