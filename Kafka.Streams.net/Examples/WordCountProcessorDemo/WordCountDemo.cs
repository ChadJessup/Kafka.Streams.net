using Confluent.Kafka;
using Kafka.Common.Extensions;
using Kafka.Streams;
using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Mappers;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.Threads.KafkaStreams;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace WordCountProcessorDemo
{
    public class Program
    {
        public static int Main(string[] args)
        {
            var streamsConfig = new StreamsConfig
            {
                ApplicationId = "streams-wordcount",
                GroupId = "streams-wordcount",
                BootstrapServers = "localhost:9092",
                KeySerde = Serdes.String().GetType(),
                ValueSerde = Serdes.String().GetType(),
                NumberOfStreamThreads = 1,
                CacheMaxBytesBuffering = 10485760L,
                // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
                // Note: To re-run the demo, you need to use the offset reset tool:
                // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
                AutoOffsetReset = AutoOffsetReset.Earliest,
                // streamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG = 0;
            };

            var services = new ServiceCollection()
                .AddSingleton(streamsConfig)
                .AddLogging(config =>
                {
                    config.SetMinimumLevel(LogLevel.Debug);
                    config.AddConsole();
                });

            var cts = new CancellationTokenSource();

            // attach shutdown handler to catch control-c
            Console.CancelKeyPress += (o, e) => cts.Cancel();

            // TestConsumer(streamsConfig, cts.Token);
            // TestProducer(streamsConfig, cts.Token);
            // TutorialOne(services);

            var latch = new ManualResetEvent(initialState: false);

            StreamsBuilder builder = new StreamsBuilder(services);

            builder
                .Stream<string, string>("TextLinesTopic")
                .FlatMapValues(new ValueMapper<string, IEnumerable<string>>(textLine => textLine.ToLower().Split("\\W+", RegexOptions.IgnoreCase).ToList()))
                .GroupBy((key, word) => word)
                .Count(Materialized.As<string, long, IKeyValueStore<Bytes, byte[]>>("Counts"))
                .ToStream()
                .To("WordsWithCountsTopic",
                    Produced.With(
                        Serdes.String(),
                        Serdes.Long()));

            // Make the topology injectable
            var topology = builder.Build();
            services.AddSingleton(topology);

            using IKafkaStreamsThread streams = builder.BuildKafkaStreams();

            // attach shutdown handler to catch control-c
            Console.CancelKeyPress += (o, e) => latch.Set();

            try
            {
                streams.Start();
                latch.WaitOne();
            }
            catch (Exception)
            {
                return 1;
            }

            return 0;
        }

        private static void TutorialOne(IServiceCollection services)
        {
            var builder = new StreamsBuilder(services);
            builder.Stream<string, string>("streams-plaintext-input")
                .To("streams-pipe-output");

            services.AddSingleton(builder.Build());

            using var streams = builder.BuildKafkaStreams();

            var latch = new ManualResetEvent(initialState: false);

            Console.CancelKeyPress += (o, e) => latch.Set();

            streams.Start();
            latch.WaitOne();
        }

        //class TestPartitioner : IPartitioner
        //{
        //    public Partition Partition(string topic, IntPtr keydata, UIntPtr keylen, int partition_cnt, IntPtr rkt_opaque, IntPtr msg_opaque)
        //    {
        //        var partition = (keydata.ToInt32() % partition_cnt);
        //        Console.WriteLine($"{nameof(TestPartitioner)} called: Topic {topic} Partition: {partition}");

        //        return partition;
        //    }

        //    public void Dispose()
        //    {
        //    }
        //}

        //class TestPartitioner2 : IPartitioner
        //{
        //    public Partition Partition(string topic, IntPtr keydata, UIntPtr keylen, int partition_cnt, IntPtr rkt_opaque, IntPtr msg_opaque)
        //    {
        //        var partition = (keydata.ToInt32() % partition_cnt);
        //        Console.WriteLine($"{nameof(TestPartitioner2)} called: Topic {topic} Partition: {partition}");

        //        return partition;
        //    }

        //    public void Dispose()
        //    {
        //    }
        //}

        private static void TestConsumer(StreamsConfig streamsConfig, CancellationToken token)
        {
            var consumerConfig = new ConsumerConfig();// ($"test-{DateTime.Now.Millisecond}", "test", 1);
            consumerConfig.Set("debug", "all");
            consumerConfig.Set("group.id", $"test-{DateTime.Now.Millisecond}");
            consumerConfig.BootstrapServers = "localhost:9092";

            using var consumer = new ConsumerBuilder<int, int>(consumerConfig)
                .SetPartitionsAssignedHandler((consumer, topics) =>
                {
                    topics.ForEach(t => Console.WriteLine($"Partition Assigned: {t.ToString()}"));
                })
                .SetPartitionsRevokedHandler((consumer, tpo) =>
                {
                    tpo.ForEach(t => Console.WriteLine($"Partition Revoked: {t.ToString()}"));
                })
                .SetOffsetsCommittedHandler((consumer, co) =>
                {

                })
                .SetStatisticsHandler((prod, stat) =>
                {
                    var json = JsonConvert.DeserializeObject<KafkaStatistics>(stat);
                })
                .SetPartitionAssignor(new TestPartitionAssignor())
                .SetErrorHandler((prod, error) =>
                {
                    Console.WriteLine($"Error: {error.Reason}");
                })
                .SetLogHandler((prod, m) =>
                {

                    if (m.Facility == "FETCH"
                        || m.Facility == "OFFSET"
                        || m.Facility == "SEND"
                        || m.Facility == "RECV"
                        || m.Facility == "COMMIT"
                        || m.Facility == "HEARTBEAT"
                        || m.Message.Contains("Fetch topic"))
                    {
                        return;
                    }

                    Console.WriteLine($"Log: {m.Level}: {m.Facility}: {m.Message}");
                })
                .Build();

            consumer.Subscribe("customassignortest");

            while (!token.IsCancellationRequested)
            {
                var message = consumer.Consume(token);

                if (message != null)
                {

                }

                Task.Delay(10000).Wait();
            }
        }

        class TestPartitionAssignor : IConsumerPartitionAssignor
        {
            public void Dispose()
            {
            }

            public string Name()
            {
                return nameof(TestPartitionAssignor);
            }

            public void OnAssignment(List<TopicPartition> assignment, Metadata metadata)
            {
            }

            public byte[] SubscriptionUserData(HashSet<string> topics)
            {
                return Array.Empty<byte>();
            }

            public short Version()
            {
                return 0;
            }
        }

        private static void TestProducer(StreamsConfig streamsConfig, CancellationToken token)
        {
            var producerConfig = streamsConfig.GetProducerConfigs("test");
            producerConfig.Set("debug", "all");
            producerConfig.Partitioner = Partitioner.Murmur2Random;
            // producerConfig.PluginLibraryPaths = Assembly.GetExecutingAssembly().Location;
            //producerConfig.Set("partitioner", typeof(Program).FullName);
            //producerConfig.Partitioner = Partitioner.Murmur2Random;

            var drCalled = false;
            var dr2Called = false;

            using var producer = new ProducerBuilder<int, int>(producerConfig)
                .SetStatisticsHandler((prod, stat) =>
                {
                    var json = JsonConvert.DeserializeObject<KafkaStatistics>(stat);
                })
                //.SetPartitioner("TextLinesTopic", new TestPartitioner())
                //.SetPartitioner("PartitionerTest", new TestPartitioner2())
                .SetErrorHandler((prod, error) =>
                {
                    Console.WriteLine($"Error: {error.Reason}");
                })
                .SetLogHandler((prod, m) =>
                {
                    Console.WriteLine($"Log: {m.Message}");
                })
                .Build();

            var ind = 0;
            while (!token.IsCancellationRequested)
            {
                drCalled = false;
                dr2Called = false;

                producer.Produce("TextLinesTopic", new Message<int, int>
                {
                    Key = ind++,
                    Value = ind++,
                },
                (dr) =>
                {
                    Console.WriteLine($"DR1: {dr.Status}");
                    drCalled = true;
                });

                producer.Produce("PartitionerTest", new Message<int, int>
                {
                    Key = ind++,
                    Value = ind++,
                },
                (dr) =>
                {
                    Console.WriteLine($"DR2: {dr.Status}");
                    dr2Called = true;
                });
            }
        }

        public void Test()
        {
            //// Serializers/deserializers (serde) for String and Long types
            //ISerde<string> stringSerde = Serdes.String();
            //ISerde<long> longSerde = Serdes.Long();

            //// Construct a `KStream` from the input topic "streams-plaintext-input", where message values
            //// represent lines of text (for the sake of this example, we ignore whatever may be stored
            //// in the message keys).
            //IKStream<string, string> textLines = builder.stream(
            //      "streams-plaintext-input",
            //      Consumed.With(stringSerde, stringSerde)
            //    );

            //IKTable<string, long> wordCounts = textLines
            //    // Split each text line, by whitespace, into words.
            //    .flatMapValues(value=>Arrays.asList(value.toLowerCase().Split("\\W+")))

            //    // Group the text words as message keys
            //    .groupBy((key, value)=>value)

            //    // Count the occurrences of each word (message key).
            //    .count();

            //// Store the running counts as a changelog stream to the output topic.
            //wordCounts.toStream().to("streams-wordcount-output", Produced.With(Serdes.String(), Serdes.Long()));

            //var source = builder
            //    .stream<string, string>("streams-plaintext-input");

            //var counts = source
            //    .flatMapValues<long>(value => value.ToLowerCase(locale).Split(" "))
            //    .groupBy<string, long>((key, value) => value)
            //    .count();

            //// need to override value serde to Long type
            //counts
            //    .toStream()
            //    .to("streams-wordcount-output", Produced<string, long>
            //    .with(Serdes.String(), Serdes.Long()));

            //KafkaStreams streams2 = new KafkaStreams(builder.build(), streamsConfig);
        }
    }
}