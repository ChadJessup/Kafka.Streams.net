using Confluent.Kafka;
using Kafka.Common.Extensions;
using Kafka.Common.Utils;
using Kafka.Streams;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Mappers;
using Kafka.Streams.State.Internals;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;

namespace WordCountProcessorDemo
{
    public class Program
    {
        public static int Main(string[] args)
        {
            var streamsConfig = new StreamsConfig
            {
                ApplicationId = "streams-wordcount",
                BootstrapServers = "localhost:9092",
                DefaultKeySerde = Serdes.String().GetType(),
                DefaultValueSerde = Serdes.String().GetType(),
                NumberOfStreamThreads = 1,
                CacheMaxBytesBuffering = 10485760L,
                // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
                // Note: To re-run the demo, you need to use the offset reset tool:
                // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
                AutoOffsetReset = AutoOffsetReset.Earliest

                // streamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG = 0;
            };

            var services = new ServiceCollection()
                .AddSingleton(streamsConfig)
                .AddLogging();

            var latch = new ManualResetEvent(initialState: false);

            StreamsBuilder builder = new StreamsBuilder(services);

            IKStream<string, string> textLines = builder
                .stream<string, string>("TextLinesTopic");

            IKStream<string, string> flatMappedValues = textLines
                .flatMapValues<string>(new ValueMapper<string, IEnumerable<string>>(textLine => textLine.ToLower().Split("\\W+", RegexOptions.IgnoreCase).ToList()));

            IKGroupedStream<string, string> groupedByValues = flatMappedValues
                .groupBy<string>((key, word) => word);

            IKTable<string, long> wordCounts = groupedByValues
                .count(Materialized<string, long, IKeyValueStore<Bytes, byte[]>>.As("Counts"));

            IKStream<string, long> wordCountsStream = wordCounts
                .toStream();

            wordCountsStream.to(
                "WordsWithCountsTopic",
                Produced<string, long>.with(
                Serdes.String(),
                Serdes.Long()));

            // Make the topology injectable
            var topology = builder.build();
            services.AddSingleton(topology);

            using KafkaStreams streams = builder.BuildKafkaStreams(); 

            // attach shutdown handler to catch control-c
            Console.CancelKeyPress += (o, e) =>
            {
                latch.Set();
            };

            try
            {
                streams.start();
                latch.WaitOne();
            }
            catch (Exception e)
            {
                return 1;
            }

            return 0;
        }

        public void test()
        {
            //// Serializers/deserializers (serde) for String and Long types
            //ISerde<string> stringSerde = Serdes.String();
            //ISerde<long> longSerde = Serdes.Long();

            //// Construct a `KStream` from the input topic "streams-plaintext-input", where message values
            //// represent lines of text (for the sake of this example, we ignore whatever may be stored
            //// in the message keys).
            //IKStream<string, string> textLines = builder.stream(
            //      "streams-plaintext-input",
            //      Consumed.with(stringSerde, stringSerde)
            //    );

            //IKTable<string, long> wordCounts = textLines
            //    // Split each text line, by whitespace, into words.
            //    .flatMapValues(value=>Arrays.asList(value.toLowerCase().split("\\W+")))

            //    // Group the text words as message keys
            //    .groupBy((key, value)=>value)

            //    // Count the occurrences of each word (message key).
            //    .count();

            //// Store the running counts as a changelog stream to the output topic.
            //wordCounts.toStream().to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

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