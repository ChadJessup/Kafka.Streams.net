using Confluent.Kafka;
using Kafka.Streams;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals;
using System;
using System.Globalization;

namespace WordCountProcessorDemo
{
    public class Program
    {
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
        }

        public static int Main(string[] args)
        {
            var streamsConfig = new StreamsConfig
            {
                ApplicationId = "streams-wordcount",
                BootstrapServers = "localhost:9092",
                //            streamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG = 0;
                DefaultKeySerde = Serdes.String().GetType(),
                DefaultValueSerde = Serdes.String().GetType(),

                // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
                // Note: To re-run the demo, you need to use the offset reset tool:
                // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var locale = CultureInfo.InvariantCulture;
            StreamsBuilder builder = new StreamsBuilder();

            IKStream<string, string> textLines = builder
                .stream<string, string>("TextLinesTopic");

            IKTable<string, long> wordCounts = textLines
                .flatMapValues<long>(textLine => textLine.ToLowerCase().Split("\\W+"))
                .groupBy((key, word) => word)
                .count("Counts");

            wordCounts.to(Serdes.String(), Serdes.Long(), "WordsWithCountsTopic");

            KafkaStreams streams = new KafkaStreams(builder, streamsConfig);
            streams.start();

            var source = builder
                .stream<string, string>("streams-plaintext-input");

            var counts = source
                .flatMapValues<long>(value => value.ToLowerCase(locale).Split(" "))
                .groupBy<string, long>((key, value) => value)
                .count();

            // need to override value serde to Long type
            counts
                .toStream()
                .to("streams-wordcount-output", Produced<string, long>
                .with(Serdes.String(), Serdes.Long()));

            KafkaStreams streams2 = new KafkaStreams(builder.build(), streamsConfig);
            //            CountDownLatch latch = new CountDownLatch(1);

            // attach shutdown handler to catch control-c
            //    Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook")
            //    {

            //        public void run()
            //    {
            //        streams.close();
            //        latch.countDown();
            //    }
            //});

            try
            {
                streams.start();
                // latch.await();
            }
            catch (Exception e)
            {
                return 1;
            }

            return 0;
        }

        private static object groupBy(Func<object, object, object> p)
        {
            throw new NotImplementedException();
        }
    }
}
