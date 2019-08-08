using Confluent.Kafka;
using Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals;
using System;
using System.Globalization;

namespace WordCountProcessorDemo
{
    public class Program
    {
        public static int Main(string[] args)
        {
            var props = new Properties
            {
                { StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount" },
                { StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092" },
                { StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0 },
                { StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().GetType().FullName },
                { StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().GetType().FullName }
            };

            var consumerConfig = new ConsumerConfig();
            // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
            // Note: To re-run the demo, you need to use the offset reset tool:
            // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
            props.Add(nameof(consumerConfig.AutoOffsetReset), AutoOffsetReset.EARLIEST);

            StreamsBuilder builder = new StreamsBuilder();

            IKStream<string, string> source = builder.stream<string, string>("streams-plaintext-input");

            var locale = CultureInfo.InvariantCulture;
            IKTable<string, long> counts = source
                .flatMapValues<long>(value => value.ToLowerCase(locale).Split(" "))
                .groupBy((key, value) => value)
                .count();

            // need to override value serde to Long type
            counts.toStream().to("streams-wordcount-output", Produced<string, long>.with(Serdes.String(), Serdes.Long()));

            KafkaStreams streams = new KafkaStreams(builder.build(), props);
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
    }
}
