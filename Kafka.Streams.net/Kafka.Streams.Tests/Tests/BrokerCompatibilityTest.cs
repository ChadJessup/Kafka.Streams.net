namespace Kafka.Streams.Tests.Tests
{
}
//using Confluent.Kafka;
//using Xunit;

//namespace Kafka.Streams.Tests.Tools
//{
//    public class BrokerCompatibilityTest
//    {

//        private static string SOURCE_TOPIC = "brokerCompatibilitySourceTopic";
//        private static string SINK_TOPIC = "brokerCompatibilitySinkTopic";

//        public static void main(string[] args) // ) throws IOException
//        {
//            if (args.Length < 2)
//            {
//                System.Console.Error.println("BrokerCompatibilityTest are expecting two parameters: propFile, eosEnabled; but only see " + args.Length + " parameter");
//                System.exit(1);
//            }

//            System.Console.Out.WriteLine("StreamsTest instance started");

//            string propFileName = args[0];
//            bool eosEnabled = Boolean.parseBoolean(args[1]);

//            StreamsConfig streamsProperties = Utils.loadProps(propFileName);
//            string kafka = streamsProperties.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);

//            if (kafka == null)
//            {
//                System.Console.Error.println("No bootstrap kafka servers specified in " + StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
//                System.exit(1);
//            }

//            streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-system-test-broker-compatibility");
//            streamsProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//            streamsProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//            streamsProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//            streamsProperties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
//            streamsProperties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
//            if (eosEnabled)
//            {
//                streamsProperties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
//            }
//            int timeout = 6000;
//            streamsProperties.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), timeout);
//            streamsProperties.put(StreamsConfig.consumerPrefix(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG), timeout);
//            streamsProperties.put(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG, timeout + 1);
//            Serde<string> stringSerde = Serdes.String();


//            StreamsBuilder builder = new StreamsBuilder();
//            builder.< string, string > stream(SOURCE_TOPIC).groupByKey(Grouped.with(stringSerde, stringSerde))
//                         .count()
//                         .toStream()
//                         .mapValues(new ValueMapper<long, string>()
//                         {



//                public string apply(long value)
//            {
//                return value.ToString();
//            }
//        })
//            .To(SINK_TOPIC);

//        KafkaStreams streams = new KafkaStreams(builder.Build(), streamsProperties);
//        streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {

//            public void uncaughtException(Thread t, Throwable e)
//        {
//            Throwable cause = e;
//            if (cause is StreamsException)
//            {
//                while (cause.getCause() != null)
//                {
//                    cause = cause.getCause();
//                }
//            }
//            System.Console.Error.println("FATAL: An unexpected exception " + cause);
//            e.printStackTrace(System.Console.Error);
//            System.Console.Error.flush();
//            streams.close(TimeSpan.ofSeconds(30));
//        }
//    });
//        System.Console.Out.WriteLine("start Kafka Streams");
//        streams.start();


//        System.Console.Out.WriteLine("send data");
//        StreamsConfig producerProperties = new StreamsConfig();
//    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
//        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().Serializer);
//        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().Serializer);

//        try {
//            try { 
// (KafkaProducer<string, string> producer = new KafkaProducer<>(producerProperties));
//                producer.send(new ProducerRecord<>(SOURCE_TOPIC, "key", "value"));

//                System.Console.Out.WriteLine("wait for result");
//                loopUntilRecordReceived(kafka, eosEnabled);
//    System.Console.Out.WriteLine("close Kafka Streams");
//                streams.close();
//            }
//        } catch (RuntimeException e) {
//            System.Console.Error.println("Non-Streams exception occurred: ");
//            e.printStackTrace(System.Console.Error);
//            System.Console.Error.flush();
//        }
//    }

//    private static void loopUntilRecordReceived(string kafka, bool eosEnabled)
//{
//    StreamsConfig consumerProperties = new StreamsConfig();
//    consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
//    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "broker-compatibility-consumer");
//    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().Deserializer);
//        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.String().Deserializer);
//        if (eosEnabled) {
//            consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT));
//        }

//        try { 
// (KafkaConsumer<string, string> consumer = new KafkaConsumer<>(consumerProperties));
//            consumer.subscribe(Collections.singletonList(SINK_TOPIC));

//            while (true) {
//                ConsumeResult<string, string> records = consumer.poll(TimeSpan.FromMilliseconds(100));
//                foreach (ConsumeResult<string, string> record in records) {
//                    if (record.Key.equals("key") && record.Value.equals("1")) {
//                        return;
//                    }
//                }
//            }
//        }
//    }
//}
