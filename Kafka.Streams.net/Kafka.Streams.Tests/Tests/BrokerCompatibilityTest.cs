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
//                System.Console.Error.WriteLine("BrokerCompatibilityTest are expecting two parameters: propFile, eosEnabled; but only see " + args.Length + " parameter");
//                System.exit(1);
//            }

//            System.Console.Out.WriteLine("StreamsTest instance started");

//            string propFileName = args[0];
//            bool eosEnabled = Boolean.parseBoolean(args[1]);

//            StreamsConfig streamsProperties = Utils.loadProps(propFileName);
//            string kafka = streamsProperties.Get(StreamsConfig.BootstrapServersConfig);

//            if (kafka == null)
//            {
//                System.Console.Error.WriteLine("No bootstrap kafka servers specified in " + StreamsConfig.BootstrapServersConfig);
//                System.exit(1);
//            }

//            streamsProperties.Put(StreamsConfig.ApplicationIdConfig, "kafka-streams-system-test-broker-compatibility");
//            streamsProperties.Put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//            streamsProperties.Put(StreamsConfig.DefaultKeySerdeClassConfig, Serdes.String().GetType());
//            streamsProperties.Put(StreamsConfig.DefaultValueSerdeClassConfig, Serdes.String().GetType());
//            streamsProperties.Put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
//            streamsProperties.Put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
//            if (eosEnabled)
//            {
//                streamsProperties.Put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.StreamsConfig.ExactlyOnceConfig);
//            }
//            int timeout = 6000;
//            streamsProperties.Put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), timeout);
//            streamsProperties.Put(StreamsConfig.consumerPrefix(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG), timeout);
//            streamsProperties.Put(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG, timeout + 1);
//            Serde<string> stringSerde = Serdes.String();


//            StreamsBuilder builder = new StreamsBuilder();
//            builder.< string, string > stream(SOURCE_TOPIC).GroupByKey(Grouped.With(stringSerde, stringSerde))
//                         .Count()
//                         .ToStream()
//                         .MapValues(new ValueMapper<long, string>()
//                         {



//                public string apply(long value)
//            {
//                return value.ToString();
//            }
//        })
//            .To(SINK_TOPIC);

//        KafkaStreamsThread streams = new KafkaStreamsThread(builder.Build(), streamsProperties);
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
//            System.Console.Error.WriteLine("FATAL: An unexpected exception " + cause);
//            e.printStackTrace(System.Console.Error);
//            System.Console.Error.Flush();
//            streams.Close(TimeSpan.FromSeconds(30));
//        }
//    });
//        System.Console.Out.WriteLine("start Kafka Streams");
//        streams.Start();


//        System.Console.Out.WriteLine("send data");
//        StreamsConfig producerProperties = new StreamsConfig();
//    producerProperties.Put(ProducerConfig.BootstrapServersConfig, kafka);
//        producerProperties.Put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().Serializer);
//        producerProperties.Put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().Serializer);

//        try {
//            try { 
// (KafkaProducer<string, string> producer = new KafkaProducer<>(producerProperties));
//                producer.send(new ProducerRecord<>(SOURCE_TOPIC, "key", "value"));

//                System.Console.Out.WriteLine("wait for result");
//                loopUntilRecordReceived(kafka, eosEnabled);
//    System.Console.Out.WriteLine("Close Kafka Streams");
//                streams.Close();
//            }
//        } catch (RuntimeException e) {
//            System.Console.Error.WriteLine("Non-Streams exception occurred: ");
//            e.printStackTrace(System.Console.Error);
//            System.Console.Error.Flush();
//        }
//    }

//    private static void loopUntilRecordReceived(string kafka, bool eosEnabled)
//{
//    StreamsConfig consumerProperties = new StreamsConfig();
//    consumerProperties.Put(ConsumerConfig.BootstrapServersConfig, kafka);
//    consumerProperties.Put(ConsumerConfig.GROUP_ID_CONFIG, "broker-compatibility-consumer");
//    consumerProperties.Put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//    consumerProperties.Put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().Deserializer);
//        consumerProperties.Put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.String().Deserializer);
//        if (eosEnabled) {
//            consumerProperties.Put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.Name().toLowerCase(Locale.ROOT));
//        }

//        try { 
// (KafkaConsumer<string, string> consumer = new KafkaConsumer<>(consumerProperties));
//            consumer.subscribe(Collections.singletonList(SINK_TOPIC));

//            while (true) {
//                ConsumeResult<string, string> records = consumer.poll(TimeSpan.FromMilliseconds(100));
//                foreach (ConsumeResult<string, string> record in records) {
//                    if (record.Key.Equals("key") && record.Value.Equals("1")) {
//                        return;
//                    }
//                }
//            }
//        }
//    }
//}
