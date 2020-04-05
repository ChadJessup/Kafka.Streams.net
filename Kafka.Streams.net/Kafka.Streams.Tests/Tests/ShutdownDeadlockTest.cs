namespace Kafka.Streams.Tests.Tests
{
}
//using Confluent.Kafka;
//using Xunit;

//namespace Kafka.Streams.Tests.Tools
//{
//    public class ShutdownDeadlockTest
//    {

//        private string kafka;

//        public ShutdownDeadlockTest(string kafka)
//        {
//            this.kafka = kafka;
//        }

//        public void start()
//        {
//            string topic = "source";
//            StreamsConfig props = new StreamsConfig();
//            props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "shouldNotDeadlock");
//            props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
//            StreamsBuilder builder = new StreamsBuilder();
//            KStream<string, string> source = builder.Stream(topic, Consumed.With(Serdes.String(), Serdes.String()));

//            source.ForEach (new ForeachAction<string, string>()
//            {


//            public void apply(string key, string value)
//                    {
//                        throw new RuntimeException("KABOOM!");
//                    }
//        });
//        KafkaStreams streams = new KafkaStreams(builder.Build(), props);
//        streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {

//            public void uncaughtException(Thread t, Throwable e)
//        {
//            Exit.exit(1);
//        }
//    });

//        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
//    {

//        public void run()
//        {
//            streams.close(Duration.ofSeconds(5));
//        }
//    }));

//        StreamsConfig producerProps = new StreamsConfig();
//    producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "SmokeTest");
//        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
//        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().Serializer);
//        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().Serializer);

//        KafkaProducer<string, string> producer = new KafkaProducer<>(producerProps);
//    producer.send(new ProducerRecord<>(topic, "a", "a"));
//        producer.flush();

//        streams.start();

//        synchronized(this)
//    {
//        try
//        {
//            wait();
//        }
//        catch (InterruptedException e)
//        {
//            // ignored
//        }
//    }


//}



//}
