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
//            props.Set(StreamsConfig.ApplicationIdConfig, "shouldNotDeadlock");
//            props.Set(StreamsConfig.BootstrapServersConfig, kafka);
//            StreamsBuilder builder = new StreamsBuilder();
//            IKStream<K, V> source = builder.Stream(topic, Consumed.With(Serdes.String(), Serdes.String()));

//            source.ForEach (new ForeachAction<string, string>()
//            {


//            public void apply(string key, string value)
//                    {
//                        throw new RuntimeException("KABOOM!");
//                    }
//        });
//        KafkaStreamsThread streams = new KafkaStreamsThread(builder.Build(), props);
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
//            streams.Close(TimeSpan.FromSeconds(5));
//        }
//    }));

//        StreamsConfig producerProps = new StreamsConfig();
//    producerProps.Put(ProducerConfig.CLIENT_ID_CONFIG, "SmokeTest");
//        producerProps.Put(ProducerConfig.BootstrapServersConfig, kafka);
//        producerProps.Put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().Serializer);
//        producerProps.Put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().Serializer);

//        KafkaProducer<string, string> producer = new KafkaProducer<>(producerProps);
//    producer.send(new ProducerRecord<>(topic, "a", "a"));
//        producer.Flush();

//        streams.Start();

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
