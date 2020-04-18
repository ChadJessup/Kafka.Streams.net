namespace Kafka.Streams.Tests.Tests
{
}
//using Confluent.Kafka;
//using Xunit;

//namespace Kafka.Streams.Tests.Tools
//{
//    public class EosTestClient : SmokeTestUtil
//    {

//        static string APP_ID = "EosTest";
//        private StreamsConfig properties;
//        private bool withRepartitioning;
//        private bool notRunningCallbackReceived = new bool(false);

//        private KafkaStreamsThread streams;
//        private bool uncaughtException;

//        EosTestClient(StreamsConfig properties, bool withRepartitioning)
//        {
//            super();
//            this.properties = properties;
//            this.withRepartitioning = withRepartitioning;
//        }

//        private volatile bool isRunning = true;

//        public void start()
//        {
//            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
//            {


//            public void run()
//            {
//                isRunning = false;
//                streams.Close(TimeSpan.FromSeconds(300));

//                // need to wait for callback to avoid race condition
//                // => make sure the callback printout to stdout is there as it is expected test output
//                waitForStateTransitionCallback();

//                // do not remove these printouts since they are needed for health scripts
//                if (!uncaughtException)
//                {
//                    System.Console.Out.WriteLine(System.currentTimeMillis());
//                    System.Console.Out.WriteLine("EOS-TEST-CLIENT-CLOSED");
//                    System.Console.Out.Flush();
//                }

//            }
//        }));

//        while (isRunning) {
//            if (streams == null) {
//                uncaughtException = false;

//                streams = createKafkaStreams(properties);
//        streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {

//                    public void uncaughtException(Thread t, Throwable e)
//        {
//            System.Console.Out.WriteLine(System.currentTimeMillis());
//            System.Console.Out.WriteLine("EOS-TEST-CLIENT-EXCEPTION");
//            e.printStackTrace();
//            System.Console.Out.Flush();
//            uncaughtException = true;
//        }
//    });
//                streams.SetStateListener(new KafkaStreamsThread.StateListener() {

//                    public void onChange(KafkaStreamsThreadStates newState, KafkaStreamsThreadStates oldState)
//    {
//        // don't remove this -- it's required test output
//        System.Console.Out.WriteLine(System.currentTimeMillis());
//        System.Console.Out.WriteLine("StateChange: " + oldState + " => " + newState);
//        System.Console.Out.Flush();
//        if (newState == KafkaStreamsThreadStates.NOT_RUNNING)
//        {
//            notRunningCallbackReceived.set(true);
//        }
//    }
//});
//                streams.Start();
//            }
//            if (uncaughtException) {
//                streams.Close(TimeSpan.FromSeconds(60_000L));
//                streams = null;
//            }
//            sleep(1000);
//        }
//    }

//    private KafkaStreamsThread createKafkaStreams(StreamsConfig props)
//{
//    props.Put(StreamsConfig.ApplicationIdConfig, APP_ID);
//    props.Put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
//    props.Put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 2);
//    props.Put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
//    props.Put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.StreamsConfig.ExactlyOnceConfig);
//    props.Put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
//    props.Put(StreamsConfig.DefaultKeySerdeClassConfig, Serdes.String().GetType());
//    props.Put(StreamsConfig.DefaultValueSerdeClassConfig, Serdes.Int().GetType());

//    StreamsBuilder builder = new StreamsBuilder();
//    IKStream<K, V> data = builder.Stream("data");

//    data.To("echo");
//    data.Process(SmokeTestUtil.printProcessorSupplier("data"));

//    KGroupedStream<string, int> groupedData = data.GroupByKey();
//    // min
//    groupedData
//        .Aggregate(
//            new Initializer<int>()
//            {


//                    public int apply()
//    {
//        return int.MaxValue;
//    }
//},
//                new Aggregator<string, int, int>() {

//                    public int apply(string aggKey,
//                                         int value,
//                                         int aggregate)
//{
//    return (value < aggregate) ? value : aggregate;
//}
//                },
//                Materialized.<string, int, IKeyValueStore<Bytes, byte[]>>with(null, intSerde))
//            .ToStream()
//            .To("min", Produced.With(stringSerde, intSerde));

//// sum
//groupedData.Aggregate(
//            new Initializer<long>() {

//                public long apply()
//{
//    return 0L;
//}
//            },
//            new Aggregator<string, int, long>() {

//                public long apply(string aggKey,
//                                  int value,
//                                  long aggregate)
//{
//    return (long)value + aggregate;
//}
//            },
//            Materialized.<string, long, IKeyValueStore<Bytes, byte[]>>with(null, longSerde))
//            .ToStream()
//            .To("sum", Produced.With(stringSerde, longSerde));

//        if (withRepartitioning) {
//            IKStream<K, V> repartitionedData = data.through("repartition");

//repartitionedData.Process(SmokeTestUtil.printProcessorSupplier("repartition"));

//            KGroupedStream<string, int> groupedDataAfterRepartitioning = repartitionedData.GroupByKey();
//// max
//groupedDataAfterRepartitioning
//    .Aggregate(
//                    new Initializer<int>() {

//                        public int apply()
//{
//    return int.MIN_VALUE;
//}
//                    },
//                    new Aggregator<string, int, int>() {

//                        public int apply(string aggKey,
//                                             int value,
//                                             int aggregate)
//{
//    return (value > aggregate) ? value : aggregate;
//}
//                    },
//                    Materialized.<string, int, IKeyValueStore<Bytes, byte[]>>with(null, intSerde))
//                .ToStream()
//                .To("max", Produced.With(stringSerde, intSerde));

//// count
//groupedDataAfterRepartitioning.Count()
//                .ToStream()
//                .To("cnt", Produced.With(stringSerde, longSerde));
//        }

//        return new KafkaStreamsThread(builder.Build(), props);
//    }

//    private void waitForStateTransitionCallback()
//{
//    long maxWaitTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(300);
//    while (!notRunningCallbackReceived.Get() && System.currentTimeMillis() < maxWaitTime)
//    {
//        try
//        {
//            Thread.Sleep(500);
//        }
//        catch (InterruptedException ignoreAndSwallow) { /* just keep waiting */ }
//    }
//    if (!notRunningCallbackReceived.Get())
//    {
//        System.Console.Error.WriteLine("State transition callback to NOT_RUNNING never received. Timed out after 5 minutes.");
//        System.Console.Error.Flush();
//    }
//}
//}
