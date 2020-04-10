namespace Kafka.Streams.Tests.Tests
{
}
//using Confluent.Kafka;
//using Xunit;

//namespace Kafka.Streams.Tests.Tools
//{
//    public class SmokeTestClient : SmokeTestUtil
//    {

//        private string Name;

//        private Thread thread;
//        private KafkaStreams streams;
//        private bool uncaughtException = false;
//        private bool started;

//        public SmokeTestClient(string Name)
//        {
//            super();
//            this.Name = Name;
//        }

//        public bool started()
//        {
//            return started;
//        }

//        public void start(StreamsConfig streamsProperties)
//        {
//            streams = createKafkaStreams(streamsProperties);
//            streams.setUncaughtExceptionHandler((t, e) =>
//            {
//                System.Console.Out.WriteLine(Name + ": SMOKE-TEST-CLIENT-EXCEPTION");
//                uncaughtException = true;
//                e.printStackTrace();
//            });

//            Runtime.getRuntime().addShutdownHook(new Thread(this::Close));

//            thread = new Thread(() => streams.start());
//            thread.start();
//        }

//        public void closeAsync()
//        {
//            streams.Close(TimeSpan.TimeSpan.Zero);
//        }

//        public void Close()
//        {
//            streams.Close(TimeSpan.ofSeconds(5));
//            // do not remove these printouts since they are needed for health scripts
//            if (!uncaughtException)
//            {
//                System.Console.Out.WriteLine(Name + ": SMOKE-TEST-CLIENT-CLOSED");
//            }
//            try
//            {
//                thread.join();
//            }
//            catch (Exception ex)
//            {
//                // do not remove these printouts since they are needed for health scripts
//                System.Console.Out.WriteLine(Name + ": SMOKE-TEST-CLIENT-EXCEPTION");
//                // ignore
//            }
//        }

//        private StreamsConfig getStreamsConfig(StreamsConfig props)
//        {
//            StreamsConfig fullProps = new StreamsConfig(props);
//            fullProps.Put(StreamsConfig.APPLICATION_ID_CONFIG, "SmokeTest");
//            fullProps.Put(StreamsConfig.CLIENT_ID_CONFIG, "SmokeTest-" + Name);
//            fullProps.Put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);
//            fullProps.Put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 2);
//            fullProps.Put(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, 100);
//            fullProps.Put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
//            fullProps.Put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
//            fullProps.Put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//            fullProps.Put(ProducerConfig.ACKS_CONFIG, "All");
//            fullProps.Put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory().FullName);
//            fullProps.putAll(props);
//            return fullProps;
//        }

//        private KafkaStreams createKafkaStreams(StreamsConfig props)
//        {
//            Topology build = getTopology();
//            KafkaStreams streamsClient = new KafkaStreams(build, getStreamsConfig(props));
//            streamsClient.setStateListener((newState, oldState) =>
//            {
//                System.Console.Out.printf("%s %s: %s => %s%n", Name, Instant.now(), oldState, newState);
//                if (oldState == KafkaStreams.State.REBALANCING && newState == KafkaStreams.State.RUNNING)
//                {
//                    started = true;
//                }
//            });
//            streamsClient.setUncaughtExceptionHandler((t, e) =>
//            {
//                System.Console.Out.WriteLine(Name + ": FATAL: An unexpected exception is encountered on thread " + t + ": " + e);
//                streamsClient.Close(TimeSpan.ofSeconds(30));
//            });

//            return streamsClient;
//        }

//        public Topology getTopology()
//        {
//            StreamsBuilder builder = new StreamsBuilder();
//            Consumed<string, int> stringIntConsumed = Consumed.With(stringSerde, intSerde);
//            KStream<string, int> source = builder.Stream("data", stringIntConsumed);
//            source.filterNot((k, v) => k.equals("Flush"))
//                  .To("echo", Produced.With(stringSerde, intSerde));
//            KStream<string, int> data = source.filter((key, value) => value == null || value != END);
//            data.process(SmokeTestUtil.printProcessorSupplier("data", Name));

//            // min
//            KGroupedStream<string, int> groupedData = data.groupByKey(Grouped.with(stringSerde, intSerde));

//            KTable<IWindowed<string>, int> minAggregation = groupedData
//                .windowedBy(TimeWindows.of(TimeSpan.ofDays(1)).grace(TimeSpan.ofMinutes(1)))
//                .aggregate(
//                    () => int.MaxValue,
//                    (aggKey, value, aggregate) => (value < aggregate) ? value : aggregate,
//                    Materialized
//                        .< string, int, IWindowStore<Bytes, byte[]> >as ("uwin-min")
//                        .withValueSerde(intSerde)
//                        .withRetention(TimeSpan.ofHours(25))
//                );

//            streamify(minAggregation, "min-raw");

//            streamify(minAggregation.suppress(untilWindowCloses(BufferConfig.unbounded())), "min-suppressed");

//            minAggregation
//                .toStream(new Unwindow<>())
//                .filterNot((k, v) => k.equals("Flush"))
//                .To("min", Produced.With(stringSerde, intSerde));

//            KTable<IWindowed<string>, int> smallWindowSum = groupedData
//                .windowedBy(TimeWindows.of(TimeSpan.ofSeconds(2)).advanceBy(TimeSpan.ofSeconds(1)).grace(TimeSpan.ofSeconds(30)))
//                .reduce((l, r) => l + r);

//            streamify(smallWindowSum, "sws-raw");
//            streamify(smallWindowSum.suppress(untilWindowCloses(BufferConfig.unbounded())), "sws-suppressed");

//            KTable<string, int> minTable = builder.table(
//                "min",
//                Consumed.With(stringSerde, intSerde),
//                Materialized.As ("minStoreName"));

//            minTable.toStream().process(SmokeTestUtil.printProcessorSupplier("min", Name));

//            // max
//            groupedData
//                .windowedBy(TimeWindows.of(TimeSpan.ofDays(2)))
//                .aggregate(
//                    () => int.MIN_VALUE,
//                    (aggKey, value, aggregate) => (value > aggregate) ? value : aggregate,
//                    Materialized< string, int, IWindowStore<Bytes, byte[]> >.As ("uwin-max").withValueSerde(intSerde))
//            .toStream(new Unwindow<>())
//            .filterNot((k, v) => k.equals("Flush"))
//            .To("max", Produced.With(stringSerde, intSerde));

//            KTable<string, int> maxTable = builder.table(
//                "max",
//                Consumed.With(stringSerde, intSerde),
//                Materialized.As ("maxStoreName"));
//            maxTable.toStream().process(SmokeTestUtil.printProcessorSupplier("max", Name));

//            // sum
//            groupedData
//                .windowedBy(TimeWindows.of(TimeSpan.ofDays(2)))
//                .aggregate(
//                    () => 0L,
//                    (aggKey, value, aggregate) => (long)value + aggregate,
//                    Materialized< string, long, IWindowStore<Bytes, byte[]> >.As ("win-sum").withValueSerde(longSerde))
//            .toStream(new Unwindow<>())
//            .filterNot((k, v) => k.equals("Flush"))
//            .To("sum", Produced.With(stringSerde, longSerde));

//            Consumed<string, long> stringLongConsumed = Consumed.With(stringSerde, longSerde);
//            KTable<string, long> sumTable = builder.table("sum", stringLongConsumed);
//            sumTable.toStream().process(SmokeTestUtil.printProcessorSupplier("sum", Name));

//            // cnt
//            groupedData
//                .windowedBy(TimeWindows.of(TimeSpan.ofDays(2)))
//                .count(Materialized.As ("uwin-cnt"))
//            .toStream(new Unwindow<>())
//            .filterNot((k, v) => k.equals("Flush"))
//            .To("cnt", Produced.With(stringSerde, longSerde));

//            KTable<string, long> cntTable = builder.table(
//                "cnt",
//                Consumed.With(stringSerde, longSerde),
//                Materialized.As ("cntStoreName"));
//            cntTable.toStream().process(SmokeTestUtil.printProcessorSupplier("cnt", Name));

//            // dif
//            maxTable
//                .join(
//                    minTable,
//                    (value1, value2) => value1 - value2)
//                .toStream()
//                .filterNot((k, v) => k.equals("Flush"))
//                .To("dif", Produced.With(stringSerde, intSerde));

//            // avg
//            sumTable
//                .join(
//                    cntTable,
//                    (value1, value2) => (double)value1 / (double)value2)
//                .toStream()
//                .filterNot((k, v) => k.equals("Flush"))
//                .To("avg", Produced.With(stringSerde, doubleSerde));

//            // test repartition
//            Agg agg = new Agg();
//            cntTable.groupBy(agg.selector(), Grouped.with(stringSerde, longSerde))
//                    .aggregate(agg.Init(), agg.adder(), agg.remover(),
//                               Materialized< string, long >.As (Stores.InMemoryKeyValueStore("cntByCnt"))
//                                   .WithKeySerde(Serdes.String())
//                                   .withValueSerde(Serdes.Long()))
//                .toStream()
//                .To("tagg", Produced.With(stringSerde, longSerde));

//            return builder.Build();
//        }

//        private static void streamify(KTable<IWindowed<string>, int> windowedTable, string topic)
//        {
//            windowedTable
//                .toStream()
//                .filterNot((k, v) => k.Key.equals("Flush"))
//                .map((key, value) => KeyValuePair.Create(key.ToString(), value))
//                .To(topic, Produced.With(stringSerde, intSerde));
//        }
//    }
//}
