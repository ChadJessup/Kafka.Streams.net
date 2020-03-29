/*






 *

 *





 */




















































/**
 * Class that provides support for a series of benchmarks. It is usually driven by
 * tests/kafkatest/benchmarks/streams/streams_simple_benchmark_test.py.
 * If ran manually through the main() function below, you must do the following:
 * 1. Have ZK and a Kafka broker set up
 * 2. Run the loading step first: SimpleBenchmark localhost:9092 /tmp/statedir numRecords true "all"
 * 3. Run the stream processing step second: SimpleBenchmark localhost:9092 /tmp/statedir numRecords false "all"
 * Note that what changed is the 4th parameter, from "true" indicating that is a load phase, to "false" indicating
 * that this is a real run.
 *
 * Note that "all" is a convenience option when running this test locally and will not work when running the test
 * at scale (through tests/kafkatest/benchmarks/streams/streams_simple_benchmark_test.py). That is due to exact syncronization
 * needs for each test (e.g., you wouldn't want one instance to run "count" while another
 * is still running "consume"
 */
public class SimpleBenchmark {
    private static string LOADING_PRODUCER_CLIENT_ID = "simple-benchmark-loading-producer";

    private static string SOURCE_TOPIC_ONE = "simpleBenchmarkSourceTopic1";
    private static string SOURCE_TOPIC_TWO = "simpleBenchmarkSourceTopic2";
    private static string SINK_TOPIC = "simpleBenchmarkSinkTopic";

    private static string YAHOO_CAMPAIGNS_TOPIC = "yahooCampaigns";
    private static string YAHOO_EVENTS_TOPIC = "yahooEvents";

    private static ValueJoiner<byte[], byte[], byte[]> VALUE_JOINER = new ValueJoiner<byte[], byte[], byte[]>() {
        
        public byte[] Apply(byte[] value1, byte[] value2) {
            // dump joiner in order to have as less join overhead as possible
            if (value1 != null) {
                return value1;
            } else if (value2 != null) {
                return value2;
            } else {
                return new byte[100];
            }
        }
    };

    private static Serde<byte[]> BYTE_SERDE = Serdes.ByteArray();
    private static Serde<int> INTEGER_SERDE = Serdes.Int();

    long processedBytes = 0L;
    int processedRecords = 0;

    private static long POLL_MS = 500L;
    private static long COMMIT_INTERVAL_MS = 30000L;
    private static int MAX_POLL_RECORDS = 1000;

    /* ----------- benchmark variables that are hard-coded ----------- */

    private static int KEY_SPACE_SIZE = 10000;

    private static long STREAM_STREAM_JOIN_WINDOW = 10000L;

    private static long AGGREGATE_WINDOW_SIZE = 1000L;

    private static long AGGREGATE_WINDOW_ADVANCE = 500L;

    private static int SOCKET_SIZE_BYTES = 1024 * 1024;

    // the following numbers are based on empirical results and should only
    // be considered for updates when perf results have significantly changed

    // with at least 10 million records, we run for at most 3 minutes
    private static int MAX_WAIT_MS = 3 * 60 * 1000;

    /* ----------- benchmark variables that can be specified ----------- */

    string testName;

    int numRecords;

    Properties props;

    private int valueSize;

    private double keySkew;

    /* ----------- ----------------------------------------- ----------- */


    private SimpleBenchmark(Properties props,
                            string testName,
                            int numRecords,
                            double keySkew,
                            int valueSize) {
        super();
        this.props = props;
        this.testName = testName;
        this.keySkew = keySkew;
        this.valueSize = valueSize;
        this.numRecords = numRecords;
    }

    private void Run() {
        switch (testName) {
            // loading phases
            case "load-one":
                produce(LOADING_PRODUCER_CLIENT_ID, SOURCE_TOPIC_ONE, numRecords, keySkew, valueSize);
                break;
            case "load-two":
                produce(LOADING_PRODUCER_CLIENT_ID, SOURCE_TOPIC_ONE, numRecords, keySkew, valueSize);
                produce(LOADING_PRODUCER_CLIENT_ID, SOURCE_TOPIC_TWO, numRecords, keySkew, valueSize);
                break;

            // testing phases
            case "consume":
                consume(SOURCE_TOPIC_ONE);
                break;
            case "consumeproduce":
                consumeAndProduce(SOURCE_TOPIC_ONE);
                break;
            case "streamcount":
                countStreamsNonWindowed(SOURCE_TOPIC_ONE);
                break;
            case "streamcountwindowed":
                countStreamsWindowed(SOURCE_TOPIC_ONE);
                break;
            case "streamprocess":
                processStream(SOURCE_TOPIC_ONE);
                break;
            case "streamprocesswithsink":
                processStreamWithSink(SOURCE_TOPIC_ONE);
                break;
            case "streamprocesswithstatestore":
                processStreamWithStateStore(SOURCE_TOPIC_ONE);
                break;
            case "streamprocesswithwindowstore":
                processStreamWithWindowStore(SOURCE_TOPIC_ONE);
                break;
            case "streamtablejoin":
                streamTableJoin(SOURCE_TOPIC_ONE, SOURCE_TOPIC_TWO);
                break;
            case "streamstreamjoin":
                streamStreamJoin(SOURCE_TOPIC_ONE, SOURCE_TOPIC_TWO);
                break;
            case "tabletablejoin":
                tableTableJoin(SOURCE_TOPIC_ONE, SOURCE_TOPIC_TWO);
                break;
            case "yahoo":
                yahooBenchmark(YAHOO_CAMPAIGNS_TOPIC, YAHOO_EVENTS_TOPIC);
                break;
            default:
                throw new RuntimeException("Unknown test name " + testName);

        }
    }

    public static void Main(string[] args){ //throws IOException
        if (args.Length < 5) {
            System.Console.Error.println("Not enough parameters are provided; expecting propFileName, testName, numRecords, keySkew, valueSize");
            System.exit(1);
        }

        string propFileName = args[0];
        string testName = args[1].toLowerCase(Locale.ROOT);
        int numRecords = int.parseInt(args[2]);
        double keySkew = Double.parseDouble(args[3]); // 0d means even distribution
        int valueSize = int.parseInt(args[4]);

        Properties props = Utils.loadProps(propFileName);
        string kafka = props.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);

        if (kafka == null) {
            System.Console.Error.println("No bootstrap kafka servers specified in " + StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
            System.exit(1);
        }

        // Note: this output is needed for automated tests and must not be removed
        System.Console.Out.WriteLine("StreamsTest instance started");

        System.Console.Out.WriteLine("testName=" + testName);
        System.Console.Out.WriteLine("streamsProperties=" + props);
        System.Console.Out.WriteLine("numRecords=" + numRecords);
        System.Console.Out.WriteLine("keySkew=" + keySkew);
        System.Console.Out.WriteLine("valueSize=" + valueSize);

        SimpleBenchmark benchmark = new SimpleBenchmark(props, testName, numRecords, keySkew, valueSize);

        benchmark.run();
    }

    public void SetStreamProperties(string applicationId) {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "simple-benchmark");
        props.put(StreamsConfig.POLL_MS_CONFIG, POLL_MS);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL_MS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Int().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        // the socket buffer needs to be large, especially when running in AWS with
        // high latency. if running locally the default is fine.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, SOCKET_SIZE_BYTES);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);

        // improve producer throughput
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5000);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 128 * 1024);
    }

    private Properties SetProduceConsumeProperties(string clientId) {
        Properties clientProps = new Properties();
        clientProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        clientProps.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        // the socket buffer needs to be large, especially when running in AWS with
        // high latency. if running locally the default is fine.
        clientProps.put(ProducerConfig.LINGER_MS_CONFIG, 5000);
        clientProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 128 * 1024);
        clientProps.put(ProducerConfig.SEND_BUFFER_CONFIG, SOCKET_SIZE_BYTES);
        clientProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer);
        clientProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer);
        clientProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer);
        clientProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer);
        clientProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // the socket buffer needs to be large, especially when running in AWS with
        // high latency. if running locally the default is fine.
        clientProps.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, SOCKET_SIZE_BYTES);
        clientProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
        return clientProps;
    }

    void ResetStats() {
        processedRecords = 0;
        processedBytes = 0L;
    }

    /**
     * Produce values to a topic
     * @param clientId string specifying client ID
     * @param topic Topic to produce to
     * @param numRecords Number of records to produce
     * @param keySkew Key zipf distribution skewness
     * @param valueSize Size of value in bytes
     */
    private void Produce(string clientId,
                         string topic,
                         int numRecords,
                         double keySkew,
                         int valueSize) {
        Properties props = setProduceConsumeProperties(clientId);
        ZipfGenerator keyGen = new ZipfGenerator(KEY_SPACE_SIZE, keySkew);

        try { 
 (KafkaProducer<int, byte[]> producer = new KafkaProducer<>(props));
            byte[] value = new byte[valueSize];
            // put some random values to increase entropy. Some devices
            // like SSDs do compression and if the array is all zeros
            // the performance will be too good.
            new Random(System.currentTimeMillis()).nextBytes(value);

            for (int i = 0; i < numRecords; i++) {
                producer.send(new ProducerRecord<>(topic, keyGen.Next(), value));
            }
        }
    }

    private void ConsumeAndProduce(string topic) {
        Properties consumerProps = setProduceConsumeProperties("simple-benchmark-consumer");
        Properties producerProps = setProduceConsumeProperties("simple-benchmark-producer");

        long startTime = System.currentTimeMillis();
        try (KafkaConsumer<int, byte[]> consumer = new KafkaConsumer<>(consumerProps);
             KafkaProducer<int, byte[]> producer = new KafkaProducer<>(producerProps)) {
            List<TopicPartition> partitions = getAllPartitions(consumer, topic);

            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            while (true) {
                ConsumerRecords<int, byte[]> records = consumer.poll(ofMillis(POLL_MS));
                if (records.isEmpty()) {
                    if (processedRecords == numRecords) {
                        break;
                    }
                } else {
                    foreach (ConsumeResult<int, byte[]> record in records) {
                        producer.send(new ProducerRecord<>(SINK_TOPIC, record.Key, record.Value));
                        processedRecords++;
                        processedBytes += record.Value.Length + int.SIZE;
                        if (processedRecords == numRecords) {
                            break;
                        }
                    }
                }
                if (processedRecords == numRecords) {
                    break;
                }
            }
        }

        long endTime = System.currentTimeMillis();

        printResults("ConsumerProducer Performance [records/latency/rec-sec/MB-sec read]: ", endTime - startTime);
    }

    private void consume(string topic) {
        Properties consumerProps = setProduceConsumeProperties("simple-benchmark-consumer");

        long startTime = System.currentTimeMillis();

        try { 
 (KafkaConsumer<int, byte[]> consumer = new KafkaConsumer<>(consumerProps));
            List<TopicPartition> partitions = getAllPartitions(consumer, topic);

            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            while (true) {
                ConsumerRecords<int, byte[]> records = consumer.poll(ofMillis(POLL_MS));
                if (records.isEmpty()) {
                    if (processedRecords == numRecords) {
                        break;
                    }
                } else {
                    foreach (ConsumeResult<int, byte[]> record in records) {
                        processedRecords++;
                        processedBytes += record.Value.Length + int.SIZE;
                        if (processedRecords == numRecords) {
                            break;
                        }
                    }
                }
                if (processedRecords == numRecords) {
                    break;
                }
            }
        }

        long endTime = System.currentTimeMillis();

        printResults("Consumer Performance [records/latency/rec-sec/MB-sec read]: ", endTime - startTime);
    }

    private void ProcessStream(string topic) {
        CountDownLatch latch = new CountDownLatch(1);

        setStreamProperties("simple-benchmark-streams-source");

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(topic, Consumed.with(INTEGER_SERDE, BYTE_SERDE)).peek(new CountDownAction(latch));

        KafkaStreams streams = createKafkaStreamsWithExceptionHandler(builder, props);
        runGenericBenchmark(streams, "Streams Source Performance [records/latency/rec-sec/MB-sec joined]: ", latch);
    }

    private void ProcessStreamWithSink(string topic) {
        CountDownLatch latch = new CountDownLatch(1);

        setStreamProperties("simple-benchmark-streams-source-sink");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<int, byte[]> source = builder.stream(topic);
        source.peek(new CountDownAction(latch)).to(SINK_TOPIC);

        KafkaStreams streams = createKafkaStreamsWithExceptionHandler(builder, props);
        runGenericBenchmark(streams, "Streams SourceSink Performance [records/latency/rec-sec/MB-sec joined]: ", latch);
    }

    private void ProcessStreamWithStateStore(string topic) {
        CountDownLatch latch = new CountDownLatch(1);

        setStreamProperties("simple-benchmark-streams-with-store");

        StreamsBuilder builder = new StreamsBuilder();
        StoreBuilder<KeyValueStore<int, byte[]>> storeBuilder =
            Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("store"), INTEGER_SERDE, BYTE_SERDE);
        builder.addStateStore(storeBuilder.withCachingEnabled());

        KStream<int, byte[]> source = builder.stream(topic);

        source.peek(new CountDownAction(latch)).process(new ProcessorSupplier<int, byte[]>() {
            
            public Processor<int, byte[]> get() {
                return new AbstractProcessor<int, byte[]>() {
                    KeyValueStore<int, byte[]> store;

                    
                    
                    public void init(ProcessorContext context) {
                        base.init(context);
                        store = (KeyValueStore<int, byte[]>) context.getStateStore("store");
                    }

                    
                    public void process(int key, byte[] value) {
                        store.get(key);
                        store.put(key, value);
                    }
                };
            }
        }, "store");

        KafkaStreams streams = createKafkaStreamsWithExceptionHandler(builder, props);
        runGenericBenchmark(streams, "Streams Stateful Performance [records/latency/rec-sec/MB-sec joined]: ", latch);
    }

    private void ProcessStreamWithWindowStore(string topic) {
        CountDownLatch latch = new CountDownLatch(1);

        setStreamProperties("simple-benchmark-streams-with-store");

        StreamsBuilder builder = new StreamsBuilder();

        StoreBuilder<WindowStore<int, byte[]>> storeBuilder = Stores.windowStoreBuilder(
            Stores.persistentWindowStore(
                "store",
                ofMillis(AGGREGATE_WINDOW_SIZE * 3),
                ofMillis(AGGREGATE_WINDOW_SIZE),
                false
            ),
            INTEGER_SERDE,
            BYTE_SERDE
        );
        builder.addStateStore(storeBuilder.withCachingEnabled());

        KStream<int, byte[]> source = builder.stream(topic);

        source.peek(new CountDownAction(latch)).process(new ProcessorSupplier<int, byte[]>() {
            
            public Processor<int, byte[]> get() {
                return new AbstractProcessor<int, byte[]>() {
                    WindowStore<int, byte[]> store;

                    
                    
                    public void init(ProcessorContext context) {
                        base.init(context);
                        store = (WindowStore<int, byte[]>) context.getStateStore("store");
                    }

                    
                    public void process(int key, byte[] value) {
                        long timestamp = context().Timestamp;
                        KeyValueIterator<Windowed<int>, byte[]> iter = store.fetch(key - 10, key + 10, ofEpochMilli(timestamp - 1000L), ofEpochMilli(timestamp));
                        while (iter.hasNext()) {
                            iter.next();
                        }
                        iter.close();

                        store.put(key, value);
                    }
                };
            }
        }, "store");

        KafkaStreams streams = createKafkaStreamsWithExceptionHandler(builder, props);
        runGenericBenchmark(streams, "Streams Stateful Performance [records/latency/rec-sec/MB-sec joined]: ", latch);
    }

    /**
     * Measure the performance of a simple aggregate like count.
     * Counts the occurrence of numbers (note that normally people count words, this
     * example counts numbers)
     */
    private void CountStreamsNonWindowed(string sourceTopic) {
        CountDownLatch latch = new CountDownLatch(1);

        setStreamProperties("simple-benchmark-nonwindowed-count");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<int, byte[]> input = builder.stream(sourceTopic);

        input.peek(new CountDownAction(latch))
                .groupByKey()
                .count();

        KafkaStreams streams = createKafkaStreamsWithExceptionHandler(builder, props);
        runGenericBenchmark(streams, "Streams Count Performance [records/latency/rec-sec/MB-sec counted]: ", latch);
    }

    /**
     * Measure the performance of a simple aggregate like count.
     * Counts the occurrence of numbers (note that normally people count words, this
     * example counts numbers)
     */
    private void CountStreamsWindowed(string sourceTopic) {
        CountDownLatch latch = new CountDownLatch(1);

        setStreamProperties("simple-benchmark-windowed-count");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<int, byte[]> input = builder.stream(sourceTopic);

        input.peek(new CountDownAction(latch))
                .groupByKey()
                .windowedBy(TimeWindows.of(ofMillis(AGGREGATE_WINDOW_SIZE)).advanceBy(ofMillis(AGGREGATE_WINDOW_ADVANCE)))
                .count();

        KafkaStreams streams = createKafkaStreamsWithExceptionHandler(builder, props);
        runGenericBenchmark(streams, "Streams Count Windowed Performance [records/latency/rec-sec/MB-sec counted]: ", latch);
    }

    /**
     * Measure the performance of a KStream-KTable left join. The setup is such that each
     * KStream record joins to exactly one element in the KTable
     */
    private void StreamTableJoin(string kStreamTopic, string kTableTopic) {
        CountDownLatch latch = new CountDownLatch(1);

        setStreamProperties("simple-benchmark-stream-table-join");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<int, byte[]> input1 = builder.stream(kStreamTopic);
        KTable<int, byte[]> input2 = builder.table(kTableTopic);

        input1.leftJoin(input2, VALUE_JOINER).foreach(new CountDownAction(latch));

        KafkaStreams streams = createKafkaStreamsWithExceptionHandler(builder, props);

        // run benchmark
        runGenericBenchmark(streams, "Streams KStreamKTable LeftJoin Performance [records/latency/rec-sec/MB-sec joined]: ", latch);
    }

    /**
     * Measure the performance of a KStream-KStream left join. The setup is such that each
     * KStream record joins to exactly one element in the other KStream
     */
    private void StreamStreamJoin(string kStreamTopic1, string kStreamTopic2) {
        CountDownLatch latch = new CountDownLatch(1);

        setStreamProperties("simple-benchmark-stream-stream-join");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<int, byte[]> input1 = builder.stream(kStreamTopic1);
        KStream<int, byte[]> input2 = builder.stream(kStreamTopic2);

        input1.leftJoin(input2, VALUE_JOINER, JoinWindows.of(ofMillis(STREAM_STREAM_JOIN_WINDOW))).foreach(new CountDownAction(latch));

        KafkaStreams streams = createKafkaStreamsWithExceptionHandler(builder, props);

        // run benchmark
        runGenericBenchmark(streams, "Streams KStreamKStream LeftJoin Performance [records/latency/rec-sec/MB-sec  joined]: ", latch);
    }

    /**
     * Measure the performance of a KTable-KTable left join. The setup is such that each
     * KTable record joins to exactly one element in the other KTable
     */
    private void TableTableJoin(string kTableTopic1, string kTableTopic2) {
        CountDownLatch latch = new CountDownLatch(1);

        // setup join
        setStreamProperties("simple-benchmark-table-table-join");

        StreamsBuilder builder = new StreamsBuilder();

        KTable<int, byte[]> input1 = builder.table(kTableTopic1);
        KTable<int, byte[]> input2 = builder.table(kTableTopic2);

        input1.leftJoin(input2, VALUE_JOINER).toStream().foreach(new CountDownAction(latch));

        KafkaStreams streams = createKafkaStreamsWithExceptionHandler(builder, props);

        // run benchmark
        runGenericBenchmark(streams, "Streams KTableKTable LeftJoin Performance [records/latency/rec-sec/MB-sec joined]: ", latch);
    }

    void PrintResults(string nameOfBenchmark, long latency) {
        System.Console.Out.WriteLine(nameOfBenchmark +
            processedRecords + "/" +
            latency + "/" +
            recordsPerSec(latency, processedRecords) + "/" +
            megabytesPerSec(latency, processedBytes));
    }

    void RunGenericBenchmark(KafkaStreams streams, string nameOfBenchmark, CountDownLatch latch) {
        streams.start();

        long startTime = System.currentTimeMillis();
        long endTime = startTime;

        while (latch.getCount() > 0 && (endTime - startTime < MAX_WAIT_MS)) {
            try {
                latch.await(1000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ex) {
                Thread.interrupted();
            }

            endTime = System.currentTimeMillis();
        }
        streams.close();

        printResults(nameOfBenchmark, endTime - startTime);
    }

    private class CountDownAction : ForeachAction<int, byte[]> {
        private CountDownLatch latch;

        CountDownAction(CountDownLatch latch) {
            this.latch = latch;
        }

        
        public void Apply(int key, byte[] value) {
            processedRecords++;
            processedBytes += int.SIZE + value.Length;

            if (processedRecords == numRecords) {
                this.latch.countDown();
            }
        }
    }

    private KafkaStreams CreateKafkaStreamsWithExceptionHandler(StreamsBuilder builder, Properties props) {
        KafkaStreams streamsClient = new KafkaStreams(builder.build(), props);
        streamsClient.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            
            public void uncaughtException(Thread t, Throwable e) {
                System.Console.Out.WriteLine("FATAL: An unexpected exception is encountered on thread " + t + ": " + e);

                streamsClient.close(ofSeconds(30));
            }
        });

        return streamsClient;
    }
    
    private double MegabytesPerSec(long time, long processedBytes) {
        return  (processedBytes / 1024.0 / 1024.0) / (time / 1000.0);
    }

    private double RecordsPerSec(long time, int numRecords) {
        return numRecords / (time / 1000.0);
    }

    private List<TopicPartition> GetAllPartitions(KafkaConsumer<?, ?> consumer, string... topics) {
        ArrayList<TopicPartition> partitions = new ArrayList<>();

        foreach (string topic in topics) {
            foreach (PartitionInfo info in consumer.partitionsFor(topic)) {
                partitions.add(new TopicPartition(info.topic(), info.partition()));
            }
        }
        return partitions;
    }

    private void YahooBenchmark(string campaignsTopic, string eventsTopic) {
        YahooBenchmark benchmark = new YahooBenchmark(this, campaignsTopic, eventsTopic);

        benchmark.run();
    }

    private class ZipfGenerator {
        private Random rand = new Random(System.currentTimeMillis());
        private int size;
        private double skew;

        private double bottom = 0.0d;

        ZipfGenerator(int size, double skew) {
            this.size = size;
            this.skew = skew;

            for (int i = 1; i < size; i++) {
                this.bottom += 1.0d / Math.pow(i, this.skew);
            }
        }

        int Next() {
            if (skew == 0.0d) {
                return rand.nextInt(size);
            } else {
                int rank;
                double dice;
                double frequency;

                rank = rand.nextInt(size);
                frequency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
                dice = rand.nextDouble();

                while (!(dice < frequency)) {
                    rank = rand.nextInt(size);
                    frequency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
                    dice = rand.nextDouble();
                }

                return rank;
            }
        }
    }
}
