namespace Kafka.Streams.Tests.Integration.utils
{
}
///*






// *

// *





// */






















































///**
// * Utility functions to make integration testing more convenient.
// */
//public class IntegrationTestUtils {

//    public static long DEFAULT_TIMEOUT = 60 * 1000L;

//    /*
//     * Records state transition for StreamThread
//     */
//    public static class StateListenerStub : StreamThread.StateListener {
//        bool startingToRevokedSeen = false;
//        bool revokedToPendingShutdownSeen = false;

//        public void onChange(Thread thread,
//                             ThreadStateTransitionValidator newState,
//                             ThreadStateTransitionValidator oldState) {
//            if (oldState == StreamThread.State.STARTING && newState == StreamThread.State.PARTITIONS_REVOKED) {
//                startingToRevokedSeen = true;
//            } else if (oldState == StreamThread.State.PARTITIONS_REVOKED && newState == StreamThread.State.PENDING_SHUTDOWN) {
//                revokedToPendingShutdownSeen = true;
//            }
//        }

//        public bool revokedToPendingShutdownSeen() {
//            return revokedToPendingShutdownSeen;
//        }

//        public bool createdToRevokedSeen() {
//            return startingToRevokedSeen;
//        }
//    }

//    /**
//     * Removes local state stores. Useful to reset state in-between integration test runs.
//     *
//     * @param streamsConfiguration Streams configuration settings
//     */
//    public static void purgeLocalStreamsState(Properties streamsConfiguration) throws
//        IOException {
//        string tmpDir = TestUtils.IO_TMP_DIR.getPath();
//        string path = streamsConfiguration.getProperty(StreamsConfig.STATE_DIR_CONFIG);
//        if (path != null) {
//            File node = Paths.get(path).normalize().toFile();
//            // Only purge state when it's under java.io.tmpdir.  This is a safety net to prevent accidentally
//            // deleting important local directory trees.
//            if (node.getAbsolutePath().startsWith(tmpDir)) {
//                Utils.delete(new File(node.getAbsolutePath()));
//            }
//        }
//    }

//    public static void cleanStateBeforeTest(EmbeddedKafkaCluster cluster, string... topics) {
//        cleanStateBeforeTest(cluster, 1, topics);
//    }

//    public static void cleanStateBeforeTest(EmbeddedKafkaCluster cluster, int partitionCount, string... topics) {
//        try {
//            cluster.deleteAllTopicsAndWait(DEFAULT_TIMEOUT);
//            foreach (string topic in topics) {
//                cluster.createTopic(topic, partitionCount, 1);
//            }
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//    }

//    public static void cleanStateAfterTest(EmbeddedKafkaCluster cluster, KafkaStreams driver) {
//        driver.cleanUp();
//        try {
//            cluster.deleteAllTopicsAndWait(DEFAULT_TIMEOUT);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//    }

//    /**
//     * @param topic          Kafka topic to write the data records to
//     * @param records        Data records to write to Kafka
//     * @param producerConfig Kafka producer configuration
//     * @param time           Timestamp provider
//     * @param <K>            Key type of the data records
//     * @param <V>            Value type of the data records
//     */
//    public static void produceKeyValuesSynchronously<K, V>(
//        string topic, Collection<KeyValuePair<K, V>> records, Properties producerConfig, Time time)
//        //throws ExecutionException, InterruptedException {
//        produceKeyValuesSynchronously(topic, records, producerConfig, time, false);
//    }

//    /**
//     * @param topic               Kafka topic to write the data records to
//     * @param records             Data records to write to Kafka
//     * @param producerConfig      Kafka producer configuration
//     * @param headers             {@link Headers} of the data records
//     * @param time                Timestamp provider
//     * @param <K>                 Key type of the data records
//     * @param <V>                 Value type of the data records
//     */
//    public static void produceKeyValuesSynchronously<K, V>(
//        string topic, Collection<KeyValuePair<K, V>> records, Properties producerConfig, Headers headers, Time time)
//        //throws ExecutionException, InterruptedException {
//        produceKeyValuesSynchronously(topic, records, producerConfig, headers, time, false);
//    }

//    /**
//     * @param topic               Kafka topic to write the data records to
//     * @param records             Data records to write to Kafka
//     * @param producerConfig      Kafka producer configuration
//     * @param time                Timestamp provider
//     * @param enableTransactions  Send messages in a transaction
//     * @param <K>                 Key type of the data records
//     * @param <V>                 Value type of the data records
//     */
//    public static void produceKeyValuesSynchronously<K, V>(
//        string topic, Collection<KeyValuePair<K, V>> records, Properties producerConfig, Time time, bool enableTransactions)
//        //throws ExecutionException, InterruptedException {
//        produceKeyValuesSynchronously(topic, records, producerConfig, null, time, enableTransactions);
//    }

//    /**
//     * @param topic               Kafka topic to write the data records to
//     * @param records             Data records to write to Kafka
//     * @param producerConfig      Kafka producer configuration
//     * @param headers             {@link Headers} of the data records
//     * @param time                Timestamp provider
//     * @param enableTransactions  Send messages in a transaction
//     * @param <K>                 Key type of the data records
//     * @param <V>                 Value type of the data records
//     */
//    public static void produceKeyValuesSynchronously<K, V>(string topic,
//                                                            Collection<KeyValuePair<K, V>> records,
//                                                            Properties producerConfig,
//                                                            Headers headers,
//                                                            Time time,
//                                                            bool enableTransactions)
//        //throws ExecutionException, InterruptedException {
//        foreach (KeyValuePair<K, V> record in records) {
//            produceKeyValuesSynchronouslyWithTimestamp(topic,
//                Collections.singleton(record),
//                producerConfig,
//                headers,
//                time.milliseconds(),
//                enableTransactions);
//            time.sleep(1L);
//        }
//    }

//    /**
//     * @param topic               Kafka topic to write the data records to
//     * @param records             Data records to write to Kafka
//     * @param producerConfig      Kafka producer configuration
//     * @param timestamp           Timestamp of the record
//     * @param <K>                 Key type of the data records
//     * @param <V>                 Value type of the data records
//     */
//    public static void produceKeyValuesSynchronouslyWithTimestamp<K, V>(string topic,
//                                                                         Collection<KeyValuePair<K, V>> records,
//                                                                         Properties producerConfig,
//                                                                         long timestamp)
//        //throws ExecutionException, InterruptedException {
//        produceKeyValuesSynchronouslyWithTimestamp(topic, records, producerConfig, timestamp, false);
//    }

//    /**
//     * @param topic               Kafka topic to write the data records to
//     * @param records             Data records to write to Kafka
//     * @param producerConfig      Kafka producer configuration
//     * @param timestamp           Timestamp of the record
//     * @param enableTransactions  Send messages in a transaction
//     * @param <K>                 Key type of the data records
//     * @param <V>                 Value type of the data records
//     */

//    public static void produceKeyValuesSynchronouslyWithTimestamp<K, V>(string topic,
//                                                                         Collection<KeyValuePair<K, V>> records,
//                                                                         Properties producerConfig,
//                                                                         long timestamp,
//                                                                         bool enableTransactions)
//            //throws ExecutionException, InterruptedException {

//        produceKeyValuesSynchronouslyWithTimestamp(topic, records, producerConfig, null, timestamp, enableTransactions);
//    }

//    /**
//     * @param topic               Kafka topic to write the data records to
//     * @param records             Data records to write to Kafka
//     * @param producerConfig      Kafka producer configuration
//     * @param headers             {@link Headers} of the data records
//     * @param timestamp           Timestamp of the record
//     * @param enableTransactions  Send messages in a transaction
//     * @param <K>                 Key type of the data records
//     * @param <V>                 Value type of the data records
//     */

//    public static void produceKeyValuesSynchronouslyWithTimestamp<K, V>(string topic,
//                                                                         Collection<KeyValuePair<K, V>> records,
//                                                                         Properties producerConfig,
//                                                                         Headers headers,
//                                                                         long timestamp,
//                                                                         bool enableTransactions)
//            //throws ExecutionException, InterruptedException {

//        try { 
// (Producer<K, V> producer = new KafkaProducer<>(producerConfig));
//            if (enableTransactions) {
//                producer.initTransactions();
//                producer.beginTransaction();
//            }
//            foreach (KeyValuePair<K, V> record in records) {
//                Future<RecordMetadata> f = producer.send(
//                    new ProducerRecord<>(topic, null, timestamp, record.key, record.value, headers));
//                f.get();
//            }
//            if (enableTransactions) {
//                producer.commitTransaction();
//            }
//            producer.flush();
//        }
//    }

//    public static void produceSynchronously<V, K>(Properties producerConfig,
//                                                   bool eos,
//                                                   string topic,
//                                                   Optional<int> partition,
//                                                   List<KeyValueTimestamp<K, V>> toProduce) {
//        try { 
// (Producer<K, V> producer = new KafkaProducer<>(producerConfig));
//            if (eos) {
//                producer.initTransactions();
//                producer.beginTransaction();
//            }
//            LinkedList<Future<RecordMetadata>> futures = new LinkedList<>();
//            foreach (KeyValueTimestamp<K, V> record in toProduce) {
//                Future<RecordMetadata> f = producer.send(
//                    new ProducerRecord<>(topic, partition.orElse(null), record.Timestamp, record.Key, record.Value, null)
//                );
//                futures.add(f);
//            }

//            if (eos) {
//                producer.commitTransaction();
//            } else {
//                producer.flush();
//            }

//            foreach (Future<RecordMetadata> future in futures) {
//                try {
//                    future.get();
//                } catch (InterruptedException | ExecutionException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//        }
//    }

//    /**
//     * Produce data records and send them synchronously in an aborted transaction; that is, a transaction is started for
//     * each data record but not committed.
//     *
//     * @param topic               Kafka topic to write the data records to
//     * @param records             Data records to write to Kafka
//     * @param producerConfig      Kafka producer configuration
//     * @param timestamp           Timestamp of the record
//     * @param <K>                 Key type of the data records
//     * @param <V>                 Value type of the data records
//     */
//    public static void produceAbortedKeyValuesSynchronouslyWithTimestamp<K, V>(string topic,
//                                                                                Collection<KeyValuePair<K, V>> records,
//                                                                                Properties producerConfig,
//                                                                                long timestamp)
//        //throws ExecutionException, InterruptedException {
//        try { 
// (Producer<K, V> producer = new KafkaProducer<>(producerConfig));
//            producer.initTransactions();
//            foreach (KeyValuePair<K, V> record in records) {
//                producer.beginTransaction();
//                Future<RecordMetadata> f = producer
//                        .send(new ProducerRecord<>(topic, null, timestamp, record.key, record.value));
//                f.get();
//                producer.abortTransaction();
//            }
//        }
//    }

//    /**
//     * @param topic               Kafka topic to write the data records to
//     * @param records             Data records to write to Kafka
//     * @param producerConfig      Kafka producer configuration
//     * @param time                Timestamp provider
//     * @param <V>                 Value type of the data records
//     */
//    public static <V> void produceValuesSynchronously(string topic,
//                                                      Collection<V> records,
//                                                      Properties producerConfig,
//                                                      Time time)
//        //throws ExecutionException, InterruptedException {
//        produceValuesSynchronously(topic, records, producerConfig, time, false);
//    }

//    /**
//     * @param topic               Kafka topic to write the data records to
//     * @param records             Data records to write to Kafka
//     * @param producerConfig      Kafka producer configuration
//     * @param time                Timestamp provider
//     * @param enableTransactions  Send messages in a transaction
//     * @param <V>                 Value type of the data records
//     */

//    public static <V> void produceValuesSynchronously(string topic,
//                                                      Collection<V> records,
//                                                      Properties producerConfig,
//                                                      Time time,
//                                                      bool enableTransactions)
//            //throws ExecutionException, InterruptedException {
//        Collection<KeyValuePair<object, V>> keyedRecords = new ArrayList<>();
//        foreach (V value in records) {
//            KeyValuePair<object, V> kv = new KeyValuePair<>(null, value);
//            keyedRecords.add(kv);
//        }
//        produceKeyValuesSynchronously(topic, keyedRecords, producerConfig, time, enableTransactions);
//    }

//    /**
//     * Wait for streams to "finish", based on the consumer lag metric.
//     *
//     * Caveats:
//     * - Inputs must be finite, fully loaded, and flushed before this method is called
//     * - expectedPartitions is the total number of partitions to watch the lag on, including both input and internal.
//     *   It's somewhat ok to get this wrong, as the main failure case would be an immediate return due to the clients
//     *   not being initialized, which you can avoid with any non-zero value. But it's probably better to get it right ;)
//     */
//    public static void waitForCompletion(KafkaStreams streams,
//                                         int expectedPartitions,
//                                         int timeoutMilliseconds) {
//        long start = System.currentTimeMillis();
//        while (true) {
//            int lagMetrics = 0;
//            double totalLag = 0.0;
//            foreach (Metric metric in streams.metrics().values()) {
//                if (metric.metricName().name().equals("records-lag")) {
//                    lagMetrics++;
//                    totalLag += ((Number) metric.metricValue()).doubleValue();
//                }
//            }
//            if (lagMetrics >= expectedPartitions && totalLag == 0.0) {
//                return;
//            }
//            if (System.currentTimeMillis() - start >= timeoutMilliseconds) {
//                throw new RuntimeException(string.format(
//                    "Timed out waiting for completion. lagMetrics=[%s/%s] totalLag=[%s]",
//                    lagMetrics, expectedPartitions, totalLag
//                ));
//            }
//        }
//    }

//    /**
//     * Wait until enough data (consumer records) has been consumed.
//     *
//     * @param consumerConfig      Kafka Consumer configuration
//     * @param topic               Kafka topic to consume from
//     * @param expectedNumRecords  Minimum number of expected records
//     * @param <K>                 Key type of the data records
//     * @param <V>                 Value type of the data records
//     * @return All the records consumed, or null if no records are consumed
//     */

//    public static List<ConsumeResult<K, V>> waitUntilMinRecordsReceived<K, V>(Properties consumerConfig,
//                                                                                string topic,
//                                                                                int expectedNumRecords) {// throws InterruptedException
//        return waitUntilMinRecordsReceived(consumerConfig, topic, expectedNumRecords, DEFAULT_TIMEOUT);
//    }

//    /**
//     * Wait until enough data (consumer records) has been consumed.
//     *
//     * @param consumerConfig      Kafka Consumer configuration
//     * @param topic               Kafka topic to consume from
//     * @param expectedNumRecords  Minimum number of expected records
//     * @param waitTime            Upper bound of waiting time in milliseconds
//     * @param <K>                 Key type of the data records
//     * @param <V>                 Value type of the data records
//     * @return All the records consumed, or null if no records are consumed
//     */

//    public static List<ConsumeResult<K, V>> waitUntilMinRecordsReceived<K, V>(Properties consumerConfig,
//                                                                                string topic,
//                                                                                int expectedNumRecords,
//                                                                                long waitTime) {// throws InterruptedException
//        List<ConsumeResult<K, V>> accumData = new ArrayList<>();
//        try { 
// (Consumer<K, V> consumer = createConsumer(consumerConfig));
//            TestCondition valuesRead = () => {
//                List<ConsumeResult<K, V>> readData =
//                    readRecords(topic, consumer, waitTime, expectedNumRecords);
//                accumData.addAll(readData);
//                return accumData.Count >= expectedNumRecords;
//            };
//            string conditionDetails = "Did not receive all " + expectedNumRecords + " records from topic " + topic;
//            TestUtils.waitForCondition(valuesRead, waitTime, conditionDetails);
//        }
//        return accumData;
//    }

//    /**
//     * Wait until enough data (key-value records) has been consumed.
//     *
//     * @param consumerConfig      Kafka Consumer configuration
//     * @param topic               Kafka topic to consume from
//     * @param expectedNumRecords  Minimum number of expected records
//     * @param <K>                 Key type of the data records
//     * @param <V>                 Value type of the data records
//     * @return All the records consumed, or null if no records are consumed
//     */
//    public static List<KeyValuePair<K, V>> waitUntilMinKeyValueRecordsReceived<K, V>(Properties consumerConfig,
//                                                                                  string topic,
//                                                                                  int expectedNumRecords) {// throws InterruptedException
//        return waitUntilMinKeyValueRecordsReceived(consumerConfig, topic, expectedNumRecords, DEFAULT_TIMEOUT);
//    }

//    /**
//     * Wait until enough data (key-value records) has been consumed.
//     *
//     * @param consumerConfig     Kafka Consumer configuration
//     * @param topic              Kafka topic to consume from
//     * @param expectedNumRecords Minimum number of expected records
//     * @param waitTime           Upper bound of waiting time in milliseconds
//     * @param <K>                Key type of the data records
//     * @param <V>                Value type of the data records
//     * @return All the records consumed, or null if no records are consumed
//     * @throws AssertionError    if the given wait time elapses
//     */
//    public static List<KeyValuePair<K, V>> waitUntilMinKeyValueRecordsReceived<K, V>(Properties consumerConfig,
//                                                                                  string topic,
//                                                                                  int expectedNumRecords,
//                                                                                  long waitTime) {// throws InterruptedException
//        List<KeyValuePair<K, V>> accumData = new ArrayList<>();
//        try { 
// (Consumer<K, V> consumer = createConsumer(consumerConfig));
//            TestCondition valuesRead = () => {
//                List<KeyValuePair<K, V>> readData =
//                    readKeyValues(topic, consumer, waitTime, expectedNumRecords);
//                accumData.addAll(readData);
//                return accumData.Count >= expectedNumRecords;
//            };
//            string conditionDetails = "Did not receive all " + expectedNumRecords + " records from topic " + topic;
//            TestUtils.waitForCondition(valuesRead, waitTime, conditionDetails);
//        }
//        return accumData;
//    }

//    /**
//     * Wait until enough data (timestamped key-value records) has been consumed.
//     *
//     * @param consumerConfig     Kafka Consumer configuration
//     * @param topic              Kafka topic to consume from
//     * @param expectedNumRecords Minimum number of expected records
//     * @param waitTime           Upper bound of waiting time in milliseconds
//     * @return All the records consumed, or null if no records are consumed
//     * @param <K>                Key type of the data records
//     * @param <V>                Value type of the data records
//     */
//    public static List<KeyValueTimestamp<K, V>> waitUntilMinKeyValueWithTimestampRecordsReceived<K, V>(Properties consumerConfig,
//                                                                                                               string topic,
//                                                                                                               int expectedNumRecords,
//                                                                                                               long waitTime) {// throws InterruptedException
//        List<KeyValueTimestamp<K, V>> accumData = new ArrayList<>();
//        try { 
// (Consumer<K, V> consumer = createConsumer(consumerConfig));
//            TestCondition valuesRead = () => {
//                List<KeyValueTimestamp<K, V>> readData =
//                    readKeyValuesWithTimestamp(topic, consumer, waitTime, expectedNumRecords);
//                accumData.addAll(readData);
//                return accumData.Count >= expectedNumRecords;
//            };
//            string conditionDetails = "Did not receive all " + expectedNumRecords + " records from topic " + topic;
//            TestUtils.waitForCondition(valuesRead, waitTime, conditionDetails);
//        }
//        return accumData;
//    }

//    /**
//     * Wait until key-value mappings have been consumed.
//     *
//     * @param consumerConfig     Kafka Consumer configuration
//     * @param topic              Kafka topic to consume from
//     * @param expectedRecords    Expected key-value mappings
//     * @param <K>                Key type of the data records
//     * @param <V>                Value type of the data records
//     * @return All the mappings consumed, or null if no records are consumed
//     */
//    public static List<KeyValuePair<K, V>> waitUntilFinalKeyValueRecordsReceived<K, V>(Properties consumerConfig,
//                                                                                    string topic,
//                                                                                    List<KeyValuePair<K, V>> expectedRecords) {// throws InterruptedException
//        return waitUntilFinalKeyValueRecordsReceived(consumerConfig, topic, expectedRecords, DEFAULT_TIMEOUT);
//    }

//    /**
//     * Wait until key-value mappings have been consumed.
//     *
//     * @param consumerConfig     Kafka Consumer configuration
//     * @param topic              Kafka topic to consume from
//     * @param expectedRecords    Expected key-value mappings
//     * @param <K>                Key type of the data records
//     * @param <V>                Value type of the data records
//     * @return All the mappings consumed, or null if no records are consumed
//     */
//    public static List<KeyValueTimestamp<K, V>> waitUntilFinalKeyValueTimestampRecordsReceived<K, V>(Properties consumerConfig,
//                                                                                                      string topic,
//                                                                                                      List<KeyValueTimestamp<K, V>> expectedRecords) {// throws InterruptedException
//        return waitUntilFinalKeyValueRecordsReceived(consumerConfig, topic, expectedRecords, DEFAULT_TIMEOUT, true);
//    }

//    /**
//     * Wait until key-value mappings have been consumed.
//     *
//     * @param consumerConfig     Kafka Consumer configuration
//     * @param topic              Kafka topic to consume from
//     * @param expectedRecords    Expected key-value mappings
//     * @param waitTime           Upper bound of waiting time in milliseconds
//     * @param <K>                Key type of the data records
//     * @param <V>                Value type of the data records
//     * @return All the mappings consumed, or null if no records are consumed
//     */

//    public static List<KeyValuePair<K, V>> waitUntilFinalKeyValueRecordsReceived<K, V>(Properties consumerConfig,
//                                                                                    string topic,
//                                                                                    List<KeyValuePair<K, V>> expectedRecords,
//                                                                                    long waitTime) {// throws InterruptedException
//        return waitUntilFinalKeyValueRecordsReceived(consumerConfig, topic, expectedRecords, waitTime, false);
//    }

//    public static List<KeyValueTimestamp<K, V>> waitUntilFinalKeyValueTimestampRecordsReceived<K, V>(Properties consumerConfig,
//                                                                                                      string topic,
//                                                                                                      List<KeyValueTimestamp<K, V>> expectedRecords,
//                                                                                                      long waitTime) {// throws InterruptedException
//        return waitUntilFinalKeyValueRecordsReceived(consumerConfig, topic, expectedRecords, waitTime, true);
//    }


//    private static List<T> waitUntilFinalKeyValueRecordsReceived<K, V, T>(Properties consumerConfig,
//                                                                           string topic,
//                                                                           List<T> expectedRecords,
//                                                                           long waitTime,
//                                                                           bool withTimestamp) {// throws InterruptedException
//        List<T> accumData = new ArrayList<>();
//        try { 
// (Consumer<K, V> consumer = createConsumer(consumerConfig));
//            TestCondition valuesRead = () => {
//                List<T> readData;
//                if (withTimestamp) {
//                    readData = (List<T>) readKeyValuesWithTimestamp(topic, consumer, waitTime, expectedRecords.Count);
//                } else {
//                    readData = (List<T>) readKeyValues(topic, consumer, waitTime, expectedRecords.Count);
//                }
//                accumData.addAll(readData);

//                // filter out all intermediate records we don't want
//                List<T> accumulatedActual = accumData.stream().filter(expectedRecords::contains).collect(Collectors.toList());

//                // still need to check that for each key, the ordering is expected
//                Dictionary<K, List<T>> finalAccumData = new HashMap<>();
//                foreach (T kv in accumulatedActual) {
//                    finalAccumData.computeIfAbsent(
//                        (K) (withTimestamp ? ((KeyValueTimestamp) kv).Key : ((KeyValue) kv).key),
//                        key => new ArrayList<>()).add(kv);
//                }
//                Dictionary<K, List<T>> finalExpected = new HashMap<>();
//                foreach (T kv in expectedRecords) {
//                    finalExpected.computeIfAbsent(
//                        (K) (withTimestamp ? ((KeyValueTimestamp) kv).Key : ((KeyValue) kv).key),
//                        key => new ArrayList<>()).add(kv);
//                }

//                // returns true only if the remaining records in both lists are the same and in the same order
//                // and the last record received matches the last expected record
//                return finalAccumData.equals(finalExpected);

//            };
//            string conditionDetails = "Did not receive all " + expectedRecords + " records from topic " + topic;
//            TestUtils.waitForCondition(valuesRead, waitTime, conditionDetails);
//        }
//        return accumData;
//    }

//    /**
//     * Wait until enough data (value records) has been consumed.
//     *
//     * @param consumerConfig     Kafka Consumer configuration
//     * @param topic              Topic to consume from
//     * @param expectedNumRecords Minimum number of expected records
//     * @return All the records consumed, or null if no records are consumed
//     * @throws AssertionError    if the given wait time elapses
//     */
//    public static <V> List<V> waitUntilMinValuesRecordsReceived(Properties consumerConfig,
//                                                                string topic,
//                                                                int expectedNumRecords) {// throws InterruptedException

//        return waitUntilMinValuesRecordsReceived(consumerConfig, topic, expectedNumRecords, DEFAULT_TIMEOUT);
//    }

//    /**
//     * Wait until enough data (value records) has been consumed.
//     *
//     * @param consumerConfig     Kafka Consumer configuration
//     * @param topic              Topic to consume from
//     * @param expectedNumRecords Minimum number of expected records
//     * @param waitTime           Upper bound of waiting time in milliseconds
//     * @return All the records consumed, or null if no records are consumed
//     * @throws AssertionError    if the given wait time elapses
//     */
//    public static <V> List<V> waitUntilMinValuesRecordsReceived(Properties consumerConfig,
//                                                                string topic,
//                                                                int expectedNumRecords,
//                                                                long waitTime) {// throws InterruptedException
//        List<V> accumData = new ArrayList<>();
//        try { 
// (Consumer<object, V> consumer = createConsumer(consumerConfig));
//            TestCondition valuesRead = () => {
//                List<V> readData =
//                    readValues(topic, consumer, waitTime, expectedNumRecords);
//                accumData.addAll(readData);
//                return accumData.Count >= expectedNumRecords;
//            };
//            string conditionDetails = "Did not receive all " + expectedNumRecords + " records from topic " + topic;
//            TestUtils.waitForCondition(valuesRead, waitTime, conditionDetails);
//        }
//        return accumData;
//    }


//    public static void waitForTopicPartitions(List<KafkaServer> servers,
//                                              List<TopicPartition> partitions,
//                                              long timeout) {// throws InterruptedException
//        long end = System.currentTimeMillis() + timeout;
//        foreach (TopicPartition partition in partitions) {
//            long remaining = end - System.currentTimeMillis();
//            if (remaining <= 0) {
//                throw new AssertionError("timed out while waiting for partitions to become available. Timeout=" + timeout);
//            }
//            waitUntilMetadataIsPropagated(servers, partition.topic(), partition.partition(), remaining);
//        }
//    }

//    public static void waitUntilMetadataIsPropagated(List<KafkaServer> servers,
//                                                     string topic,
//                                                     int partition,
//                                                     long timeout) {// throws InterruptedException
//        TestUtils.waitForCondition(() => {
//            foreach (KafkaServer server in servers) {
//                MetadataCache metadataCache = server.dataPlaneRequestProcessor().metadataCache();
//                Option<UpdateMetadataRequest.PartitionState> partitionInfo =
//                        metadataCache.getPartitionInfo(topic, partition);
//                if (partitionInfo.isEmpty()) {
//                    return false;
//                }
//                UpdateMetadataRequest.PartitionState metadataPartitionState = partitionInfo.get();
//                if (!Request.isValidBrokerId(metadataPartitionState.basePartitionState.leader)) {
//                    return false;
//                }
//            }
//            return true;
//        }, timeout, "metadata for topic=" + topic + " partition=" + partition + " not propagated to all brokers");

//    }

//    public static void verifyKeyValueTimestamps<K, V>(Properties consumerConfig,
//                                                       string topic,
//                                                       List<KeyValueTimestamp<K, V>> expected) {

//        List<ConsumeResult<K, V>> results;
//        try {
//            results = waitUntilMinRecordsReceived(consumerConfig, topic, expected.Count);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }

//        if (results.Count != expected.Count) {
//            throw new AssertionError(printRecords(results) + " != " + expected);
//        }
//        Iterator<KeyValueTimestamp<K, V>> expectedIterator = expected.iterator();
//        foreach (ConsumeResult<K, V> result in results) {
//            KeyValueTimestamp<K, V> expected1 = expectedIterator.next();
//            try {
//                compareKeyValueTimestamp(result, expected1.Key, expected1.Value, expected1.Timestamp);
//            } catch (AssertionError e) {
//                throw new AssertionError(printRecords(results) + " != " + expected, e);
//            }
//        }
//    }

//    public static void verifyKeyValueTimestamps(Properties consumerConfig,
//                                                string topic,
//                                                HashSet<KeyValueTimestamp<string, long>> expected) {
//        List<ConsumeResult<string, long>> results;
//        try {
//            results = waitUntilMinRecordsReceived(consumerConfig, topic, expected.Count);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }

//        if (results.Count != expected.Count) {
//            throw new AssertionError(printRecords(results) + " != " + expected);
//        }

//        HashSet<KeyValueTimestamp<string, long>> actual =
//            results.stream()
//                   .map(result => new KeyValueTimestamp<>(result.Key, result.Value, result.Timestamp))
//                   .collect(Collectors.toSet());

//        Assert.Equal(actual, (expected));
//    }

//    private static void compareKeyValueTimestamp<K, V>(ConsumeResult<K, V> record,
//                                                        K expectedKey,
//                                                        V expectedValue,
//                                                        long expectedTimestamp) {
//        Objects.requireNonNull(record);
//        K recordKey = record.Key;
//        V recordValue = record.Value;
//        long recordTimestamp = record.Timestamp;
//        AssertionError error = new AssertionError("Expected <" + expectedKey + ", " + expectedValue + "> with timestamp=" + expectedTimestamp +
//                                                            " but was <" + recordKey + ", " + recordValue + "> with timestamp=" + recordTimestamp);
//        if (recordKey != null) {
//            if (!recordKey.equals(expectedKey)) {
//                throw error;
//            }
//        } else if (expectedKey != null) {
//            throw error;
//        }
//        if (recordValue != null) {
//            if (!recordValue.equals(expectedValue)) {
//                throw error;
//            }
//        } else if (expectedValue != null) {
//            throw error;
//        }
//        if (recordTimestamp != expectedTimestamp) {
//            throw error;
//        }
//    }

//    private static string printRecords<K, V>(List<ConsumeResult<K, V>> result) {
//        StringBuilder resultStr = new StringBuilder();
//        resultStr.append("[\n");
//        foreach (ConsumeResult<?, ?> record in result) {
//            resultStr.append("  ").append(record.toString()).append("\n");
//        }
//        resultStr.append("]");
//        return resultStr.toString();
//    }

//    /**
//     * Returns up to `maxMessages` message-values from the topic.
//     *
//     * @param topic          Kafka topic to read messages from
//     * @param consumerConfig Kafka consumer config
//     * @param waitTime       Maximum wait time in milliseconds
//     * @param maxMessages    Maximum number of messages to read via the consumer.
//     * @return The values retrieved via the consumer.
//     */
//    public static <V> List<V> readValues(string topic, Properties consumerConfig,
//        long waitTime, int maxMessages) {
//        List<V> returnList;
//        try { 
// (Consumer<object, V> consumer = createConsumer(consumerConfig));
//            returnList = readValues(topic, consumer, waitTime, maxMessages);
//        }
//        return returnList;
//    }

//    /**
//     * Returns up to `maxMessages` by reading via the provided consumer (the topic(s) to read from
//     * are already configured in the consumer).
//     *
//     * @param topic          Kafka topic to read messages from
//     * @param consumerConfig Kafka consumer config
//     * @param waitTime       Maximum wait time in milliseconds
//     * @param maxMessages    Maximum number of messages to read via the consumer
//     * @return The KeyValue elements retrieved via the consumer
//     */
//    public static List<KeyValuePair<K, V>> readKeyValues<K, V>(string topic,
//        Properties consumerConfig, long waitTime, int maxMessages) {
//        List<KeyValuePair<K, V>> consumedValues;
//        try { 
// (Consumer<K, V> consumer = createConsumer(consumerConfig));
//            consumedValues = readKeyValues(topic, consumer, waitTime, maxMessages);
//        }
//        return consumedValues;
//    }

//    public static KafkaStreams getStartedStreams(Properties streamsConfig, StreamsBuilder builder, bool clean) {
//        KafkaStreams driver = new KafkaStreams(builder.build(), streamsConfig);
//        if (clean) {
//            driver.cleanUp();
//        }
//        driver.start();
//        return driver;
//    }

//    /**
//     * Returns up to `maxMessages` message-values from the topic.
//     *
//     * @param topic          Kafka topic to read messages from
//     * @param consumer       Kafka consumer
//     * @param waitTime       Maximum wait time in milliseconds
//     * @param maxMessages    Maximum number of messages to read via the consumer.
//     * @return The values retrieved via the consumer.
//     */
//    private static <V> List<V> readValues(string topic,
//                                          Consumer<object, V> consumer,
//                                          long waitTime,
//                                          int maxMessages) {
//        List<V> returnList = new ArrayList<>();
//        List<KeyValuePair<object, V>> kvs = readKeyValues(topic, consumer, waitTime, maxMessages);
//        foreach (KeyValuePair<?, V> kv in kvs) {
//            returnList.add(kv.value);
//        }
//        return returnList;
//    }

//    /**
//     * Returns up to `maxMessages` by reading via the provided consumer (the topic(s) to read from
//     * are already configured in the consumer).
//     *
//     * @param topic          Kafka topic to read messages from
//     * @param consumer       Kafka consumer
//     * @param waitTime       Maximum wait time in milliseconds
//     * @param maxMessages    Maximum number of messages to read via the consumer
//     * @return The KeyValue elements retrieved via the consumer
//     */
//    private static List<KeyValuePair<K, V>> readKeyValues<K, V>(string topic,
//                                                             Consumer<K, V> consumer,
//                                                             long waitTime,
//                                                             int maxMessages) {
//        List<KeyValuePair<K, V>> consumedValues = new ArrayList<>();
//        List<ConsumeResult<K, V>> records = readRecords(topic, consumer, waitTime, maxMessages);
//        foreach (ConsumeResult<K, V> record in records) {
//            consumedValues.add(new KeyValuePair<>(record.Key, record.Value));
//        }
//        return consumedValues;
//    }

//    /**
//     * Returns up to `maxMessages` by reading via the provided consumer (the topic(s) to read from
//     * are already configured in the consumer).
//     *
//     * @param topic          Kafka topic to read messages from
//     * @param consumer       Kafka consumer
//     * @param waitTime       Maximum wait time in milliseconds
//     * @param maxMessages    Maximum number of messages to read via the consumer
//     * @return The KeyValue elements retrieved via the consumer
//     */
//    private static List<KeyValueTimestamp<K, V>> readKeyValuesWithTimestamp<K, V>(string topic,
//                                                                                   Consumer<K, V> consumer,
//                                                                                   long waitTime,
//                                                                                   int maxMessages) {
//        List<KeyValueTimestamp<K, V>> consumedValues = new ArrayList<>();
//        List<ConsumeResult<K, V>> records = readRecords(topic, consumer, waitTime, maxMessages);
//        foreach (ConsumeResult<K, V> record in records) {
//            consumedValues.add(new KeyValueTimestamp<>(record.Key, record.Value, record.Timestamp));
//        }
//        return consumedValues;
//    }

//    private static List<ConsumeResult<K, V>> readRecords<K, V>(string topic,
//                                                                 Consumer<K, V> consumer,
//                                                                 long waitTime,
//                                                                 int maxMessages) {
//        List<ConsumeResult<K, V>> consumerRecords;
//        consumer.subscribe(Collections.singletonList(topic));
//        int pollIntervalMs = 100;
//        consumerRecords = new ArrayList<>();
//        int totalPollTimeMs = 0;
//        while (totalPollTimeMs < waitTime &&
//            continueConsuming(consumerRecords.Count, maxMessages)) {
//            totalPollTimeMs += pollIntervalMs;
//            ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(pollIntervalMs));

//            foreach (ConsumeResult<K, V> record in records) {
//                consumerRecords.add(record);
//            }
//        }
//        return consumerRecords;
//    }

//    private static bool continueConsuming(int messagesConsumed, int maxMessages) {
//        return maxMessages <= 0 || messagesConsumed < maxMessages;
//    }

//    /**
//     * Sets up a {@link KafkaConsumer} from a copy of the given configuration that has
//     * {@link ConsumerConfig#AUTO_OFFSET_RESET_CONFIG} set to "earliest" and {@link ConsumerConfig#ENABLE_AUTO_COMMIT_CONFIG}
//     * set to "true" to prevent missing events as well as repeat consumption.
//     * @param consumerConfig Consumer configuration
//     * @return Consumer
//     */
//    private static KafkaConsumer<K, V> createConsumer<K, V>(Properties consumerConfig) {
//        Properties filtered = new Properties();
//        filtered.putAll(consumerConfig);
//        filtered.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        filtered.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
//        return new KafkaConsumer<>(filtered);
//    }
//}
