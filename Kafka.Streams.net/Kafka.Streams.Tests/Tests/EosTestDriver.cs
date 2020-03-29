//using Confluent.Kafka;
//using Xunit;

//namespace Kafka.Streams.Tests.Tools
//{
//    public class EosTestDriver : SmokeTestUtil
//    {

//        private static int MAX_NUMBER_OF_KEYS = 20000;
//        private static long MAX_IDLE_TIME_MS = 600000L;

//        private static bool isRunning = true;

//        private static int numRecordsProduced = 0;

//        private static synchronized void updateNumRecordsProduces(int delta)
//        {
//            numRecordsProduced += delta;
//        }

//        static void generate(string kafka)
//        {

//            Runtime.getRuntime().addShutdownHook(new Thread(() =>
//            {
//                System.Console.Out.WriteLine("Terminating");
//                System.Console.Out.flush();
//                isRunning = false;
//            }));

//            Properties producerProps = new Properties();
//            producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "EosTest");
//            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
//            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer);
//        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer);
//        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

//        KafkaProducer<string, int> producer = new KafkaProducer<>(producerProps);

//        Random rand = new Random(System.currentTimeMillis());

//        while (isRunning) {
//            string key = "" + rand.nextInt(MAX_NUMBER_OF_KEYS);
//        int value = rand.nextInt(10000);

//        ProducerRecord<string, int> record = new ProducerRecord<>("data", key, value);

//        producer.send(record, (metadata, exception) => {
//                if (exception != null) {
//                    exception.printStackTrace(System.Console.Error);
//                    System.Console.Error.flush();
//                    if (exception is TimeoutException) {
//                        try {
//                            // message == org.apache.kafka.common.errors.TimeoutException: Expiring 4 record(s) for data-0: 30004 ms has passed since last attempt plus backoff time
//                            int expired = int.parseInt(exception.getMessage().split(" ")[2]);
//        updateNumRecordsProduces(-expired);
//    } catch (Exception ignore) { }
//                    }
//                }
//            });

//            updateNumRecordsProduces(1);
//            if (numRecordsProduced % 1000 == 0) {
//                System.Console.Out.WriteLine(numRecordsProduced + " records produced");
//                System.Console.Out.flush();
//            }
//            Utils.sleep(rand.nextInt(10));
//        }
//        producer.close();
//        System.Console.Out.WriteLine("Producer closed: " + numRecordsProduced + " records produced");

//        Properties props = new Properties();
//props.put(ConsumerConfig.CLIENT_ID_CONFIG, "verifier");
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer);
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer);
//        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT));

//        try { 
// (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props));
//            List<TopicPartition> partitions = getAllPartitions(consumer, "data");
//System.Console.Out.WriteLine("Partitions: " + partitions);
//            consumer.assign(partitions);
//            consumer.seekToEnd(partitions);

//            foreach (TopicPartition tp in partitions) {
//                System.Console.Out.WriteLine("End-offset for " + tp + " is " + consumer.position(tp));
//            }
//        }
//        System.Console.Out.flush();
//    }

//    public static void verify(string kafka, bool withRepartitioning)
//{
//    Properties props = new Properties();
//    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "verifier");
//    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
//    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer);
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer);
//        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT));

//        Dictionary<TopicPartition, long> committedOffsets;
//        try { 
// (Admin adminClient = Admin.create(props));
//            ensureStreamsApplicationDown(adminClient);

//committedOffsets = getCommittedOffsets(adminClient, withRepartitioning);
//        }

//        string[] allInputTopics;
//string[] allOutputTopics;
//        if (withRepartitioning) {
//            allInputTopics = new string[] {"data", "repartition"};
//            allOutputTopics = new string[] {"echo", "min", "sum", "repartition", "max", "cnt"};
//        } else {
//            allInputTopics = new string[] {"data"};
//            allOutputTopics = new string[] {"echo", "min", "sum"};
//        }

//        Dictionary<string, Map<TopicPartition, List<ConsumeResult<byte[], byte[]>>>> inputRecordsPerTopicPerPartition;
//        try { 
// (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props));
//            List<TopicPartition> partitions = getAllPartitions(consumer, allInputTopics);
//consumer.assign(partitions);
//            consumer.seekToBeginning(partitions);

//            inputRecordsPerTopicPerPartition = getRecords(consumer, committedOffsets, withRepartitioning, true);
//        } catch (Exception e) {
//            e.printStackTrace(System.Console.Error);
//            System.Console.Out.WriteLine("FAILED");
//            return;
//        }

//        Dictionary<string, Map<TopicPartition, List<ConsumeResult<byte[], byte[]>>>> outputRecordsPerTopicPerPartition;
//        try { 
// (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props));
//            List<TopicPartition> partitions = getAllPartitions(consumer, allOutputTopics);
//consumer.assign(partitions);
//            consumer.seekToBeginning(partitions);

//            outputRecordsPerTopicPerPartition = getRecords(consumer, consumer.endOffsets(partitions), withRepartitioning, false);
//        } catch (Exception e) {
//            e.printStackTrace(System.Console.Error);
//            System.Console.Out.WriteLine("FAILED");
//            return;
//        }

//        verifyReceivedAllRecords(inputRecordsPerTopicPerPartition.get("data"), outputRecordsPerTopicPerPartition.get("echo"));
//        if (withRepartitioning) {
//            verifyReceivedAllRecords(inputRecordsPerTopicPerPartition.get("data"), outputRecordsPerTopicPerPartition.get("repartition"));
//        }

//        verifyMin(inputRecordsPerTopicPerPartition.get("data"), outputRecordsPerTopicPerPartition.get("min"));
//        verifySum(inputRecordsPerTopicPerPartition.get("data"), outputRecordsPerTopicPerPartition.get("sum"));

//        if (withRepartitioning) {
//            verifyMax(inputRecordsPerTopicPerPartition.get("repartition"), outputRecordsPerTopicPerPartition.get("max"));
//            verifyCnt(inputRecordsPerTopicPerPartition.get("repartition"), outputRecordsPerTopicPerPartition.get("cnt"));
//        }

//        try { 
// (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props));
//            List<TopicPartition> partitions = getAllPartitions(consumer, allOutputTopics);
//consumer.assign(partitions);
//            consumer.seekToBeginning(partitions);

//            verifyAllTransactionFinished(consumer, kafka, withRepartitioning);
//        } catch (Exception e) {
//            e.printStackTrace(System.Console.Error);
//            System.Console.Out.WriteLine("FAILED");
//            return;
//        }

//        // do not modify: required test output
//        System.Console.Out.WriteLine("ALL-RECORDS-DELIVERED");
//        System.Console.Out.flush();
//    }

//    private static void ensureStreamsApplicationDown(Admin adminClient)
//{

//    long maxWaitTime = System.currentTimeMillis() + MAX_IDLE_TIME_MS;
//    ConsumerGroupDescription description;
//    do
//    {
//        description = getConsumerGroupDescription(adminClient);

//        if (System.currentTimeMillis() > maxWaitTime && !description.members().isEmpty())
//        {
//            throw new RuntimeException(
//                "Streams application not down after " + (MAX_IDLE_TIME_MS / 1000) + " seconds. " +
//                    "Group: " + description
//            );
//        }
//        sleep(1000);
//    } while (!description.members().isEmpty());
//}


//private static Dictionary<TopicPartition, long> getCommittedOffsets(Admin adminClient,
//                                                             bool withRepartitioning)
//{
//    Dictionary<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap;

//    try
//    {
//        ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult = adminClient.listConsumerGroupOffsets(EosTestClient.APP_ID);
//        topicPartitionOffsetAndMetadataMap = listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
//    }
//    catch (InterruptedException | ExecutionException | java.util.concurrent.TimeoutException e) {
//        e.printStackTrace();
//        throw new RuntimeException(e);
//    }

//    Dictionary<TopicPartition, long> committedOffsets = new HashMap<>();

//    foreach (Map.Entry<TopicPartition, OffsetAndMetadata> entry in topicPartitionOffsetAndMetadataMap.entrySet())
//    {
//        string topic = entry.getKey().topic();
//        if (topic.equals("data") || withRepartitioning && topic.equals("repartition"))
//        {
//            committedOffsets.put(entry.getKey(), entry.getValue().Offset);
//        }
//    }

//    return committedOffsets;
//    }

//    private static Dictionary<string, Map<TopicPartition, List<ConsumeResult<byte[], byte[]>>>> getRecords(KafkaConsumer<byte[], byte[]> consumer,
//                                                                                                     Dictionary<TopicPartition, long> readEndOffsets,
//                                                                                                     bool withRepartitioning,
//                                                                                                     bool isInputTopic)
//    {
//        System.Console.Error.println("read end offset: " + readEndOffsets);
//        Dictionary<string, Map<TopicPartition, List<ConsumeResult<byte[], byte[]>>>> recordPerTopicPerPartition = new HashMap<>();
//        Dictionary<TopicPartition, long> maxReceivedOffsetPerPartition = new HashMap<>();
//        Dictionary<TopicPartition, long> maxConsumerPositionPerPartition = new HashMap<>();

//        long maxWaitTime = System.currentTimeMillis() + MAX_IDLE_TIME_MS;
//        bool allRecordsReceived = false;
//        while (!allRecordsReceived && System.currentTimeMillis() < maxWaitTime)
//        {
//            ConsumerRecords<byte[], byte[]> receivedRecords = consumer.poll(Duration.ofMillis(100));

//            foreach (ConsumeResult<byte[], byte[]> record in receivedRecords)
//            {
//                maxWaitTime = System.currentTimeMillis() + MAX_IDLE_TIME_MS;
//                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
//                maxReceivedOffsetPerPartition.put(tp, record.Offset);
//                long readEndOffset = readEndOffsets.get(tp);
//                if (record.Offset < readEndOffset)
//                {
//                    addRecord(record, recordPerTopicPerPartition, withRepartitioning);
//                }
//                else if (!isInputTopic)
//                {
//                    throw new RuntimeException("FAIL: did receive more records than expected for " + tp
//                        + " (expected EOL offset: " + readEndOffset + "; current offset: " + record.Offset);
//                }
//            }

//            foreach (TopicPartition tp in readEndOffsets.keySet())
//            {
//                maxConsumerPositionPerPartition.put(tp, consumer.position(tp));
//                if (consumer.position(tp) >= readEndOffsets.get(tp))
//                {
//                    consumer.pause(Collections.singletonList(tp));
//                }
//            }

//            allRecordsReceived = consumer.paused().Count == readEndOffsets.keySet().Count;
//        }

//        if (!allRecordsReceived)
//        {
//            System.Console.Error.println("Pause partitions (ie, received all data): " + consumer.paused());
//            System.Console.Error.println("Max received offset per partition: " + maxReceivedOffsetPerPartition);
//            System.Console.Error.println("Max consumer position per partition: " + maxConsumerPositionPerPartition);
//            throw new RuntimeException("FAIL: did not receive all records after " + (MAX_IDLE_TIME_MS / 1000) + " sec idle time.");
//        }

//        return recordPerTopicPerPartition;
//    }

//    private static void addRecord(ConsumeResult<byte[], byte[]> record,
//                                  Dictionary<string, Map<TopicPartition, List<ConsumeResult<byte[], byte[]>>>> recordPerTopicPerPartition,
//                                  bool withRepartitioning)
//    {

//        string topic = record.topic();
//        TopicPartition partition = new TopicPartition(topic, record.partition());

//        if (verifyTopic(topic, withRepartitioning))
//        {
//            Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>> topicRecordsPerPartition =
//                recordPerTopicPerPartition.computeIfAbsent(topic, k => new HashMap<>());

//            List<ConsumeResult<byte[], byte[]>> records =
//                topicRecordsPerPartition.computeIfAbsent(partition, k => new ArrayList<>());

//            records.add(record);
//        }
//        else
//        {
//            throw new RuntimeException("FAIL: received data from unexpected topic: " + record);
//        }
//    }

//    private static bool verifyTopic(string topic,
//                                       bool withRepartitioning)
//    {
//        bool validTopic = "data".equals(topic) || "echo".equals(topic) || "min".equals(topic) || "sum".equals(topic);

//        if (withRepartitioning)
//        {
//            return validTopic || "repartition".equals(topic) || "max".equals(topic) || "cnt".equals(topic);
//        }

//        return validTopic;
//    }

//    private static void verifyReceivedAllRecords(Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>> expectedRecords,
//                                                 Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>> receivedRecords)
//    {
//        if (expectedRecords.Count != receivedRecords.Count)
//        {
//            throw new RuntimeException("Result verification failed. Received " + receivedRecords.Count + " records but expected " + expectedRecords.Count);
//        }

//        StringDeserializer stringDeserializer = new StringDeserializer();
//        IntegerDeserializer integerDeserializer = Serializers.Int32;
//        foreach (Map.Entry<TopicPartition, List<ConsumeResult<byte[], byte[]>>> partitionRecords in receivedRecords.entrySet())
//        {
//            TopicPartition inputTopicPartition = new TopicPartition("data", partitionRecords.getKey().partition());
//            Iterator<ConsumeResult<byte[], byte[]>> expectedRecord = expectedRecords.get(inputTopicPartition).iterator();

//            foreach (ConsumeResult<byte[], byte[]> receivedRecord in partitionRecords.getValue())
//            {
//                ConsumeResult<byte[], byte[]> expected = expectedRecord.next();

//                string receivedKey = stringDeserializer.deserialize(receivedRecord.topic(), receivedRecord.Key);
//                int receivedValue = integerDeserializer.deserialize(receivedRecord.topic(), receivedRecord.Value);
//                string expectedKey = stringDeserializer.deserialize(expected.topic(), expected.Key);
//                int expectedValue = integerDeserializer.deserialize(expected.topic(), expected.Value);

//                if (!receivedKey.equals(expectedKey) || receivedValue != expectedValue)
//                {
//                    throw new RuntimeException("Result verification failed for " + receivedRecord + " expected <" + expectedKey + "," + expectedValue + "> but was <" + receivedKey + "," + receivedValue + ">");
//                }
//            }
//        }
//    }

//    private static void verifyMin(Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>> inputPerTopicPerPartition,
//                                  Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>> minPerTopicPerPartition)
//    {
//        StringDeserializer stringDeserializer = new StringDeserializer();
//        IntegerDeserializer integerDeserializer = Serializers.Int32;

//        HashDictionary<string, int> currentMinPerKey = new HashMap<>();
//        foreach (Map.Entry<TopicPartition, List<ConsumeResult<byte[], byte[]>>> partitionRecords in minPerTopicPerPartition.entrySet())
//        {
//            TopicPartition inputTopicPartition = new TopicPartition("data", partitionRecords.getKey().partition());
//            List<ConsumeResult<byte[], byte[]>> partitionInput = inputPerTopicPerPartition.get(inputTopicPartition);
//            List<ConsumeResult<byte[], byte[]>> partitionMin = partitionRecords.getValue();

//            if (partitionInput.Count != partitionMin.Count)
//            {
//                throw new RuntimeException("Result verification failed: expected " + partitionInput.Count + " records for "
//                    + partitionRecords.getKey() + " but received " + partitionMin.Count);
//            }

//            Iterator<ConsumeResult<byte[], byte[]>> inputRecords = partitionInput.iterator();

//            foreach (ConsumeResult<byte[], byte[]> receivedRecord in partitionMin)
//            {
//                ConsumeResult<byte[], byte[]> input = inputRecords.next();

//                string receivedKey = stringDeserializer.deserialize(receivedRecord.topic(), receivedRecord.Key);
//                int receivedValue = integerDeserializer.deserialize(receivedRecord.topic(), receivedRecord.Value);
//                string key = stringDeserializer.deserialize(input.topic(), input.Key);
//                int value = integerDeserializer.deserialize(input.topic(), input.Value);

//                int min = currentMinPerKey.get(key);
//                if (min == null)
//                {
//                    min = value;
//                }
//                else
//                {
//                    min = Math.min(min, value);
//                }
//                currentMinPerKey.put(key, min);

//                if (!receivedKey.equals(key) || receivedValue != min)
//                {
//                    throw new RuntimeException("Result verification failed for " + receivedRecord + " expected <" + key + "," + min + "> but was <" + receivedKey + "," + receivedValue + ">");
//                }
//            }
//        }
//    }

//    private static void verifySum(Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>> inputPerTopicPerPartition,
//                                  Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>> minPerTopicPerPartition)
//    {
//        StringDeserializer stringDeserializer = new StringDeserializer();
//        IntegerDeserializer integerDeserializer = Serializers.Int32;
//        LongDeserializer longDeserializer = new LongDeserializer();

//        HashDictionary<string, long> currentSumPerKey = new HashMap<>();
//        foreach (Map.Entry<TopicPartition, List<ConsumeResult<byte[], byte[]>>> partitionRecords in minPerTopicPerPartition.entrySet())
//        {
//            TopicPartition inputTopicPartition = new TopicPartition("data", partitionRecords.getKey().partition());
//            List<ConsumeResult<byte[], byte[]>> partitionInput = inputPerTopicPerPartition.get(inputTopicPartition);
//            List<ConsumeResult<byte[], byte[]>> partitionSum = partitionRecords.getValue();

//            if (partitionInput.Count != partitionSum.Count)
//            {
//                throw new RuntimeException("Result verification failed: expected " + partitionInput.Count + " records for "
//                    + partitionRecords.getKey() + " but received " + partitionSum.Count);
//            }

//            Iterator<ConsumeResult<byte[], byte[]>> inputRecords = partitionInput.iterator();

//            foreach (ConsumeResult<byte[], byte[]> receivedRecord in partitionSum)
//            {
//                ConsumeResult<byte[], byte[]> input = inputRecords.next();

//                string receivedKey = stringDeserializer.deserialize(receivedRecord.topic(), receivedRecord.Key);
//                long receivedValue = longDeserializer.deserialize(receivedRecord.topic(), receivedRecord.Value);
//                string key = stringDeserializer.deserialize(input.topic(), input.Key);
//                int value = integerDeserializer.deserialize(input.topic(), input.Value);

//                long sum = currentSumPerKey.get(key);
//                if (sum == null)
//                {
//                    sum = (long)value;
//                }
//                else
//                {
//                    sum += value;
//                }
//                currentSumPerKey.put(key, sum);

//                if (!receivedKey.equals(key) || receivedValue != sum)
//                {
//                    throw new RuntimeException("Result verification failed for " + receivedRecord + " expected <" + key + "," + sum + "> but was <" + receivedKey + "," + receivedValue + ">");
//                }
//            }
//        }
//    }

//    private static void verifyMax(Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>> inputPerTopicPerPartition,
//                                  Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>> maxPerTopicPerPartition)
//    {
//        StringDeserializer stringDeserializer = new StringDeserializer();
//        IntegerDeserializer integerDeserializer = Serializers.Int32;

//        HashDictionary<string, int> currentMinPerKey = new HashMap<>();
//        foreach (Map.Entry<TopicPartition, List<ConsumeResult<byte[], byte[]>>> partitionRecords in maxPerTopicPerPartition.entrySet())
//        {
//            TopicPartition inputTopicPartition = new TopicPartition("repartition", partitionRecords.getKey().partition());
//            List<ConsumeResult<byte[], byte[]>> partitionInput = inputPerTopicPerPartition.get(inputTopicPartition);
//            List<ConsumeResult<byte[], byte[]>> partitionMax = partitionRecords.getValue();

//            if (partitionInput.Count != partitionMax.Count)
//            {
//                throw new RuntimeException("Result verification failed: expected " + partitionInput.Count + " records for "
//                    + partitionRecords.getKey() + " but received " + partitionMax.Count);
//            }

//            Iterator<ConsumeResult<byte[], byte[]>> inputRecords = partitionInput.iterator();

//            foreach (ConsumeResult<byte[], byte[]> receivedRecord in partitionMax)
//            {
//                ConsumeResult<byte[], byte[]> input = inputRecords.next();

//                string receivedKey = stringDeserializer.deserialize(receivedRecord.topic(), receivedRecord.Key);
//                int receivedValue = integerDeserializer.deserialize(receivedRecord.topic(), receivedRecord.Value);
//                string key = stringDeserializer.deserialize(input.topic(), input.Key);
//                int value = integerDeserializer.deserialize(input.topic(), input.Value);


//                int max = currentMinPerKey.get(key);
//                if (max == null)
//                {
//                    max = int.MIN_VALUE;
//                }
//                max = Math.max(max, value);
//                currentMinPerKey.put(key, max);

//                if (!receivedKey.equals(key) || receivedValue != max)
//                {
//                    throw new RuntimeException("Result verification failed for " + receivedRecord + " expected <" + key + "," + max + "> but was <" + receivedKey + "," + receivedValue + ">");
//                }
//            }
//        }
//    }

//    private static void verifyCnt(Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>> inputPerTopicPerPartition,
//                                  Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>> cntPerTopicPerPartition)
//    {
//        StringDeserializer stringDeserializer = new StringDeserializer();
//        LongDeserializer longDeserializer = new LongDeserializer();

//        HashDictionary<string, long> currentSumPerKey = new HashMap<>();
//        foreach (Map.Entry<TopicPartition, List<ConsumeResult<byte[], byte[]>>> partitionRecords in cntPerTopicPerPartition.entrySet())
//        {
//            TopicPartition inputTopicPartition = new TopicPartition("repartition", partitionRecords.getKey().partition());
//            List<ConsumeResult<byte[], byte[]>> partitionInput = inputPerTopicPerPartition.get(inputTopicPartition);
//            List<ConsumeResult<byte[], byte[]>> partitionCnt = partitionRecords.getValue();

//            if (partitionInput.Count != partitionCnt.Count)
//            {
//                throw new RuntimeException("Result verification failed: expected " + partitionInput.Count + " records for "
//                    + partitionRecords.getKey() + " but received " + partitionCnt.Count);
//            }

//            Iterator<ConsumeResult<byte[], byte[]>> inputRecords = partitionInput.iterator();

//            foreach (ConsumeResult<byte[], byte[]> receivedRecord in partitionCnt)
//            {
//                ConsumeResult<byte[], byte[]> input = inputRecords.next();

//                string receivedKey = stringDeserializer.deserialize(receivedRecord.topic(), receivedRecord.Key);
//                long receivedValue = longDeserializer.deserialize(receivedRecord.topic(), receivedRecord.Value);
//                string key = stringDeserializer.deserialize(input.topic(), input.Key);

//                long cnt = currentSumPerKey.get(key);
//                if (cnt == null)
//                {
//                    cnt = 0L;
//                }
//                currentSumPerKey.put(key, ++cnt);

//                if (!receivedKey.equals(key) || receivedValue != cnt)
//                {
//                    throw new RuntimeException("Result verification failed for " + receivedRecord + " expected <" + key + "," + cnt + "> but was <" + receivedKey + "," + receivedValue + ">");
//                }
//            }
//        }
//    }

//    private static void verifyAllTransactionFinished(KafkaConsumer<byte[], byte[]> consumer,
//                                                     string kafka,
//                                                     bool withRepartitioning)
//    {
//        string[] topics;
//        if (withRepartitioning)
//        {
//            topics = new string[] { "echo", "min", "sum", "repartition", "max", "cnt" };
//        }
//        else
//        {
//            topics = new string[] { "echo", "min", "sum" };
//        }

//        List<TopicPartition> partitions = getAllPartitions(consumer, topics);
//        consumer.assign(partitions);
//        consumer.seekToEnd(partitions);
//        foreach (TopicPartition tp in partitions)
//        {
//            System.Console.Out.WriteLine(tp + " at position " + consumer.position(tp));
//        }

//        Properties producerProps = new Properties();
//        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "VerifyProducer");
//        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
//        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer);
//        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer);
//        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

//        try { 
// (KafkaProducer<string, string> producer = new KafkaProducer<>(producerProps));
//            foreach (TopicPartition tp in partitions) {
//                ProducerRecord<string, string> record = new ProducerRecord<>(tp.topic(), tp.partition(), "key", "value");

//producer.send(record, (metadata, exception) => {
//                    if (exception != null) {
//                        exception.printStackTrace(System.Console.Error);
//                        System.Console.Error.flush();
//                        Exit.exit(1);
//                    }
//                });
//            }
//        }

//        StringDeserializer stringDeserializer = new StringDeserializer();

//long maxWaitTime = System.currentTimeMillis() + MAX_IDLE_TIME_MS;
//        while (!partitions.isEmpty() && System.currentTimeMillis() < maxWaitTime) {
//            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
//            if (records.isEmpty()) {
//                System.Console.Out.WriteLine("No data received.");
//                foreach (TopicPartition tp in partitions) {
//                    System.Console.Out.WriteLine(tp + " at position " + consumer.position(tp));
//                }
//            }
//            foreach (ConsumeResult<byte[], byte[]> record in records) {
//                maxWaitTime = System.currentTimeMillis() + MAX_IDLE_TIME_MS;
//                string topic = record.topic();
//TopicPartition tp = new TopicPartition(topic, record.partition());

//                try {
//                    string key = stringDeserializer.deserialize(topic, record.Key);
//string value = stringDeserializer.deserialize(topic, record.Value);

//                    if (!("key".equals(key) && "value".equals(value) && partitions.remove(tp))) {
//                        throw new RuntimeException("Post transactions verification failed. Received unexpected verification record: " +
//                            "Expected record <'key','value'> from one of " + partitions + " but got"
//                            + " <" + key + "," + value + "> [" + record.topic() + ", " + record.partition() + "]");
//                    } else {
//                        System.Console.Out.WriteLine("Verifying " + tp + " successful.");
//                    }
//                } catch (SerializationException e) {
//                    throw new RuntimeException("Post transactions verification failed. Received unexpected verification record: " +
//                        "Expected record <'key','value'> from one of " + partitions + " but got " + record, e);
//                }

//            }
//        }
//        if (!partitions.isEmpty()) {
//            throw new RuntimeException("Could not read all verification records. Did not receive any new record within the last " + (MAX_IDLE_TIME_MS / 1000) + " sec.");
//        }
//    }

//    private static List<TopicPartition> getAllPartitions(KafkaConsumer<?, ?> consumer,
//                                                         string... topics)
//{
//    ArrayList<TopicPartition> partitions = new ArrayList<>();

//    foreach (string topic in topics)
//    {
//        foreach (PartitionInfo info in consumer.partitionsFor(topic))
//        {
//            partitions.add(new TopicPartition(info.topic(), info.partition()));
//        }
//    }
//    return partitions;
//}


//private static ConsumerGroupDescription getConsumerGroupDescription(Admin adminClient)
//{
//    ConsumerGroupDescription description;
//    try
//    {
//        description = adminClient.describeConsumerGroups(Collections.singleton(EosTestClient.APP_ID))
//            .describedGroups()
//            .get(EosTestClient.APP_ID)
//            .get(10, TimeUnit.SECONDS);
//    }
//    catch (InterruptedException | ExecutionException | java.util.concurrent.TimeoutException e) {
//        e.printStackTrace();
//        throw new RuntimeException("Unexpected Exception getting group description", e);
//    }
//    return description;
//    }
//}
