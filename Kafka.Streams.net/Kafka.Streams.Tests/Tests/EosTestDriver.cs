namespace Kafka.Streams.Tests.Tests
{
}
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

//        private static void updateNumRecordsProduces(int delta)
//        {
//            numRecordsProduced += delta;
//        }

//        static void generate(string kafka)
//        {

//            Runtime.getRuntime().addShutdownHook(new Thread(() =>
//            {
//                System.Console.Out.WriteLine("Terminating");
//                System.Console.Out.Flush();
//                isRunning = false;
//            }));

//            StreamsConfig producerProps = new StreamsConfig();
//            producerProps.Put(ProducerConfig.CLIENT_ID_CONFIG, "EosTest");
//            producerProps.Put(ProducerConfig.BootstrapServersConfig, kafka);
//            producerProps.Put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().Serializer);
//        producerProps.Put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.Int().Serializer);
//        producerProps.Put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

//        KafkaProducer<string, int> producer = new KafkaProducer<>(producerProps);

//        Random rand = new Random(System.currentTimeMillis());

//        while (isRunning) {
//            string key = "" + rand.nextInt(MAX_NUMBER_OF_KEYS);
//        int value = rand.nextInt(10000);

//        ProducerRecord<string, int> record = new ProducerRecord<>("data", key, value);

//        producer.send(record, (metadata, exception) => {
//                if (exception != null) {
//                    exception.printStackTrace(System.Console.Error);
//                    System.Console.Error.Flush();
//                    if (exception is TimeoutException) {
//                        try {
//                            // message == org.apache.kafka.common.errors.TimeoutException: Expiring 4 record(s) for data-0: 30004 ms has passed since last attempt plus backoff time
//                            int expired = int.parseInt(exception.getMessage().Split(" ")[2]);
//        updateNumRecordsProduces(-expired);
//    } catch (Exception ignore) { }
//                    }
//                }
//            });

//            updateNumRecordsProduces(1);
//            if (numRecordsProduced % 1000 == 0) {
//                System.Console.Out.WriteLine(numRecordsProduced + " records produced");
//                System.Console.Out.Flush();
//            }
//            Utils.Sleep(rand.nextInt(10));
//        }
//        producer.Close();
//        System.Console.Out.WriteLine("Producer closed: " + numRecordsProduced + " records produced");

//        StreamsConfig props = new StreamsConfig();
//props.Put(ConsumerConfig.CLIENT_ID_CONFIG, "verifier");
//        props.Put(ConsumerConfig.BootstrapServersConfig, kafka);
//        props.Put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer);
//        props.Put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer);
//        props.Put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.ToString().toLowerCase(Locale.ROOT));

//        try { 
// (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props));
//            List<TopicPartition> partitions = getAllPartitions(consumer, "data");
//System.Console.Out.WriteLine("Partitions: " + partitions);
//            consumer.Assign(partitions);
//            consumer.seekToEnd(partitions);

//            foreach (TopicPartition tp in partitions) {
//                System.Console.Out.WriteLine("End-offset for " + tp + " is " + consumer.position(tp));
//            }
//        }
//        System.Console.Out.Flush();
//    }

//    public static void verify(string kafka, bool withRepartitioning)
//{
//    StreamsConfig props = new StreamsConfig();
//    props.Put(ConsumerConfig.CLIENT_ID_CONFIG, "verifier");
//    props.Put(ConsumerConfig.BootstrapServersConfig, kafka);
//    props.Put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer);
//        props.Put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer);
//        props.Put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.ToString().toLowerCase(Locale.ROOT));

//        Dictionary<TopicPartition, long> committedOffsets;
//        try { 
// (Admin adminClient = Admin.Create(props));
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
//consumer.Assign(partitions);
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
//consumer.Assign(partitions);
//            consumer.seekToBeginning(partitions);

//            outputRecordsPerTopicPerPartition = getRecords(consumer, consumer.endOffsets(partitions), withRepartitioning, false);
//        } catch (Exception e) {
//            e.printStackTrace(System.Console.Error);
//            System.Console.Out.WriteLine("FAILED");
//            return;
//        }

//        verifyReceivedAllRecords(inputRecordsPerTopicPerPartition.Get("data"), outputRecordsPerTopicPerPartition.Get("echo"));
//        if (withRepartitioning) {
//            verifyReceivedAllRecords(inputRecordsPerTopicPerPartition.Get("data"), outputRecordsPerTopicPerPartition.Get("repartition"));
//        }

//        verifyMin(inputRecordsPerTopicPerPartition.Get("data"), outputRecordsPerTopicPerPartition.Get("min"));
//        verifySum(inputRecordsPerTopicPerPartition.Get("data"), outputRecordsPerTopicPerPartition.Get("sum"));

//        if (withRepartitioning) {
//            verifyMax(inputRecordsPerTopicPerPartition.Get("repartition"), outputRecordsPerTopicPerPartition.Get("max"));
//            verifyCnt(inputRecordsPerTopicPerPartition.Get("repartition"), outputRecordsPerTopicPerPartition.Get("cnt"));
//        }

//        try { 
// (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props));
//            List<TopicPartition> partitions = getAllPartitions(consumer, allOutputTopics);
//consumer.Assign(partitions);
//            consumer.seekToBeginning(partitions);

//            verifyAllTransactionFinished(consumer, kafka, withRepartitioning);
//        } catch (Exception e) {
//            e.printStackTrace(System.Console.Error);
//            System.Console.Out.WriteLine("FAILED");
//            return;
//        }

//        // do not modify: required test output
//        System.Console.Out.WriteLine("ALL-RECORDS-DELIVERED");
//        System.Console.Out.Flush();
//    }

//    private static void ensureStreamsApplicationDown(Admin adminClient)
//{

//    long maxWaitTime = System.currentTimeMillis() + MAX_IDLE_TIME_MS;
//    ConsumerGroupDescription description;
//    do
//    {
//        description = getConsumerGroupDescription(adminClient);

//        if (System.currentTimeMillis() > maxWaitTime && !description.members().IsEmpty())
//        {
//            throw new RuntimeException(
//                "Streams application not down after " + (MAX_IDLE_TIME_MS / 1000) + " seconds. " +
//                    "Group: " + description
//            );
//        }
//        sleep(1000);
//    } while (!description.members().IsEmpty());
//}


//private static Dictionary<TopicPartition, long> getCommittedOffsets(Admin adminClient,
//                                                             bool withRepartitioning)
//{
//    Dictionary<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap;

//    try
//    {
//        ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult = adminClient.listConsumerGroupOffsets(EosTestClient.APP_ID);
//        topicPartitionOffsetAndMetadataMap = listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata().Get(10, TimeUnit.SECONDS);
//    }
//    catch (InterruptedException | ExecutionException | java.util.concurrent.TimeoutException e) {
//        e.printStackTrace();
//        throw new RuntimeException(e);
//    }

//    Dictionary<TopicPartition, long> committedOffsets = new HashMap<>();

//    foreach (Map.Entry<TopicPartition, OffsetAndMetadata> entry in topicPartitionOffsetAndMetadataMap)
//    {
//        string topic = entry.Key.Topic;
//        if (topic.Equals("data") || withRepartitioning && topic.Equals("repartition"))
//        {
//            committedOffsets.Put(entry.Key, entry.Value.Offset);
//        }
//    }

//    return committedOffsets;
//    }

//    private static Dictionary<string, Map<TopicPartition, List<ConsumeResult<byte[], byte[]>>>> getRecords(KafkaConsumer<byte[], byte[]> consumer,
//                                                                                                     Dictionary<TopicPartition, long> readEndOffsets,
//                                                                                                     bool withRepartitioning,
//                                                                                                     bool isInputTopic)
//    {
//        System.Console.Error.WriteLine("read end offset: " + readEndOffsets);
//        Dictionary<string, Map<TopicPartition, List<ConsumeResult<byte[], byte[]>>>> recordPerTopicPerPartition = new HashMap<>();
//        Dictionary<TopicPartition, long> maxReceivedOffsetPerPartition = new HashMap<>();
//        Dictionary<TopicPartition, long> maxConsumerPositionPerPartition = new HashMap<>();

//        long maxWaitTime = System.currentTimeMillis() + MAX_IDLE_TIME_MS;
//        bool allRecordsReceived = false;
//        while (!allRecordsReceived && System.currentTimeMillis() < maxWaitTime)
//        {
//            ConsumeResult<byte[], byte[]> receivedRecords = consumer.poll(TimeSpan.FromMilliseconds(100));

//            foreach (ConsumeResult<byte[], byte[]> record in receivedRecords)
//            {
//                maxWaitTime = System.currentTimeMillis() + MAX_IDLE_TIME_MS;
//                TopicPartition tp = new TopicPartition(record.Topic, record.Partition);
//                maxReceivedOffsetPerPartition.Put(tp, record.Offset);
//                long readEndOffset = readEndOffsets.Get(tp);
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
//                maxConsumerPositionPerPartition.Put(tp, consumer.position(tp));
//                if (consumer.position(tp) >= readEndOffsets.Get(tp))
//                {
//                    consumer.pause(Collections.singletonList(tp));
//                }
//            }

//            allRecordsReceived = consumer.paused().Count == readEndOffsets.keySet().Count;
//        }

//        if (!allRecordsReceived)
//        {
//            System.Console.Error.WriteLine("Pause partitions (ie, received All data): " + consumer.paused());
//            System.Console.Error.WriteLine("Max received offset per partition: " + maxReceivedOffsetPerPartition);
//            System.Console.Error.WriteLine("Max consumer position per partition: " + maxConsumerPositionPerPartition);
//            throw new RuntimeException("FAIL: did not receive All records after " + (MAX_IDLE_TIME_MS / 1000) + " sec idle time.");
//        }

//        return recordPerTopicPerPartition;
//    }

//    private static void addRecord(ConsumeResult<byte[], byte[]> record,
//                                  Dictionary<string, Map<TopicPartition, List<ConsumeResult<byte[], byte[]>>>> recordPerTopicPerPartition,
//                                  bool withRepartitioning)
//    {

//        string topic = record.Topic;
//        TopicPartition partition = new TopicPartition(topic, record.Partition);

//        if (verifyTopic(topic, withRepartitioning))
//        {
//            Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>> topicRecordsPerPartition =
//                recordPerTopicPerPartition.computeIfAbsent(topic, k => new HashMap<>());

//            List<ConsumeResult<byte[], byte[]>> records =
//                topicRecordsPerPartition.computeIfAbsent(partition, k => new List<>());

//            records.Add(record);
//        }
//        else
//        {
//            throw new RuntimeException("FAIL: received data from unexpected topic: " + record);
//        }
//    }

//    private static bool verifyTopic(string topic,
//                                       bool withRepartitioning)
//    {
//        bool validTopic = "data".Equals(topic) || "echo".Equals(topic) || "min".Equals(topic) || "sum".Equals(topic);

//        if (withRepartitioning)
//        {
//            return validTopic || "repartition".Equals(topic) || "max".Equals(topic) || "cnt".Equals(topic);
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

//        Serdes.String().Deserializer stringDeserializer = new Serdes.String().Deserializer();
//        Serdes.Int().Deserializer integerDeserializer = Serializers.Int32;
//        foreach (Map.Entry<TopicPartition, List<ConsumeResult<byte[], byte[]>>> partitionRecords in receivedRecords)
//        {
//            TopicPartition inputTopicPartition = new TopicPartition("data", partitionRecords.Key.Partition);
//            Iterator<ConsumeResult<byte[], byte[]>> expectedRecord = expectedRecords.Get(inputTopicPartition).iterator();

//            foreach (ConsumeResult<byte[], byte[]> receivedRecord in partitionRecords.Value)
//            {
//                ConsumeResult<byte[], byte[]> expected = expectedRecord.MoveNext();

//                string receivedKey = stringDeserializer.Deserialize(receivedRecord.Topic, receivedRecord.Key);
//                int receivedValue = integerDeserializer.Deserialize(receivedRecord.Topic, receivedRecord.Value);
//                string expectedKey = stringDeserializer.Deserialize(expected.Topic, expected.Key);
//                int expectedValue = integerDeserializer.Deserialize(expected.Topic, expected.Value);

//                if (!receivedKey.Equals(expectedKey) || receivedValue != expectedValue)
//                {
//                    throw new RuntimeException("Result verification failed for " + receivedRecord + " expected <" + expectedKey + "," + expectedValue + "> but was <" + receivedKey + "," + receivedValue + ">");
//                }
//            }
//        }
//    }

//    private static void verifyMin(Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>> inputPerTopicPerPartition,
//                                  Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>> minPerTopicPerPartition)
//    {
//        Serdes.String().Deserializer stringDeserializer = new Serdes.String().Deserializer();
//        Serdes.Int().Deserializer integerDeserializer = Serializers.Int32;

//        HashDictionary<string, int> currentMinPerKey = new HashMap<>();
//        foreach (Map.Entry<TopicPartition, List<ConsumeResult<byte[], byte[]>>> partitionRecords in minPerTopicPerPartition)
//        {
//            TopicPartition inputTopicPartition = new TopicPartition("data", partitionRecords.Key.Partition);
//            List<ConsumeResult<byte[], byte[]>> partitionInput = inputPerTopicPerPartition.Get(inputTopicPartition);
//            List<ConsumeResult<byte[], byte[]>> partitionMin = partitionRecords.Value;

//            if (partitionInput.Count != partitionMin.Count)
//            {
//                throw new RuntimeException("Result verification failed: expected " + partitionInput.Count + " records for "
//                    + partitionRecords.Key + " but received " + partitionMin.Count);
//            }

//            Iterator<ConsumeResult<byte[], byte[]>> inputRecords = partitionInput.iterator();

//            foreach (ConsumeResult<byte[], byte[]> receivedRecord in partitionMin)
//            {
//                ConsumeResult<byte[], byte[]> input = inputRecords.MoveNext();

//                string receivedKey = stringDeserializer.Deserialize(receivedRecord.Topic, receivedRecord.Key);
//                int receivedValue = integerDeserializer.Deserialize(receivedRecord.Topic, receivedRecord.Value);
//                string key = stringDeserializer.Deserialize(input.Topic, input.Key);
//                int value = integerDeserializer.Deserialize(input.Topic, input.Value);

//                int min = currentMinPerKey.Get(key);
//                if (min == null)
//                {
//                    min = value;
//                }
//                else
//                {
//                    min = Math.min(min, value);
//                }
//                currentMinPerKey.Put(key, min);

//                if (!receivedKey.Equals(key) || receivedValue != min)
//                {
//                    throw new RuntimeException("Result verification failed for " + receivedRecord + " expected <" + key + "," + min + "> but was <" + receivedKey + "," + receivedValue + ">");
//                }
//            }
//        }
//    }

//    private static void verifySum(Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>> inputPerTopicPerPartition,
//                                  Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>> minPerTopicPerPartition)
//    {
//        Serdes.String().Deserializer stringDeserializer = new Serdes.String().Deserializer();
//        Serdes.Int().Deserializer integerDeserializer = Serializers.Int32;
//        LongDeserializer longDeserializer = new LongDeserializer();

//        HashDictionary<string, long> currentSumPerKey = new HashMap<>();
//        foreach (Map.Entry<TopicPartition, List<ConsumeResult<byte[], byte[]>>> partitionRecords in minPerTopicPerPartition)
//        {
//            TopicPartition inputTopicPartition = new TopicPartition("data", partitionRecords.Key.Partition);
//            List<ConsumeResult<byte[], byte[]>> partitionInput = inputPerTopicPerPartition.Get(inputTopicPartition);
//            List<ConsumeResult<byte[], byte[]>> partitionSum = partitionRecords.Value;

//            if (partitionInput.Count != partitionSum.Count)
//            {
//                throw new RuntimeException("Result verification failed: expected " + partitionInput.Count + " records for "
//                    + partitionRecords.Key + " but received " + partitionSum.Count);
//            }

//            Iterator<ConsumeResult<byte[], byte[]>> inputRecords = partitionInput.iterator();

//            foreach (ConsumeResult<byte[], byte[]> receivedRecord in partitionSum)
//            {
//                ConsumeResult<byte[], byte[]> input = inputRecords.MoveNext();

//                string receivedKey = stringDeserializer.Deserialize(receivedRecord.Topic, receivedRecord.Key);
//                long receivedValue = longDeserializer.Deserialize(receivedRecord.Topic, receivedRecord.Value);
//                string key = stringDeserializer.Deserialize(input.Topic, input.Key);
//                int value = integerDeserializer.Deserialize(input.Topic, input.Value);

//                long sum = currentSumPerKey.Get(key);
//                if (sum == null)
//                {
//                    sum = (long)value;
//                }
//                else
//                {
//                    sum += value;
//                }
//                currentSumPerKey.Put(key, sum);

//                if (!receivedKey.Equals(key) || receivedValue != sum)
//                {
//                    throw new RuntimeException("Result verification failed for " + receivedRecord + " expected <" + key + "," + sum + "> but was <" + receivedKey + "," + receivedValue + ">");
//                }
//            }
//        }
//    }

//    private static void verifyMax(Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>> inputPerTopicPerPartition,
//                                  Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>> maxPerTopicPerPartition)
//    {
//        Serdes.String().Deserializer stringDeserializer = new Serdes.String().Deserializer();
//        Serdes.Int().Deserializer integerDeserializer = Serializers.Int32;

//        HashDictionary<string, int> currentMinPerKey = new HashMap<>();
//        foreach (Map.Entry<TopicPartition, List<ConsumeResult<byte[], byte[]>>> partitionRecords in maxPerTopicPerPartition)
//        {
//            TopicPartition inputTopicPartition = new TopicPartition("repartition", partitionRecords.Key.Partition);
//            List<ConsumeResult<byte[], byte[]>> partitionInput = inputPerTopicPerPartition.Get(inputTopicPartition);
//            List<ConsumeResult<byte[], byte[]>> partitionMax = partitionRecords.Value;

//            if (partitionInput.Count != partitionMax.Count)
//            {
//                throw new RuntimeException("Result verification failed: expected " + partitionInput.Count + " records for "
//                    + partitionRecords.Key + " but received " + partitionMax.Count);
//            }

//            Iterator<ConsumeResult<byte[], byte[]>> inputRecords = partitionInput.iterator();

//            foreach (ConsumeResult<byte[], byte[]> receivedRecord in partitionMax)
//            {
//                ConsumeResult<byte[], byte[]> input = inputRecords.MoveNext();

//                string receivedKey = stringDeserializer.Deserialize(receivedRecord.Topic, receivedRecord.Key);
//                int receivedValue = integerDeserializer.Deserialize(receivedRecord.Topic, receivedRecord.Value);
//                string key = stringDeserializer.Deserialize(input.Topic, input.Key);
//                int value = integerDeserializer.Deserialize(input.Topic, input.Value);


//                int max = currentMinPerKey.Get(key);
//                if (max == null)
//                {
//                    max = int.MIN_VALUE;
//                }
//                max = Math.Max(max, value);
//                currentMinPerKey.Put(key, max);

//                if (!receivedKey.Equals(key) || receivedValue != max)
//                {
//                    throw new RuntimeException("Result verification failed for " + receivedRecord + " expected <" + key + "," + max + "> but was <" + receivedKey + "," + receivedValue + ">");
//                }
//            }
//        }
//    }

//    private static void verifyCnt(Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>> inputPerTopicPerPartition,
//                                  Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>> cntPerTopicPerPartition)
//    {
//        Serdes.String().Deserializer stringDeserializer = new Serdes.String().Deserializer();
//        LongDeserializer longDeserializer = new LongDeserializer();

//        HashDictionary<string, long> currentSumPerKey = new HashMap<>();
//        foreach (Map.Entry<TopicPartition, List<ConsumeResult<byte[], byte[]>>> partitionRecords in cntPerTopicPerPartition)
//        {
//            TopicPartition inputTopicPartition = new TopicPartition("repartition", partitionRecords.Key.Partition);
//            List<ConsumeResult<byte[], byte[]>> partitionInput = inputPerTopicPerPartition.Get(inputTopicPartition);
//            List<ConsumeResult<byte[], byte[]>> partitionCnt = partitionRecords.Value;

//            if (partitionInput.Count != partitionCnt.Count)
//            {
//                throw new RuntimeException("Result verification failed: expected " + partitionInput.Count + " records for "
//                    + partitionRecords.Key + " but received " + partitionCnt.Count);
//            }

//            Iterator<ConsumeResult<byte[], byte[]>> inputRecords = partitionInput.iterator();

//            foreach (ConsumeResult<byte[], byte[]> receivedRecord in partitionCnt)
//            {
//                ConsumeResult<byte[], byte[]> input = inputRecords.MoveNext();

//                string receivedKey = stringDeserializer.Deserialize(receivedRecord.Topic, receivedRecord.Key);
//                long receivedValue = longDeserializer.Deserialize(receivedRecord.Topic, receivedRecord.Value);
//                string key = stringDeserializer.Deserialize(input.Topic, input.Key);

//                long cnt = currentSumPerKey.Get(key);
//                if (cnt == null)
//                {
//                    cnt = 0L;
//                }
//                currentSumPerKey.Put(key, ++cnt);

//                if (!receivedKey.Equals(key) || receivedValue != cnt)
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
//        consumer.Assign(partitions);
//        consumer.seekToEnd(partitions);
//        foreach (TopicPartition tp in partitions)
//        {
//            System.Console.Out.WriteLine(tp + " at position " + consumer.position(tp));
//        }

//        StreamsConfig producerProps = new StreamsConfig();
//        producerProps.Put(ProducerConfig.CLIENT_ID_CONFIG, "VerifyProducer");
//        producerProps.Put(ProducerConfig.BootstrapServersConfig, kafka);
//        producerProps.Put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().Serializer);
//        producerProps.Put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().Serializer);
//        producerProps.Put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

//        try { 
// (KafkaProducer<string, string> producer = new KafkaProducer<>(producerProps));
//            foreach (TopicPartition tp in partitions) {
//                ProducerRecord<string, string> record = new ProducerRecord<>(tp.Topic, tp.Partition, "key", "value");

//producer.send(record, (metadata, exception) => {
//                    if (exception != null) {
//                        exception.printStackTrace(System.Console.Error);
//                        System.Console.Error.Flush();
//                        Exit.exit(1);
//                    }
//                });
//            }
//        }

//        Serdes.String().Deserializer stringDeserializer = new Serdes.String().Deserializer();

//long maxWaitTime = System.currentTimeMillis() + MAX_IDLE_TIME_MS;
//        while (!partitions.IsEmpty() && System.currentTimeMillis() < maxWaitTime) {
//            ConsumeResult<byte[], byte[]> records = consumer.poll(TimeSpan.FromMilliseconds(100));
//            if (records.IsEmpty()) {
//                System.Console.Out.WriteLine("No data received.");
//                foreach (TopicPartition tp in partitions) {
//                    System.Console.Out.WriteLine(tp + " at position " + consumer.position(tp));
//                }
//            }
//            foreach (ConsumeResult<byte[], byte[]> record in records) {
//                maxWaitTime = System.currentTimeMillis() + MAX_IDLE_TIME_MS;
//                string topic = record.Topic;
//TopicPartition tp = new TopicPartition(topic, record.Partition);

//                try {
//                    string key = stringDeserializer.Deserialize(topic, record.Key);
//string value = stringDeserializer.Deserialize(topic, record.Value);

//                    if (!("key".Equals(key) && "value".Equals(value) && partitions.remove(tp))) {
//                        throw new RuntimeException("Post transactions verification failed. Received unexpected verification record: " +
//                            "Expected record <'key','value'> from one of " + partitions + " but got"
//                            + " <" + key + "," + value + "> [" + record.Topic + ", " + record.Partition + "]");
//                    } else {
//                        System.Console.Out.WriteLine("Verifying " + tp + " successful.");
//                    }
//                } catch (SerializationException e) {
//                    throw new RuntimeException("Post transactions verification failed. Received unexpected verification record: " +
//                        "Expected record <'key','value'> from one of " + partitions + " but got " + record, e);
//                }

//            }
//        }
//        if (!partitions.IsEmpty()) {
//            throw new RuntimeException("Could not read All verification records. Did not receive any new record within the last " + (MAX_IDLE_TIME_MS / 1000) + " sec.");
//        }
//    }

//    private static List<TopicPartition> getAllPartitions(KafkaConsumer<?, ?> consumer,
//                                                         string... topics)
//{
//    ArrayList<TopicPartition> partitions = new List<TopicPartition>();

//    foreach (string topic in topics)
//    {
//        foreach (PartitionInfo info in consumer.partitionsFor(topic))
//        {
//            partitions.Add(new TopicPartition(info.Topic, info.Partition));
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
//            .Get(EosTestClient.APP_ID)
//            .Get(10, TimeUnit.SECONDS);
//    }
//    catch (InterruptedException | ExecutionException | java.util.concurrent.TimeoutException e) {
//        e.printStackTrace();
//        throw new RuntimeException("Unexpected Exception getting group description", e);
//    }
//    return description;
//    }
//}
