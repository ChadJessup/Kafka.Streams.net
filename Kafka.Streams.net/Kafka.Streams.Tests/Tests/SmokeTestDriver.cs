namespace Kafka.Streams.Tests.Tests
{
}
//using Confluent.Kafka;
//using Xunit;

//namespace Kafka.Streams.Tests.Tools
//{
//    public class SmokeTestDriver : SmokeTestUtil
//    {
//        private static string[] TOPICS = {
//        "data",
//        "echo",
//        "max",
//        "min", "min-suppressed", "min-raw",
//        "dif",
//        "sum",
//        "sws-raw", "sws-suppressed",
//        "cnt",
//        "avg",
//        "tagg"
//    };

//        private static int MAX_RECORD_EMPTY_RETRIES = 30;

//        private static class ValueList
//        {
//            public string key;
//            private int[] values;
//            private int index;

//            ValueList(int min, int max)
//            {
//                key = min + "-" + max;

//                values = new int[max - min + 1];
//                for (int i = 0; i < values.Length; i++)
//                {
//                    values[i] = min + i;
//                }
//                // We want to randomize the order of data to test not completely predictable processing order
//                // However, values are also use as a timestamp of the record. (TODO: separate data and timestamp)
//                // We keep some correlation of time and order. Thus, the shuffling is done with a sliding window
//                shuffle(values, 10);

//                index = 0;
//            }

//            int next()
//            {
//                return (index < values.Length) ? values[index++] : -1;
//            }
//        }

//        public static string[] topics()
//        {
//            return Array.copyOf(TOPICS, TOPICS.Length);
//        }

//        static void generatePerpetually(string kafka,
//                                        int numKeys,
//                                        int maxRecordsPerKey)
//        {
//            StreamsConfig producerProps = generatorProperties(kafka);

//            int numRecordsProduced = 0;

//            ValueList[] data = new ValueList[numKeys];
//            for (int i = 0; i < numKeys; i++)
//            {
//                data[i] = new ValueList(i, i + maxRecordsPerKey - 1);
//            }

//            Random rand = new Random();

//            try
//            {
//                (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps));
//                while (true)
//                {
//                    int index = rand.nextInt(numKeys);
//                    string key = data[index].Key;
//                    int value = data[index].MoveNext();

//                    ProducerRecord<byte[], byte[]> record =
//                        new ProducerRecord<>(
//                            "data",
//                            stringSerde.Serializer.Serialize("", key),
//                            intSerde.Serializer.Serialize("", value)
//                        );

//                    producer.send(record);

//                    numRecordsProduced++;
//                    if (numRecordsProduced % 100 == 0)
//                    {
//                        System.Console.Out.WriteLine(Instant.now() + " " + numRecordsProduced + " records produced");
//                    }
//                    Utils.Sleep(2);
//                }
//            }
//    }

//        public static Dictionary<string, HashSet<int>> generate(string kafka,
//                                                         int numKeys,
//                                                         int maxRecordsPerKey,
//                                                         TimeSpan timeToSpend)
//        {
//            StreamsConfig producerProps = generatorProperties(kafka);


//            int numRecordsProduced = 0;

//            Dictionary<string, HashSet<int>> allData = new HashMap<>();
//            ValueList[] data = new ValueList[numKeys];
//            for (int i = 0; i < numKeys; i++)
//            {
//                data[i] = new ValueList(i, i + maxRecordsPerKey - 1);
//                allData.Put(data[i].Key, new HashSet<>());
//            }
//            Random rand = new Random();

//            int remaining = data.Length;

//            long recordPauseTime = timeToSpend.TotalMilliseconds / numKeys / maxRecordsPerKey;

//            List<ProducerRecord<byte[], byte[]>> needRetry = new List<ProducerRecord<byte[], byte[]>>();

//            try
//            {
//                (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps));
//                while (remaining > 0)
//                {
//                    int index = rand.nextInt(remaining);
//                    string key = data[index].Key;
//                    int value = data[index].MoveNext();

//                    if (value < 0)
//                    {
//                        remaining--;
//                        data[index] = data[remaining];
//                    }
//                    else
//                    {

//                        ProducerRecord<byte[], byte[]> record =
//                            new ProducerRecord<>(
//                                "data",
//                                stringSerde.Serializer.Serialize("", key),
//                                intSerde.Serializer.Serialize("", value)
//                            );

//                        producer.send(record, new TestCallback(record, needRetry));

//                        numRecordsProduced++;
//                        allData.Get(key).Add(value);
//                        if (numRecordsProduced % 100 == 0)
//                        {
//                            System.Console.Out.WriteLine(Instant.now() + " " + numRecordsProduced + " records produced");
//                        }
//                        Utils.Sleep(Math.Max(recordPauseTime, 2));
//                    }
//                }
//                producer.Flush();

//                int remainingRetries = 5;
//                while (!needRetry.IsEmpty())
//                {
//                    List<ProducerRecord<byte[], byte[]>> needRetry2 = new List<ProducerRecord<byte[], byte[]>>();
//                    foreach (ProducerRecord<byte[], byte[]> record in needRetry)
//                    {
//                        System.Console.Out.WriteLine("retry producing " + stringSerde.deserializer().Deserialize("", record.Key));
//                        producer.send(record, new TestCallback(record, needRetry2));
//                    }
//                    producer.Flush();
//                    needRetry = needRetry2;

//                    if (--remainingRetries == 0 && !needRetry.IsEmpty())
//                    {
//                        System.Console.Error.WriteLine("Failed to produce All records after multiple retries");
//                        Exit.exit(1);
//                    }
//                }

//                // now that we've sent everything, we'll send some records with a timestamp high enough to Flush out
//                // All suppressed records.
//                List<PartitionInfo> partitions = producer.partitionsFor("data");
//                foreach (PartitionInfo partition in partitions)
//                {
//                    producer.send(new ProducerRecord<>(
//                        partition.Topic,
//                        partition.Partition,
//                        System.currentTimeMillis() + TimeSpan.FromDays(2).TotalMilliseconds,
//                        stringSerde.Serializer.Serialize("", "Flush"),
//                        intSerde.Serializer.Serialize("", 0)
//                    ));
//                }
//            }
//        return Collections.unmodifiableMap(allData);
//        }

//        private static StreamsConfig generatorProperties(string kafka)
//        {
//            StreamsConfig producerProps = new StreamsConfig();
//            producerProps.Put(ProducerConfig.CLIENT_ID_CONFIG, "SmokeTest");
//            producerProps.Put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
//            producerProps.Put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer);
//        producerProps.Put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer);
//        producerProps.Put(ProducerConfig.ACKS_CONFIG, "All");
//        return producerProps;
//    }

//    private static class TestCallback : Callback
//    {
//        private ProducerRecord<byte[], byte[]> originalRecord;
//    private List<ProducerRecord<byte[], byte[]>> needRetry;

//    TestCallback(ProducerRecord<byte[], byte[]> originalRecord,
//                 List<ProducerRecord<byte[], byte[]>> needRetry)
//    {
//        this.originalRecord = originalRecord;
//        this.needRetry = needRetry;
//    }


//    public void onCompletion(RecordMetadata metadata, Exception exception)
//    {
//        if (exception != null)
//        {
//            if (exception is TimeoutException)
//            {
//                needRetry.Add(originalRecord);
//            }
//            else
//            {
//                exception.printStackTrace();
//                Exit.exit(1);
//            }
//        }
//    }
//}

//private static void shuffle(int[] data,  {
//    Random rand = new Random();
//    for (int i = 0; i < data.Length; i++)
//    {
//        // we shuffle data within windowSize
//        int j = rand.nextInt(Math.min(data.Length - i, windowSize)) + i;

//        // swap
//        int tmp = data[i];
//        data[i] = data[j];
//        data[j] = tmp;
//    }
//}

//public static class NumberDeserializer : Deserializer<Number> {


//        public Number deserialize(string topic, byte[] data)
//{
//    Number value;
//    switch (topic)
//    {
//        case "data":
//        case "echo":
//        case "min":
//        case "min-raw":
//        case "min-suppressed":
//        case "sws-raw":
//        case "sws-suppressed":
//        case "max":
//        case "dif":
//            value = intSerde.deserializer().Deserialize(topic, data);
//            break;
//        case "sum":
//        case "cnt":
//        case "tagg":
//            value = longSerde.deserializer().Deserialize(topic, data);
//            break;
//        case "avg":
//            value = doubleSerde.deserializer().Deserialize(topic, data);
//            break;
//        default:
//            throw new RuntimeException("unknown topic: " + topic);
//    }
//    return value;
//}
//    }

//    public static VerificationResult verify(string kafka,
//                                            Dictionary<string, HashSet<int>> inputs,
//                                            int maxRecordsPerKey)
//{
//    StreamsConfig props = new StreamsConfig();
//    props.Put(ConsumerConfig.CLIENT_ID_CONFIG, "verifier");
//    props.Put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
//    props.Put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().Deserializer);
//        props.Put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, NumberDeserializer);
//        props.Put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

//        KafkaConsumer<string, Number> consumer = new KafkaConsumer<>(props);
//List<TopicPartition> partitions = getAllPartitions(consumer, TOPICS);
//consumer.Assign(partitions);
//        consumer.seekToBeginning(partitions);

//        int recordsGenerated = inputs.Count * maxRecordsPerKey;
//int recordsProcessed = 0;
//Dictionary<string, int> processed =
//    Stream.of(TOPICS)
//          .collect(Collectors.toMap(t => t, t => new int(0)));

//Dictionary<string, Map<string, LinkedList<ConsumeResult<string, Number>>>> events = new HashMap<>();

//VerificationResult verificationResult = new VerificationResult(false, "no results yet");
//int retry = 0;
//long start = System.currentTimeMillis();
//        while (System.currentTimeMillis() - start<TimeUnit.MINUTES.toMillis(6)) {
//            ConsumeResult<string, Number> records = consumer.poll(TimeSpan.FromSeconds(1));
//            if (records.IsEmpty() && recordsProcessed >= recordsGenerated) {
//                verificationResult = verifyAll(inputs, events);
//                if (verificationResult.passed()) {
//                    break;
//                } else if (retry++ > MAX_RECORD_EMPTY_RETRIES) {
//                    System.Console.Out.WriteLine(Instant.now() + " Didn't get any more results, verification hasn't passed, and out of retries.");
//                    break;
//                } else {
//                    System.Console.Out.WriteLine(Instant.now() + " Didn't get any more results, but verification hasn't passed (yet). Retrying...");
//                }
//            } else {
//                retry = 0;
//                foreach (ConsumeResult<string, Number> record in records) {
//                    string key = record.Key;

//string topic = record.Topic;
//processed.Get(topic).incrementAndGet();

//                    if (topic.Equals("echo")) {
//                        recordsProcessed++;
//                        if (recordsProcessed % 100 == 0) {
//                            System.Console.Out.WriteLine("Echo records processed = " + recordsProcessed);
//                        }
//                    }

//                    events.computeIfAbsent(topic, t => new HashMap<>())
//                          .computeIfAbsent(key, k => new LinkedList<>())
//                          .Add(record);
//                }

//                System.Console.Out.WriteLine(processed);
//            }
//        }
//        consumer.Close();
//        long finished = System.currentTimeMillis() - start;
//System.Console.Out.WriteLine("Verification time=" + finished);
//        System.Console.Out.WriteLine("-------------------");
//        System.Console.Out.WriteLine("Result Verification");
//        System.Console.Out.WriteLine("-------------------");
//        System.Console.Out.WriteLine("recordGenerated=" + recordsGenerated);
//        System.Console.Out.WriteLine("recordProcessed=" + recordsProcessed);

//        if (recordsProcessed > recordsGenerated) {
//            System.Console.Out.WriteLine("PROCESSED-MORE-THAN-GENERATED");
//        } else if (recordsProcessed<recordsGenerated) {
//            System.Console.Out.WriteLine("PROCESSED-LESS-THAN-GENERATED");
//        }

//        bool success;

//Dictionary<string, HashSet<Number>> received =
//    events.Get("echo")
//          
//          .Stream()
//          .map(entry => mkEntry(
//              entry.Key,
//              entry.Value.Stream().map(ConsumeResult::value).collect(Collectors.toSet()))
//          )
//          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

//success = inputs.Equals(received);

//        if (success) {
//            System.Console.Out.WriteLine("ALL-RECORDS-DELIVERED");
//        } else {
//            int missedCount = 0;
//            foreach (Map.Entry<string, HashSet<int>> entry in inputs) {
//                missedCount += received.Get(entry.Key).Count;
//            }
//            System.Console.Out.WriteLine("missedRecords=" + missedCount);
//        }

//        // give it one more try if it's not already passing.
//        if (!verificationResult.passed()) {
//            verificationResult = verifyAll(inputs, events);
//        }
//        success &= verificationResult.passed();

//        System.Console.Out.WriteLine(verificationResult.result());

//        System.Console.Out.WriteLine(success? "SUCCESS" : "FAILURE");
//        return verificationResult;
//    }

//    public static class VerificationResult
//{
//    private bool passed;
//    private string result;

//    VerificationResult(bool passed, string result)
//    {
//        this.passed = passed;
//        this.result = result;
//    }

//    public bool passed()
//    {
//        return passed;
//    }

//    public string result()
//    {
//        return result;
//    }
//}

//private static VerificationResult verifyAll(Dictionary<string, HashSet<int>> inputs,
//                                            Dictionary<string, Map<string, LinkedList<ConsumeResult<string, Number>>>> events)
//{
//    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//    bool pass;
//    try
//    {
//        (PrintStream resultStream = new PrintStream(byteArrayOutputStream));
//        pass = verifyTAgg(resultStream, inputs, events.Get("tagg"));
//        pass &= verifySuppressed(resultStream, "min-suppressed", events);
//        pass &= verify(resultStream, "min-suppressed", inputs, events, windowedKey =>
//        {
//            string unwindowedKey = windowedKey.substring(1, windowedKey.Length() - 1).replaceAll("@.*", "");
//            return getMin(unwindowedKey);
//        });
//        pass &= verifySuppressed(resultStream, "sws-suppressed", events);
//        pass &= verify(resultStream, "min", inputs, events, SmokeTestDriver::getMin);
//        pass &= verify(resultStream, "max", inputs, events, SmokeTestDriver::getMax);
//        pass &= verify(resultStream, "dif", inputs, events, key => getMax(key).intValue() - getMin(key).intValue());
//        pass &= verify(resultStream, "sum", inputs, events, SmokeTestDriver::getSum);
//        pass &= verify(resultStream, "cnt", inputs, events, key1 => getMax(key1).intValue() - getMin(key1).intValue() + 1L);
//        pass &= verify(resultStream, "avg", inputs, events, SmokeTestDriver::getAvg);
//    }
//        return new VerificationResult(pass, new string(byteArrayOutputStream.toByteArray(), StandardCharsets.UTF_8));
//}

//private static bool verify(PrintStream resultStream,
//                              string topic,
//                              Dictionary<string, HashSet<int>> inputData,
//                              Dictionary<string, Map<string, LinkedList<ConsumeResult<string, Number>>>> events,
//                              Function<string, Number> keyToExpectation)
//{
//    Dictionary<string, LinkedList<ConsumeResult<string, Number>>> observedInputEvents = events.Get("data");
//    Dictionary<string, LinkedList<ConsumeResult<string, Number>>> outputEvents = events.getOrDefault(topic, emptyMap());
//    if (outputEvents.IsEmpty())
//    {
//        resultStream.WriteLine(topic + " is empty");
//        return false;
//    }
//    else
//    {
//        resultStream.printf("verifying %s with %d keys%n", topic, outputEvents.Count);

//        if (outputEvents.Count != inputData.Count)
//        {
//            resultStream.printf("fail: resultCount=%d expectedCount=%s%n\tresult=%s%n\texpected=%s%n",
//                                outputEvents.Count, inputData.Count, outputEvents.keySet(), inputData.keySet());
//            return false;
//        }
//        foreach (Map.Entry<string, LinkedList<ConsumeResult<string, Number>>> entry in outputEvents)
//        {
//            string key = entry.Key;
//            Number expected = keyToExpectation.apply(key);
//            Number actual = entry.Value.getLast().Value;
//            if (!expected.Equals(actual))
//            {
//                resultStream.printf("%s fail: key=%s actual=%s expected=%s%n\t inputEvents=%n%s%n\toutputEvents=%n%s%n",
//                                    topic,
//                                    key,
//                                    actual,
//                                    expected,
//                                    indent("\t\t", observedInputEvents.Get(key)),
//                                    indent("\t\t", entry.Value));
//                return false;
//            }
//        }
//        return true;
//    }
//}


//private static bool verifySuppressed(PrintStream resultStream,
//                                         string topic,
//                                        Dictionary<string, Map<string, LinkedList<ConsumeResult<string, Number>>>> events)
//{
//    resultStream.WriteLine("verifying suppressed " + topic);
//    Dictionary<string, LinkedList<ConsumeResult<string, Number>>> topicEvents = events.getOrDefault(topic, emptyMap());
//    foreach (Map.Entry<string, LinkedList<ConsumeResult<string, Number>>> entry in topicEvents)
//    {
//        if (entry.Value.Count != 1)
//        {
//            string unsuppressedTopic = topic.replace("-suppressed", "-raw");
//            string key = entry.Key;
//            string unwindowedKey = key.substring(1, key.Length() - 1).replaceAll("@.*", "");
//            resultStream.printf("fail: key=%s%n\tnon-unique result:%n%s%n\traw results:%n%s%n\tinput data:%n%s%n",
//                                key,
//                                indent("\t\t", entry.Value),
//                                indent("\t\t", events.Get(unsuppressedTopic).Get(key)),
//                                indent("\t\t", events.Get("data").Get(unwindowedKey))
//            );
//            return false;
//        }
//    }
//    return true;
//}

//private static string indent(string prefix,
//                             Iterable<ConsumeResult<string, Number>> list)
//{
//    StringBuilder stringBuilder = new StringBuilder();
//    foreach (ConsumeResult<string, Number> record in list)
//    {
//        stringBuilder.append(prefix).append(record).append('\n');
//    }
//    return stringBuilder.ToString();
//}

//private static long getSum(string key)
//{
//    int min = getMin(key).intValue();
//    int max = getMax(key).intValue();
//    return ((long)min + max) * (max - min + 1L) / 2L;
//}

//private static Double getAvg(string key)
//{
//    int min = getMin(key).intValue();
//    int max = getMax(key).intValue();
//    return ((long)min + max) / 2.0;
//}


//private static bool verifyTAgg(PrintStream resultStream,
//                                  Dictionary<string, HashSet<int>> allData,
//                                  Dictionary<string, LinkedList<ConsumeResult<string, Number>>> taggEvents)
//{
//    if (taggEvents == null)
//    {
//        resultStream.WriteLine("tagg is missing");
//        return false;
//    }
//    else if (taggEvents.IsEmpty())
//    {
//        resultStream.WriteLine("tagg is empty");
//        return false;
//    }
//    else
//    {
//        resultStream.WriteLine("verifying tagg");

//        // generate expected answer
//        Dictionary<string, long> expected = new HashMap<>();
//        foreach (string key in allData.keySet())
//        {
//            int min = getMin(key).intValue();
//            int max = getMax(key).intValue();
//            string cnt = long.ToString(max - min + 1L);

//            expected.Put(cnt, expected.getOrDefault(cnt, 0L) + 1);
//        }

//        // check the result
//        foreach (Map.Entry<string, LinkedList<ConsumeResult<string, Number>>> entry in taggEvents)
//        {
//            string key = entry.Key;
//            long expectedCount = expected.remove(key);
//            if (expectedCount == null)
//            {
//                expectedCount = 0L;
//            }

//            if (entry.Value.getLast().Value.longValue() != expectedCount)
//            {
//                resultStream.WriteLine("fail: key=" + key + " tagg=" + entry.Value + " expected=" + expected.Get(key));
//                resultStream.WriteLine("\t outputEvents: " + entry.Value);
//                return false;
//            }
//        }

//    }
//    return true;
//}

//private static Number getMin(string key)
//{
//    return int.parseInt(key.Split("-")[0]);
//}

//private static Number getMax(string key)
//{
//    return int.parseInt(key.Split("-")[1]);
//}

//private static List<TopicPartition> getAllPartitions(KafkaConsumer<?, ?> consumer, string... topics)
//{
//    List<TopicPartition> partitions = new List<TopicPartition>();

//    foreach (string topic in topics)
//    {
//        foreach (PartitionInfo info in consumer.partitionsFor(topic))
//        {
//            partitions.Add(new TopicPartition(info.Topic, info.Partition));
//        }
//    }
//    return partitions;
//}

//}
