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
//            Properties producerProps = generatorProperties(kafka);

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
//                    string key = data[index].key;
//                    int value = data[index].next();

//                    ProducerRecord<byte[], byte[]> record =
//                        new ProducerRecord<>(
//                            "data",
//                            stringSerde.Serializer.serialize("", key),
//                            intSerde.Serializer.serialize("", value)
//                        );

//                    producer.send(record);

//                    numRecordsProduced++;
//                    if (numRecordsProduced % 100 == 0)
//                    {
//                        System.Console.Out.WriteLine(Instant.now() + " " + numRecordsProduced + " records produced");
//                    }
//                    Utils.sleep(2);
//                }
//            }
//    }

//        public static Dictionary<string, HashSet<int>> generate(string kafka,
//                                                         int numKeys,
//                                                         int maxRecordsPerKey,
//                                                         Duration timeToSpend)
//        {
//            Properties producerProps = generatorProperties(kafka);


//            int numRecordsProduced = 0;

//            Dictionary<string, HashSet<int>> allData = new HashMap<>();
//            ValueList[] data = new ValueList[numKeys];
//            for (int i = 0; i < numKeys; i++)
//            {
//                data[i] = new ValueList(i, i + maxRecordsPerKey - 1);
//                allData.put(data[i].key, new HashSet<>());
//            }
//            Random rand = new Random();

//            int remaining = data.Length;

//            long recordPauseTime = timeToSpend.toMillis() / numKeys / maxRecordsPerKey;

//            List<ProducerRecord<byte[], byte[]>> needRetry = new ArrayList<>();

//            try
//            {
//                (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps));
//                while (remaining > 0)
//                {
//                    int index = rand.nextInt(remaining);
//                    string key = data[index].key;
//                    int value = data[index].next();

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
//                                stringSerde.Serializer.serialize("", key),
//                                intSerde.Serializer.serialize("", value)
//                            );

//                        producer.send(record, new TestCallback(record, needRetry));

//                        numRecordsProduced++;
//                        allData.get(key).add(value);
//                        if (numRecordsProduced % 100 == 0)
//                        {
//                            System.Console.Out.WriteLine(Instant.now() + " " + numRecordsProduced + " records produced");
//                        }
//                        Utils.sleep(Math.max(recordPauseTime, 2));
//                    }
//                }
//                producer.flush();

//                int remainingRetries = 5;
//                while (!needRetry.isEmpty())
//                {
//                    List<ProducerRecord<byte[], byte[]>> needRetry2 = new ArrayList<>();
//                    foreach (ProducerRecord<byte[], byte[]> record in needRetry)
//                    {
//                        System.Console.Out.WriteLine("retry producing " + stringSerde.deserializer().deserialize("", record.Key));
//                        producer.send(record, new TestCallback(record, needRetry2));
//                    }
//                    producer.flush();
//                    needRetry = needRetry2;

//                    if (--remainingRetries == 0 && !needRetry.isEmpty())
//                    {
//                        System.Console.Error.println("Failed to produce all records after multiple retries");
//                        Exit.exit(1);
//                    }
//                }

//                // now that we've sent everything, we'll send some records with a timestamp high enough to flush out
//                // all suppressed records.
//                List<PartitionInfo> partitions = producer.partitionsFor("data");
//                foreach (PartitionInfo partition in partitions)
//                {
//                    producer.send(new ProducerRecord<>(
//                        partition.topic(),
//                        partition.partition(),
//                        System.currentTimeMillis() + Duration.ofDays(2).toMillis(),
//                        stringSerde.Serializer.serialize("", "flush"),
//                        intSerde.Serializer.serialize("", 0)
//                    ));
//                }
//            }
//        return Collections.unmodifiableMap(allData);
//        }

//        private static Properties generatorProperties(string kafka)
//        {
//            Properties producerProps = new Properties();
//            producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "SmokeTest");
//            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
//            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer);
//        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer);
//        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
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
//                needRetry.add(originalRecord);
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
//            value = intSerde.deserializer().deserialize(topic, data);
//            break;
//        case "sum":
//        case "cnt":
//        case "tagg":
//            value = longSerde.deserializer().deserialize(topic, data);
//            break;
//        case "avg":
//            value = doubleSerde.deserializer().deserialize(topic, data);
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
//    Properties props = new Properties();
//    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "verifier");
//    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
//    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer);
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, NumberDeserializer);
//        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

//        KafkaConsumer<string, Number> consumer = new KafkaConsumer<>(props);
//List<TopicPartition> partitions = getAllPartitions(consumer, TOPICS);
//consumer.assign(partitions);
//        consumer.seekToBeginning(partitions);

//        int recordsGenerated = inputs.Count * maxRecordsPerKey;
//int recordsProcessed = 0;
//Dictionary<string, AtomicInteger> processed =
//    Stream.of(TOPICS)
//          .collect(Collectors.toMap(t => t, t => new AtomicInteger(0)));

//Dictionary<string, Map<string, LinkedList<ConsumeResult<string, Number>>>> events = new HashMap<>();

//VerificationResult verificationResult = new VerificationResult(false, "no results yet");
//int retry = 0;
//long start = System.currentTimeMillis();
//        while (System.currentTimeMillis() - start<TimeUnit.MINUTES.toMillis(6)) {
//            ConsumerRecords<string, Number> records = consumer.poll(Duration.ofSeconds(1));
//            if (records.isEmpty() && recordsProcessed >= recordsGenerated) {
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

//string topic = record.topic();
//processed.get(topic).incrementAndGet();

//                    if (topic.equals("echo")) {
//                        recordsProcessed++;
//                        if (recordsProcessed % 100 == 0) {
//                            System.Console.Out.WriteLine("Echo records processed = " + recordsProcessed);
//                        }
//                    }

//                    events.computeIfAbsent(topic, t => new HashMap<>())
//                          .computeIfAbsent(key, k => new LinkedList<>())
//                          .add(record);
//                }

//                System.Console.Out.WriteLine(processed);
//            }
//        }
//        consumer.close();
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
//    events.get("echo")
//          .entrySet()
//          .stream()
//          .map(entry => mkEntry(
//              entry.getKey(),
//              entry.getValue().stream().map(ConsumeResult::value).collect(Collectors.toSet()))
//          )
//          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

//success = inputs.equals(received);

//        if (success) {
//            System.Console.Out.WriteLine("ALL-RECORDS-DELIVERED");
//        } else {
//            int missedCount = 0;
//            foreach (Map.Entry<string, HashSet<int>> entry in inputs.entrySet()) {
//                missedCount += received.get(entry.getKey()).Count;
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
//        pass = verifyTAgg(resultStream, inputs, events.get("tagg"));
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
//    Dictionary<string, LinkedList<ConsumeResult<string, Number>>> observedInputEvents = events.get("data");
//    Dictionary<string, LinkedList<ConsumeResult<string, Number>>> outputEvents = events.getOrDefault(topic, emptyMap());
//    if (outputEvents.isEmpty())
//    {
//        resultStream.println(topic + " is empty");
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
//        foreach (Map.Entry<string, LinkedList<ConsumeResult<string, Number>>> entry in outputEvents.entrySet())
//        {
//            string key = entry.getKey();
//            Number expected = keyToExpectation.apply(key);
//            Number actual = entry.getValue().getLast().Value;
//            if (!expected.equals(actual))
//            {
//                resultStream.printf("%s fail: key=%s actual=%s expected=%s%n\t inputEvents=%n%s%n\toutputEvents=%n%s%n",
//                                    topic,
//                                    key,
//                                    actual,
//                                    expected,
//                                    indent("\t\t", observedInputEvents.get(key)),
//                                    indent("\t\t", entry.getValue()));
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
//    resultStream.println("verifying suppressed " + topic);
//    Dictionary<string, LinkedList<ConsumeResult<string, Number>>> topicEvents = events.getOrDefault(topic, emptyMap());
//    foreach (Map.Entry<string, LinkedList<ConsumeResult<string, Number>>> entry in topicEvents.entrySet())
//    {
//        if (entry.getValue().Count != 1)
//        {
//            string unsuppressedTopic = topic.replace("-suppressed", "-raw");
//            string key = entry.getKey();
//            string unwindowedKey = key.substring(1, key.Length() - 1).replaceAll("@.*", "");
//            resultStream.printf("fail: key=%s%n\tnon-unique result:%n%s%n\traw results:%n%s%n\tinput data:%n%s%n",
//                                key,
//                                indent("\t\t", entry.getValue()),
//                                indent("\t\t", events.get(unsuppressedTopic).get(key)),
//                                indent("\t\t", events.get("data").get(unwindowedKey))
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
//    return stringBuilder.toString();
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
//        resultStream.println("tagg is missing");
//        return false;
//    }
//    else if (taggEvents.isEmpty())
//    {
//        resultStream.println("tagg is empty");
//        return false;
//    }
//    else
//    {
//        resultStream.println("verifying tagg");

//        // generate expected answer
//        Dictionary<string, long> expected = new HashMap<>();
//        foreach (string key in allData.keySet())
//        {
//            int min = getMin(key).intValue();
//            int max = getMax(key).intValue();
//            string cnt = long.toString(max - min + 1L);

//            expected.put(cnt, expected.getOrDefault(cnt, 0L) + 1);
//        }

//        // check the result
//        foreach (Map.Entry<string, LinkedList<ConsumeResult<string, Number>>> entry in taggEvents.entrySet())
//        {
//            string key = entry.getKey();
//            long expectedCount = expected.remove(key);
//            if (expectedCount == null)
//            {
//                expectedCount = 0L;
//            }

//            if (entry.getValue().getLast().Value.longValue() != expectedCount)
//            {
//                resultStream.println("fail: key=" + key + " tagg=" + entry.getValue() + " expected=" + expected.get(key));
//                resultStream.println("\t outputEvents: " + entry.getValue());
//                return false;
//            }
//        }

//    }
//    return true;
//}

//private static Number getMin(string key)
//{
//    return int.parseInt(key.split("-")[0]);
//}

//private static Number getMax(string key)
//{
//    return int.parseInt(key.split("-")[1]);
//}

//private static List<TopicPartition> getAllPartitions(KafkaConsumer<?, ?> consumer, string... topics)
//{
//    List<TopicPartition> partitions = new ArrayList<>();

//    foreach (string topic in topics)
//    {
//        foreach (PartitionInfo info in consumer.partitionsFor(topic))
//        {
//            partitions.add(new TopicPartition(info.topic(), info.partition()));
//        }
//    }
//    return partitions;
//}

//}
