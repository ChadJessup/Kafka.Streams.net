//using Kafka.Streams.Interfaces;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.KStream.Mappers;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.Processors.Internals;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KStreamImplTest
//    {

//        private Consumed<string, string> stringConsumed = Consumed.with(Serdes.String(), Serdes.String());
//        private MockProcessorSupplier<string, string> processorSupplier = new MockProcessorSupplier<>();

//        private IKStream<string, string> testStream;
//        private StreamsBuilder builder;

//        private ConsumerRecordFactory<string, string> recordFactory =
//            new ConsumerRecordFactory<>(Serdes.String(), Serdes.String(), 0L);
//        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.String());

//        private ISerde<string> mySerde = new Serdes.StringSerde();


//        public void before()
//        {
//            builder = new StreamsBuilder();
//            testStream = builder.Stream("source");
//        }

//        [Fact]
//        public void testNumProcesses()
//        {
//            StreamsBuilder builder = new StreamsBuilder();

//            IKStream<string, string> source1 = builder.Stream(new List<string> { "topic-1", "topic-2" }, stringConsumed);

//            IKStream<string, string> source2 = builder.Stream(new List<string> { "topic-3", "topic-4" }, stringConsumed);

//            IKStream<string, string> stream1 = source1.filter((key, value) => true)
//                                                           .filterNot((key, value) => false);

//            IKStream<string, int> stream2 = stream1.mapValues(new ValueMapper((k, v) => new int()));

//            IKStream<string, int> stream3 = source2.flatMapValues((IValueMapper<string, Iterable<int>>)
//                value => Collections.singletonList(new List<int> { value }));

//            IKStream<string, int>[] streams2 = stream2.branch(
//                (key, value) => (value % 2) == 0,
//                (key, value) => true
//            );

//            IKStream<string, int>[] streams3 = stream3.branch(
//                (key, value) => (value % 2) == 0,
//                (key, value) => true
//            );

//            int anyWindowSize = 1;
//            Joined<string, int, int> joined = Joined.with(Serdes.String(), Serdes.Int(), Serdes.Int());
//            IKStream<string, int> stream4 = streams2[0].join(streams3[0],
//                (value1, value2) => value1 + value2, JoinWindows.of(Duration.FromMilliseconds(anyWindowSize)), joined);

//            streams2[1].join(streams3[1], (value1, value2) => value1 + value2,
//                JoinWindows.of(Duration.FromMilliseconds(anyWindowSize)), joined);

//            stream4.to("topic-5");

//            streams2[1].through("topic-6").process(new MockProcessorSupplier<>());

//            Assert.Equal(2 + // sources
//                 2 + // stream1
//                 1 + // stream2
//                 1 + // stream3
//                 1 + 2 + // streams2
//                 1 + 2 + // streams3
//                 5 * 2 + // stream2-stream3 joins
//                 1 + // to
//                 2 + // through
//                 1, // process
//                 TopologyWrapper.getInternalTopologyBuilder(builder.Build()).setApplicationId("X").Build(null).processors().size());
//        }

//        [Fact]
//        public void shouldPreserveSerdesForOperators()
//        {
//            StreamsBuilder builder = new StreamsBuilder();
//            IKStream<string, string> stream1 = builder.Stream(Collections.singleton("topic-1"), stringConsumed);
//            IKTable<string, string> table1 = builder.Table("topic-2", stringConsumed);
//            IGlobalKTable<string, string> table2 = builder.globalTable("topic-2", stringConsumed);
//            ConsumedInternal<string, string> consumedInternal = new ConsumedInternal<>(stringConsumed);

//            IKeyValueMapper<string, string, string> selector = (key, value) => key;
//            IKeyValueMapper<string, string, Iterable<KeyValue<string, string>>> flatSelector = (key, value) => Collections.singleton(new KeyValue<>(key, value));
//            IValueMapper<string, string> mapper = value => value;
//            IValueMapper<string, Iterable<string>> flatMapper = Collections::singleton;
//            ValueJoiner<string, string, string> joiner = (value1, value2) => value1;
//            TransformerSupplier<string, string, KeyValue<string, string>> transformerSupplier = () => new Transformer<string, string, KeyValue<string, string>>()
//            {


//            public void init(IProcessorContext context) { }


//            public KeyValue<string, string> transform(string key, string value)
//            {
//                return new KeyValue<>(key, value);
//            }


//            public void close() { }
//        };
//        ValueTransformerSupplier<string, string> valueTransformerSupplier = () => new ValueTransformer<string, string>()
//        {


//            public void init(IProcessorContext context) { }


//        public string transform(string value)
//        {
//            return value;
//        }


//        public void close() { }
//    };

//    Assert.Equal(((AbstractStream) stream1.filter((key, value) => false)).keySerde(), consumedInternal.keySerde());
//       Assert.Equal(((AbstractStream) stream1.filter((key, value) => false)).valueSerde(), consumedInternal.valueSerde());

//       Assert.Equal(((AbstractStream) stream1.filterNot((key, value) => false)).keySerde(), consumedInternal.keySerde());
//       Assert.Equal(((AbstractStream) stream1.filterNot((key, value) => false)).valueSerde(), consumedInternal.valueSerde());

//       Assert.Null(((AbstractStream) stream1.selectKey(selector)).keySerde());
//       Assert.Equal(((AbstractStream) stream1.selectKey(selector)).valueSerde(), consumedInternal.valueSerde());

//       Assert.Null(((AbstractStream) stream1.map(KeyValue::new)).keySerde());
//       Assert.Null(((AbstractStream) stream1.map(KeyValue::new)).valueSerde());

//       Assert.Equal(((AbstractStream) stream1.mapValues(mapper)).keySerde(), consumedInternal.keySerde());
//       Assert.Null(((AbstractStream) stream1.mapValues(mapper)).valueSerde());

//       Assert.Null(((AbstractStream) stream1.flatMap(flatSelector)).keySerde());
//       Assert.Null(((AbstractStream) stream1.flatMap(flatSelector)).valueSerde());

//       Assert.Equal(((AbstractStream) stream1.flatMapValues(flatMapper)).keySerde(), consumedInternal.keySerde());
//       Assert.Null(((AbstractStream) stream1.flatMapValues(flatMapper)).valueSerde());

//       Assert.Null(((AbstractStream) stream1.transform(transformerSupplier)).keySerde());
//       Assert.Null(((AbstractStream) stream1.transform(transformerSupplier)).valueSerde());

//       Assert.Equal(((AbstractStream) stream1.transformValues(valueTransformerSupplier)).keySerde(), consumedInternal.keySerde());
//       Assert.Null(((AbstractStream) stream1.transformValues(valueTransformerSupplier)).valueSerde());

//       Assert.Null(((AbstractStream) stream1.merge(stream1)).keySerde());
//       Assert.Null(((AbstractStream) stream1.merge(stream1)).valueSerde());

//       Assert.Equal(((AbstractStream) stream1.through("topic-3")).keySerde(), consumedInternal.keySerde());
//       Assert.Equal(((AbstractStream) stream1.through("topic-3")).valueSerde(), consumedInternal.valueSerde());
//       Assert.Equal(((AbstractStream) stream1.through("topic-3", Produced.with(mySerde, mySerde))).keySerde(), mySerde);
//       Assert.Equal(((AbstractStream) stream1.through("topic-3", Produced.with(mySerde, mySerde))).valueSerde(), mySerde);

//       Assert.Equal(((AbstractStream) stream1.groupByKey()).keySerde(), consumedInternal.keySerde());
//       Assert.Equal(((AbstractStream) stream1.groupByKey()).valueSerde(), consumedInternal.valueSerde());
//       Assert.Equal(((AbstractStream) stream1.groupByKey(Grouped.with(mySerde, mySerde))).keySerde(), mySerde);
//       Assert.Equal(((AbstractStream) stream1.groupByKey(Grouped.with(mySerde, mySerde))).valueSerde(), mySerde);

//       Assert.Null(((AbstractStream) stream1.groupBy(selector)).keySerde());
//       Assert.Equal(((AbstractStream) stream1.groupBy(selector)).valueSerde(), consumedInternal.valueSerde());
//       Assert.Equal(((AbstractStream) stream1.groupBy(selector, Grouped.with(mySerde, mySerde))).keySerde(), mySerde);
//       Assert.Equal(((AbstractStream) stream1.groupBy(selector, Grouped.with(mySerde, mySerde))).valueSerde(), mySerde);

//       Assert.Null(((AbstractStream) stream1.join(stream1, joiner, JoinWindows.of(Duration.FromMilliseconds(100L)))).keySerde());
//       Assert.Null(((AbstractStream) stream1.join(stream1, joiner, JoinWindows.of(Duration.FromMilliseconds(100L)))).valueSerde());
//       Assert.Equal(((AbstractStream) stream1.join(stream1, joiner, JoinWindows.of(Duration.FromMilliseconds(100L)), Joined.with(mySerde, mySerde, mySerde))).keySerde(), mySerde);
//       Assert.Null(((AbstractStream) stream1.join(stream1, joiner, JoinWindows.of(Duration.FromMilliseconds(100L)), Joined.with(mySerde, mySerde, mySerde))).valueSerde());

//       Assert.Null(((AbstractStream) stream1.leftJoin(stream1, joiner, JoinWindows.of(Duration.FromMilliseconds(100L)))).keySerde());
//       Assert.Null(((AbstractStream) stream1.leftJoin(stream1, joiner, JoinWindows.of(Duration.FromMilliseconds(100L)))).valueSerde());
//       Assert.Equal(((AbstractStream) stream1.leftJoin(stream1, joiner, JoinWindows.of(Duration.FromMilliseconds(100L)), Joined.with(mySerde, mySerde, mySerde))).keySerde(), mySerde);
//       Assert.Null(((AbstractStream) stream1.leftJoin(stream1, joiner, JoinWindows.of(Duration.FromMilliseconds(100L)), Joined.with(mySerde, mySerde, mySerde))).valueSerde());

//       Assert.Null(((AbstractStream) stream1.outerJoin(stream1, joiner, JoinWindows.of(Duration.FromMilliseconds(100L)))).keySerde());
//       Assert.Null(((AbstractStream) stream1.outerJoin(stream1, joiner, JoinWindows.of(Duration.FromMilliseconds(100L)))).valueSerde());
//       Assert.Equal(((AbstractStream) stream1.outerJoin(stream1, joiner, JoinWindows.of(Duration.FromMilliseconds(100L)), Joined.with(mySerde, mySerde, mySerde))).keySerde(), mySerde);
//       Assert.Null(((AbstractStream) stream1.outerJoin(stream1, joiner, JoinWindows.of(Duration.FromMilliseconds(100L)), Joined.with(mySerde, mySerde, mySerde))).valueSerde());

//       Assert.Equal(((AbstractStream) stream1.join(table1, joiner)).keySerde(), consumedInternal.keySerde());
//       Assert.Null(((AbstractStream) stream1.join(table1, joiner)).valueSerde());
//       Assert.Equal(((AbstractStream) stream1.join(table1, joiner, Joined.with(mySerde, mySerde, mySerde))).keySerde(), mySerde);
//       Assert.Null(((AbstractStream) stream1.join(table1, joiner, Joined.with(mySerde, mySerde, mySerde))).valueSerde());

//       Assert.Equal(((AbstractStream) stream1.leftJoin(table1, joiner)).keySerde(), consumedInternal.keySerde());
//       Assert.Null(((AbstractStream) stream1.leftJoin(table1, joiner)).valueSerde());
//       Assert.Equal(((AbstractStream) stream1.leftJoin(table1, joiner, Joined.with(mySerde, mySerde, mySerde))).keySerde(), mySerde);
//       Assert.Null(((AbstractStream) stream1.leftJoin(table1, joiner, Joined.with(mySerde, mySerde, mySerde))).valueSerde());

//       Assert.Equal(((AbstractStream) stream1.join(table2, selector, joiner)).keySerde(), consumedInternal.keySerde());
//       Assert.Null(((AbstractStream) stream1.join(table2, selector, joiner)).valueSerde());

//       Assert.Equal(((AbstractStream) stream1.leftJoin(table2, selector, joiner)).keySerde(), consumedInternal.keySerde());
//       Assert.Null(((AbstractStream) stream1.leftJoin(table2, selector, joiner)).valueSerde());
//    }

//[Fact]
//public void shouldUseRecordMetadataTimestampExtractorWithThrough()
//{
//    StreamsBuilder builder = new StreamsBuilder();
//    IKStream<string, string> stream1 = builder.Stream(new List<string> { "topic-1", "topic-2" }, stringConsumed);
//    IKStream<string, string> stream2 = builder.Stream(new List<string> { "topic-3", "topic-4" }, stringConsumed);

//    stream1.to("topic-5");
//    stream2.through("topic-6");

//    ProcessorTopology processorTopology = TopologyWrapper.getInternalTopologyBuilder(builder.Build()).setApplicationId("X").Build(null);
//    Assert.Equal(typeof(processorTopology.source("topic-6").getTimestampExtractor(), instanceOf(FailOnInvalidTimestamp)));
//       Assert.Null(processorTopology.source("topic-4").getTimestampExtractor());
//       Assert.Null(processorTopology.source("topic-3").getTimestampExtractor());
//       Assert.Null(processorTopology.source("topic-2").getTimestampExtractor());
//       Assert.Null(processorTopology.source("topic-1").getTimestampExtractor());
//    }

//    [Fact]
//public void shouldSendDataThroughTopicUsingProduced()
//{
//    StreamsBuilder builder = new StreamsBuilder();
//    string input = "topic";
//    IKStream<string, string> stream = builder.Stream(input, stringConsumed);
//    stream.through("through-topic", Produced.with(Serdes.String(), Serdes.String())).process(processorSupplier);

//    try {
//var driver = new TopologyTestDriver(builder.Build(), props);
//        driver.pipeInput(recordFactory.create(input, "a", "b"));
//    }
//    Assert.Equal(processorSupplier.theCapturedProcessor().processed, equalTo(Collections.singletonList(new KeyValueTimestamp<>("a", "b", 0))));
//    }

//    [Fact]
//public void shouldSendDataToTopicUsingProduced()
//{
//    StreamsBuilder builder = new StreamsBuilder();
//    string input = "topic";
//    IKStream<string, string> stream = builder.Stream(input, stringConsumed);
//    stream.to("to-topic", Produced.with(Serdes.String(), Serdes.String()));
//    builder.Stream("to-topic", stringConsumed).process(processorSupplier);

//    try {
//var driver = new TopologyTestDriver(builder.Build(), props);
//        driver.pipeInput(recordFactory.create(input, "e", "f"));
//    }
//    Assert.Equal(processorSupplier.theCapturedProcessor().processed, equalTo(Collections.singletonList(new KeyValueTimestamp<>("e", "f", 0))));
//    }

//    [Fact]
//public void shouldSendDataToDynamicTopics()
//{
//    StreamsBuilder builder = new StreamsBuilder();
//    string input = "topic";
//    IKStream<string, string> stream = builder.Stream(input, stringConsumed);
//    stream.to((key, value, context) => context.Topic + "-" + key + "-" + value.substring(0, 1),
//              Produced.with(Serdes.String(), Serdes.String()));
//    builder.Stream(input + "-a-v", stringConsumed).process(processorSupplier);
//    builder.Stream(input + "-b-v", stringConsumed).process(processorSupplier);

//    try {
//var driver = new TopologyTestDriver(builder.Build(), props);
//        driver.pipeInput(recordFactory.create(input, "a", "v1"));
//        driver.pipeInput(recordFactory.create(input, "a", "v2"));
//        driver.pipeInput(recordFactory.create(input, "b", "v1"));
//    }
//    List<MockProcessor<string, string>> mockProcessors = processorSupplier.capturedProcessors(2);
//    Assert.Equal(mockProcessors.get(0).processed, equalToasList(new KeyValueTimestamp<>("a", "v1", 0),
//            new KeyValueTimestamp<>("a", "v2", 0))));
//    Assert.Equal(mockProcessors.get(1).processed, equalTo(Collections.singletonList(new KeyValueTimestamp<>("b", "v1", 0))));
//    }

//     // specifically testing the deprecated variant
//[Fact]
//public void shouldUseRecordMetadataTimestampExtractorWhenInternalRepartitioningTopicCreatedWithRetention()
//{
//    StreamsBuilder builder = new StreamsBuilder();
//    IKStream<string, string> kStream = builder.Stream("topic-1", stringConsumed);
//    ValueJoiner<string, string, string> valueJoiner = MockValueJoiner.instance(":");
//    long windowSize = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
//    IKStream<string, string> stream = kStream
//        .map((key, value) => KeyValue.Pair(value, value));
//    stream.join(kStream,
//                valueJoiner,
//                JoinWindows.of(Duration.FromMilliseconds(windowSize)).grace(Duration.FromMilliseconds(3 * windowSize)),
//                Joined.with(Serdes.String(),
//                            Serdes.String(),
//                            Serdes.String()))
//          .to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

//    ProcessorTopology topology = TopologyWrapper.getInternalTopologyBuilder(builder.Build()).setApplicationId("X").Build();

//    SourceNode originalSourceNode = topology.source("topic-1");

//    foreach (SourceNode sourceNode in topology.sources())
//    {
//        if (sourceNode.name().equals(originalSourceNode.name()))
//        {
//            Assert.Null(sourceNode.getTimestampExtractor());
//        }
//        else
//        {
//            Assert.Equal(typeof(sourceNode.getTimestampExtractor(), instanceOf(FailOnInvalidTimestamp)));
//            }
//        }
//    }

//    [Fact]
//public void shouldUseRecordMetadataTimestampExtractorWhenInternalRepartitioningTopicCreated()
//{
//    StreamsBuilder builder = new StreamsBuilder();
//    IKStream<string, string> kStream = builder.Stream("topic-1", stringConsumed);
//    ValueJoiner<string, string, string> valueJoiner = MockValueJoiner.instance(":");
//    long windowSize = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
//    IKStream<string, string> stream = kStream
//        .map((key, value) => KeyValue.Pair(value, value));
//    stream.join(
//        kStream,
//        valueJoiner,
//        JoinWindows.of(Duration.FromMilliseconds(windowSize)).grace(Duration.FromMilliseconds(3L * windowSize)),
//        Joined.with(Serdes.String(), Serdes.String(), Serdes.String())
//    )
//          .to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

//    ProcessorTopology topology = TopologyWrapper.getInternalTopologyBuilder(builder.Build()).setApplicationId("X").Build();

//    SourceNode originalSourceNode = topology.source("topic-1");

//    foreach (SourceNode sourceNode in topology.sources())
//    {
//        if (sourceNode.name().equals(originalSourceNode.name()))
//        {
//            Assert.Null(sourceNode.getTimestampExtractor());
//        }
//        else
//        {
//            Assert.Equal(typeof(sourceNode.getTimestampExtractor(), instanceOf(FailOnInvalidTimestamp)));
//            }
//        }
//    }

//    [Fact]
//public void shouldPropagateRepartitionFlagAfterGlobalKTableJoin()
//{
//    StreamsBuilder builder = new StreamsBuilder();
//    IGlobalKTable<string, string> globalKTable = builder.globalTable("globalTopic");
//    IKeyValueMapper<string, string, string> kvMappper = (k, v) => k + v;
//    ValueJoiner<string, string, string> valueJoiner = (v1, v2) => v1 + v2;
//    builder.< string, string> stream("topic").selectKey((k, v) => v)
//         .join(globalKTable, kvMappper, valueJoiner)
//         .groupByKey()
//         .count();

//    Regex repartitionTopicPattern = Pattern.compile("Sink: .*-repartition");
//    string topology = builder.Build().describe().ToString();
//    Matcher matcher = repartitionTopicPattern.matcher(topology);
//    Assert.True(matcher.find());
//    string match = matcher.group();
//    Assert.Equal(match, notNullValue());
//    Assert.True(match.endsWith("repartition"));
//}

//[Fact]
//public void testToWithNullValueSerdeDoesntNPE()
//{
//    StreamsBuilder builder = new StreamsBuilder();
//    Consumed<string, string> consumed = Consumed.with(Serdes.String(), Serdes.String());
//    IKStream<string, string> inputStream = builder.Stream(Collections.singleton("input"), consumed);

//    inputStream.to("output", Produced.with(Serdes.String(), Serdes.String()));
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldNotAllowNullPredicateOnFilter()
//{
//    testStream.filter(null);
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldNotAllowNullPredicateOnFilterNot()
//{
//    testStream.filterNot(null);
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldNotAllowNullMapperOnSelectKey()
//{
//    testStream.selectKey(null);
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldNotAllowNullMapperOnMap()
//{
//    testStream.map(null);
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldNotAllowNullMapperOnMapValues()
//{
//    testStream.mapValues((ValueMapper)null);
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldNotAllowNullMapperOnMapValuesWithKey()
//{
//    testStream.mapValues((ValueMapperWithKey)null);
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldNotAllowNullMapperOnFlatMap()
//{
//    testStream.flatMap(null);
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldNotAllowNullMapperOnFlatMapValues()
//{
//    testStream.flatMapValues((ValueMapper <? super string, ? : Iterable <? : string >>) null);
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldNotAllowNullMapperOnFlatMapValuesWithKey()
//{
//    testStream.flatMapValues((ValueMapperWithKey <? super string, ? super string, ? : Iterable <? : string >>) null);
//}

//[Fact] (typeof(expected = ArgumentException))
//    public void shouldHaveAtL.AstOnPredicateWhenBranching()
//{
//    testStream.branch();
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldCantHaveNullPredicate()
//{
//    testStream.branch((Predicate)null);
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldNotAllowNullTopicOnThrough()
//{
//    testStream.through(null);
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldNotAllowNullTopicOnTo()
//{
//    testStream.to((string)null);
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldNotAllowNullTopicChooserOnTo()
//{
//    testStream.to((TopicNameExtractor<string, string>)null);
//}

//[Fact]
//public void shouldNotAllowNullTransformerSupplierOnTransform()
//{
//    Exception e =Assert.Throws(NullPointerException.class, () => testStream.transform(null));
//       Assert.Equal("transformerSupplier can't be null", e.getMessage());
//    }

//    [Fact]
//public void shouldNotAllowNullTransformerSupplierOnFlatTransform()
//{
//    Exception e =Assert.Throws(NullPointerException.class, () => testStream.flatTransform(null));
//       Assert.Equal("transformerSupplier can't be null", e.getMessage());
//    }

//    [Fact]
//public void shouldNotAllowNullValueTransformerWithKeySupplierOnTransformValues()
//{
//    Exception e =

//       Assert.Throws(NullPointerException.class, () => testStream.transformValues((ValueTransformerWithKeySupplier) null));
//       Assert.Equal("valueTransformerSupplier can't be null", e.getMessage());
//    }

//    [Fact]
//public void shouldNotAllowNullValueTransformerSupplierOnTransformValues()
//{
//    Exception e =

//       Assert.Throws(NullPointerException.class, () => testStream.transformValues((ValueTransformerSupplier) null));
//       Assert.Equal("valueTransformerSupplier can't be null", e.getMessage());
//    }

//    [Fact]
//public void shouldNotAllowNullValueTransformerWithKeySupplierOnFlatTransformValues()
//{
//    Exception e =

//       Assert.Throws(NullPointerException.class, () => testStream.flatTransformValues((ValueTransformerWithKeySupplier) null));
//       Assert.Equal("valueTransformerSupplier can't be null", e.getMessage());
//    }

//    [Fact]
//public void shouldNotAllowNullValueTransformerSupplierOnFlatTransformValues()
//{
//    Exception e =

//       Assert.Throws(NullPointerException.class, () => testStream.flatTransformValues((ValueTransformerSupplier) null));
//       Assert.Equal("valueTransformerSupplier can't be null", e.getMessage());
//    }

//    [Fact] (typeof(expected = NullPointerException))
//    public void shouldNotAllowNullProcessSupplier()
//{
//    testStream.process(null);
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldNotAllowNullOtherStreamOnJoin()
//{
//    testStream.join(null, MockValueJoiner.TOSTRING_JOINER, JoinWindows.of(Duration.FromMilliseconds(10)));
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldNotAllowNullValueJoinerOnJoin()
//{
//    testStream.join(testStream, null, JoinWindows.of(Duration.FromMilliseconds(10)));
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldNotAllowNullJoinWindowsOnJoin()
//{
//    testStream.join(testStream, MockValueJoiner.TOSTRING_JOINER, null);
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldNotAllowNullTableOnTableJoin()
//{
//    testStream.leftJoin((IKTable)null, MockValueJoiner.TOSTRING_JOINER);
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldNotAllowNullValueMapperOnTableJoin()
//{
//    testStream.leftJoin(builder.Table("topic", stringConsumed), null);
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldNotAllowNullSelectorOnGroupBy()
//{
//    testStream.groupBy(null);
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldNotAllowNullActionOnForEach()
//{
//    testStream.ForEach (null) ;
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldNotAllowNullTableOnJoinWithGlobalTable()
//{
//    testStream.join((GlobalKTable)null,
//                    MockMapper.selectValueMapper(),
//                    MockValueJoiner.TOSTRING_JOINER);
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldNotAllowNullMapperOnJoinWithGlobalTable()
//{
//    testStream.join(builder.globalTable("global", stringConsumed),
//                    null,
//                    MockValueJoiner.TOSTRING_JOINER);
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldNotAllowNullJoinerOnJoinWithGlobalTable()
//{
//    testStream.join(builder.globalTable("global", stringConsumed),
//                    MockMapper.selectValueMapper(),
//                    null);
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldNotAllowNullTableOnJLeftJoinWithGlobalTable()
//{
//    testStream.leftJoin((GlobalKTable)null,
//                    MockMapper.selectValueMapper(),
//                    MockValueJoiner.TOSTRING_JOINER);
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldNotAllowNullMapperOnLeftJoinWithGlobalTable()
//{
//    testStream.leftJoin(builder.globalTable("global", stringConsumed),
//                    null,
//                    MockValueJoiner.TOSTRING_JOINER);
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldNotAllowNullJoinerOnLeftJoinWithGlobalTable()
//{
//    testStream.leftJoin(builder.globalTable("global", stringConsumed),
//                    MockMapper.selectValueMapper(),
//                    null);
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldThrowNullPointerOnPrintIfPrintedIsNull()
//{
//    testStream.print(null);
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldThrowNullPointerOnThroughWhenProducedIsNull()
//{
//    testStream.through("topic", null);
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldThrowNullPointerOnToWhenProducedIsNull()
//{
//    testStream.to("topic", null);
//}

//[Fact]
//public void shouldThrowNullPointerOnLeftJoinWithTableWhenJoinedIsNull()
//{
//    IKTable<string, string> table = builder.Table("blah", stringConsumed);
//    try
//    {
//        testStream.leftJoin(table,
//                            MockValueJoiner.TOSTRING_JOINER,
//                            null);
//        Assert.False(true, "Should have thrown NullPointerException");
//    }
//    catch (NullPointerException e)
//    {
//        // ok
//    }
//}

//[Fact]
//public void shouldThrowNullPointerOnJoinWithTableWhenJoinedIsNull()
//{
//    IKTable<string, string> table = builder.Table("blah", stringConsumed);
//    try
//    {
//        testStream.join(table,
//                        MockValueJoiner.TOSTRING_JOINER,
//                        null);
//        Assert.False(true, "Should have thrown NullPointerException");
//    }
//    catch (NullPointerException e)
//    {
//        // ok
//    }
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldThrowNullPointerOnJoinWithStreamWhenJoinedIsNull()
//{
//    testStream.join(testStream, MockValueJoiner.TOSTRING_JOINER, JoinWindows.of(Duration.FromMilliseconds(10)), null);
//}

//[Fact] (typeof(expected = NullPointerException))
//    public void shouldThrowNullPointerOnOuterJoinJoinedIsNull()
//{
//    testStream.outerJoin(testStream, MockValueJoiner.TOSTRING_JOINER, JoinWindows.of(Duration.FromMilliseconds(10)), null);
//}

//[Fact]
//public void shouldMergeTwoStreams()
//{
//    string topic1 = "topic-1";
//    string topic2 = "topic-2";

//    IKStream<string, string> source1 = builder.Stream(topic1);
//    IKStream<string, string> source2 = builder.Stream(topic2);
//    IKStream<string, string> merged = source1.merge(source2);

//    merged.process(processorSupplier);

//    try {
//var driver = new TopologyTestDriver(builder.Build(), props);
//        driver.pipeInput(recordFactory.create(topic1, "A", "aa"));
//        driver.pipeInput(recordFactory.create(topic2, "B", "bb"));
//        driver.pipeInput(recordFactory.create(topic2, "C", "cc"));
//        driver.pipeInput(recordFactory.create(topic1, "D", "dd"));
//    }

//    Assert.EqualasList(new KeyValueTimestamp<>("A", "aa", 0),
//             new KeyValueTimestamp<>("B", "bb", 0),
//             new KeyValueTimestamp<>("C", "cc", 0),
//             new KeyValueTimestamp<>("D", "dd", 0)), processorSupplier.theCapturedProcessor().processed);
//    }

//    [Fact]
//public void shouldMergeMultipleStreams()
//{
//    string topic1 = "topic-1";
//    string topic2 = "topic-2";
//    string topic3 = "topic-3";
//    string topic4 = "topic-4";

//    IKStream<string, string> source1 = builder.Stream(topic1);
//    IKStream<string, string> source2 = builder.Stream(topic2);
//    IKStream<string, string> source3 = builder.Stream(topic3);
//    IKStream<string, string> source4 = builder.Stream(topic4);
//    IKStream<string, string> merged = source1.merge(source2).merge(source3).merge(source4);

//    merged.process(processorSupplier);

//    try {
//var driver = new TopologyTestDriver(builder.Build(), props);
//        driver.pipeInput(recordFactory.create(topic1, "A", "aa", 1L));
//        driver.pipeInput(recordFactory.create(topic2, "B", "bb", 9L));
//        driver.pipeInput(recordFactory.create(topic3, "C", "cc", 2L));
//        driver.pipeInput(recordFactory.create(topic4, "D", "dd", 8L));
//        driver.pipeInput(recordFactory.create(topic4, "E", "ee", 3L));
//        driver.pipeInput(recordFactory.create(topic3, "F", "ff", 7L));
//        driver.pipeInput(recordFactory.create(topic2, "G", "gg", 4L));
//        driver.pipeInput(recordFactory.create(topic1, "H", "hh", 6L));
//    }

//    Assert.EqualasList(new KeyValueTimestamp<>("A", "aa", 1),
//             new KeyValueTimestamp<>("B", "bb", 9),
//             new KeyValueTimestamp<>("C", "cc", 2),
//             new KeyValueTimestamp<>("D", "dd", 8),
//             new KeyValueTimestamp<>("E", "ee", 3),
//             new KeyValueTimestamp<>("F", "ff", 7),
//             new KeyValueTimestamp<>("G", "gg", 4),
//             new KeyValueTimestamp<>("H", "hh", 6)),
//                     processorSupplier.theCapturedProcessor().processed);
//    }

//    [Fact]
//public void shouldProcessFromSourceThatMatchPattern()
//{
//    IKStream<string, string> pattern2Source = builder.Stream(Pattern.compile("topic-\\d"));

//    pattern2Source.process(processorSupplier);

//    try {
//var driver = new TopologyTestDriver(builder.Build(), props);
//        driver.pipeInput(recordFactory.create("topic-3", "A", "aa", 1L));
//        driver.pipeInput(recordFactory.create("topic-4", "B", "bb", 5L));
//        driver.pipeInput(recordFactory.create("topic-5", "C", "cc", 10L));
//        driver.pipeInput(recordFactory.create("topic-6", "D", "dd", 8L));
//        driver.pipeInput(recordFactory.create("topic-7", "E", "ee", 3L));
//    }

//    Assert.EqualasList(new KeyValueTimestamp<>("A", "aa", 1),
//             new KeyValueTimestamp<>("B", "bb", 5),
//             new KeyValueTimestamp<>("C", "cc", 10),
//             new KeyValueTimestamp<>("D", "dd", 8),
//             new KeyValueTimestamp<>("E", "ee", 3)),
//                processorSupplier.theCapturedProcessor().processed);
//    }

//    [Fact]
//public void shouldProcessFromSourcesThatMatchMultiplePattern()
//{
//    string topic3 = "topic-without-pattern";

//    IKStream<string, string> pattern2Source1 = builder.Stream(Pattern.compile("topic-\\d"));
//    IKStream<string, string> pattern2Source2 = builder.Stream(Pattern.compile("topic-[A-Z]"));
//    IKStream<string, string> source3 = builder.Stream(topic3);
//    IKStream<string, string> merged = pattern2Source1.merge(pattern2Source2).merge(source3);

//    merged.process(processorSupplier);

//    try {
//var driver = new TopologyTestDriver(builder.Build(), props);
//        driver.pipeInput(recordFactory.create("topic-3", "A", "aa", 1L));
//        driver.pipeInput(recordFactory.create("topic-4", "B", "bb", 5L));
//        driver.pipeInput(recordFactory.create("topic-A", "C", "cc", 10L));
//        driver.pipeInput(recordFactory.create("topic-Z", "D", "dd", 8L));
//        driver.pipeInput(recordFactory.create(topic3, "E", "ee", 3L));
//    }

//    Assert.EqualasList(new KeyValueTimestamp<>("A", "aa", 1),
//             new KeyValueTimestamp<>("B", "bb", 5),
//             new KeyValueTimestamp<>("C", "cc", 10),
//             new KeyValueTimestamp<>("D", "dd", 8),
//             new KeyValueTimestamp<>("E", "ee", 3)),
//                processorSupplier.theCapturedProcessor().processed);
//    }
//}
