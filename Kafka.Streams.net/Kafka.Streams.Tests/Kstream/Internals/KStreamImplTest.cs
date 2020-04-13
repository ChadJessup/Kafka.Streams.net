using Kafka.Streams.Configs;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.KStream.Mappers;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Temporary;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Mocks;
using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{
    public class KStreamImplTest
    {
        private Consumed<string, string> stringConsumed = Consumed.With(Serdes.String(), Serdes.String());
        private MockProcessorSupplier<string, string> processorSupplier = new MockProcessorSupplier<string, string>();

        private IKStream<string, string> testStream;
        private StreamsBuilder builder;

        private ConsumerRecordFactory<string, string> recordFactory =
            new ConsumerRecordFactory<string, string>(Serdes.String().Serializer, Serdes.String().Serializer, 0L);
        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.String());

        private ISerde<string> mySerde = Serdes.String();


        public KStreamImplTest()
        {
            builder = new StreamsBuilder();
            testStream = builder.Stream<string, string>("source");
        }

        [Fact]
        public void testNumProcesses()
        {
            StreamsBuilder builder = new StreamsBuilder();

            IKStream<string, string> source1 = builder.Stream(new List<string> { "topic-1", "topic-2" }, stringConsumed);
            IKStream<string, string> source2 = builder.Stream(new List<string> { "topic-3", "topic-4" }, stringConsumed);
            IKStream<string, string> stream1 = source1
                .Filter((key, value) => true)
                .FilterNot((key, value) => false);

            IKStream<string, int> stream2 = stream1.MapValues(new ValueMapper<string, int>(v => int.Parse(v)));
            IKStream<string, int> stream3 = source2.FlatMapValues(value => Collections.singletonList(int.Parse(value)));

            IKStream<string, int>[] streams2 = stream2.Branch(
                (key, value) => (value % 2) == 0,
                (key, value) => true);

            IKStream<string, int>[] streams3 = stream3.Branch(
                (key, value) => (value % 2) == 0,
                (key, value) => true
            );

            int anyWindowSize = 1;
            Joined<string, int, int> joined = Joined.With(Serdes.String(), Serdes.Int(), Serdes.Int());
            IKStream<string, int> stream4 = streams2[0].Join(
                streams3[0],
                (value1, value2) => value1 + value2,
                JoinWindows.Of(TimeSpan.FromMilliseconds(anyWindowSize)),
                joined);

            streams2[1].Join<int, int>(
                streams3[1],
                (value1, value2) => value1 + value2,
                JoinWindows.Of(TimeSpan.FromMilliseconds(anyWindowSize)),
                joined);

            stream4.To("topic-5");

            streams2[1].Through("topic-6").Process(new MockProcessorSupplier<string, int>());

            Assert.Equal(2 + // sources
                 2 + // stream1
                 1 + // stream2
                 1 + // stream3
                 1 + 2 + // streams2
                 1 + 2 + // streams3
                 5 * 2 + // stream2-stream3 joins
                 1 + // to
                 2 + // through
                 1, // process
                 TopologyWrapper.GetInternalTopologyBuilder(builder.Build()).SetApplicationId("X").Build(null).Processors().Count);
        }

        [Fact]
        public void shouldPreserveSerdesForOperators()
        {
            StreamsBuilder builder = new StreamsBuilder();
            IKStream<string, string> stream1 = builder.Stream(Collections.singleton("topic-1"), stringConsumed);
            IKTable<string, string> table1 = builder.Table("topic-2", stringConsumed);
            IGlobalKTable<string, string> table2 = builder.GlobalTable("topic-2", stringConsumed);
            ConsumedInternal<string, string> consumedInternal = new ConsumedInternal<string, string>(stringConsumed);

            IKeyValueMapper<string, string, string> selector = (key, value) => key;
            IKeyValueMapper<string, string, IEnumerable<KeyValuePair<string, string>>> flatSelector = (key, value) => Collections.singleton(KeyValuePair.Create(key, value));
            IValueMapper<string, string> mapper = value => value;
            IValueMapper<string, IEnumerable<string>> flatMapper = Collections.singleton;
            IValueJoiner<string, string, string> joiner = (value1, value2) => value1;
            ITransformerSupplier<string, string, KeyValuePair<string, string>> transformerSupplier = () => new Transformer<string, string, KeyValuePair<string, string>>();
            //            {
            //
            //
            //            public void Init(IProcessorContext context) { }
            //
            //
            //            public KeyValuePair<string, string> transform(string key, string value)
            //            {
            //                return KeyValuePair.Create(key, value);
            //            }
            //
            //
            //            public void Close() { }
            //        };
            IValueTransformerSupplier<string, string> valueTransformerSupplier = () => new ValueTransformer<string, string>();
            //        {
            //
            //
            //            public void Init(IProcessorContext context) { }
            //
            //
            //        public string transform(string value)
            //        {
            //            return value;
            //        }
            //
            //
            //        public void Close() { }
            //    };

            Assert.Equal(stream1.filter((key, value) => false)).keySerde(), consumedInternal.keySerde());
            Assert.Equal(stream1.filter((key, value) => false)).valueSerde(), consumedInternal.valueSerde());

            Assert.Equal(stream1.filterNot((key, value) => false)).keySerde(), consumedInternal.keySerde());
            Assert.Equal(stream1.filterNot((key, value) => false)).valueSerde(), consumedInternal.valueSerde());

            Assert.Null(stream1.selectKey(selector)).keySerde());
            Assert.Equal(stream1.selectKey(selector)).valueSerde(), consumedInternal.valueSerde());

            Assert.Null(stream1.map(KeyValuePair::new)).keySerde());
            Assert.Null(stream1.map(KeyValuePair::new)).valueSerde());

            Assert.Equal(stream1.mapValues(mapper)).keySerde(), consumedInternal.keySerde());
            Assert.Null(stream1.mapValues(mapper)).valueSerde());

            Assert.Null(stream1.flatMap(flatSelector)).keySerde());
            Assert.Null(stream1.flatMap(flatSelector)).valueSerde());

            Assert.Equal(stream1.flatMapValues(flatMapper)).keySerde(), consumedInternal.keySerde());
            Assert.Null(stream1.flatMapValues(flatMapper)).valueSerde());

            Assert.Null(stream1.transform(transformerSupplier)).keySerde());
            Assert.Null(stream1.transform(transformerSupplier)).valueSerde());

            Assert.Equal(stream1.transformValues(valueTransformerSupplier)).keySerde(), consumedInternal.keySerde());
            Assert.Null(stream1.transformValues(valueTransformerSupplier)).valueSerde());

            Assert.Null(stream1.Merge(stream1)).keySerde());
            Assert.Null(stream1.Merge(stream1)).valueSerde());

            Assert.Equal(stream1.Through("topic-3")).keySerde(), consumedInternal.keySerde());
            Assert.Equal(stream1.Through("topic-3")).valueSerde(), consumedInternal.valueSerde());
            Assert.Equal(stream1.Through("topic-3", Produced.With(mySerde, mySerde))).keySerde(), mySerde);
            Assert.Equal(stream1.Through("topic-3", Produced.With(mySerde, mySerde))).valueSerde(), mySerde);

            Assert.Equal(stream1.groupByKey()).keySerde(), consumedInternal.keySerde());
            Assert.Equal(stream1.groupByKey()).valueSerde(), consumedInternal.valueSerde());
            Assert.Equal(stream1.groupByKey(Grouped.with(mySerde, mySerde))).keySerde(), mySerde);
            Assert.Equal(stream1.groupByKey(Grouped.with(mySerde, mySerde))).valueSerde(), mySerde);

            Assert.Null(stream1.GroupBy(selector)).keySerde());
            Assert.Equal(stream1.GroupBy(selector)).valueSerde(), consumedInternal.valueSerde());
            Assert.Equal(stream1.GroupBy(selector, Grouped.with(mySerde, mySerde))).keySerde(), mySerde);
            Assert.Equal(stream1.GroupBy(selector, Grouped.with(mySerde, mySerde))).valueSerde(), mySerde);

            Assert.Null(stream1.Join(stream1, joiner, JoinWindows.of(TimeSpan.FromMilliseconds(100L)))).keySerde());
            Assert.Null(stream1.Join(stream1, joiner, JoinWindows.of(TimeSpan.FromMilliseconds(100L)))).valueSerde());
            Assert.Equal(stream1.Join(stream1, joiner, JoinWindows.of(TimeSpan.FromMilliseconds(100L)), Joined.with(mySerde, mySerde, mySerde))).keySerde(), mySerde);
            Assert.Null(stream1.Join(stream1, joiner, JoinWindows.of(TimeSpan.FromMilliseconds(100L)), Joined.with(mySerde, mySerde, mySerde))).valueSerde());

            Assert.Null(stream1.LeftJoin(stream1, joiner, JoinWindows.of(TimeSpan.FromMilliseconds(100L)))).keySerde());
            Assert.Null(stream1.LeftJoin(stream1, joiner, JoinWindows.of(TimeSpan.FromMilliseconds(100L)))).valueSerde());
            Assert.Equal(stream1.LeftJoin(stream1, joiner, JoinWindows.of(TimeSpan.FromMilliseconds(100L)), Joined.with(mySerde, mySerde, mySerde))).keySerde(), mySerde);
            Assert.Null(stream1.LeftJoin(stream1, joiner, JoinWindows.of(TimeSpan.FromMilliseconds(100L)), Joined.with(mySerde, mySerde, mySerde))).valueSerde());

            Assert.Null(stream1.OuterJoin(stream1, joiner, JoinWindows.of(TimeSpan.FromMilliseconds(100L)))).keySerde());
            Assert.Null(stream1.OuterJoin(stream1, joiner, JoinWindows.of(TimeSpan.FromMilliseconds(100L)))).valueSerde());
            Assert.Equal(stream1.OuterJoin(stream1, joiner, JoinWindows.of(TimeSpan.FromMilliseconds(100L)), Joined.with(mySerde, mySerde, mySerde))).keySerde(), mySerde);
            Assert.Null(stream1.OuterJoin(stream1, joiner, JoinWindows.of(TimeSpan.FromMilliseconds(100L)), Joined.with(mySerde, mySerde, mySerde))).valueSerde());

            Assert.Equal(stream1.Join(table1, joiner)).keySerde(), consumedInternal.keySerde());
            Assert.Null(stream1.Join(table1, joiner)).valueSerde());
            Assert.Equal(stream1.Join(table1, joiner, Joined.with(mySerde, mySerde, mySerde))).keySerde(), mySerde);
            Assert.Null(stream1.Join(table1, joiner, Joined.with(mySerde, mySerde, mySerde))).valueSerde());

            Assert.Equal(stream1.LeftJoin(table1, joiner)).keySerde(), consumedInternal.keySerde());
            Assert.Null(stream1.LeftJoin(table1, joiner)).valueSerde());
            Assert.Equal(stream1.LeftJoin(table1, joiner, Joined.with(mySerde, mySerde, mySerde))).keySerde(), mySerde);
            Assert.Null(stream1.LeftJoin(table1, joiner, Joined.with(mySerde, mySerde, mySerde))).valueSerde());

            Assert.Equal(stream1.Join(table2, selector, joiner)).keySerde(), consumedInternal.keySerde());
            Assert.Null(stream1.Join(table2, selector, joiner)).valueSerde());

            Assert.Equal(stream1.LeftJoin(table2, selector, joiner)).keySerde(), consumedInternal.keySerde());
            Assert.Null(stream1.LeftJoin(table2, selector, joiner)).valueSerde());
        }

        [Fact]
        public void shouldUseRecordMetadataTimestampExtractorWithThrough()
        {
            StreamsBuilder builder = new StreamsBuilder();
            IKStream<string, string> stream1 = builder.Stream(new List<string> { "topic-1", "topic-2" }, stringConsumed);
            IKStream<string, string> stream2 = builder.Stream(new List<string> { "topic-3", "topic-4" }, stringConsumed);

            stream1.To("topic-5");
            stream2.Through("topic-6");

            ProcessorTopology processorTopology = TopologyWrapper.getInternalTopologyBuilder(builder.Build()).setApplicationId("X").Build(null);
            Assert.Equal(typeof(processorTopology.source("topic-6").getTimestampExtractor(), instanceOf(FailOnInvalidTimestamp)));
            Assert.Null(processorTopology.source("topic-4").getTimestampExtractor());
            Assert.Null(processorTopology.source("topic-3").getTimestampExtractor());
            Assert.Null(processorTopology.source("topic-2").getTimestampExtractor());
            Assert.Null(processorTopology.source("topic-1").getTimestampExtractor());
        }

        [Fact]
        public void shouldSendDataThroughTopicUsingProduced()
        {
            StreamsBuilder builder = new StreamsBuilder();
            string input = "topic";
            IKStream<string, string> stream = builder.Stream(input, stringConsumed);
            stream.Through("through-topic", Produced.With(Serdes.String(), Serdes.String())).Process(processorSupplier);

            try
            {
                var driver = new TopologyTestDriver(builder.Build(), props);
                driver.PipeInput(recordFactory.Create(input, "a", "b"));
            }
    Assert.Equal(processorSupplier.theCapturedProcessor().processed, (Collections.singletonList(new KeyValueTimestamp<>("a", "b", 0))));
        }

        [Fact]
        public void shouldSendDataToTopicUsingProduced()
        {
            StreamsBuilder builder = new StreamsBuilder();
            string input = "topic";
            IKStream<string, string> stream = builder.Stream(input, stringConsumed);
            stream.To("to-topic", Produced.With(Serdes.String(), Serdes.String()));
            builder.Stream("to-topic", stringConsumed).Process(processorSupplier);

            try
            {
                var driver = new TopologyTestDriver(builder.Build(), props);
                driver.PipeInput(recordFactory.Create(input, "e", "f"));
            }
    Assert.Equal(processorSupplier.theCapturedProcessor().processed, (Collections.singletonList(new KeyValueTimestamp<>("e", "f", 0))));
        }

        [Fact]
        public void shouldSendDataToDynamicTopics()
        {
            StreamsBuilder builder = new StreamsBuilder();
            string input = "topic";
            IKStream<string, string> stream = builder.Stream(input, stringConsumed);
            stream.To((key, value, context) => context.Topic + "-" + key + "-" + value.substring(0, 1),
                      Produced.With(Serdes.String(), Serdes.String()));
            builder.Stream(input + "-a-v", stringConsumed).Process(processorSupplier);
            builder.Stream(input + "-b-v", stringConsumed).Process(processorSupplier);

            try
            {
                var driver = new TopologyTestDriver(builder.Build(), props);
                driver.PipeInput(recordFactory.Create(input, "a", "v1"));
                driver.PipeInput(recordFactory.Create(input, "a", "v2"));
                driver.PipeInput(recordFactory.Create(input, "b", "v1"));
            }
    List<MockProcessor<string, string>> mockProcessors = processorSupplier.capturedProcessors(2);
            Assert.Equal(mockProcessors.Get(0).processed, equalToasList(new KeyValueTimestamp<>("a", "v1", 0),
                    new KeyValueTimestamp<>("a", "v2", 0))));
            Assert.Equal(mockProcessors.Get(1).processed, (Collections.singletonList(new KeyValueTimestamp<>("b", "v1", 0))));
        }

        // specifically testing the deprecated variant
        [Fact]
        public void shouldUseRecordMetadataTimestampExtractorWhenInternalRepartitioningTopicCreatedWithRetention()
        {
            StreamsBuilder builder = new StreamsBuilder();
            IKStream<string, string> kStream = builder.Stream("topic-1", stringConsumed);
            ValueJoiner<string, string, string> valueJoiner = MockValueJoiner.instance(":");
            long windowSize = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
            IKStream<string, string> stream = kStream
                .map((key, value) => KeyValuePair.Create(value, value));
            stream.Join(kStream,
                        valueJoiner,
                        JoinWindows.of(TimeSpan.FromMilliseconds(windowSize)).grace(TimeSpan.FromMilliseconds(3 * windowSize)),
                        Joined.with(Serdes.String(),
                                    Serdes.String(),
                                    Serdes.String()))
                  .To("output-topic", Produced.With(Serdes.String(), Serdes.String()));

            ProcessorTopology topology = TopologyWrapper.getInternalTopologyBuilder(builder.Build()).setApplicationId("X").Build();

            SourceNode originalSourceNode = topology.source("topic-1");

            foreach (SourceNode sourceNode in topology.sources())
            {
                if (sourceNode.Name().equals(originalSourceNode.Name()))
                {
                    Assert.Null(sourceNode.getTimestampExtractor());
                }
                else
                {
                    Assert.Equal(typeof(sourceNode.getTimestampExtractor(), instanceOf(FailOnInvalidTimestamp)));
                }
            }
        }

        [Fact]
        public void shouldUseRecordMetadataTimestampExtractorWhenInternalRepartitioningTopicCreated()
        {
            StreamsBuilder builder = new StreamsBuilder();
            IKStream<string, string> kStream = builder.Stream("topic-1", stringConsumed);
            ValueJoiner<string, string, string> valueJoiner = MockValueJoiner.instance(":");
            long windowSize = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
            IKStream<string, string> stream = kStream
                .map((key, value) => KeyValuePair.Create(value, value));
            stream.Join(
                kStream,
                valueJoiner,
                JoinWindows.of(TimeSpan.FromMilliseconds(windowSize)).grace(TimeSpan.FromMilliseconds(3L * windowSize)),
                Joined.with(Serdes.String(), Serdes.String(), Serdes.String())
            )
                  .To("output-topic", Produced.With(Serdes.String(), Serdes.String()));

            ProcessorTopology topology = TopologyWrapper.getInternalTopologyBuilder(builder.Build()).setApplicationId("X").Build();

            SourceNode originalSourceNode = topology.source("topic-1");

            foreach (SourceNode sourceNode in topology.sources())
            {
                if (sourceNode.Name().equals(originalSourceNode.Name()))
                {
                    Assert.Null(sourceNode.getTimestampExtractor());
                }
                else
                {
                    Assert.Equal(typeof(sourceNode.getTimestampExtractor(), instanceOf(FailOnInvalidTimestamp)));
                }
            }
        }

        [Fact]
        public void shouldPropagateRepartitionFlagAfterGlobalKTableJoin()
        {
            StreamsBuilder builder = new StreamsBuilder();
            IGlobalKTable<string, string> globalKTable = builder.GlobalTable("globalTopic");
            IKeyValueMapper<string, string, string> kvMappper = (k, v) => k + v;
            ValueJoiner<string, string, string> valueJoiner = (v1, v2) => v1 + v2;
            builder.< string, string> stream("topic").selectKey((k, v) => v)
                 .Join(globalKTable, kvMappper, valueJoiner)
                 .groupByKey()
                 .count();

            Regex repartitionTopicPattern = new Regex("Sink: .*-repartition", RegexOptions.Compiled);
            string topology = builder.Build().describe().ToString();
            Matcher matcher = repartitionTopicPattern.matcher(topology);
            Assert.True(matcher.find());
            string match = matcher.group();
            Assert.Equal(match, notNullValue());
            Assert.True(match.endsWith("repartition"));
        }

        [Fact]
        public void testToWithNullValueSerdeDoesntNPE()
        {
            StreamsBuilder builder = new StreamsBuilder();
            Consumed<string, string> consumed = Consumed.With(Serdes.String(), Serdes.String());
            IKStream<string, string> inputStream = builder.Stream(Collections.singleton("input"), consumed);

            inputStream.To("output", Produced.With(Serdes.String(), Serdes.String()));
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullPredicateOnFilter()
        {
            testStream.filter(null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullPredicateOnFilterNot()
        {
            testStream.filterNot(null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullMapperOnSelectKey()
        {
            testStream.selectKey(null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullMapperOnMap()
        {
            testStream.map(null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullMapperOnMapValues()
        {
            testStream.mapValues((ValueMapper)null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullMapperOnMapValuesWithKey()
        {
            testStream.mapValues((ValueMapperWithKey)null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullMapperOnFlatMap()
        {
            testStream.flatMap(null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullMapperOnFlatMapValues()
        {
            testStream.flatMapValues((ValueMapper <? super string, ? : Iterable <? : string >>) null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullMapperOnFlatMapValuesWithKey()
        {
            testStream.flatMapValues((ValueMapperWithKey <? super string, ? super string, ? : Iterable <? : string >>) null);
        }

        [Fact] // (typeof(expected = ArgumentException))
        public void shouldHaveAtL.AstOnPredicateWhenBranching()
        {
            testStream.branch();
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldCantHaveNullPredicate()
        {
            testStream.branch((Predicate)null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullTopicOnThrough()
        {
            testStream.Through(null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullTopicOnTo()
        {
            testStream.To((string)null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullTopicChooserOnTo()
        {
            testStream.To((TopicNameExtractor<string, string>)null);
        }

        [Fact]
        public void shouldNotAllowNullTransformerSupplierOnTransform()
        {
            Exception e = Assert.Throws<NullReferenceException>(() => testStream.transform(null));
            Assert.Equal("transformerSupplier can't be null", e.ToString());
        }

        [Fact]
        public void shouldNotAllowNullTransformerSupplierOnFlatTransform()
        {
            Exception e = Assert.Throws<NullReferenceException>(() => testStream.flatTransform(null));
            Assert.Equal("transformerSupplier can't be null", e.ToString());
        }

        [Fact]
        public void shouldNotAllowNullValueTransformerWithKeySupplierOnTransformValues()
        {
            Exception e =

               Assert.Throws<NullReferenceException>(() => testStream.transformValues((ValueTransformerWithKeySupplier)null));
            Assert.Equal("valueTransformerSupplier can't be null", e.ToString());
        }

        [Fact]
        public void shouldNotAllowNullValueTransformerSupplierOnTransformValues()
        {
            Exception e =

               Assert.Throws<NullReferenceException>(() => testStream.transformValues((ValueTransformerSupplier)null));
            Assert.Equal("valueTransformerSupplier can't be null", e.ToString());
        }

        [Fact]
        public void shouldNotAllowNullValueTransformerWithKeySupplierOnFlatTransformValues()
        {
            Exception e =

               Assert.Throws<NullReferenceException>(() => testStream.flatTransformValues((ValueTransformerWithKeySupplier)null));
            Assert.Equal("valueTransformerSupplier can't be null", e.ToString());
        }

        [Fact]
        public void shouldNotAllowNullValueTransformerSupplierOnFlatTransformValues()
        {
            Exception e =

               Assert.Throws<NullReferenceException>(() => testStream.flatTransformValues((ValueTransformerSupplier)null));
            Assert.Equal("valueTransformerSupplier can't be null", e.ToString());
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullProcessSupplier()
        {
            testStream.Process(null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullOtherStreamOnJoin()
        {
            testStream.Join(null, MockValueJoiner.TOSTRING_JOINER, JoinWindows.of(TimeSpan.FromMilliseconds(10)));
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullValueJoinerOnJoin()
        {
            testStream.Join(testStream, null, JoinWindows.of(TimeSpan.FromMilliseconds(10)));
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullJoinWindowsOnJoin()
        {
            testStream.Join(testStream, MockValueJoiner.TOSTRING_JOINER, null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullTableOnTableJoin()
        {
            testStream.LeftJoin((IKTable)null, MockValueJoiner.TOSTRING_JOINER);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullValueMapperOnTableJoin()
        {
            testStream.LeftJoin(builder.Table("topic", stringConsumed), null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullSelectorOnGroupBy()
        {
            testStream.GroupBy(null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullActionOnForEach()
        {
            testStream.ForEach(null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullTableOnJoinWithGlobalTable()
        {
            testStream.Join((GlobalKTable)null,
                            MockMapper.selectValueMapper(),
                            MockValueJoiner.TOSTRING_JOINER);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullMapperOnJoinWithGlobalTable()
        {
            testStream.Join(builder.GlobalTable("global", stringConsumed),
                            null,
                            MockValueJoiner.TOSTRING_JOINER);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullJoinerOnJoinWithGlobalTable()
        {
            testStream.Join(builder.GlobalTable("global", stringConsumed),
                            MockMapper.selectValueMapper(),
                            null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullTableOnJLeftJoinWithGlobalTable()
        {
            testStream.LeftJoin((GlobalKTable)null,
                            MockMapper.selectValueMapper(),
                            MockValueJoiner.TOSTRING_JOINER);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullMapperOnLeftJoinWithGlobalTable()
        {
            testStream.LeftJoin(builder.GlobalTable("global", stringConsumed),
                            null,
                            MockValueJoiner.TOSTRING_JOINER);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullJoinerOnLeftJoinWithGlobalTable()
        {
            testStream.LeftJoin(builder.GlobalTable("global", stringConsumed),
                            MockMapper.selectValueMapper(),
                            null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldThrowNullPointerOnPrintIfPrintedIsNull()
        {
            testStream.Print(null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldThrowNullPointerOnThroughWhenProducedIsNull()
        {
            testStream.Through("topic", null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldThrowNullPointerOnToWhenProducedIsNull()
        {
            testStream.To("topic", null);
        }

        [Fact]
        public void shouldThrowNullPointerOnLeftJoinWithTableWhenJoinedIsNull()
        {
            IKTable<string, string> table = builder.Table("blah", stringConsumed);
            try
            {
                testStream.LeftJoin(table,
                                    MockValueJoiner.TOSTRING_JOINER,
                                    null);
                Assert.False(true, "Should have thrown NullPointerException");
            }
            catch (NullReferenceException e)
            {
                // ok
            }
        }

        [Fact]
        public void shouldThrowNullPointerOnJoinWithTableWhenJoinedIsNull()
        {
            IKTable<string, string> table = builder.Table("blah", stringConsumed);
            try
            {
                testStream.Join(table,
                                MockValueJoiner.TOSTRING_JOINER,
                                null);
                Assert.False(true, "Should have thrown NullPointerException");
            }
            catch (NullPointerException e)
            {
                // ok
            }
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldThrowNullPointerOnJoinWithStreamWhenJoinedIsNull()
        {
            testStream.Join(testStream, MockValueJoiner.TOSTRING_JOINER, JoinWindows.of(TimeSpan.FromMilliseconds(10)), null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldThrowNullPointerOnOuterJoinJoinedIsNull()
        {
            testStream.OuterJoin(testStream, MockValueJoiner.TOSTRING_JOINER, JoinWindows.of(TimeSpan.FromMilliseconds(10)), null);
        }

        [Fact]
        public void shouldMergeTwoStreams()
        {
            string topic1 = "topic-1";
            string topic2 = "topic-2";

            IKStream<string, string> source1 = builder.Stream(topic1);
            IKStream<string, string> source2 = builder.Stream(topic2);
            IKStream<string, string> merged = source1.Merge(source2);

            merged.Process(processorSupplier);

            try
            {
                var driver = new TopologyTestDriver(builder.Build(), props);
                driver.PipeInput(recordFactory.Create(topic1, "A", "aa"));
                driver.PipeInput(recordFactory.Create(topic2, "B", "bb"));
                driver.PipeInput(recordFactory.Create(topic2, "C", "cc"));
                driver.PipeInput(recordFactory.Create(topic1, "D", "dd"));
            }

    Assert.EqualasList(new KeyValueTimestamp<>("A", "aa", 0),
             new KeyValueTimestamp<>("B", "bb", 0),
             new KeyValueTimestamp<>("C", "cc", 0),
             new KeyValueTimestamp<>("D", "dd", 0)), processorSupplier.theCapturedProcessor().processed);
        }

        [Fact]
        public void shouldMergeMultipleStreams()
        {
            string topic1 = "topic-1";
            string topic2 = "topic-2";
            string topic3 = "topic-3";
            string topic4 = "topic-4";

            IKStream<string, string> source1 = builder.Stream(topic1);
            IKStream<string, string> source2 = builder.Stream(topic2);
            IKStream<string, string> source3 = builder.Stream(topic3);
            IKStream<string, string> source4 = builder.Stream(topic4);
            IKStream<string, string> merged = source1.Merge(source2).Merge(source3).Merge(source4);

            merged.Process(processorSupplier);

            var driver = new TopologyTestDriver(builder.Build(), props);
            driver.PipeInput(recordFactory.Create(topic1, "A", "aa", 1L));
            driver.PipeInput(recordFactory.Create(topic2, "B", "bb", 9L));
            driver.PipeInput(recordFactory.Create(topic3, "C", "cc", 2L));
            driver.PipeInput(recordFactory.Create(topic4, "D", "dd", 8L));
            driver.PipeInput(recordFactory.Create(topic4, "E", "ee", 3L));
            driver.PipeInput(recordFactory.Create(topic3, "F", "ff", 7L));
            driver.PipeInput(recordFactory.Create(topic2, "G", "gg", 4L));
            driver.PipeInput(recordFactory.Create(topic1, "H", "hh", 6L));

            Assert.EqualasList(new KeyValueTimestamp<>("A", "aa", 1),
                     new KeyValueTimestamp<>("B", "bb", 9),
                     new KeyValueTimestamp<>("C", "cc", 2),
                     new KeyValueTimestamp<>("D", "dd", 8),
                     new KeyValueTimestamp<>("E", "ee", 3),
                     new KeyValueTimestamp<>("F", "ff", 7),
                     new KeyValueTimestamp<>("G", "gg", 4),
                     new KeyValueTimestamp<>("H", "hh", 6)),
                     processorSupplier.theCapturedProcessor().processed);
        }

        [Fact]
        public void shouldProcessFromSourceThatMatchPattern()
        {
            IKStream<string, string> pattern2Source = builder.Stream(new Regex("topic-\\d", RegexOptions.Compiled));

            pattern2Source.Process(processorSupplier);

            try
            {
                var driver = new TopologyTestDriver(builder.Build(), props);
                driver.PipeInput(recordFactory.Create("topic-3", "A", "aa", 1L));
                driver.PipeInput(recordFactory.Create("topic-4", "B", "bb", 5L));
                driver.PipeInput(recordFactory.Create("topic-5", "C", "cc", 10L));
                driver.PipeInput(recordFactory.Create("topic-6", "D", "dd", 8L));
                driver.PipeInput(recordFactory.Create("topic-7", "E", "ee", 3L));
            }

    Assert.EqualasList(new KeyValueTimestamp<>("A", "aa", 1),
             new KeyValueTimestamp<>("B", "bb", 5),
             new KeyValueTimestamp<>("C", "cc", 10),
             new KeyValueTimestamp<>("D", "dd", 8),
             new KeyValueTimestamp<>("E", "ee", 3)),
                processorSupplier.theCapturedProcessor().processed);
        }

        [Fact]
        public void shouldProcessFromSourcesThatMatchMultiplePattern()
        {
            string topic3 = "topic-without-pattern";

            IKStream<string, string> pattern2Source1 = builder.Stream(new Regex("topic-\\d", RegexOptions.Compiled));
            IKStream<string, string> pattern2Source2 = builder.Stream(new Regex("topic-[A-Z]", RegexOptions.Compiled));
            IKStream<string, string> source3 = builder.Stream(topic3);
            IKStream<string, string> merged = pattern2Source1.Merge(pattern2Source2).Merge(source3);

            merged.Process(processorSupplier);

            try
            {
                var driver = new TopologyTestDriver(builder.Build(), props);
                driver.PipeInput(recordFactory.Create("topic-3", "A", "aa", 1L));
                driver.PipeInput(recordFactory.Create("topic-4", "B", "bb", 5L));
                driver.PipeInput(recordFactory.Create("topic-A", "C", "cc", 10L));
                driver.PipeInput(recordFactory.Create("topic-Z", "D", "dd", 8L));
                driver.PipeInput(recordFactory.Create(topic3, "E", "ee", 3L));
            }

    Assert.EqualasList(new KeyValueTimestamp<>("A", "aa", 1),
             new KeyValueTimestamp<>("B", "bb", 5),
             new KeyValueTimestamp<>("C", "cc", 10),
             new KeyValueTimestamp<>("D", "dd", 8),
             new KeyValueTimestamp<>("E", "ee", 3)),
                processorSupplier.theCapturedProcessor().processed);
        }
    }
}
