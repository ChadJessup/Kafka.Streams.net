using Kafka.Common;
using Kafka.Streams.Configs;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Temporary;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Integration;
using Kafka.Streams.Tests.Mocks;
using System;
using System.Collections.Generic;
using System.Linq;
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
        public void ShouldPreserveSerdesForOperators()
        {
            StreamsBuilder builder = new StreamsBuilder();
            IKStream<string, string> stream1 = builder.Stream(Collections.singleton("topic-1"), stringConsumed);
            IKTable<string, string> table1 = builder.Table("topic-2", stringConsumed);
            IGlobalKTable<string, string> table2 = builder.GlobalTable("topic-2", stringConsumed);
            ConsumedInternal<string, string> consumedInternal = new ConsumedInternal<string, string>(stringConsumed);

            KeyValueMapper<string, string, string> selector = (key, value) => key;
            KeyValueMapper<string, string, IEnumerable<KeyValuePair<string, string>>> flatSelector = (key, value) => Collections.singleton(KeyValuePair.Create(key, value));
            ValueMapper<string, string> mapper = value => value;
            ValueMapper<string, IEnumerable<string>> flatMapper = Collections.singleton;

            ValueJoiner<string, string, string> joiner = (value1, value2) => value1;
            ITransformerSupplier<string, string, KeyValuePair<string, string>> transformerSupplier = new Transformer<string, string, KeyValuePair<string, string>>();
            IValueTransformerSupplier<string, string> valueTransformerSupplier = new ValueTransformer<string, string>();

            Assert.Equal(((AbstractStream<string, string>)stream1.Filter((key, value) => false)).KeySerde, consumedInternal.KeySerde);
            Assert.Equal(((AbstractStream<string, string>)stream1.Filter((key, value) => false)).ValueSerde, consumedInternal.ValueSerde);

            Assert.Equal(((AbstractStream<string, string>)stream1.FilterNot((key, value) => false)).KeySerde, consumedInternal.KeySerde);
            Assert.Equal(((AbstractStream<string, string>)stream1.FilterNot((key, value) => false)).ValueSerde, consumedInternal.ValueSerde);

            Assert.Null(((AbstractStream<string, string>)stream1.SelectKey(selector)).KeySerde);
            Assert.Equal(((AbstractStream<string, string>)stream1.SelectKey(selector)).ValueSerde, consumedInternal.ValueSerde);

            Assert.Null(((AbstractStream<string, string>)stream1.Map(KeyValuePair.Create)).KeySerde);
            Assert.Null(((AbstractStream<string, string>)stream1.Map(KeyValuePair.Create)).ValueSerde);

            Assert.Equal(((AbstractStream<string, string>)stream1.MapValues(mapper)).KeySerde, consumedInternal.KeySerde);
            Assert.Null(((AbstractStream<string, string>)stream1.MapValues(mapper)).ValueSerde);

            Assert.Null(((AbstractStream<string, string>)stream1.FlatMap(flatSelector)).KeySerde);
            Assert.Null(((AbstractStream<string, string>)stream1.FlatMap(flatSelector)).ValueSerde);

            Assert.Equal(((AbstractStream<string, string>)stream1.FlatMapValues(flatMapper)).KeySerde, consumedInternal.KeySerde);
            Assert.Null(((AbstractStream<string, string>)stream1.FlatMapValues(flatMapper)).ValueSerde);

            Assert.Null(((AbstractStream<string, string>)stream1.Transform(transformerSupplier)).KeySerde);
            Assert.Null(((AbstractStream<string, string>)stream1.Transform(transformerSupplier)).ValueSerde);

            Assert.Equal(((AbstractStream<string, string>)stream1.TransformValues(valueTransformerSupplier)).KeySerde, consumedInternal.KeySerde);
            Assert.Null(((AbstractStream<string, string>)stream1.TransformValues(valueTransformerSupplier)).ValueSerde);

            Assert.Null(((AbstractStream<string, string>)stream1.Merge(stream1)).KeySerde);
            Assert.Null(((AbstractStream<string, string>)stream1.Merge(stream1)).ValueSerde);

            Assert.Equal(((AbstractStream<string, string>)stream1.Through("topic-3")).KeySerde, consumedInternal.KeySerde);
            Assert.Equal(((AbstractStream<string, string>)stream1.Through("topic-3")).ValueSerde, consumedInternal.ValueSerde);
            Assert.Equal(((AbstractStream<string, string>)stream1.Through("topic-3", Produced.With(mySerde, mySerde))).KeySerde, mySerde);
            Assert.Equal(((AbstractStream<string, string>)stream1.Through("topic-3", Produced.With(mySerde, mySerde))).ValueSerde, mySerde);

            Assert.Equal(((AbstractStream<string, string>)stream1.GroupByKey()).KeySerde, consumedInternal.KeySerde);
            Assert.Equal(((AbstractStream<string, string>)stream1.GroupByKey()).ValueSerde, consumedInternal.ValueSerde);
            Assert.Equal(((AbstractStream<string, string>)stream1.GroupByKey(Grouped.With(mySerde, mySerde))).KeySerde, mySerde);
            Assert.Equal(((AbstractStream<string, string>)stream1.GroupByKey(Grouped.With(mySerde, mySerde))).ValueSerde, mySerde);

            Assert.Null(((AbstractStream<string, string>)stream1.GroupBy(selector)).KeySerde);
            Assert.Equal(((AbstractStream<string, string>)stream1.GroupBy(selector)).ValueSerde, consumedInternal.ValueSerde);
            Assert.Equal(((AbstractStream<string, string>)stream1.GroupBy(selector, Grouped.With(mySerde, mySerde))).KeySerde, mySerde);
            Assert.Equal(((AbstractStream<string, string>)stream1.GroupBy(selector, Grouped.With(mySerde, mySerde))).ValueSerde, mySerde);

            Assert.Null(((AbstractStream<string, string>)stream1.Join(stream1, joiner, JoinWindows.Of(TimeSpan.FromMilliseconds(100L)))).KeySerde);
            Assert.Null(((AbstractStream<string, string>)stream1.Join(stream1, joiner, JoinWindows.Of(TimeSpan.FromMilliseconds(100L)))).ValueSerde);
            Assert.Equal(((AbstractStream<string, string>)stream1.Join(stream1, joiner, JoinWindows.Of(TimeSpan.FromMilliseconds(100L)), Joined.With(mySerde, mySerde, mySerde))).KeySerde, mySerde);
            Assert.Null(((AbstractStream<string, string>)stream1.Join(stream1, joiner, JoinWindows.Of(TimeSpan.FromMilliseconds(100L)), Joined.With(mySerde, mySerde, mySerde))).ValueSerde);

            Assert.Null(((AbstractStream<string, string>)stream1.LeftJoin(stream1, joiner, JoinWindows.Of(TimeSpan.FromMilliseconds(100L)))).KeySerde);
            Assert.Null(((AbstractStream<string, string>)stream1.LeftJoin(stream1, joiner, JoinWindows.Of(TimeSpan.FromMilliseconds(100L)))).ValueSerde);
            Assert.Equal(((AbstractStream<string, string>)stream1.LeftJoin(stream1, joiner, JoinWindows.Of(TimeSpan.FromMilliseconds(100L)), Joined.With(mySerde, mySerde, mySerde))).KeySerde, mySerde);
            Assert.Null(((AbstractStream<string, string>)stream1.LeftJoin(stream1, joiner, JoinWindows.Of(TimeSpan.FromMilliseconds(100L)), Joined.With(mySerde, mySerde, mySerde))).ValueSerde);

            Assert.Null(((AbstractStream<string, string>)stream1.OuterJoin(stream1, joiner, JoinWindows.Of(TimeSpan.FromMilliseconds(100L)))).KeySerde);
            Assert.Null(((AbstractStream<string, string>)stream1.OuterJoin(stream1, joiner, JoinWindows.Of(TimeSpan.FromMilliseconds(100L)))).ValueSerde);
            Assert.Equal(((AbstractStream<string, string>)stream1.OuterJoin(stream1, joiner, JoinWindows.Of(TimeSpan.FromMilliseconds(100L)), Joined.With(mySerde, mySerde, mySerde))).KeySerde, mySerde);
            Assert.Null(((AbstractStream<string, string>)stream1.OuterJoin(stream1, joiner, JoinWindows.Of(TimeSpan.FromMilliseconds(100L)), Joined.With(mySerde, mySerde, mySerde))).ValueSerde);

            Assert.Equal(((AbstractStream<string, string>)stream1.Join(table1, joiner)).KeySerde, consumedInternal.KeySerde);
            Assert.Null(((AbstractStream<string, string>)stream1.Join(table1, joiner)).ValueSerde);
            Assert.Equal(((AbstractStream<string, string>)stream1.Join(table1, joiner, Joined.With(mySerde, mySerde, mySerde))).KeySerde, mySerde);
            Assert.Null(((AbstractStream<string, string>)stream1.Join(table1, joiner, Joined.With(mySerde, mySerde, mySerde))).ValueSerde);

            Assert.Equal(((AbstractStream<string, string>)stream1.LeftJoin(table1, joiner)).KeySerde, consumedInternal.KeySerde);
            Assert.Null(((AbstractStream<string, string>)stream1.LeftJoin(table1, joiner)).ValueSerde);
            Assert.Equal(((AbstractStream<string, string>)stream1.LeftJoin(table1, joiner, Joined.With(mySerde, mySerde, mySerde))).KeySerde, mySerde);
            Assert.Null(((AbstractStream<string, string>)stream1.LeftJoin(table1, joiner, Joined.With(mySerde, mySerde, mySerde))).ValueSerde);

            Assert.Equal(((AbstractStream<string, string>)stream1.Join(table2, selector, joiner)).KeySerde, consumedInternal.KeySerde);
            Assert.Null(((AbstractStream<string, string>)stream1.Join(table2, selector, joiner)).ValueSerde);

            Assert.Equal(((AbstractStream<string, string>)stream1.LeftJoin(table2, selector, joiner)).KeySerde, consumedInternal.KeySerde);
            Assert.Null(((AbstractStream<string, string>)stream1.LeftJoin(table2, selector, joiner)).ValueSerde);
        }

        [Fact]
        public void ShouldUseRecordMetadataTimestampExtractorWithThrough()
        {
            StreamsBuilder builder = new StreamsBuilder();
            IKStream<string, string> stream1 = builder.Stream(new List<string> { "topic-1", "topic-2" }, stringConsumed);
            IKStream<string, string> stream2 = builder.Stream(new List<string> { "topic-3", "topic-4" }, stringConsumed);

            stream1.To("topic-5");
            stream2.Through("topic-6");

            ProcessorTopology processorTopology = TopologyWrapper.getInternalTopologyBuilder(builder.Build()).setApplicationId("X").Build(null);
            Assert.IsAssignableFrom<FailOnInvalidTimestamp>(processorTopology.Source("topic-6").TimestampExtractor.GetType());
            Assert.Null(processorTopology.Source("topic-4").TimestampExtractor);
            Assert.Null(processorTopology.Source("topic-3").TimestampExtractor);
            Assert.Null(processorTopology.Source("topic-2").TimestampExtractor);
            Assert.Null(processorTopology.Source("topic-1").TimestampExtractor);
        }

        [Fact]
        public void shouldSendDataThroughTopicUsingProduced()
        {
            StreamsBuilder builder = new StreamsBuilder();
            string input = "topic";
            IKStream<string, string> stream = builder.Stream(input, stringConsumed);
            stream.Through("through-topic", Produced.With(Serdes.String(), Serdes.String()).Process(processorSupplier));

            var driver = new TopologyTestDriver(builder.Build(), props);
            driver.PipeInput(recordFactory.Create(input, "a", "b"));
            Assert.Equal(processorSupplier.TheCapturedProcessor().processed, Collections.singletonList(new KeyValueTimestamp<string, string>("a", "b", 0)));
        }

        [Fact]
        public void shouldSendDataToTopicUsingProduced()
        {
            StreamsBuilder builder = new StreamsBuilder();
            string input = "topic";
            IKStream<string, string> stream = builder.Stream(input, stringConsumed);
            stream.To("to-topic", Produced.With(Serdes.String(), Serdes.String()));
            builder.Stream("to-topic", stringConsumed).Process(processorSupplier);

            var driver = new TopologyTestDriver(builder.Build(), props);
            driver.PipeInput(recordFactory.Create(input, "e", "f"));
            Assert.Equal(processorSupplier.TheCapturedProcessor().processed, (Collections.singletonList(new KeyValueTimestamp<string, string>("e", "f", 0))));
        }

        [Fact]
        public void ShouldSendDataToDynamicTopics()
        {
            StreamsBuilder builder = new StreamsBuilder();
            string input = "topic";
            IKStream<string, string> stream = builder.Stream(input, stringConsumed);
            stream.To((key, value, context) => context.Topic + "-" + key + "-" + value.Substring(0, 1),
                      Produced.With(Serdes.String(), Serdes.String()));

            builder.Stream(input + "-a-v", stringConsumed).Process(processorSupplier);
            builder.Stream(input + "-b-v", stringConsumed).Process(processorSupplier);

            var driver = new TopologyTestDriver(builder.Build(), props);
            driver.PipeInput(recordFactory.Create(input, "a", "v1"));
            driver.PipeInput(recordFactory.Create(input, "a", "v2"));
            driver.PipeInput(recordFactory.Create(input, "b", "v1"));

            List<MockProcessor<string, string>> mockProcessors = processorSupplier.CapturedProcessors(2);
            Assert.Equal(mockProcessors.ElementAt(0).processed, Arrays.asList(new KeyValueTimestamp<string, string>("a", "v1", 0),
                    new KeyValueTimestamp<string, string>("a", "v2", 0)));
            Assert.Equal(mockProcessors.ElementAt(1).processed, (Collections.singletonList(new KeyValueTimestamp<string, string>("b", "v1", 0))));
        }

        // specifically testing the deprecated variant
        [Fact]
        public void shouldUseRecordMetadataTimestampExtractorWhenInternalRepartitioningTopicCreatedWithRetention()
        {
            StreamsBuilder builder = new StreamsBuilder();
            IKStream<string, string> kStream = builder.Stream("topic-1", stringConsumed);
            ValueJoiner<string, string, string> valueJoiner = MockValueJoiner.Instance(":");
            long windowSize = TimeUnit.MILLISECONDS.Convert(1, TimeUnit.DAYS);
            IKStream<string, string> stream = kStream
                .Map((key, value) => KeyValuePair.Create(value, value));
            stream.Join(kStream,
                        valueJoiner,
                        JoinWindows.Of(TimeSpan.FromMilliseconds(windowSize)).Grace(TimeSpan.FromMilliseconds(3 * windowSize)),
                        Joined.With(Serdes.String(),
                                    Serdes.String(),
                                    Serdes.String()))
                  .To("output-topic", Produced.With(Serdes.String(), Serdes.String()));

            ProcessorTopology topology = TopologyWrapper.getInternalTopologyBuilder(builder.Build()).setApplicationId("X").Build();

            ISourceNode originalSourceNode = topology.Source("topic-1");

            foreach (SourceNode sourceNode in topology.Sources())
            {
                if (sourceNode.Name.Equals(originalSourceNode.Name))
                {
                    Assert.Null(sourceNode.TimestampExtractor);
                }
                else
                {
                    Assert.IsAssignableFrom<FailOnInvalidTimestamp>(sourceNode.TimestampExtractor);
                }
            }
        }

        [Fact]
        public void shouldUseRecordMetadataTimestampExtractorWhenInternalRepartitioningTopicCreated()
        {
            StreamsBuilder builder = new StreamsBuilder();
            IKStream<string, string> kStream = builder.Stream("topic-1", stringConsumed);
            ValueJoiner<string, string, string> valueJoiner = MockValueJoiner.Instance(":");
            long windowSize = TimeUnit.MILLISECONDS.Convert(1, TimeUnit.DAYS);
            IKStream<string, string> stream = kStream
                .Map((key, value) => KeyValuePair.Create(value, value));
            stream.Join(
                kStream,
                valueJoiner,
                JoinWindows.Of(TimeSpan.FromMilliseconds(windowSize)).Grace(TimeSpan.FromMilliseconds(3L * windowSize)),
                Joined.With(Serdes.String(), Serdes.String(), Serdes.String())
            )
                  .To("output-topic", Produced.With(Serdes.String(), Serdes.String()));

            ProcessorTopology topology = TopologyWrapper.getInternalTopologyBuilder(builder.Build())
                .SetApplicationId("X")
                .Build();

            ISourceNode originalSourceNode = topology.Source("topic-1");

            foreach (ISourceNode sourceNode in topology.Sources())
            {
                if (sourceNode.Name.Equals(originalSourceNode.Name))
                {
                    Assert.Null(sourceNode.TimestampExtractor);
                }
                else
                {
                    Assert.IsAssignableFrom<FailOnInvalidTimestamp>(sourceNode.TimestampExtractor.GetType());
                }
            }
        }

        [Fact]
        public void shouldPropagateRepartitionFlagAfterGlobalKTableJoin()
        {
            StreamsBuilder builder = new StreamsBuilder();
            IGlobalKTable<string, string> globalKTable = builder.GlobalTable<string, string>("globalTopic");
            KeyValueMapper<string, string, string> kvMappper = (k, v) => k + v;
            ValueJoiner<string, string, string> valueJoiner = (v1, v2) => v1 + v2;
            builder.Stream<string, string>("topic").SelectKey((k, v) => v)
                 .Join(globalKTable, kvMappper, valueJoiner)
                 .GroupByKey()
                 .Count();

            Regex repartitionTopicPattern = new Regex("Sink: .*-repartition", RegexOptions.Compiled);
            string topology = builder.Build().Describe().ToString();
            var matcher = repartitionTopicPattern.Matches(topology);
            Assert.True(matcher.Any());
            string match = matcher[0].Value;
            Assert.NotNull(match);
            Assert.EndsWith("repartition", match);
        }

        [Fact]
        public void testToWithNullValueSerdeDoesntNPE()
        {
            StreamsBuilder builder = new StreamsBuilder();
            Consumed<string, string> consumed = Consumed.With(Serdes.String(), Serdes.String());
            IKStream<string, string> inputStream = builder.Stream(Collections.singleton("input"), consumed);

            inputStream.To("output", Produced.With(Serdes.String(), Serdes.String()));
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullPredicateOnFilter()
        {
            testStream.Filter(null);
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullPredicateOnFilterNot()
        {
            testStream.FilterNot(null);
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullMapperOnSelectKey()
        {
            testStream.SelectKey<string>(null);
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullMapperOnMap()
        {
            testStream.Map<string, string>(null);
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void ShouldNotAllowNullMapperOnMapValues()
        {
            testStream.MapValues((ValueMapper<string, string>)null);
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullMapperOnMapValuesWithKey()
        {
            testStream.MapValues((ValueMapperWithKey<string, string, string>)null);
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullMapperOnFlatMap()
        {
            testStream.FlatMap<string, string>(null);
        }

        [Fact]
        public void ShouldNotAllowNullMapperOnFlatMapValues()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.FlatMapValues((ValueMapper<string, IEnumerable<string>>)null));
        }

        [Fact] // (typeof(expected = ArgumentException))
        public void ShouldHaveAtLeastOnPredicateWhenBranching()
        {
            Assert.Throws<ArgumentException>(() => testStream.Branch());
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void ShouldCantHaveNullPredicate()
        {
            Assert.Throws<NullReferenceException>(() => testStream.Branch(null));
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullTopicOnThrough()
        {
            testStream.Through(null);
        }

        [Fact]
        public void ShouldNotAllowNullTopicOnTo()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.To((string)null));
        }

        [Fact]
        public void ShouldNotAllowNullTopicChooserOnTo()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.To((TopicNameExtractor<string, string>)null));
        }

        [Fact]
        public void ShouldNotAllowNullTransformerSupplierOnTransform()
        {
            Exception e = Assert.Throws<NullReferenceException>(() => testStream.Transform<string, string>(null, null));
            Assert.Equal("transformerSupplier can't be null", e.ToString());
        }

        [Fact]
        public void ShouldNotAllowNullTransformerSupplierOnFlatTransform()
        {
            Exception e = Assert.Throws<NullReferenceException>(() => testStream.FlatTransform<string, string>(null));
            Assert.Equal("transformerSupplier can't be null", e.ToString());
        }

        [Fact]
        public void ShouldNotAllowNullValueTransformerWithKeySupplierOnTransformValues()
        {
            Exception e =

               Assert.Throws<NullReferenceException>(() => testStream.TransformValues((IValueTransformerWithKeySupplier<string, string, string>)null));
            Assert.Equal("valueTransformerSupplier can't be null", e.ToString());
        }

        [Fact]
        public void shouldNotAllowNullValueTransformerSupplierOnTransformValues()
        {
            Exception e =

               Assert.Throws<NullReferenceException>(() => testStream.TransformValues((IValueTransformerSupplier<string, string>)null));
            Assert.Equal("valueTransformerSupplier can't be null", e.ToString());
        }

        [Fact]
        public void shouldNotAllowNullValueTransformerWithKeySupplierOnFlatTransformValues()
        {
            Exception e =

               Assert.Throws<NullReferenceException>(() => testStream.FlatTransformValues<string>((IValueTransformerWithKeySupplier<string, string, IEnumerable<string>>)null));
            Assert.Equal("valueTransformerSupplier can't be null", e.ToString());
        }

        [Fact]
        public void shouldNotAllowNullValueTransformerSupplierOnFlatTransformValues()
        {
            Exception e =
               Assert.Throws<NullReferenceException>(() => testStream.FlatTransformValues<string>(
                   (IValueTransformerSupplier<string, IEnumerable<string>>)null));

            Assert.Equal("valueTransformerSupplier can't be null", e.ToString());
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullProcessSupplier()
        {
            testStream.Process((IProcessorSupplier<string, string>)null);
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void ShouldNotAllowNullOtherStreamOnJoin()
        {
            testStream.Join(null, MockValueJoiner.TOSTRING_JOINER(), JoinWindows.Of(TimeSpan.FromMilliseconds(10)));
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullValueJoinerOnJoin()
        {
            testStream.Join<string, string>(testStream, null, JoinWindows.Of(TimeSpan.FromMilliseconds(10)));
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void ShouldNotAllowNullJoinWindowsOnJoin()
        {
            testStream.Join(testStream, MockValueJoiner.TOSTRING_JOINER(), null);
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullTableOnTableJoin()
        {
            testStream.LeftJoin<string, string>(null, MockValueJoiner.TOSTRING_JOINER());
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullValueMapperOnTableJoin()
        {
            testStream.LeftJoin<string, string>(builder.Table("topic", stringConsumed), null);
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullSelectorOnGroupBy()
        {
            testStream.GroupBy<string>(null);
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullActionOnForEach()
        {
            testStream.ForEach(null);
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void ShouldNotAllowNullTableOnJoinWithGlobalTable()
        {
            testStream.Join<string, string, string>(
                (IGlobalKTable<string, string>)null,
                MockMapper.GetSelectValueMapper<string, string>(),
                MockValueJoiner.TOSTRING_JOINER());
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullMapperOnJoinWithGlobalTable()
        {
            testStream.Join<string, string, string>(
                builder.GlobalTable("global", stringConsumed),
                null,
                MockValueJoiner.TOSTRING_JOINER());
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullJoinerOnJoinWithGlobalTable()
        {
            testStream.Join<string, string, string>(
                builder.GlobalTable("global", stringConsumed),
                MockMapper.GetSelectValueMapper<string, string>(),
                null);
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullTableOnJLeftJoinWithGlobalTable()
        {
            testStream.LeftJoin(
                null,
                MockMapper.GetSelectValueMapper<string, string>(),
                MockValueJoiner.TOSTRING_JOINER());
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullMapperOnLeftJoinWithGlobalTable()
        {
            testStream.LeftJoin<string, string, string>(
                builder.GlobalTable("global", stringConsumed),
                null,
                MockValueJoiner.TOSTRING_JOINER());
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void ShouldNotAllowNullJoinerOnLeftJoinWithGlobalTable()
        {
            var globalTable = builder.GlobalTable("global", stringConsumed);
            testStream.LeftJoin<string, string, string>(
                globalTable,
                (K, V) => V,
                null);
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldThrowNullPointerOnPrintIfPrintedIsNull()
        {
            //testStream.Print(null);
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldThrowNullPointerOnThroughWhenProducedIsNull()
        {
            testStream.Through("topic", null);
        }

        [Fact] // (typeof(expected = NullReferenceException))
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
                testStream.LeftJoin<string, string>(
                    table,
                    MockValueJoiner.TOSTRING_JOINER(),
                    null);

                Assert.False(true, "Should have thrown NullReferenceException");
            }
            catch (NullReferenceException e)
            {
                // ok
            }
        }

        [Fact]
        public void ShouldThrowNullPointerOnJoinWithTableWhenJoinedIsNull()
        {
            IKTable<string, string> table = builder.Table("blah", stringConsumed);
            try
            {
                testStream.Join<string, string>(
                    table,
                    MockValueJoiner.TOSTRING_JOINER(),
                    null);

                Assert.False(true, "Should have thrown NullReferenceException");
            }
            catch (NullReferenceException e)
            {
                // ok
            }
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void ShouldThrowNullPointerOnJoinWithStreamWhenJoinedIsNull()
        {
            testStream.Join<string, string>(
                testStream,
                MockValueJoiner.TOSTRING_JOINER(),
                JoinWindows.Of(TimeSpan.FromMilliseconds(10)), null);
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void ShouldThrowNullPointerOnOuterJoinJoinedIsNull()
        {
            testStream.OuterJoin<string, string>(
                testStream,
                MockValueJoiner.TOSTRING_JOINER(),
                JoinWindows.Of(TimeSpan.FromMilliseconds(10)), null);
        }

        [Fact]
        public void shouldMergeTwoStreams()
        {
            string topic1 = "topic-1";
            string topic2 = "topic-2";

            IKStream<string, string> source1 = builder.Stream<string, string>(topic1);
            IKStream<string, string> source2 = builder.Stream<string, string>(topic2);
            IKStream<string, string> merged = source1.Merge(source2);

            merged.Process(processorSupplier);

            var driver = new TopologyTestDriver(builder.Build(), props);
            driver.PipeInput(recordFactory.Create(topic1, "A", "aa"));
            driver.PipeInput(recordFactory.Create(topic2, "B", "bb"));
            driver.PipeInput(recordFactory.Create(topic2, "C", "cc"));
            driver.PipeInput(recordFactory.Create(topic1, "D", "dd"));

            Assert.Equal(
                Arrays.asList(
                    new KeyValueTimestamp<string, string>("A", "aa", 0),
                    new KeyValueTimestamp<string, string>("B", "bb", 0),
                    new KeyValueTimestamp<string, string>("C", "cc", 0),
                    new KeyValueTimestamp<string, string>("D", "dd", 0)),
                processorSupplier.TheCapturedProcessor().processed);
        }

        [Fact]
        public void shouldMergeMultipleStreams()
        {
            string topic1 = "topic-1";
            string topic2 = "topic-2";
            string topic3 = "topic-3";
            string topic4 = "topic-4";

            IKStream<string, string> source1 = builder.Stream<string, string>(topic1);
            IKStream<string, string> source2 = builder.Stream<string, string>(topic2);
            IKStream<string, string> source3 = builder.Stream<string, string>(topic3);
            IKStream<string, string> source4 = builder.Stream<string, string>(topic4);
            IKStream<string, string> merged = source1
                .Merge(source2)
                .Merge(source3)
                .Merge(source4);

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

            Assert.Equal(
                Arrays.asList(
                    new KeyValueTimestamp<string, string>("A", "aa", 1),
                    new KeyValueTimestamp<string, string>("B", "bb", 9),
                    new KeyValueTimestamp<string, string>("C", "cc", 2),
                    new KeyValueTimestamp<string, string>("D", "dd", 8),
                    new KeyValueTimestamp<string, string>("E", "ee", 3),
                    new KeyValueTimestamp<string, string>("F", "ff", 7),
                    new KeyValueTimestamp<string, string>("G", "gg", 4),
                    new KeyValueTimestamp<string, string>("H", "hh", 6)),
                processorSupplier.TheCapturedProcessor().processed);
        }

        [Fact]
        public void ShouldProcessFromSourceThatMatchPattern()
        {
            IKStream<string, string> pattern2Source = builder.Stream<string, string>(new Regex("topic-\\d", RegexOptions.Compiled));

            pattern2Source.Process(processorSupplier);

            var driver = new TopologyTestDriver(builder.Build(), props);
            driver.PipeInput(recordFactory.Create("topic-3", "A", "aa", 1L));
            driver.PipeInput(recordFactory.Create("topic-4", "B", "bb", 5L));
            driver.PipeInput(recordFactory.Create("topic-5", "C", "cc", 10L));
            driver.PipeInput(recordFactory.Create("topic-6", "D", "dd", 8L));
            driver.PipeInput(recordFactory.Create("topic-7", "E", "ee", 3L));

            Assert.Equal(
                Arrays.asList(
                    new KeyValueTimestamp<string, string>("A", "aa", 1),
                    new KeyValueTimestamp<string, string>("B", "bb", 5),
                    new KeyValueTimestamp<string, string>("C", "cc", 10),
                    new KeyValueTimestamp<string, string>("D", "dd", 8),
                    new KeyValueTimestamp<string, string>("E", "ee", 3)),
                processorSupplier.TheCapturedProcessor().processed);
        }

        [Fact]
        public void shouldProcessFromSourcesThatMatchMultiplePattern()
        {
            string topic3 = "topic-without-pattern";

            IKStream<string, string> pattern2Source1 = builder.Stream<string, string>(new Regex("topic-\\d", RegexOptions.Compiled));
            IKStream<string, string> pattern2Source2 = builder.Stream<string, string>(new Regex("topic-[A-Z]", RegexOptions.Compiled));
            IKStream<string, string> source3 = builder.Stream<string, string>(topic3);
            IKStream<string, string> merged = pattern2Source1.Merge(pattern2Source2).Merge(source3);

            merged.Process(processorSupplier);

            var driver = new TopologyTestDriver(builder.Build(), props);
            driver.PipeInput(recordFactory.Create("topic-3", "A", "aa", 1L));
            driver.PipeInput(recordFactory.Create("topic-4", "B", "bb", 5L));
            driver.PipeInput(recordFactory.Create("topic-A", "C", "cc", 10L));
            driver.PipeInput(recordFactory.Create("topic-Z", "D", "dd", 8L));
            driver.PipeInput(recordFactory.Create(topic3, "E", "ee", 3L));

            Assert.Equal(
                Arrays.asList(
                    new KeyValueTimestamp<string, string>("A", "aa", 1),
                    new KeyValueTimestamp<string, string>("B", "bb", 5),
                    new KeyValueTimestamp<string, string>("C", "cc", 10),
                    new KeyValueTimestamp<string, string>("D", "dd", 8),
                    new KeyValueTimestamp<string, string>("E", "ee", 3)),
                processorSupplier.TheCapturedProcessor().processed);
        }

        private class Transformer : ITransformer<string, string, KeyValuePair<string, string>>
        {
            public void Init(IProcessorContext context) { }

            public void Close() { }

            public KeyValuePair<string, string> Transform(string key, string value)
            {
                return KeyValuePair.Create(key, value);
            }
        }
    }
}
