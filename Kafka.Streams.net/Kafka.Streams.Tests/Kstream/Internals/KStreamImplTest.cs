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
        private readonly Consumed<string, string> stringConsumed = Consumed.With(Serdes.String(), Serdes.String());
        private readonly MockProcessorSupplier<string, string> processorSupplier = new MockProcessorSupplier<string, string>();

        private readonly IKStream<string, string> testStream;
        private readonly StreamsBuilder builder;

        private readonly ConsumerRecordFactory<string, string> recordFactory =
            new ConsumerRecordFactory<string, string>(Serdes.String().Serializer, Serdes.String().Serializer, 0L);
        private readonly StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.String());

        private readonly ISerde<string> mySerde = Serdes.String();


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
                 TopologyWrapper.getInternalTopologyBuilder(builder.Build()).SetApplicationId("X").Build(null).Processors().Count);
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
            ITransformerSupplier<string, string, KeyValuePair<string, string>> transformerSupplier = new TransformerSupplier<string, string>();
            IValueTransformerSupplier<string, string> valueTransformerSupplier = new ValueTransformerSupplier<string, string>();

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

            ProcessorTopology processorTopology = TopologyWrapper.getInternalTopologyBuilder(builder.Build()).SetApplicationId("X").Build(null);
            Assert.IsAssignableFrom<ITimestampExtractor>(processorTopology.Source("topic-6").TimestampExtractor);
            Assert.Null(processorTopology.Source("topic-4").TimestampExtractor);
            Assert.Null(processorTopology.Source("topic-3").TimestampExtractor);
            Assert.Null(processorTopology.Source("topic-2").TimestampExtractor);
            Assert.Null(processorTopology.Source("topic-1").TimestampExtractor);
        }

        // [Fact]
        // public void shouldSendDataThroughTopicUsingProduced()
        // {
        //     StreamsBuilder builder = new StreamsBuilder();
        //     string input = "topic";
        //     IKStream<string, string> stream = builder.Stream(input, stringConsumed);
        //     stream.Through("through-topic", Produced.With(Serdes.String(), Serdes.String()).Process(processorSupplier));
        //
        //     var driver = new TopologyTestDriver(builder.Build(), props);
        //     driver.PipeInput(recordFactory.Create(input, "a", "b"));
        //     Assert.Equal(processorSupplier.TheCapturedProcessor().processed, Collections.singletonList(new KeyValueTimestamp<string, string>("a", "b", 0)));
        // }

        [Fact]
        public void ShouldSendDataToTopicUsingProduced()
        {
            StreamsBuilder builder = new StreamsBuilder();
            string input = "topic";
            IKStream<string, string> stream = builder.Stream(input, stringConsumed);
            stream.To("to-topic", Produced.With(Serdes.String(), Serdes.String()));
            builder.Stream("to-topic", stringConsumed).Process(processorSupplier);

            var driver = new TopologyTestDriver(builder.Context, builder.Build(), props);
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

            var driver = new TopologyTestDriver(builder.Context, builder.Build(), props);
            driver.PipeInput(recordFactory.Create(input, "a", "v1"));
            driver.PipeInput(recordFactory.Create(input, "a", "v2"));
            driver.PipeInput(recordFactory.Create(input, "b", "v1"));

            List<MockProcessor<string, string>> mockProcessors = processorSupplier.CapturedProcessors(2);
            Assert.Equal(mockProcessors.ElementAt(0).processed, Arrays.asList(new KeyValueTimestamp<string, string>("a", "v1", 0),
                    new KeyValueTimestamp<string, string>("a", "v2", 0)));
            Assert.Equal(mockProcessors.ElementAt(1).processed, (Collections.singletonList(new KeyValueTimestamp<string, string>("b", "v1", 0))));
        }

        // specifically testing the deprecated variant
        // [Fact]
        // public void shouldUseRecordMetadataTimestampExtractorWhenInternalRepartitioningTopicCreatedWithRetention()
        // {
        //     StreamsBuilder builder = new StreamsBuilder();
        //     IKStream<string, string> kStream = builder.Stream("topic-1", stringConsumed);
        //     ValueJoiner<string, string, string> valueJoiner = MockValueJoiner.Instance(":");
        //     long windowSize = TimeUnit.MILLISECONDS.Convert(1, TimeUnit.DAYS);
        //     IKStream<string, string> stream = kStream
        //         .Map((key, value) => KeyValuePair.Create(value, value));
        //     stream.Join(kStream,
        //                 valueJoiner,
        //                 JoinWindows.Of(TimeSpan.FromMilliseconds(windowSize)).Grace(TimeSpan.FromMilliseconds(3 * windowSize)),
        //                 Joined.With(Serdes.String(),
        //                             Serdes.String(),
        //                             Serdes.String()))
        //           .To("output-topic", Produced.With(Serdes.String(), Serdes.String()));
        //
        //     ProcessorTopology topology = TopologyWrapper.getInternalTopologyBuilder(builder.Build()).SetApplicationId("X").Build();
        //
        //     ISourceNode originalSourceNode = topology.Source("topic-1");
        //
        //     foreach (SourceNode sourceNode in topology.Sources())
        //     {
        //         if (sourceNode.Name.Equals(originalSourceNode.Name))
        //         {
        //             Assert.Null(sourceNode.TimestampExtractor);
        //         }
        //         else
        //         {
        //             Assert.IsAssignableFrom<FailOnInvalidTimestamp>(sourceNode.TimestampExtractor);
        //         }
        //     }
        // }

        [Fact]
        public void shouldUseRecordMetadataTimestampExtractorWhenInternalRepartitioningTopicCreated()
        {
            StreamsBuilder builder = new StreamsBuilder();
            IKStream<string, string> kStream = builder.Stream("topic-1", stringConsumed);
            ValueJoiner<string, string, string> valueJoiner = MockValueJoiner.Instance(":");
            long windowSize = 0; // TimeUnit.MILLISECONDS.Convert(1, TimeUnit.DAYS);
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
                    Assert.IsAssignableFrom<FailOnInvalidTimestamp>(sourceNode.TimestampExtractor);
                }
            }
        }

        [Fact]
        public void ShouldPropagateRepartitionFlagAfterGlobalKTableJoin()
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
            Assert.Throws<ArgumentNullException>(() => testStream.Filter(null));
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullPredicateOnFilterNot()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.FilterNot(null));
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullMapperOnSelectKey()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.SelectKey<string>(null));
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullMapperOnMap()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.Map<string, string>(null));
        }

        [Fact]
        public void ShouldNotAllowNullMapperOnMapValues()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.MapValues((ValueMapper<string, string>)null));
        }

        [Fact]
        public void ShouldNotAllowNullMapperOnMapValuesWithKey()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.MapValues((ValueMapperWithKey<string, string, string>)null));
        }

        [Fact]
        public void ShouldNotAllowNullMapperOnFlatMap()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.FlatMap<string, string>(null));
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

        [Fact]
        public void ShouldCantHaveNullPredicate()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.Branch(null));
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullTopicOnThrough()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.Through(null));
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
            Assert.Throws<ArgumentNullException>(() => testStream.Transform<string, string>(null, null));
        }

        [Fact]
        public void ShouldNotAllowNullTransformerSupplierOnFlatTransform()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.FlatTransform<string, string>(null));
        }

        [Fact]
        public void ShouldNotAllowNullValueTransformerWithKeySupplierOnTransformValues()
        {
            Assert.Throws<ArgumentNullException>(() =>
                testStream.TransformValues((IValueTransformerWithKeySupplier<string, string, string>)null));
        }

        [Fact]
        public void shouldNotAllowNullValueTransformerSupplierOnTransformValues()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.TransformValues((IValueTransformerSupplier<string, string>)null));
        }

        [Fact]
        public void shouldNotAllowNullValueTransformerWithKeySupplierOnFlatTransformValues()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.FlatTransformValues<string>((IValueTransformerWithKeySupplier<string, string, IEnumerable<string>>)null));
        }

        [Fact]
        public void shouldNotAllowNullValueTransformerSupplierOnFlatTransformValues()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.FlatTransformValues<string>(
                (IValueTransformerSupplier<string, IEnumerable<string>>)null));
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullProcessSupplier()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.Process((IProcessorSupplier<string, string>)null));
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void ShouldNotAllowNullOtherStreamOnJoin()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.Join(null, MockValueJoiner.TOSTRING_JOINER(), JoinWindows.Of(TimeSpan.FromMilliseconds(10))));
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullValueJoinerOnJoin()
        {
            Assert.Throws<ArgumentNullException>(() =>
                testStream.Join<string, string>(testStream, null, JoinWindows.Of(TimeSpan.FromMilliseconds(10))));
        }

        [Fact]
        public void ShouldNotAllowNullJoinWindowsOnJoin()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.Join(testStream, MockValueJoiner.TOSTRING_JOINER(), null));
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullTableOnTableJoin()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.LeftJoin<string, string>(null, MockValueJoiner.TOSTRING_JOINER()));
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullValueMapperOnTableJoin()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.LeftJoin<string, string>(builder.Table("topic", stringConsumed), null));
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullSelectorOnGroupBy()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.GroupBy<string>(null));
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullActionOnForEach()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.ForEach(null));
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void ShouldNotAllowNullTableOnJoinWithGlobalTable()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.Join<string, string, string>(
                (IGlobalKTable<string, string>)null,
                MockMapper.GetSelectValueMapper<string, string>(),
                MockValueJoiner.TOSTRING_JOINER()));
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullMapperOnJoinWithGlobalTable()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.Join<string, string, string>(
                builder.GlobalTable("global", stringConsumed),
                null,
                MockValueJoiner.TOSTRING_JOINER()));
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullJoinerOnJoinWithGlobalTable()
        {
            Assert.Throws<ArgumentNullException>(() =>
                testStream.Join<string, string, string>(
                    builder.GlobalTable("global", stringConsumed),
                    MockMapper.GetSelectValueMapper<string, string>(),
                    null));
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullTableOnJLeftJoinWithGlobalTable()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.LeftJoin(
                null,
                MockMapper.GetSelectValueMapper<string, string>(),
                MockValueJoiner.TOSTRING_JOINER()));
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullMapperOnLeftJoinWithGlobalTable()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.LeftJoin<string, string, string>(
                builder.GlobalTable("global", stringConsumed),
                null,
                MockValueJoiner.TOSTRING_JOINER()));
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void ShouldNotAllowNullJoinerOnLeftJoinWithGlobalTable()
        {
            var globalTable = builder.GlobalTable("global", stringConsumed);
            Assert.Throws<ArgumentNullException>(() => testStream.LeftJoin<string, string, string>(
                globalTable,
                (K, V) => V,
                null));
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldThrowNullPointerOnPrintIfPrintedIsNull()
        {
            //testStream.Print(null);
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldThrowNullPointerOnThroughWhenProducedIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.Through("topic", null));
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldThrowNullPointerOnToWhenProducedIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.To("topic", null));
        }

        [Fact]
        public void shouldThrowNullPointerOnLeftJoinWithTableWhenJoinedIsNull()
        {
            IKTable<string, string> table = builder.Table("blah", stringConsumed);
            Assert.Throws<ArgumentNullException>(() => testStream.LeftJoin<string, string>(
                    table,
                    MockValueJoiner.TOSTRING_JOINER(),
                    null));
        }

        [Fact]
        public void ShouldThrowNullPointerOnJoinWithTableWhenJoinedIsNull()
        {
            IKTable<string, string> table = builder.Table("blah", stringConsumed);
            Assert.Throws<ArgumentNullException>(() => testStream.Join<string, string>(
                table,
                MockValueJoiner.TOSTRING_JOINER(),
                null));
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void ShouldThrowNullPointerOnJoinWithStreamWhenJoinedIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.Join<string, string>(
                testStream,
                MockValueJoiner.TOSTRING_JOINER(),
                JoinWindows.Of(TimeSpan.FromMilliseconds(10)), null));
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void ShouldThrowNullPointerOnOuterJoinJoinedIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => testStream.OuterJoin<string, string>(
                testStream,
                MockValueJoiner.TOSTRING_JOINER(),
                JoinWindows.Of(TimeSpan.FromMilliseconds(10)), null));
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

            var driver = new TopologyTestDriver(builder.Context, builder.Build(), props);
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

            var driver = new TopologyTestDriver(builder.Context, builder.Build(), props);
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

            var driver = new TopologyTestDriver(builder.Context, builder.Build(), props);
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

            var driver = new TopologyTestDriver(builder.Context, builder.Build(), props);

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

        private class Transformer<T1, T2> : ITransformer<T1, T2, KeyValuePair<T1, T2>>
        {
            public void Init(IProcessorContext context) { }

            public void Close() { }

            public KeyValuePair<T1, T2> Transform(T1 key, T2 value)
            {
                return KeyValuePair.Create(key, value);
            }
        }

        private class TransformerSupplier<T1, T2> : ITransformerSupplier<T1, T2, KeyValuePair<T1, T2>>
        {
            public ITransformer<T1, T2, KeyValuePair<T1, T2>> Get()
            {
                return new Transformer<T1, T2>();
            }
        }

        private class ValueTransformerSupplier<T1, T2> : IValueTransformerSupplier<T1, T2>
        {
            private class ValueTransformer : IValueTransformer<T1, T2>
            {
                public void Close()
                {
                }

                public void Init(IProcessorContext context)
                {
                }

                public T2 Transform(T1 key, T2 value)
                {
                    throw new NotImplementedException();
                }
            }

            public IValueTransformer<T1, T2> Get()
            {
                return new ValueTransformer();
            }
        }
    }
}
