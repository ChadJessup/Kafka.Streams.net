using Kafka.Streams.Errors;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.KStream.Internals.Graph;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals.Graph
{
    public class GraphGraceSearchUtilTest
    {
        [Fact]
        public void shouldThrowOnNull()
        {
            try
            {
                GraphGraceSearchUtil.findAndVerifyWindowGrace(null);
                Assert.False(true, "Should have thrown.");
            }
            catch (TopologyException e)
            {
                Assert.Equal("Invalid topology: Window Close time is only defined for windowed computations. Got [].", e.Message);
            }
        }

        [Fact]
        public void shouldFailIfThereIsNoGraceAncestor()
        {
            // doesn't matter if this ancestor is stateless or stateful. The important thing it that there is
            // no grace period defined on any ancestor of the node
            StatefulProcessorNode<string, long> gracelessAncestor = new StatefulProcessorNode<>(
                "stateful", null);
            //        new ProcessorParameters<>(
            //            () => new Processor<string, long>()
            //            {



            //            public void Init(IProcessorContext context) { }


            //    public void process(string key, long value) { }


            //    public void Close() { }
            //},
            //        "dummy"
            //    )//,
            //    //(IStoreBuilder<? : IStateStore>) null
            //);

            ProcessorGraphNode<string, long> node = new ProcessorGraphNode<>("stateless", null);
            gracelessAncestor.AddChild(node);

            try
            {
                GraphGraceSearchUtil.findAndVerifyWindowGrace(node);
                Assert.False(true, "should have thrown.");
            }
            catch (TopologyException e)
            {
                Assert.Equal("Invalid topology: Window Close time is only defined for windowed computations. Got [stateful=>stateless].", e.Message);
            }
        }

        [Fact]
        public void shouldExtractGraceFromKStreamWindowAggregateNode()
        {
            TimeWindow windows = TimeWindows.Of(TimeSpan.FromMilliseconds(10L)).Grace(TimeSpan.FromMilliseconds(1234L));
            StatefulProcessorNode<string, long> node = new StatefulProcessorNode<>(
                "asdf",
                new ProcessorParameters<>(
                    new KStreamWindowAggregate<string, long, int, TimeWindow>(
                        windows,
                        "asdf",
                        null,
                        null
                    ),

                    "asdf"
                ), null
            //(IStoreBuilder <? : IStateStore >) null
            );

            var extracted = GraphGraceSearchUtil.findAndVerifyWindowGrace(node);
            Assert.Equal(extracted, windows.gracePeriodMs());
        }

        [Fact]
        public void shouldExtractGraceFromKStreamSessionWindowAggregateNode()
        {
            SessionWindows windows = SessionWindows.With(TimeSpan.FromMilliseconds(10L)).Grace(TimeSpan.FromMilliseconds(1234L));

            StatefulProcessorNode<string, long> node = new StatefulProcessorNode<>(
                    "asdf",

                new ProcessorParameters<>(
                    new KStreamSessionWindowAggregate<string, long, int>(
                        windows,

                        "asdf",

                        null,
                        null,
                        null
                    ),

                    "asdf"
                ), null
            //(IStoreBuilder <? : IStateStore >) null
            );

            var extracted = GraphGraceSearchUtil.findAndVerifyWindowGrace(node);
            Assert.Equal(extracted, windows.gracePeriodMs() + windows.inactivityGap());
        }

        [Fact]
        public void shouldExtractGraceFromSessionAncestorThroughStatefulParent()
        {
            SessionWindows windows = SessionWindows.With(TimeSpan.FromMilliseconds(10L)).Grace(TimeSpan.FromMilliseconds(1234L));
            StatefulProcessorNode<string, long> graceGrandparent = new StatefulProcessorNode<>(
                    "asdf",

                new ProcessorParameters<>(new KStreamSessionWindowAggregate<string, long, int>(
                    windows, "asdf", null, null, null
                ), "asdf"), null
            //       (IStoreBuilder <? : IStateStore >) null
            );

            StatefulProcessorNode<string, long> statefulParent = new StatefulProcessorNode<>(
                "stateful", null);
            //        new ProcessorParameters<>(
            //            () => new Processor<string, long>()
            //            {



            //            public void Init(IProcessorContext context) { }


            //    public void process(string key, long value) { }


            //    public void Close() { }
            //},
            //        "dummy"
            //    ),
            //    (IStoreBuilder<? : IStateStore>) null
            //);
            graceGrandparent.AddChild(statefulParent);

            ProcessorGraphNode<string, long> node = new ProcessorGraphNode<>("stateless", null);
            statefulParent.AddChild(node);

            var extracted = GraphGraceSearchUtil.findAndVerifyWindowGrace(node);
            Assert.Equal(extracted, windows.gracePeriodMs() + windows.inactivityGap());
        }

        [Fact]
        public void shouldExtractGraceFromSessionAncestorThroughStatelessParent()
        {
            SessionWindows windows = SessionWindows.With(TimeSpan.FromMilliseconds(10L)).Grace(TimeSpan.FromMilliseconds(1234L));
            StatefulProcessorNode<string, long> graceGrandparent = new StatefulProcessorNode<>(
                    "asdf",

                new ProcessorParameters<>(
                    new KStreamSessionWindowAggregate<string, long, int>(
                        windows,

                        "asdf",

                        null,
                        null,
                        null
                    ),

                    "asdf"
                )//,
                 //       (IStoreBuilder <? : IStateStore >) null
            );

            ProcessorGraphNode<string, long> statelessParent = new ProcessorGraphNode<>("stateless", null);
            graceGrandparent.AddChild(statelessParent);

            ProcessorGraphNode<string, long> node = new ProcessorGraphNode<>("stateless", null);
            statelessParent.AddChild(node);

            var extracted = GraphGraceSearchUtil.findAndVerifyWindowGrace(node);
            Assert.Equal(extracted, windows.gracePeriodMs() + windows.inactivityGap());
        }

        [Fact]
        public void shouldUseMaxIfMultiParentsDoNotAgreeOnGrace()
        {
            StatefulProcessorNode<string, long> leftParent = new StatefulProcessorNode<>(
                    "asdf",

                new ProcessorParameters<>(
                    new KStreamSessionWindowAggregate<string, long, int>(
                        SessionWindows.With(TimeSpan.FromMilliseconds(10L)).Grace(TimeSpan.FromMilliseconds(1234L)),

                        "asdf",

                        null,
                        null,
                        null
                    ),

                    "asdf"
                ), null
            //       (IStoreBuilder <? : IStateStore >) null
            );

            StatefulProcessorNode<string, long> rightParent = new StatefulProcessorNode<>(
                    "asdf",

                new ProcessorParameters<>(
                    new KStreamWindowAggregate<string, long, int, TimeWindow>(
                        TimeWindows.Of(TimeSpan.FromMilliseconds(10L)).Grace(TimeSpan.FromMilliseconds(4321L)),

                        "asdf",

                        null,
                        null
                    ),

                    "asdf"
                ), null
            //       (IStoreBuilder <? : IStateStore >) null
            );

            ProcessorGraphNode<string, long> node = new ProcessorGraphNode<>("stateless", null);
            leftParent.AddChild(node);
            rightParent.AddChild(node);

            var extracted = GraphGraceSearchUtil.findAndVerifyWindowGrace(node);
            Assert.Equal(4321L, extracted);
        }
    }
}
