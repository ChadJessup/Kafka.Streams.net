using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Tests.Mocks;
using System;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class PunctuationQueueTest
    {
        private static readonly MockProcessorNode<string, string> node = new MockProcessorNode<string, string>();
        private readonly PunctuationQueue queue = new PunctuationQueue();
        private readonly Action<DateTime> punctuator = (dt) =>
        {
            node.mockProcessor.punctuatedStreamTime.Add(dt);
        };

        [Fact]
        public void TestPunctuationInterval()
        {
            PunctuationSchedule sched = new PunctuationSchedule(node, DateTime.MinValue, TimeSpan.FromMilliseconds(100L), punctuator);
            var now = sched.timestamp - TimeSpan.FromMilliseconds(100L);

            queue.Schedule(sched);

            ProcessorNodePunctuator<string, string> processorNodePunctuator =
                (node, now, type, punctuator) =>
                {
                    punctuator(now);
                };

            queue.MayPunctuate(now, PunctuationType.STREAM_TIME, processorNodePunctuator);
            Assert.Empty(node.mockProcessor.punctuatedStreamTime);

            queue.MayPunctuate(now + TimeSpan.FromMilliseconds(99L), PunctuationType.STREAM_TIME, processorNodePunctuator);
            Assert.Empty(node.mockProcessor.punctuatedStreamTime);

            queue.MayPunctuate(now + TimeSpan.FromMilliseconds(100L), PunctuationType.STREAM_TIME, processorNodePunctuator);
            Assert.Single(node.mockProcessor.punctuatedStreamTime);

            queue.MayPunctuate(now + TimeSpan.FromMilliseconds(199L), PunctuationType.STREAM_TIME, processorNodePunctuator);
            Assert.Single(node.mockProcessor.punctuatedStreamTime);

            queue.MayPunctuate(now + TimeSpan.FromMilliseconds(200L), PunctuationType.STREAM_TIME, processorNodePunctuator);
            Assert.Equal(2, node.mockProcessor.punctuatedStreamTime.Count);

            queue.MayPunctuate(now + TimeSpan.FromMilliseconds(1001L), PunctuationType.STREAM_TIME, processorNodePunctuator);
            Assert.Equal(3, node.mockProcessor.punctuatedStreamTime.Count);

            queue.MayPunctuate(now + TimeSpan.FromMilliseconds(1002L), PunctuationType.STREAM_TIME, processorNodePunctuator);
            Assert.Equal(3, node.mockProcessor.punctuatedStreamTime.Count);

            queue.MayPunctuate(now + TimeSpan.FromMilliseconds(1100L), PunctuationType.STREAM_TIME, processorNodePunctuator);
            Assert.Equal(4, node.mockProcessor.punctuatedStreamTime.Count);
        }

        [Fact]
        public void TestPunctuationIntervalCustomAlignment()
        {
            PunctuationSchedule sched = new PunctuationSchedule(node, DateTime.MinValue.AddMilliseconds(50L), TimeSpan.FromMilliseconds(100L), punctuator);
            var now = sched.timestamp - TimeSpan.FromMilliseconds(50L);

            queue.Schedule(sched);

            ProcessorNodePunctuator<string, string> processorNodePunctuator =
                (node, now, type, punctuator) => punctuator(now);

            queue.MayPunctuate(now, PunctuationType.STREAM_TIME, processorNodePunctuator);
            Assert.Empty(node.mockProcessor.punctuatedStreamTime);

            queue.MayPunctuate(now + TimeSpan.FromMilliseconds(49L), PunctuationType.STREAM_TIME, processorNodePunctuator);
            Assert.Empty(node.mockProcessor.punctuatedStreamTime);

            queue.MayPunctuate(now + TimeSpan.FromMilliseconds(50L), PunctuationType.STREAM_TIME, processorNodePunctuator);
            Assert.Single(node.mockProcessor.punctuatedStreamTime);

            queue.MayPunctuate(now + TimeSpan.FromMilliseconds(149L), PunctuationType.STREAM_TIME, processorNodePunctuator);
            Assert.Single(node.mockProcessor.punctuatedStreamTime);

            queue.MayPunctuate(now + TimeSpan.FromMilliseconds(150L), PunctuationType.STREAM_TIME, processorNodePunctuator);
            Assert.Equal(2, node.mockProcessor.punctuatedStreamTime.Count);

            queue.MayPunctuate(now + TimeSpan.FromMilliseconds(1051L), PunctuationType.STREAM_TIME, processorNodePunctuator);
            Assert.Equal(3, node.mockProcessor.punctuatedStreamTime.Count);

            queue.MayPunctuate(now + TimeSpan.FromMilliseconds(1052L), PunctuationType.STREAM_TIME, processorNodePunctuator);
            Assert.Equal(3, node.mockProcessor.punctuatedStreamTime.Count);

            queue.MayPunctuate(now + TimeSpan.FromMilliseconds(1150L), PunctuationType.STREAM_TIME, processorNodePunctuator);
            Assert.Equal(4, node.mockProcessor.punctuatedStreamTime.Count);
        }

        [Fact]
        public void TestPunctuationIntervalCancelFromPunctuator()
        {
            PunctuationSchedule sched = new PunctuationSchedule(node, DateTime.MinValue, TimeSpan.FromMilliseconds(100L), punctuator);
            var now = sched.timestamp - TimeSpan.FromMilliseconds(100L);

            ICancellable cancellable = queue.Schedule(sched);

            ProcessorNodePunctuator<string, string> processorNodePunctuator =
                (node, now, type, punctuator) =>
                {
                    punctuator(now);
                    // simulate scheduler cancelled from within punctuator
                    cancellable.Cancel();
                };

            queue.MayPunctuate(now, PunctuationType.STREAM_TIME, processorNodePunctuator);
            Assert.Empty(node.mockProcessor.punctuatedStreamTime);

            queue.MayPunctuate(now + TimeSpan.FromMilliseconds(100L), PunctuationType.STREAM_TIME, processorNodePunctuator);
            Assert.Single(node.mockProcessor.punctuatedStreamTime);

            queue.MayPunctuate(now + TimeSpan.FromMilliseconds(200L), PunctuationType.STREAM_TIME, processorNodePunctuator);
            Assert.Single(node.mockProcessor.punctuatedStreamTime);
        }

        private class TestProcessor : AbstractProcessor<string, string>
        {
            public override void Init(IProcessorContext context) { }
            public override void Process(string key, string value) { }
            public override void Close() { }
        }
    }
}
