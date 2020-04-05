using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Internals;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class ForwardingDisabledProcessorContextTest
    {
        // private ProcessorContext del;
        private ForwardingDisabledProcessorContext<string, string> context;

        public void SetUp()
        {
            //context = new ForwardingDisabledProcessorContext(del);
        }

        [Xunit.Fact]// (expected = StreamsException)
        public void ShouldThrowOnForward()
        {
            context.Forward("key", "value");
        }

        [Xunit.Fact]// (expected = StreamsException)
        public void ShouldThrowOnForwardWithTo()
        {
            context.Forward("key", "value", To.All());
        }

        // need to test deprecated code until removed
        [Xunit.Fact]// (expected = StreamsException)
        public void ShouldThrowOnForwardWithChildIndex()
        {
            context.Forward("key", "value", 1);
        }

        // need to test deprecated code until removed
        [Xunit.Fact]// (expected = StreamsException)
        public void ShouldThrowOnForwardWithChildName()
        {
            context.Forward("key", "value", "child1");
        }
    }
}
