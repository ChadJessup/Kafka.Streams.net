using Kafka.Streams.Errors;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Moq;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class ForwardingDisabledProcessorContextTest
    {
        private readonly IProcessorContext del;
        private readonly ForwardingDisabledProcessorContext<string, string> context;

        public ForwardingDisabledProcessorContextTest()
        {
            del = Mock.Of<IProcessorContext>();
            context = new ForwardingDisabledProcessorContext<string, string>(del);
        }

        [Fact]
        public void ShouldThrowOnForward()
        {
            Assert.Throws<StreamsException>(() => context.Forward("key", "value"));
        }

        [Fact]
        public void ShouldThrowOnForwardWithTo()
        {
            Assert.Throws<StreamsException>(() => context.Forward("key", "value", To.All()));
        }

        // need to test deprecated code until removed
        [Fact]
        public void ShouldThrowOnForwardWithChildIndex()
        {
#pragma warning disable CS0612 // Type or member is obsolete
            Assert.Throws<StreamsException>(() => context.Forward("key", "value", 1));
#pragma warning restore CS0612 // Type or member is obsolete
        }

        // need to test deprecated code until removed
        [Fact]
        public void ShouldThrowOnForwardWithChildName()
        {
            Assert.Throws<StreamsException>(() => context.Forward("key", "value", "child1"));
        }
    }
}
