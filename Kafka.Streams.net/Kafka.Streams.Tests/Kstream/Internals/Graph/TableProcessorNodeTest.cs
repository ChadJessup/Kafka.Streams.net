using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.TimeStamped;
using Moq;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals.Graph
{
    public class TableProcessorNodeTest
    {
        private class TestProcessor : AbstractProcessor<string, string>
        {
            public override void Init(IProcessorContext context)
            {
            }

            public override void Process(string key, string value)
            {
            }

            public override void Close()
            {
            }
        }

        [Fact]
        public void shouldConvertToStringWithNullStoreBuilder()
        {
            var node = new TableProcessorNode<string, string, ITimestampedKeyValueStore<string, string>>(
                "Name",
                new ProcessorParameters(Mock.Of<IProcessorSupplier>(), "processor"),
                null,
                new string[] { "store1", "store2" });

            string asString = node.ToString();
            var expected = "storeBuilder=null";
            Assert.True(asString.Contains(expected), $"Expected ToString to return string with \"{expected}\", received: {asString}");
        }
    }
}
