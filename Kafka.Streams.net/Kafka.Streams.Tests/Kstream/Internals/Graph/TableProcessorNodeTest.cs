//using Kafka.Streams.KStream.Internals.Graph;
//using Kafka.Streams.Processors;
//using Kafka.Streams.Processors.Interfaces;

//namespace Kafka.Streams.KStream.Internals.graph
//{
//    public class TableProcessorNodeTest
//    {
//        private class TestProcessor : AbstractProcessor<string, string>
//        {

//            public void init(IProcessorContext context)
//            {
//            }


//            public void process(string key, string value)
//            {
//            }


//            public void close()
//            {
//            }
//        }

//        [Fact]
//        public void shouldConvertToStringWithNullStoreBuilder()
//        {
//            TableProcessorNode<string, string> node = new TableProcessorNode<>(
//                "name",
//                new ProcessorParameters<>(typeof(TestProcessor), "processor"),
//                null,
//                new string[] { "store1", "store2" }
//            );

//            string asString = node.ToString();
//            var expected = "storeBuilder=null";
//            Assert.True(
//                string.Format(
//                    "Expected ToString to return string with \"%s\", received: %s",
//                    expected,


//                   asString),


//               asString.Contains(expected)
//            );
//        }
//    }
//}
