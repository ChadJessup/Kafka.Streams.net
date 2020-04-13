using Kafka.Streams.Nodes;
using Kafka.Streams.State;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class ProcessorNodeTest
    {


        [Fact]// (expected = StreamsException)
        public void ShouldThrowStreamsExceptionIfExceptionCaughtDuringInit()
        {
            ProcessorNode node = new ProcessorNode("Name", new ExceptionalProcessor(), Collections.emptySet());
            node.Init(null);
        }


        [Fact]// (expected = StreamsException)
        public void ShouldThrowStreamsExceptionIfExceptionCaughtDuringClose()
        {
            ProcessorNode node = new ProcessorNode("Name", new ExceptionalProcessor(), Collections.emptySet());
            node.Close();
        }

        private static class ExceptionalProcessor : Processor
        {

            public void Init(IProcessorContext context)
            {
                throw new RuntimeException();
            }


            public void Process(object key, object value)
            {
                throw new RuntimeException();
            }


            public void Close()
            {
                throw new RuntimeException();
            }
        }

        private static class NoOpProcessor : Processor<object, object>
        {

            public void Init(IProcessorContext context)
            {

            }


            public void Process(object key, object value)
            {

            }


            public void Close()
            {

            }
        }

        [Fact]
        public void TestMetrics()
        {
            StateSerdes anyStateSerde = StateSerdes.WithBuiltinTypes("anyName", Bytes, Bytes);

            Metrics metrics = new Metrics();
            InternalMockProcessorContext context = new InternalMockProcessorContext(
                anyStateSerde,
                new RecordCollectorImpl(
                    null,
                    new LogContext("processnode-test "),
                    new DefaultProductionExceptionHandler(),
                    metrics.sensor("skipped-records")
                ),
                metrics
            );

            ProcessorNode<object, object> node = new ProcessorNode<>("Name", new NoOpProcessor(), Collections.< string > emptySet());
            node.Init(context);

            string[] latencyOperations = { "process", "punctuate", "Create", "destroy" };
            string throughputOperation = "forward";
            string groupName = "stream-processor-node-metrics";
            Dictionary<string, string> metricTags = new LinkedHashMap<>();
            metricTags.Put("processor-node-id", node.Name());
            metricTags.Put("task-id", context.taskId().ToString());
            metricTags.Put("client-id", "mock");

            foreach (string opName in latencyOperations)
            {
                StreamsTestUtils.getMetricByNameFilterByTags(metrics.metrics(), opName + "-latency-avg", groupName, metricTags);
                StreamsTestUtils.getMetricByNameFilterByTags(metrics.metrics(), opName + "-latency-max", groupName, metricTags);
                StreamsTestUtils.getMetricByNameFilterByTags(metrics.metrics(), opName + "-rate", groupName, metricTags);
                StreamsTestUtils.getMetricByNameFilterByTags(metrics.metrics(), opName + "-total", groupName, metricTags);
            }
            Assert.NotNull(metrics.metrics().Get(metrics.metricName(throughputOperation + "-rate", groupName,
                                                                   "The average number of occurrence of " + throughputOperation + " operation per second.",
                                                                   metricTags)));

            // test "All"
            metricTags.Put("processor-node-id", "All");
            foreach (string opName in latencyOperations)
            {
                StreamsTestUtils.getMetricByNameFilterByTags(metrics.metrics(), opName + "-latency-avg", groupName, metricTags);
                StreamsTestUtils.getMetricByNameFilterByTags(metrics.metrics(), opName + "-latency-max", groupName, metricTags);
                StreamsTestUtils.getMetricByNameFilterByTags(metrics.metrics(), opName + "-rate", groupName, metricTags);
                StreamsTestUtils.getMetricByNameFilterByTags(metrics.metrics(), opName + "-total", groupName, metricTags);
            }
            Assert.NotNull(metrics.metrics().Get(metrics.metricName(throughputOperation + "-rate",
                                                                   groupName,
                                                                   "The average number of occurrence of " + throughputOperation + " operation per second.",
                                                                   metricTags)));

            JmxReporter reporter = new JmxReporter("kafka.streams");
            metrics.addReporter(reporter);
            Assert.True(reporter.containsMbean(string.format("kafka.streams:type=%s,client-id=mock,task-id=%s,processor-node-id=%s",
                    groupName, context.taskId().ToString(), node.Name())));
        }

    }
}
