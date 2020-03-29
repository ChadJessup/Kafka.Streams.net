///*
//
//
//
//
//
//
// *
//
// *
//
//
//
//
//
// */
//namespace Kafka.Streams.KStream.Internals.suppress
//{


























//    public class KTableSuppressProcessorMetricsTest
//    {
//        private static long ARBITRARY_LONG = 5L;

//        private static MetricName EVICTION_TOTAL_METRIC = new MetricName(
//            "suppression-emit-total",
//            "stream-processor-node-metrics",
//            "The total number of occurrence of suppression-emit operations.",
//            mkMap(
//                mkEntry("client-id", "mock-processor-context-virtual-thread"),
//                mkEntry("Task-id", "0_0"),
//                mkEntry("processor-node-id", "testNode")
//            )
//        );

//        private static MetricName EVICTION_RATE_METRIC = new MetricName(
//            "suppression-emit-rate",
//            "stream-processor-node-metrics",
//            "The average number of occurrence of suppression-emit operation per second.",
//            mkMap(
//                mkEntry("client-id", "mock-processor-context-virtual-thread"),
//                mkEntry("Task-id", "0_0"),
//                mkEntry("processor-node-id", "testNode")
//            )
//        );

//        private static MetricName BUFFER_SIZE_AVG_METRIC = new MetricName(
//            "suppression-buffer-size-avg",
//            "stream-buffer-metrics",
//            "The average size of buffered records.",
//            mkMap(
//                mkEntry("client-id", "mock-processor-context-virtual-thread"),
//                mkEntry("Task-id", "0_0"),
//                mkEntry("buffer-id", "test-store")
//            )
//        );

//        private static MetricName BUFFER_SIZE_CURRENT_METRIC = new MetricName(
//            "suppression-buffer-size-current",
//            "stream-buffer-metrics",
//            "The current size of buffered records.",
//            mkMap(
//                mkEntry("client-id", "mock-processor-context-virtual-thread"),
//                mkEntry("Task-id", "0_0"),
//                mkEntry("buffer-id", "test-store")
//            )
//        );

//        private static MetricName BUFFER_SIZE_MAX_METRIC = new MetricName(
//            "suppression-buffer-size-max",
//            "stream-buffer-metrics",
//            "The max size of buffered records.",
//            mkMap(
//                mkEntry("client-id", "mock-processor-context-virtual-thread"),
//                mkEntry("Task-id", "0_0"),
//                mkEntry("buffer-id", "test-store")
//            )
//        );

//        private static MetricName BUFFER_COUNT_AVG_METRIC = new MetricName(
//            "suppression-buffer-count-avg",
//            "stream-buffer-metrics",
//            "The average count of buffered records.",
//            mkMap(
//                mkEntry("client-id", "mock-processor-context-virtual-thread"),
//                mkEntry("Task-id", "0_0"),
//                mkEntry("buffer-id", "test-store")
//            )
//        );

//        private static MetricName BUFFER_COUNT_CURRENT_METRIC = new MetricName(
//            "suppression-buffer-count-current",
//            "stream-buffer-metrics",
//            "The current count of buffered records.",
//            mkMap(
//                mkEntry("client-id", "mock-processor-context-virtual-thread"),
//                mkEntry("Task-id", "0_0"),
//                mkEntry("buffer-id", "test-store")
//            )
//        );

//        private static MetricName BUFFER_COUNT_MAX_METRIC = new MetricName(
//            "suppression-buffer-count-max",
//            "stream-buffer-metrics",
//            "The max count of buffered records.",
//            mkMap(
//                mkEntry("client-id", "mock-processor-context-virtual-thread"),
//                mkEntry("Task-id", "0_0"),
//                mkEntry("buffer-id", "test-store")
//            )
//        );

//        [Fact]
//        public void shouldRecordMetrics()
//        {
//            var storeName = "test-store";

//            StateStore buffer = new InMemoryTimeOrderedKeyValueBuffer.Builder<>(
//                storeName, Serdes.String(),
//                Serdes.Long()
//            )
//                .withLoggingDisabled()
//                .Build();

//            KTable mock = EasyMock.mock< string, ?, long>(KTable));
//            Processor<string, Change<long>> processor =
//                new KTableSuppressProcessorSupplier<>(
//                    (SuppressedInternal<string>)Suppressed.< string > untilTimeLimit(Duration.ofDays(100), maxRecords(1)),
//                    storeName,
//                    mock
//                ).get();

//            var context = new MockInternalProcessorContext();
//            context.setCurrentNode(new ProcessorNode("testNode"));

//            buffer.init(context, buffer);
//            processor.init(context);

//            var timestamp = 100L;
//            context.setRecordMetadata("", 0, 0L, null, timestamp);
//            var key = "longKey";
//            Change<long> value = new Change<>(null, ARBITRARY_LONG);
//            processor.process(key, value);

//            {
//                Dictionary metrics = context.metrics().metrics< MetricName, ? : Metric >();

//                verifyMetric(metrics, EVICTION_RATE_METRIC, (0.0));
//                verifyMetric(metrics, EVICTION_TOTAL_METRIC, (0.0));
//                verifyMetric(metrics, BUFFER_SIZE_AVG_METRIC, (21.5));
//                verifyMetric(metrics, BUFFER_SIZE_CURRENT_METRIC, (43.0));
//                verifyMetric(metrics, BUFFER_SIZE_MAX_METRIC, (43.0));
//                verifyMetric(metrics, BUFFER_COUNT_AVG_METRIC, (0.5));
//                verifyMetric(metrics, BUFFER_COUNT_CURRENT_METRIC, (1.0));
//                verifyMetric(metrics, BUFFER_COUNT_MAX_METRIC, (1.0));
//            }

//            context.setRecordMetadata("", 0, 1L, null, timestamp + 1);
//            processor.process("key", value);

//            {
//                Dictionary metrics = context.metrics().metrics< MetricName, ? : Metric >();

//                verifyMetric(metrics, EVICTION_RATE_METRIC, greaterThan(0.0));
//                verifyMetric(metrics, EVICTION_TOTAL_METRIC, (1.0));
//                verifyMetric(metrics, BUFFER_SIZE_AVG_METRIC, (41.0));
//                verifyMetric(metrics, BUFFER_SIZE_CURRENT_METRIC, (39.0));
//                verifyMetric(metrics, BUFFER_SIZE_MAX_METRIC, (82.0));
//                verifyMetric(metrics, BUFFER_COUNT_AVG_METRIC, (1.0));
//                verifyMetric(metrics, BUFFER_COUNT_CURRENT_METRIC, (1.0));
//                verifyMetric(metrics, BUFFER_COUNT_MAX_METRIC, (2.0));
//            }
//        }


//        private static void verifyMetric<T>(Dictionary<MetricName, ? : Metric> metrics,
//                                             MetricName metricName,
//                                             Matcher<T> matcher)
//        {
//            Assert.Equal(metrics.get(metricName).metricName().description(), (metricName.description()));
//            Assert.Equal((T)metrics.get(metricName).metricValue(), matcher);

//        }
//    }
//}
