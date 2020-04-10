//using System;
//using Xunit;

//namespace Kafka.Streams.Tests.Processor.Internals.metrics
//{
//    /*






//    *

//    *





//    */





























//    public class StreamsMetricsImplTest : EasyMockSupport
//    {

//        private const string SENSOR_PREFIX_DELIMITER = ".";
//        private const string SENSOR_NAME_DELIMITER = ".s.";
//        private const string INTERNAL_PREFIX = "internal";

//        [Fact]
//        public void ShouldGetThreadLevelSensor()
//        {
//            Metrics metrics = mock(Metrics);
//            string threadName = "thread1";
//            string sensorName = "sensor1";
//            string expectedFullSensorName =
//                INTERNAL_PREFIX + SENSOR_PREFIX_DELIMITER + threadName + SENSOR_NAME_DELIMITER + sensorName;
//            RecordingLevel recordingLevel = RecordingLevel.DEBUG;
//            Sensor[] parents = System.Array.Empty<Sensor>();
//            EasyMock.expect(metrics.sensor(expectedFullSensorName, recordingLevel, parents)).andReturn(null);

//            replayAll();

//            StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, threadName);
//            Sensor sensor = streamsMetrics.threadLevelSensor(sensorName, recordingLevel);

//            verifyAll();

//            Assert.Null(sensor);
//        }

//        [Fact]// (expected = NullPointerException)
//        public void TestNullMetrics()
//        {
//            new StreamsMetricsImpl(null, "");
//        }

//        [Fact]// (expected = NullPointerException)
//        public void TestRemoveNullSensor()
//        {
//            StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), "");
//            streamsMetrics.removeSensor(null);
//        }

//        [Fact]
//        public void TestRemoveSensor()
//        {
//            string sensorName = "sensor1";
//            string scope = "scope";
//            string entity = "entity";
//            string operation = "Put";
//            StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), "");

//            Sensor sensor1 = streamsMetrics.addSensor(sensorName, Sensor.RecordingLevel.DEBUG);
//            streamsMetrics.removeSensor(sensor1);

//            Sensor sensor1a = streamsMetrics.addSensor(sensorName, Sensor.RecordingLevel.DEBUG, sensor1);
//            streamsMetrics.removeSensor(sensor1a);

//            Sensor sensor2 = streamsMetrics.addLatencyAndThroughputSensor(scope, entity, operation, Sensor.RecordingLevel.DEBUG);
//            streamsMetrics.removeSensor(sensor2);

//            Sensor sensor3 = streamsMetrics.addThroughputSensor(scope, entity, operation, Sensor.RecordingLevel.DEBUG);
//            streamsMetrics.removeSensor(sensor3);

//            Assert.Equal(Collections.emptyMap(), streamsMetrics.parentSensors());
//        }

//        [Fact]
//        public void TestMutiLevelSensorRemoval()
//        {
//            Metrics registry = new Metrics();
//            StreamsMetricsImpl metrics = new StreamsMetricsImpl(registry, "");
//            foreach (MetricName defaultMetric in registry.metrics().keySet())
//            {
//                registry.removeMetric(defaultMetric);
//            }

//            string taskName = "taskName";
//            string operation = "operation";
//            Dictionary<string, string> taskTags = mkMap(mkEntry("tkey", "value"));

//            string processorNodeName = "processorNodeName";
//            Dictionary<string, string> nodeTags = mkMap(mkEntry("nkey", "value"));

//            Sensor parent1 = metrics.taskLevelSensor(taskName, operation, Sensor.RecordingLevel.DEBUG);
//            addAvgMaxLatency(parent1, PROCESSOR_NODE_METRICS_GROUP, taskTags, operation);
//            addInvocationRateAndCount(parent1, PROCESSOR_NODE_METRICS_GROUP, taskTags, operation, "", "");

//            int numberOfTaskMetrics = registry.metrics().Count;

//            Sensor sensor1 = metrics.nodeLevelSensor(taskName, processorNodeName, operation, Sensor.RecordingLevel.DEBUG, parent1);
//            addAvgMaxLatency(sensor1, PROCESSOR_NODE_METRICS_GROUP, nodeTags, operation);
//            addInvocationRateAndCount(sensor1, PROCESSOR_NODE_METRICS_GROUP, nodeTags, operation, "", "");

//            Assert.Equal(registry.metrics().Count, greaterThan(numberOfTaskMetrics));

//            metrics.removeAllNodeLevelSensors(taskName, processorNodeName);

//            Assert.Equal(registry.metrics().Count, (numberOfTaskMetrics));

//            Sensor parent2 = metrics.taskLevelSensor(taskName, operation, Sensor.RecordingLevel.DEBUG);
//            addAvgMaxLatency(parent2, PROCESSOR_NODE_METRICS_GROUP, taskTags, operation);
//            addInvocationRateAndCount(parent2, PROCESSOR_NODE_METRICS_GROUP, taskTags, operation, "", "");

//            Assert.Equal(registry.metrics().Count, (numberOfTaskMetrics));

//            Sensor sensor2 = metrics.nodeLevelSensor(taskName, processorNodeName, operation, Sensor.RecordingLevel.DEBUG, parent2);
//            addAvgMaxLatency(sensor2, PROCESSOR_NODE_METRICS_GROUP, nodeTags, operation);
//            addInvocationRateAndCount(sensor2, PROCESSOR_NODE_METRICS_GROUP, nodeTags, operation, "", "");

//            Assert.Equal(registry.metrics().Count, greaterThan(numberOfTaskMetrics));

//            metrics.removeAllNodeLevelSensors(taskName, processorNodeName);

//            Assert.Equal(registry.metrics().Count, (numberOfTaskMetrics));

//            metrics.removeAllTaskLevelSensors(taskName);

//            Assert.Equal(registry.metrics().Count, (0));
//        }

//        [Fact]
//        public void TestLatencyMetrics()
//        {
//            StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), "");
//            int defaultMetrics = streamsMetrics.metrics().Count;

//            string scope = "scope";
//            string entity = "entity";
//            string operation = "Put";

//            Sensor sensor1 = streamsMetrics.addLatencyAndThroughputSensor(scope, entity, operation, Sensor.RecordingLevel.DEBUG);

//            // 2 meters and 4 non-meter metrics plus a common metric that keeps track of total registered metrics in Metrics() constructor
//            int meterMetricsCount = 2; // Each Meter is a combination of a Rate and a Total
//            int otherMetricsCount = 4;
//            Assert.Equal(defaultMetrics + meterMetricsCount * 2 + otherMetricsCount, streamsMetrics.metrics().Count);

//            streamsMetrics.removeSensor(sensor1);
//            Assert.Equal(defaultMetrics, streamsMetrics.metrics().Count);
//        }

//        [Fact]
//        public void TestThroughputMetrics()
//        {
//            StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), "");
//            int defaultMetrics = streamsMetrics.metrics().Count;

//            string scope = "scope";
//            string entity = "entity";
//            string operation = "Put";

//            Sensor sensor1 = streamsMetrics.addThroughputSensor(scope, entity, operation, Sensor.RecordingLevel.DEBUG);

//            int meterMetricsCount = 2; // Each Meter is a combination of a Rate and a Total
//                                       // 2 meter metrics plus a common metric that keeps track of total registered metrics in Metrics() constructor
//            Assert.Equal(defaultMetrics + meterMetricsCount * 2, streamsMetrics.metrics().Count);

//            streamsMetrics.removeSensor(sensor1);
//            Assert.Equal(defaultMetrics, streamsMetrics.metrics().Count);
//        }

//        //[Fact]
//        //public void TestTotalMetricDoesntDecrease()
//        //{
//        //    MockTime time = new MockTime(1);
//        //    MetricConfig config = new MetricConfig().timeWindow(1, TimeUnit.MILLISECONDS);
//        //    Metrics metrics = new Metrics(config, time);
//        //    StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, "");
//        //
//        //    string scope = "scope";
//        //    string entity = "entity";
//        //    string operation = "op";
//        //
//        //    Sensor sensor = streamsMetrics.addLatencyAndThroughputSensor(
//        //        scope,
//        //        entity,
//        //        operation,
//        //        Sensor.RecordingLevel.INFO
//        //    );
//        //
//        //    double latency = 100.0;
//        //    MetricName totalMetricName = metrics.metricName(
//        //        "op-total",
//        //        "stream-scope-metrics",
//        //        "",
//        //        "client-id",
//        //        "",
//        //        "scope-id",
//        //        "entity"
//        //    );
//        //
//        //    KafkaMetric totalMetric = metrics.metric(totalMetricName);
//        //
//        //    for (int i = 0; i < 10; i++)
//        //    {
//        //        Assert.Equal(i, Math.Round(totalMetric.measurable().measure(config, time.NowAsEpochMilliseconds;)));
//        //        sensor.record(latency, time.NowAsEpochMilliseconds);
//        //    }
//        //
//        //}
//    }
//}
///*






//*

//*





//*/





























