/*






 *

 *





 */





























public class StreamsMetricsImplTest : EasyMockSupport {

    private static string SENSOR_PREFIX_DELIMITER = ".";
    private static string SENSOR_NAME_DELIMITER = ".s.";
    private static string INTERNAL_PREFIX = "internal";

    [Xunit.Fact]
    public void ShouldGetThreadLevelSensor() {
        Metrics metrics = mock(Metrics);
        string threadName = "thread1";
        string sensorName = "sensor1";
        string expectedFullSensorName =
            INTERNAL_PREFIX + SENSOR_PREFIX_DELIMITER + threadName + SENSOR_NAME_DELIMITER + sensorName;
        RecordingLevel recordingLevel = RecordingLevel.DEBUG;
        Sensor[] parents = {};
        EasyMock.expect(metrics.sensor(expectedFullSensorName, recordingLevel, parents)).andReturn(null);

        replayAll();

        StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, threadName);
        Sensor sensor = streamsMetrics.threadLevelSensor(sensorName, recordingLevel);

        verifyAll();

        assertNull(sensor);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void TestNullMetrics() {
        new StreamsMetricsImpl(null, "");
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void TestRemoveNullSensor() {
        StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), "");
        streamsMetrics.removeSensor(null);
    }

    [Xunit.Fact]
    public void TestRemoveSensor() {
        string sensorName = "sensor1";
        string scope = "scope";
        string entity = "entity";
        string operation = "put";
        StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), "");

        Sensor sensor1 = streamsMetrics.addSensor(sensorName, Sensor.RecordingLevel.DEBUG);
        streamsMetrics.removeSensor(sensor1);

        Sensor sensor1a = streamsMetrics.addSensor(sensorName, Sensor.RecordingLevel.DEBUG, sensor1);
        streamsMetrics.removeSensor(sensor1a);

        Sensor sensor2 = streamsMetrics.addLatencyAndThroughputSensor(scope, entity, operation, Sensor.RecordingLevel.DEBUG);
        streamsMetrics.removeSensor(sensor2);

        Sensor sensor3 = streamsMetrics.addThroughputSensor(scope, entity, operation, Sensor.RecordingLevel.DEBUG);
        streamsMetrics.removeSensor(sensor3);

        Assert.Equal(Collections.emptyMap(), streamsMetrics.parentSensors());
    }

    [Xunit.Fact]
    public void TestMutiLevelSensorRemoval() {
        Metrics registry = new Metrics();
        StreamsMetricsImpl metrics = new StreamsMetricsImpl(registry, "");
        foreach (MetricName defaultMetric in registry.metrics().keySet()) {
            registry.removeMetric(defaultMetric);
        }

        string taskName = "taskName";
        string operation = "operation";
        Dictionary<string, string> taskTags = mkMap(mkEntry("tkey", "value"));

        string processorNodeName = "processorNodeName";
        Dictionary<string, string> nodeTags = mkMap(mkEntry("nkey", "value"));

        Sensor parent1 = metrics.taskLevelSensor(taskName, operation, Sensor.RecordingLevel.DEBUG);
        addAvgMaxLatency(parent1, PROCESSOR_NODE_METRICS_GROUP, taskTags, operation);
        addInvocationRateAndCount(parent1, PROCESSOR_NODE_METRICS_GROUP, taskTags, operation, "", "");

        int numberOfTaskMetrics = registry.metrics().Count;

        Sensor sensor1 = metrics.nodeLevelSensor(taskName, processorNodeName, operation, Sensor.RecordingLevel.DEBUG, parent1);
        addAvgMaxLatency(sensor1, PROCESSOR_NODE_METRICS_GROUP, nodeTags, operation);
        addInvocationRateAndCount(sensor1, PROCESSOR_NODE_METRICS_GROUP, nodeTags, operation, "", "");

        Assert.Equal(registry.metrics().Count, greaterThan(numberOfTaskMetrics));

        metrics.removeAllNodeLevelSensors(taskName, processorNodeName);

        Assert.Equal(registry.metrics().Count, (numberOfTaskMetrics));

        Sensor parent2 = metrics.taskLevelSensor(taskName, operation, Sensor.RecordingLevel.DEBUG);
        addAvgMaxLatency(parent2, PROCESSOR_NODE_METRICS_GROUP, taskTags, operation);
        addInvocationRateAndCount(parent2, PROCESSOR_NODE_METRICS_GROUP, taskTags, operation, "", "");

        Assert.Equal(registry.metrics().Count, (numberOfTaskMetrics));

        Sensor sensor2 = metrics.nodeLevelSensor(taskName, processorNodeName, operation, Sensor.RecordingLevel.DEBUG, parent2);
        addAvgMaxLatency(sensor2, PROCESSOR_NODE_METRICS_GROUP, nodeTags, operation);
        addInvocationRateAndCount(sensor2, PROCESSOR_NODE_METRICS_GROUP, nodeTags, operation, "", "");

        Assert.Equal(registry.metrics().Count, greaterThan(numberOfTaskMetrics));

        metrics.removeAllNodeLevelSensors(taskName, processorNodeName);

        Assert.Equal(registry.metrics().Count, (numberOfTaskMetrics));

        metrics.removeAllTaskLevelSensors(taskName);

        Assert.Equal(registry.metrics().Count, (0));
    }

    [Xunit.Fact]
    public void TestLatencyMetrics() {
        StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), "");
        int defaultMetrics = streamsMetrics.metrics().Count;

        string scope = "scope";
        string entity = "entity";
        string operation = "put";

        Sensor sensor1 = streamsMetrics.addLatencyAndThroughputSensor(scope, entity, operation, Sensor.RecordingLevel.DEBUG);

        // 2 meters and 4 non-meter metrics plus a common metric that keeps track of total registered metrics in Metrics() constructor
        int meterMetricsCount = 2; // Each Meter is a combination of a Rate and a Total
        int otherMetricsCount = 4;
        Assert.Equal(defaultMetrics + meterMetricsCount * 2 + otherMetricsCount, streamsMetrics.metrics().Count);

        streamsMetrics.removeSensor(sensor1);
        Assert.Equal(defaultMetrics, streamsMetrics.metrics().Count);
    }

    [Xunit.Fact]
    public void TestThroughputMetrics() {
        StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), "");
        int defaultMetrics = streamsMetrics.metrics().Count;

        string scope = "scope";
        string entity = "entity";
        string operation = "put";

        Sensor sensor1 = streamsMetrics.addThroughputSensor(scope, entity, operation, Sensor.RecordingLevel.DEBUG);

        int meterMetricsCount = 2; // Each Meter is a combination of a Rate and a Total
        // 2 meter metrics plus a common metric that keeps track of total registered metrics in Metrics() constructor
        Assert.Equal(defaultMetrics + meterMetricsCount * 2, streamsMetrics.metrics().Count);

        streamsMetrics.removeSensor(sensor1);
        Assert.Equal(defaultMetrics, streamsMetrics.metrics().Count);
    }

    [Xunit.Fact]
    public void TestTotalMetricDoesntDecrease() {
        MockTime time = new MockTime(1);
        MetricConfig config = new MetricConfig().timeWindow(1, TimeUnit.MILLISECONDS);
        Metrics metrics = new Metrics(config, time);
        StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, "");

        string scope = "scope";
        string entity = "entity";
        string operation = "op";

        Sensor sensor = streamsMetrics.addLatencyAndThroughputSensor(
            scope,
            entity,
            operation,
            Sensor.RecordingLevel.INFO
        );

        double latency = 100.0;
        MetricName totalMetricName = metrics.metricName(
            "op-total",
            "stream-scope-metrics",
            "",
            "client-id",
            "",
            "scope-id",
            "entity"
        );

        KafkaMetric totalMetric = metrics.metric(totalMetricName);

        for (int i = 0; i < 10; i++) {
            Assert.Equal(i, Math.round(totalMetric.measurable().measure(config, time.milliseconds())));
            sensor.record(latency, time.milliseconds());
        }

    }
}
