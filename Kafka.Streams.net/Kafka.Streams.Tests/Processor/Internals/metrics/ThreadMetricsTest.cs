namespace Kafka.Streams.Tests.Processor.Internals.metrics
{
    /*






    *

    *





    */


























    @PrepareForTest(StreamsMetricsImpl)
public class ThreadMetricsTest
    {

        private static readonly string THREAD_LEVEL_GROUP = "stream-metrics";
        private static readonly string TASK_LEVEL_GROUP = "stream-task-metrics";

        private Metrics dummyMetrics = new Metrics();
        private Sensor dummySensor = dummyMetrics.sensor("dummy");
        private StreamsMetricsImpl streamsMetrics = createStrictMock(StreamsMetricsImpl);
        private Dictionary<string, string> dummyTagMap = Collections.singletonMap("hello", "world");

        [Xunit.Fact]
        public void ShouldGetCreateTaskSensor()
        {
            string operation = "task-created";
            string totalDescription = "The total number of newly created tasks";
            string rateDescription = "The average per-second number of newly created tasks";
            mockStatic(StreamsMetricsImpl);
            expect(streamsMetrics.threadLevelSensor(operation, RecordingLevel.INFO)).andReturn(dummySensor);
            expect(streamsMetrics.threadLevelTagMap()).andReturn(dummyTagMap);
            StreamsMetricsImpl.addInvocationRateAndCount(
                dummySensor, THREAD_LEVEL_GROUP, dummyTagMap, operation, totalDescription, rateDescription);

            replayAll();
            replay(StreamsMetricsImpl);

            Sensor sensor = ThreadMetrics.createTaskSensor(streamsMetrics);

            verifyAll();
            verify(StreamsMetricsImpl);

            Assert.Equal(sensor, (dummySensor));
        }

        [Xunit.Fact]
        public void ShouldGetCloseTaskSensor()
        {
            string operation = "task-closed";
            string totalDescription = "The total number of closed tasks";
            string rateDescription = "The average per-second number of closed tasks";
            mockStatic(StreamsMetricsImpl);
            expect(streamsMetrics.threadLevelSensor(operation, RecordingLevel.INFO)).andReturn(dummySensor);
            expect(streamsMetrics.threadLevelTagMap()).andReturn(dummyTagMap);
            StreamsMetricsImpl.addInvocationRateAndCount(
                dummySensor, THREAD_LEVEL_GROUP, dummyTagMap, operation, totalDescription, rateDescription);

            replayAll();
            replay(StreamsMetricsImpl);

            Sensor sensor = ThreadMetrics.closeTaskSensor(streamsMetrics);

            verifyAll();
            verify(StreamsMetricsImpl);

            Assert.Equal(sensor, (dummySensor));
        }

        [Xunit.Fact]
        public void ShouldGetCommitSensor()
        {
            string operation = "commit";
            string operationLatency = operation + StreamsMetricsImpl.LATENCY_SUFFIX;
            string totalDescription = "The total number of commit calls";
            string rateDescription = "The average per-second number of commit calls";
            mockStatic(StreamsMetricsImpl);
            expect(streamsMetrics.threadLevelSensor(operation, RecordingLevel.INFO)).andReturn(dummySensor);
            expect(streamsMetrics.threadLevelTagMap()).andReturn(dummyTagMap);
            StreamsMetricsImpl.addInvocationRateAndCount(
                dummySensor, THREAD_LEVEL_GROUP, dummyTagMap, operation, totalDescription, rateDescription);
            StreamsMetricsImpl.addAvgAndMax(
                dummySensor, THREAD_LEVEL_GROUP, dummyTagMap, operationLatency);

            replayAll();
            replay(StreamsMetricsImpl);

            Sensor sensor = ThreadMetrics.commitSensor(streamsMetrics);

            verifyAll();
            verify(StreamsMetricsImpl);

            Assert.Equal(sensor, (dummySensor));
        }

        [Xunit.Fact]
        public void ShouldGetPollSensor()
        {
            string operation = "poll";
            string operationLatency = operation + StreamsMetricsImpl.LATENCY_SUFFIX;
            string totalDescription = "The total number of poll calls";
            string rateDescription = "The average per-second number of poll calls";
            mockStatic(StreamsMetricsImpl);
            expect(streamsMetrics.threadLevelSensor(operation, RecordingLevel.INFO)).andReturn(dummySensor);
            expect(streamsMetrics.threadLevelTagMap()).andReturn(dummyTagMap);
            StreamsMetricsImpl.addInvocationRateAndCount(
                dummySensor, THREAD_LEVEL_GROUP, dummyTagMap, operation, totalDescription, rateDescription);
            StreamsMetricsImpl.addAvgAndMax(
                dummySensor, THREAD_LEVEL_GROUP, dummyTagMap, operationLatency);

            replayAll();
            replay(StreamsMetricsImpl);

            Sensor sensor = ThreadMetrics.pollSensor(streamsMetrics);

            verifyAll();
            verify(StreamsMetricsImpl);

            Assert.Equal(sensor, (dummySensor));
        }

        [Xunit.Fact]
        public void ShouldGetProcessSensor()
        {
            string operation = "process";
            string operationLatency = operation + StreamsMetricsImpl.LATENCY_SUFFIX;
            string totalDescription = "The total number of process calls";
            string rateDescription = "The average per-second number of process calls";
            mockStatic(StreamsMetricsImpl);
            expect(streamsMetrics.threadLevelSensor(operation, RecordingLevel.INFO)).andReturn(dummySensor);
            expect(streamsMetrics.threadLevelTagMap()).andReturn(dummyTagMap);
            StreamsMetricsImpl.addInvocationRateAndCount(
                dummySensor, THREAD_LEVEL_GROUP, dummyTagMap, operation, totalDescription, rateDescription);
            StreamsMetricsImpl.addAvgAndMax(
                dummySensor, THREAD_LEVEL_GROUP, dummyTagMap, operationLatency);

            replayAll();
            replay(StreamsMetricsImpl);

            Sensor sensor = ThreadMetrics.processSensor(streamsMetrics);

            verifyAll();
            verify(StreamsMetricsImpl);

            Assert.Equal(sensor, (dummySensor));
        }

        [Xunit.Fact]
        public void ShouldGetPunctuateSensor()
        {
            string operation = "punctuate";
            string operationLatency = operation + StreamsMetricsImpl.LATENCY_SUFFIX;
            string totalDescription = "The total number of punctuate calls";
            string rateDescription = "The average per-second number of punctuate calls";
            mockStatic(StreamsMetricsImpl);
            expect(streamsMetrics.threadLevelSensor(operation, RecordingLevel.INFO)).andReturn(dummySensor);
            expect(streamsMetrics.threadLevelTagMap()).andReturn(dummyTagMap);
            StreamsMetricsImpl.addInvocationRateAndCount(
                dummySensor, THREAD_LEVEL_GROUP, dummyTagMap, operation, totalDescription, rateDescription);
            StreamsMetricsImpl.addAvgAndMax(
                dummySensor, THREAD_LEVEL_GROUP, dummyTagMap, operationLatency);

            replayAll();
            replay(StreamsMetricsImpl);

            Sensor sensor = ThreadMetrics.punctuateSensor(streamsMetrics);

            verifyAll();
            verify(StreamsMetricsImpl);

            Assert.Equal(sensor, (dummySensor));
        }

        [Xunit.Fact]
        public void ShouldGetSkipRecordSensor()
        {
            string operation = "skipped-records";
            string totalDescription = "The total number of skipped records";
            string rateDescription = "The average per-second number of skipped records";
            mockStatic(StreamsMetricsImpl);
            expect(streamsMetrics.threadLevelSensor(operation, RecordingLevel.INFO)).andReturn(dummySensor);
            expect(streamsMetrics.threadLevelTagMap()).andReturn(dummyTagMap);
            StreamsMetricsImpl.addInvocationRateAndCount(
                dummySensor, THREAD_LEVEL_GROUP, dummyTagMap, operation, totalDescription, rateDescription);

            replayAll();
            replay(StreamsMetricsImpl);

            Sensor sensor = ThreadMetrics.skipRecordSensor(streamsMetrics);

            verifyAll();
            verify(StreamsMetricsImpl);

            Assert.Equal(sensor, (dummySensor));
        }

        [Xunit.Fact]
        public void ShouldGetCommitOverTasksSensor()
        {
            string operation = "commit";
            string operationLatency = operation + StreamsMetricsImpl.LATENCY_SUFFIX;
            string totalDescription = "The total number of commit calls over all tasks";
            string rateDescription = "The average per-second number of commit calls over all tasks";
            mockStatic(StreamsMetricsImpl);
            expect(streamsMetrics.threadLevelSensor(operation, RecordingLevel.DEBUG)).andReturn(dummySensor);
            expect(streamsMetrics.threadLevelTagMap(TASK_ID_TAG, ALL_TASKS)).andReturn(dummyTagMap);
            StreamsMetricsImpl.addInvocationRateAndCount(
                dummySensor, TASK_LEVEL_GROUP, dummyTagMap, operation, totalDescription, rateDescription);
            StreamsMetricsImpl.addAvgAndMax(
                dummySensor, TASK_LEVEL_GROUP, dummyTagMap, operationLatency);

            replayAll();
            replay(StreamsMetricsImpl);

            Sensor sensor = ThreadMetrics.commitOverTasksSensor(streamsMetrics);

            verifyAll();
            verify(StreamsMetricsImpl);

            Assert.Equal(sensor, (dummySensor));
        }
    }
}
/*






*

*





*/


























