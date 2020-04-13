//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */













































//    public class MeteredKeyValueStoreTest
//    {

//        private TaskId taskId = new TaskId(0, 0);
//        private Dictionary<string, string> tags = mkMap(
//            mkEntry("client-id", "test"),
//            mkEntry("task-id", taskId.ToString()),
//            mkEntry("scope-id", "metered")
//        );
//        (type = MockType.NICE)
//    private IKeyValueStore<Bytes, byte[]> inner;
//        (type = MockType.NICE)
//    private ProcessorContext context;

//        private MeteredKeyValueStore<string, string> metered;
//        private readonly string key = "key";
//        private Bytes keyBytes = Bytes.Wrap(key.getBytes());
//        private readonly string value = "value";
//        private readonly byte[] valueBytes = value.getBytes();
//        private KeyValuePair<Bytes, byte[]> byteKeyValuePair = KeyValuePair.Create(keyBytes, valueBytes);
//        private Metrics metrics = new Metrics();


//        public void Before()
//        {
//            metered = new MeteredKeyValueStore<>(
//                inner,
//                "scope",
//                new MockTime(),
//                Serdes.String(),
//                Serdes.String()
//            );
//            metrics.config().recordLevel(Sensor.RecordingLevel.DEBUG);
//            expect(context.metrics()).andReturn(new MockStreamsMetrics(metrics));
//            expect(context.taskId()).andReturn(taskId);
//            expect(inner.Name()).andReturn("metered").anyTimes();
//        }

//        private void Init()
//        {
//            replay(inner, context);
//            metered.Init(context, metered);
//        }

//        [Fact]
//        public void TestMetrics()
//        {
//            Init();
//            JmxReporter reporter = new JmxReporter("kafka.streams");
//            metrics.addReporter(reporter);
//            Assert.True(reporter.containsMbean(string.format("kafka.streams:type=stream-%s-metrics,client-id=%s,task-id=%s,%s-id=%s",
//                    "scope", "test", taskId.ToString(), "scope", "metered")));
//            Assert.True(reporter.containsMbean(string.format("kafka.streams:type=stream-%s-metrics,client-id=%s,task-id=%s,%s-id=%s",
//                    "scope", "test", taskId.ToString(), "scope", "All")));
//        }

//        [Fact]
//        public void ShouldWriteBytesToInnerStoreAndRecordPutMetric()
//        {
//            inner.Put(eq(keyBytes), aryEq(valueBytes));
//            expectLastCall();
//            Init();

//            metered.Put(key, value);

//            KafkaMetric metric = metric("Put-rate");
//            Assert.True((Double)metric.metricValue() > 0);
//            verify(inner);
//        }

//        [Fact]
//        public void ShouldGetBytesFromInnerStoreAndReturnGetMetric()
//        {
//            expect(inner.Get(keyBytes)).andReturn(valueBytes);
//            Init();

//            Assert.Equal(metered.Get(key), (value));

//            KafkaMetric metric = metric("get-rate");
//            Assert.True((Double)metric.metricValue() > 0);
//            verify(inner);
//        }

//        [Fact]
//        public void ShouldPutIfAbsentAndRecordPutIfAbsentMetric()
//        {
//            expect(inner.PutIfAbsent(eq(keyBytes), aryEq(valueBytes))).andReturn(null);
//            Init();

//            metered.PutIfAbsent(key, value);

//            KafkaMetric metric = metric("Put-if-absent-rate");
//            Assert.True((Double)metric.metricValue() > 0);
//            verify(inner);
//        }

//        private KafkaMetric Metric(string Name)
//        {
//            return this.metrics.metric(new MetricName(Name, "stream-scope-metrics", "", this.tags));
//        }


//        [Fact]
//        public void ShouldPutAllToInnerStoreAndRecordPutAllMetric()
//        {
//            inner.PutAll(default(List));
//            expectLastCall();
//            Init();

//            metered.PutAll(Collections.singletonList(KeyValuePair.Create(key, value)));

//            KafkaMetric metric = metric("Put-All-rate");
//            Assert.True((Double)metric.metricValue() > 0);
//            verify(inner);
//        }

//        [Fact]
//        public void ShouldDeleteFromInnerStoreAndRecordDeleteMetric()
//        {
//            expect(inner.Delete(keyBytes)).andReturn(valueBytes);
//            Init();

//            metered.Delete(key);

//            KafkaMetric metric = metric("delete-rate");
//            Assert.True((Double)metric.metricValue() > 0);
//            verify(inner);
//        }

//        [Fact]
//        public void ShouldGetRangeFromInnerStoreAndRecordRangeMetric()
//        {
//            expect(inner.Range(keyBytes, keyBytes))
//                .andReturn(new KeyValueIteratorStub<>(Collections.singletonList(byteKeyValuePair).iterator()));
//            Init();

//            IKeyValueIterator<string, string> iterator = metered.Range(key, key);
//            Assert.Equal(iterator.MoveNext().Value, (value));
//            Assert.False(iterator.HasNext());
//            iterator.Close();

//            KafkaMetric metric = metric("range-rate");
//            Assert.True((Double)metric.metricValue() > 0);
//            verify(inner);
//        }

//        [Fact]
//        public void ShouldGetAllFromInnerStoreAndRecordAllMetric()
//        {
//            expect(inner.All()).andReturn(new KeyValueIteratorStub<>(Collections.singletonList(byteKeyValuePair).iterator()));
//            Init();

//            IKeyValueIterator<string, string> iterator = metered.All();
//            Assert.Equal(iterator.MoveNext().Value, (value));
//            Assert.False(iterator.HasNext());
//            iterator.Close();

//            KafkaMetric metric = metric(new MetricName("All-rate", "stream-scope-metrics", "", tags));
//            Assert.True((Double)metric.metricValue() > 0);
//            verify(inner);
//        }

//        [Fact]
//        public void ShouldFlushInnerWhenFlushTimeRecords()
//        {
//            inner.Flush();
//            expectLastCall().once();
//            Init();

//            metered.Flush();

//            KafkaMetric metric = metric("Flush-rate");
//            Assert.True((Double)metric.metricValue() > 0);
//            verify(inner);
//        }

//        private interface CachedKeyValueStore : IKeyValueStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> { }


//        [Fact]
//        public void ShouldSetFlushListenerOnWrappedCachingStore()
//        {
//            CachedKeyValueStore cachedKeyValueStore = Mock.Of<CachedKeyValueStore);

//            expect(cachedKeyValueStore.SetFlushListener(default(CacheFlushListener), eq(false))).andReturn(true);
//            replay(cachedKeyValueStore);

//            metered = new MeteredKeyValueStore<>(
//                cachedKeyValueStore,
//                "scope",
//                new MockTime(),
//                Serdes.String(),
//                Serdes.String()
//            );
//            Assert.True(metered.SetFlushListener(null, false));

//            verify(cachedKeyValueStore);
//        }

//        [Fact]
//        public void ShouldNotThrowNullPointerExceptionIfGetReturnsNull()
//        {
//            expect(inner.Get(Bytes.Wrap("a".getBytes()))).andReturn(null);

//            Init();
//            Assert.Null(metered.Get("a"));
//        }

//        [Fact]
//        public void ShouldNotSetFlushListenerOnWrappedNoneCachingStore()
//        {
//            Assert.False(metered.SetFlushListener(null, false));
//        }

//        private KafkaMetric Metric(MetricName metricName)
//        {
//            return this.metrics.metric(metricName);
//        }

//    }
//}
///*






//*

//*





//*/













































