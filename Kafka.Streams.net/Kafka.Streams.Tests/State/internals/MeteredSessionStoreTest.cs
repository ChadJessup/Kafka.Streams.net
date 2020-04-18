//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */















































//    public class MeteredSessionStoreTest
//    {

//        private TaskId taskId = new TaskId(0, 0);
//        private Dictionary<string, string> tags = mkMap(
//            mkEntry("client-id", "test"),
//            mkEntry("task-id", taskId.ToString()),
//            mkEntry("scope-id", "metered")
//        );
//        private Metrics metrics = new Metrics();
//        private MeteredSessionStore<string, string> metered;
//        (type = MockType.NICE)
//    private ISessionStore<Bytes, byte[]> inner;
//        (type = MockType.NICE)
//    private ProcessorContext context;

//        private readonly string key = "a";
//        private readonly byte[] keyBytes = key.getBytes();
//        private IWindowed<Bytes> windowedKeyBytes = new Windowed<>(Bytes.Wrap(keyBytes), new SessionWindow(0, 0));


//        public void Before()
//        {
//            metered = new MeteredSessionStore<>(
//                inner,
//                "scope",
//                Serdes.String(),
//                Serdes.String(),
//                new MockTime());
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
//            inner.Put(eq(windowedKeyBytes), aryEq(keyBytes));
//            expectLastCall();
//            Init();

//            metered.Put(new Windowed<>(key, new SessionWindow(0, 0)), key);

//            KafkaMetric metric = metric("Put-rate");
//            Assert.True(((Double)metric.metricValue()) > 0);
//            verify(inner);
//        }

//        [Fact]
//        public void ShouldFindSessionsFromStoreAndRecordFetchMetric()
//        {
//            expect(inner.findSessions(Bytes.Wrap(keyBytes), 0, 0))
//                    .andReturn(new KeyValueIteratorStub<>(
//                            Collections.singleton(KeyValuePair.Create(windowedKeyBytes, keyBytes)).iterator()));
//            Init();

//            IKeyValueIterator<IWindowed<string>, string> iterator = metered.findSessions(key, 0, 0);
//            Assert.Equal(iterator.MoveNext().Value, (key));
//            Assert.False(iterator.HasNext());
//            iterator.Close();

//            KafkaMetric metric = metric("Fetch-rate");
//            Assert.True((Double)metric.metricValue() > 0);
//            verify(inner);
//        }

//        [Fact]
//        public void ShouldFindSessionRangeFromStoreAndRecordFetchMetric()
//        {
//            expect(inner.findSessions(Bytes.Wrap(keyBytes), Bytes.Wrap(keyBytes), 0, 0))
//                    .andReturn(new KeyValueIteratorStub<>(
//                            Collections.singleton(KeyValuePair.Create(windowedKeyBytes, keyBytes)).iterator()));
//            Init();

//            IKeyValueIterator<IWindowed<string>, string> iterator = metered.findSessions(key, key, 0, 0);
//            Assert.Equal(iterator.MoveNext().Value, (key));
//            Assert.False(iterator.HasNext());
//            iterator.Close();

//            KafkaMetric metric = metric("Fetch-rate");
//            Assert.True((Double)metric.metricValue() > 0);
//            verify(inner);
//        }

//        [Fact]
//        public void ShouldRemoveFromStoreAndRecordRemoveMetric()
//        {
//            inner.remove(windowedKeyBytes);
//            expectLastCall();

//            Init();

//            metered.remove(new Windowed<>(key, new SessionWindow(0, 0)));

//            KafkaMetric metric = metric("remove-rate");
//            Assert.True((Double)metric.metricValue() > 0);
//            verify(inner);
//        }

//        [Fact]
//        public void ShouldFetchForKeyAndRecordFetchMetric()
//        {
//            expect(inner.Fetch(Bytes.Wrap(keyBytes)))
//                    .andReturn(new KeyValueIteratorStub<>(
//                            Collections.singleton(KeyValuePair.Create(windowedKeyBytes, keyBytes)).iterator()));
//            Init();

//            IKeyValueIterator<IWindowed<string>, string> iterator = metered.Fetch(key);
//            Assert.Equal(iterator.MoveNext().Value, (key));
//            Assert.False(iterator.HasNext());
//            iterator.Close();

//            KafkaMetric metric = metric("Fetch-rate");
//            Assert.True((Double)metric.metricValue() > 0);
//            verify(inner);
//        }

//        [Fact]
//        public void ShouldFetchRangeFromStoreAndRecordFetchMetric()
//        {
//            expect(inner.Fetch(Bytes.Wrap(keyBytes), Bytes.Wrap(keyBytes)))
//                    .andReturn(new KeyValueIteratorStub<>(
//                            Collections.singleton(KeyValuePair.Create(windowedKeyBytes, keyBytes)).iterator()));
//            Init();

//            IKeyValueIterator<IWindowed<string>, string> iterator = metered.Fetch(key, key);
//            Assert.Equal(iterator.MoveNext().Value, (key));
//            Assert.False(iterator.HasNext());
//            iterator.Close();

//            KafkaMetric metric = metric("Fetch-rate");
//            Assert.True((Double)metric.metricValue() > 0);
//            verify(inner);
//        }

//        [Fact]
//        public void ShouldRecordRestoreTimeOnInit()
//        {
//            Init();
//            KafkaMetric metric = metric("restore-rate");
//            Assert.True((Double)metric.metricValue() > 0);
//        }

//        [Fact]
//        public void ShouldNotThrowNullPointerExceptionIfFetchSessionReturnsNull()
//        {
//            expect(inner.FetchSession(Bytes.Wrap("a".getBytes()), 0, long.MaxValue)).andReturn(null);

//            Init();
//            Assert.Null(metered.FetchSession("a", 0, long.MaxValue));
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerOnPutIfKeyIsNull()
//        {
//            metered.Put(null, "a");
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerOnRemoveIfKeyIsNull()
//        {
//            metered.remove(null);
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerOnFetchIfKeyIsNull()
//        {
//            metered.Fetch(null);
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerOnFetchRangeIfFromIsNull()
//        {
//            metered.Fetch(null, "to");
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerOnFetchRangeIfToIsNull()
//        {
//            metered.Fetch("from", null);
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerOnFindSessionsIfKeyIsNull()
//        {
//            metered.findSessions(null, 0, 0);
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerOnFindSessionsRangeIfFromIsNull()
//        {
//            metered.findSessions(null, "a", 0, 0);
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerOnFindSessionsRangeIfToIsNull()
//        {
//            metered.findSessions("a", null, 0, 0);
//        }

//        private interface CachedSessionStore : ISessionStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> { }


//        [Fact]
//        public void ShouldSetFlushListenerOnWrappedCachingStore()
//        {
//            CachedSessionStore cachedSessionStore = Mock.Of<CachedSessionStore);

//            expect(cachedSessionStore.SetFlushListener(default(CacheFlushListener), eq(false))).andReturn(true);
//            replay(cachedSessionStore);

//            metered = new MeteredSessionStore<>(
//                cachedSessionStore,
//                "scope",
//                Serdes.String(),
//                Serdes.String(),
//                new MockTime());
//            Assert.True(metered.SetFlushListener(null, false));

//            verify(cachedSessionStore);
//        }

//        [Fact]
//        public void ShouldNotSetFlushListenerOnWrappedNoneCachingStore()
//        {
//            Assert.False(metered.SetFlushListener(null, false));
//        }

//        private KafkaMetric Metric(string Name)
//        {
//            return this.metrics.metric(new MetricName(Name, "stream-scope-metrics", "", this.tags));
//        }

//    }
//}
///*






//*

//*





//*/















































