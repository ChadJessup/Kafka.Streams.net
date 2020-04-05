//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */
















































//    public class MeteredTimestampedKeyValueStoreTest
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

//        private MeteredTimestampedKeyValueStore<string, string> metered;
//        private readonly string key = "key";
//        private Bytes keyBytes = Bytes.Wrap(key.getBytes());
//        private readonly string value = "value";
//        private ValueAndTimestamp<string> valueAndTimestamp = ValueAndTimestamp.Make("value", 97L);
//        // timestamp is 97 what is ASCII of 'a'
//        private readonly byte[] valueAndTimestampBytes = "\0\0\0\0\0\0\0avalue".getBytes();
//        private KeyValuePair<Bytes, byte[]> byteKeyValueTimestampPair = KeyValuePair.Create(keyBytes, valueAndTimestampBytes);
//        private Metrics metrics = new Metrics();


//        public void Before()
//        {
//            metered = new MeteredTimestampedKeyValueStore<>(
//                inner,
//                "scope",
//                new MockTime(),
//                Serdes.String(),
//                new ValueAndTimestampSerde<>(Serdes.String())
//            );
//            metrics.config().recordLevel(Sensor.RecordingLevel.DEBUG);
//            expect(context.metrics()).andReturn(new MockStreamsMetrics(metrics));
//            expect(context.taskId()).andReturn(taskId);
//            expect(inner.name()).andReturn("metered").anyTimes();
//        }

//        private void Init()
//        {
//            replay(inner, context);
//            metered.Init(context, metered);
//        }

//        [Xunit.Fact]
//        public void TestMetrics()
//        {
//            Init();
//            JmxReporter reporter = new JmxReporter("kafka.streams");
//            metrics.addReporter(reporter);
//            Assert.True(reporter.containsMbean(string.format("kafka.streams:type=stream-%s-metrics,client-id=%s,task-id=%s,%s-id=%s",
//                    "scope", "test", taskId.ToString(), "scope", "metered")));
//            Assert.True(reporter.containsMbean(string.format("kafka.streams:type=stream-%s-metrics,client-id=%s,task-id=%s,%s-id=%s",
//                    "scope", "test", taskId.ToString(), "scope", "all")));
//        }
//        [Xunit.Fact]
//        public void ShouldWriteBytesToInnerStoreAndRecordPutMetric()
//        {
//            inner.put(eq(keyBytes), aryEq(valueAndTimestampBytes));
//            expectLastCall();
//            Init();

//            metered.put(key, valueAndTimestamp);

//            KafkaMetric metric = metric("put-rate");
//            Assert.True((Double)metric.metricValue() > 0);
//            verify(inner);
//        }

//        [Xunit.Fact]
//        public void ShouldGetBytesFromInnerStoreAndReturnGetMetric()
//        {
//            expect(inner.Get(keyBytes)).andReturn(valueAndTimestampBytes);
//            Init();

//            Assert.Equal(metered.Get(key), (valueAndTimestamp));

//            KafkaMetric metric = metric("get-rate");
//            Assert.True((Double)metric.metricValue() > 0);
//            verify(inner);
//        }

//        [Xunit.Fact]
//        public void ShouldPutIfAbsentAndRecordPutIfAbsentMetric()
//        {
//            expect(inner.putIfAbsent(eq(keyBytes), aryEq(valueAndTimestampBytes))).andReturn(null);
//            Init();

//            metered.putIfAbsent(key, valueAndTimestamp);

//            KafkaMetric metric = metric("put-if-absent-rate");
//            Assert.True((Double)metric.metricValue() > 0);
//            verify(inner);
//        }

//        private KafkaMetric Metric(string name)
//        {
//            return this.metrics.metric(new MetricName(name, "stream-scope-metrics", "", this.tags));
//        }


//        [Xunit.Fact]
//        public void ShouldPutAllToInnerStoreAndRecordPutAllMetric()
//        {
//            inner.putAll(anyObject(List));
//            expectLastCall();
//            Init();

//            metered.putAll(Collections.singletonList(KeyValuePair.Create(key, valueAndTimestamp)));

//            KafkaMetric metric = metric("put-all-rate");
//            Assert.True((Double)metric.metricValue() > 0);
//            verify(inner);
//        }

//        [Xunit.Fact]
//        public void ShouldDeleteFromInnerStoreAndRecordDeleteMetric()
//        {
//            expect(inner.delete(keyBytes)).andReturn(valueAndTimestampBytes);
//            Init();

//            metered.delete(key);

//            KafkaMetric metric = metric("delete-rate");
//            Assert.True((Double)metric.metricValue() > 0);
//            verify(inner);
//        }

//        [Xunit.Fact]
//        public void ShouldGetRangeFromInnerStoreAndRecordRangeMetric()
//        {
//            expect(inner.Range(keyBytes, keyBytes)).andReturn(
//                new KeyValueIteratorStub<>(Collections.singletonList(byteKeyValueTimestampPair).iterator()));
//            Init();

//            IKeyValueIterator<string, ValueAndTimestamp<string>> iterator = metered.Range(key, key);
//            Assert.Equal(iterator.MoveNext().value, (valueAndTimestamp));
//            Assert.False(iterator.hasNext());
//            iterator.close();

//            KafkaMetric metric = metric("range-rate");
//            Assert.True((Double)metric.metricValue() > 0);
//            verify(inner);
//        }

//        [Xunit.Fact]
//        public void ShouldGetAllFromInnerStoreAndRecordAllMetric()
//        {
//            expect(inner.all())
//                .andReturn(new KeyValueIteratorStub<>(Collections.singletonList(byteKeyValueTimestampPair).iterator()));
//            Init();

//            IKeyValueIterator<string, ValueAndTimestamp<string>> iterator = metered.all();
//            Assert.Equal(iterator.MoveNext().value, (valueAndTimestamp));
//            Assert.False(iterator.hasNext());
//            iterator.close();

//            KafkaMetric metric = metric(new MetricName("all-rate", "stream-scope-metrics", "", tags));
//            Assert.True((Double)metric.metricValue() > 0);
//            verify(inner);
//        }

//        [Xunit.Fact]
//        public void ShouldFlushInnerWhenFlushTimeRecords()
//        {
//            inner.flush();
//            expectLastCall().once();
//            Init();

//            metered.flush();

//            KafkaMetric metric = metric("flush-rate");
//            Assert.True((Double)metric.metricValue() > 0);
//            verify(inner);
//        }

//        private interface CachedKeyValueStore : IKeyValueStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> { }


//        [Xunit.Fact]
//        public void ShouldSetFlushListenerOnWrappedCachingStore()
//        {
//            CachedKeyValueStore cachedKeyValueStore = mock(CachedKeyValueStore);

//            expect(cachedKeyValueStore.setFlushListener(anyObject(CacheFlushListener), eq(false))).andReturn(true);
//            replay(cachedKeyValueStore);

//            metered = new MeteredTimestampedKeyValueStore<>(
//                cachedKeyValueStore,
//                "scope",
//                new MockTime(),
//                Serdes.String(),
//                new ValueAndTimestampSerde<>(Serdes.String()));
//            Assert.True(metered.setFlushListener(null, false));

//            verify(cachedKeyValueStore);
//        }

//        [Xunit.Fact]
//        public void ShouldNotSetFlushListenerOnWrappedNoneCachingStore()
//        {
//            Assert.False(metered.setFlushListener(null, false));
//        }

//        private KafkaMetric Metric(MetricName metricName)
//        {
//            return this.metrics.metric(metricName);
//        }

//        [Xunit.Fact]

//        public void ShouldNotThrowExceptionIfSerdesCorrectlySetFromProcessorContext()
//        {
//            expect(context.keySerde()).andStubReturn((Serde)Serdes.String());
//            expect(context.valueSerde()).andStubReturn((Serde)Serdes.Long());
//            MeteredTimestampedKeyValueStore<string, long> store = new MeteredTimestampedKeyValueStore<>(
//                inner,
//                "scope",
//                new MockTime(),
//                null,
//                null
//            );
//            replay(inner, context);
//            store.Init(context, inner);

//            try
//            {
//                store.put("key", ValueAndTimestamp.Make(42L, 60000));
//            }
//            catch (StreamsException exception)
//            {
//                if (exception.getCause() is ClassCastException)
//                {
//                    Assert.True(false, "Serdes are not correctly set from processor context.");
//                }
//                throw exception;
//            }
//        }

//        [Xunit.Fact]

//        public void ShouldNotThrowExceptionIfSerdesCorrectlySetFromConstructorParameters()
//        {
//            expect(context.keySerde()).andStubReturn((Serde)Serdes.String());
//            expect(context.valueSerde()).andStubReturn((Serde)Serdes.Long());
//            MeteredTimestampedKeyValueStore<string, long> store = new MeteredTimestampedKeyValueStore<>(
//                inner,
//                "scope",
//                new MockTime(),
//                Serdes.String(),
//                new ValueAndTimestampSerde<>(Serdes.Long())
//            );
//            replay(inner, context);
//            store.Init(context, inner);

//            try
//            {
//                store.put("key", ValueAndTimestamp.Make(42L, 60000));
//            }
//            catch (StreamsException exception)
//            {
//                if (exception.getCause() is ClassCastException)
//                {
//                    Assert.True(false, "Serdes are not correctly set from constructor parameters.");
//                }
//                throw exception;
//            }
//        }
//    }
//}
///*






//*

//*





//*/
















































