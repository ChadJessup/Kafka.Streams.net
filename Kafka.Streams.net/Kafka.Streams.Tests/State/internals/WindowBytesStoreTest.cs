//using Confluent.Kafka;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.KStream;
//using Kafka.Streams.Processors.Internals;
//using Kafka.Streams.State;
//using Kafka.Streams.State.KeyValues;
//using Kafka.Streams.State.Windowed;
//using Kafka.Streams.Tests.Helpers;
//using System.Collections.Generic;
//using System.IO;
//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    public abstract class WindowBytesStoreTest
//    {
//        const long WINDOW_SIZE = 3L;
//        const long SEGMENT_INTERVAL = 60_000L;
//        const long RETENTION_PERIOD = 2 * SEGMENT_INTERVAL;

//        IWindowStore<int, string> windowStore;
//        InternalMockProcessorContext context;
//        DirectoryInfo baseDir = new DirectoryInfo(TestUtils.GetTempDirectory());//"test");

//        private StateSerdes<int, string> serdes = new StateSerdes<int, string>("", Serdes.Int(), Serdes.String());

//        List<KeyValuePair<byte[], byte[]>> changeLog = new List<KeyValuePair<byte[], byte[]>>();

//        private IProducer<byte[], byte[]> producer = new MockProducer<>(true,
//            Serdes.ByteArray().Serializer,
//            Serdes.ByteArray().Serializer);

//        public abstract IWindowStore<K, V> BuildWindowStore<K, V>(
//            long retentionPeriod,
//            long windowSize,
//            bool retainDuplicates,
//            ISerde<K> keySerde,
//            ISerde<V> valueSerde);

//        public abstract string GetMetricsScope();

//        public abstract void SetClassLoggerToDebug();

//        private RecordCollectorImpl CreateRecordCollector(string Name)
//        {
//            return new RecordCollectorImpl(Name,
//                new LogContext(Name),
//                new DefaultProductionExceptionHandler())
////                new Metrics().sensor("skipped-records"))
//            {


//            public void send<K1, V1>(string topic,
//                K1 key,
//                V1 value,
//                Headers headers,
//                int partition,
//                long timestamp,
//                ISerializer<K1> keySerializer,
//                ISerializer<V1> valueSerializer)
//            {
//                changeLog.Add(KeyValuePair.Create(
//                    keySerializer.Serialize(topic, headers, key),
//                    valueSerializer.Serialize(topic, headers, value))
//                );
//            }
//        };
//    }


//    public void Setup()
//    {
//        windowStore = buildWindowStore(RETENTION_PERIOD, WINDOW_SIZE, false, Serdes.Int(), Serdes.String());

//        RecordCollector recordCollector = createRecordCollector(windowStore.Name());
//        recordCollector.Init(producer);

//        context = new InternalMockProcessorContext(
//            baseDir,
//            Serdes.String(),
//            Serdes.Int(),
//            recordCollector,
//            new ThreadCache(
//                new LogContext("testCache"),
//                0,
//                new MockStreamsMetrics(new Metrics())));

//        windowStore.Init(context, windowStore);
//    }


//    public void After()
//    {
//        windowStore.Close();
//    }

//    [Fact]
//    public void TestRangeAndSinglePointFetch()
//    {
//        long startTime = SEGMENT_INTERVAL - 4L;

//        putFirstBatch(windowStore, startTime, context);

//        Assert.Equal("zero", windowStore.Fetch(0, startTime));
//        Assert.Equal("one", windowStore.Fetch(1, startTime + 1L));
//        Assert.Equal("two", windowStore.Fetch(2, startTime + 2L));
//        Assert.Equal("four", windowStore.Fetch(4, startTime + 4L));
//        Assert.Equal("five", windowStore.Fetch(5, startTime + 5L));

//        Assert.Equal(
//            new HashSet<>(Collections.singletonList("zero")),
//            toSet(windowStore.Fetch(
//                0,
//                ofEpochMilli(startTime + 0 - WINDOW_SIZE),
//                ofEpochMilli(startTime + 0 + WINDOW_SIZE))));

//        putSecondBatch(windowStore, startTime, context);

//        Assert.Equal("two+1", windowStore.Fetch(2, startTime + 3L));
//        Assert.Equal("two+2", windowStore.Fetch(2, startTime + 4L));
//        Assert.Equal("two+3", windowStore.Fetch(2, startTime + 5L));
//        Assert.Equal("two+4", windowStore.Fetch(2, startTime + 6L));
//        Assert.Equal("two+5", windowStore.Fetch(2, startTime + 7L));
//        Assert.Equal("two+6", windowStore.Fetch(2, startTime + 8L));

//        Assert.Equal(
//            new HashSet<>(Collections.emptyList()),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime - 2L - WINDOW_SIZE),
//                ofEpochMilli(startTime - 2L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(Collections.singletonList("two")),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime - 1L - WINDOW_SIZE),
//                ofEpochMilli(startTime - 1L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(asList("two", "two+1")),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime - WINDOW_SIZE),
//                ofEpochMilli(startTime + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(asList("two", "two+1", "two+2")),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime + 1L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 1L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(asList("two", "two+1", "two+2", "two+3")),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime + 2L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 2L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(asList("two", "two+1", "two+2", "two+3", "two+4")),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime + 3L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 3L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(asList("two", "two+1", "two+2", "two+3", "two+4", "two+5")),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime + 4L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 4L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(asList("two", "two+1", "two+2", "two+3", "two+4", "two+5", "two+6")),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime + 5L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 5L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(asList("two+1", "two+2", "two+3", "two+4", "two+5", "two+6")),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime + 6L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 6L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(asList("two+2", "two+3", "two+4", "two+5", "two+6")),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime + 7L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 7L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(asList("two+3", "two+4", "two+5", "two+6")),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime + 8L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 8L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(asList("two+4", "two+5", "two+6")),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime + 9L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 9L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(asList("two+5", "two+6")),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime + 10L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 10L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(Collections.singletonList("two+6")),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime + 11L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 11L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(Collections.emptyList()),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime + 12L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 12L + WINDOW_SIZE))));

//        // Flush the store and verify All current entries were properly flushed ...
//        windowStore.Flush();

//        Dictionary<int, HashSet<string>> entriesByKey = entriesByKey(changeLog, startTime);

//        Assert.Equal(Utils.mkSet("zero@0"), entriesByKey.Get(0));
//        Assert.Equal(Utils.mkSet("one@1"), entriesByKey.Get(1));
//        Assert.Equal(
//            Utils.mkSet("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"),
//            entriesByKey.Get(2));
//        Assert.Null(entriesByKey.Get(3));
//        Assert.Equal(Utils.mkSet("four@4"), entriesByKey.Get(4));
//        Assert.Equal(Utils.mkSet("five@5"), entriesByKey.Get(5));
//        Assert.Null(entriesByKey.Get(6));
//    }

//    [Fact]
//    public void ShouldGetAll()
//    {
//        long startTime = SEGMENT_INTERVAL - 4L;

//        putFirstBatch(windowStore, startTime, context);

//        KeyValuePair<IWindowed<int>, string> zero = windowedPair(0, "zero", startTime + 0);
//        KeyValuePair<IWindowed<int>, string> one = windowedPair(1, "one", startTime + 1);
//        KeyValuePair<IWindowed<int>, string> two = windowedPair(2, "two", startTime + 2);
//        KeyValuePair<IWindowed<int>, string> four = windowedPair(4, "four", startTime + 4);
//        KeyValuePair<IWindowed<int>, string> five = windowedPair(5, "five", startTime + 5);

//        Assert.Equal(
//            new HashSet<>(asList(zero, one, two, four, five)),
//            toSet(windowStore.All())
//        );
//    }

//    [Fact]
//    public void ShouldFetchAllInTimeRange()
//    {
//        long startTime = SEGMENT_INTERVAL - 4L;

//        putFirstBatch(windowStore, startTime, context);

//        KeyValuePair<IWindowed<int>, string> zero = windowedPair(0, "zero", startTime + 0);
//        KeyValuePair<IWindowed<int>, string> one = windowedPair(1, "one", startTime + 1);
//        KeyValuePair<IWindowed<int>, string> two = windowedPair(2, "two", startTime + 2);
//        KeyValuePair<IWindowed<int>, string> four = windowedPair(4, "four", startTime + 4);
//        KeyValuePair<IWindowed<int>, string> five = windowedPair(5, "five", startTime + 5);

//        Assert.Equal(
//            new HashSet<>(asList(one, two, four)),
//            toSet(windowStore.FetchAll(ofEpochMilli(startTime + 1), ofEpochMilli(startTime + 4)))
//        );
//        Assert.Equal(
//            new HashSet<>(asList(zero, one, two)),
//            toSet(windowStore.FetchAll(ofEpochMilli(startTime + 0), ofEpochMilli(startTime + 3)))
//        );
//        Assert.Equal(
//            new HashSet<>(asList(one, two, four, five)),
//            toSet(windowStore.FetchAll(ofEpochMilli(startTime + 1), ofEpochMilli(startTime + 5)))
//        );
//    }

//    [Fact]
//    public void TestFetchRange()
//    {
//        long startTime = SEGMENT_INTERVAL - 4L;

//        putFirstBatch(windowStore, startTime, context);

//        KeyValuePair<IWindowed<int>, string> zero = windowedPair(0, "zero", startTime + 0);
//        KeyValuePair<IWindowed<int>, string> one = windowedPair(1, "one", startTime + 1);
//        KeyValuePair<IWindowed<int>, string> two = windowedPair(2, "two", startTime + 2);
//        KeyValuePair<IWindowed<int>, string> four = windowedPair(4, "four", startTime + 4);
//        KeyValuePair<IWindowed<int>, string> five = windowedPair(5, "five", startTime + 5);

//        Assert.Equal(
//            new HashSet<>(asList(zero, one)),
//            toSet(windowStore.Fetch(
//                0,
//                1,
//                ofEpochMilli(startTime + 0L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 0L + WINDOW_SIZE)))
//        );
//        Assert.Equal(
//            new HashSet<>(Collections.singletonList(one)),
//            toSet(windowStore.Fetch(
//                1,
//                1,
//                ofEpochMilli(startTime + 0L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 0L + WINDOW_SIZE)))
//        );
//        Assert.Equal(
//            new HashSet<>(asList(one, two)),
//            toSet(windowStore.Fetch(
//                1,
//                3,
//                ofEpochMilli(startTime + 0L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 0L + WINDOW_SIZE)))
//        );
//        Assert.Equal(
//            new HashSet<>(asList(zero, one, two)),
//            toSet(windowStore.Fetch(
//                0,
//                5,
//                ofEpochMilli(startTime + 0L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 0L + WINDOW_SIZE)))
//        );
//        Assert.Equal(
//            new HashSet<>(asList(zero, one, two, four, five)),
//            toSet(windowStore.Fetch(
//                0,
//                5,
//                ofEpochMilli(startTime + 0L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 0L + WINDOW_SIZE + 5L)))
//        );
//        Assert.Equal(
//            new HashSet<>(asList(two, four, five)),
//            toSet(windowStore.Fetch(
//                0,
//                5,
//                ofEpochMilli(startTime + 2L),
//                ofEpochMilli(startTime + 0L + WINDOW_SIZE + 5L)))
//        );
//        Assert.Equal(
//            new HashSet<>(Collections.emptyList()),
//            toSet(windowStore.Fetch(
//                4,
//                5,
//                ofEpochMilli(startTime + 2L),
//                ofEpochMilli(startTime + WINDOW_SIZE)))
//        );
//        Assert.Equal(
//            new HashSet<>(Collections.emptyList()),
//            toSet(windowStore.Fetch(
//                0,
//                3,
//                ofEpochMilli(startTime + 3L),
//                ofEpochMilli(startTime + WINDOW_SIZE + 5)))
//        );
//    }

//    [Fact]
//    public void TestPutAndFetchBefore()
//    {
//        long startTime = SEGMENT_INTERVAL - 4L;

//        putFirstBatch(windowStore, startTime, context);

//        Assert.Equal(
//            new HashSet<>(Collections.singletonList("zero")),
//            toSet(windowStore.Fetch(
//                0,
//                ofEpochMilli(startTime + 0L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 0L))));
//        Assert.Equal(
//            new HashSet<>(Collections.singletonList("one")),
//            toSet(windowStore.Fetch(
//                1,
//                ofEpochMilli(startTime + 1L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 1L))));
//        Assert.Equal(
//            new HashSet<>(Collections.singletonList("two")),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime + 2L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 2L))));
//        Assert.Equal(
//            new HashSet<>(Collections.emptyList()),
//            toSet(windowStore.Fetch(
//                3,
//                ofEpochMilli(startTime + 3L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 3L))));
//        Assert.Equal(
//            new HashSet<>(Collections.singletonList("four")),
//            toSet(windowStore.Fetch(
//                4,
//                ofEpochMilli(startTime + 4L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 4L))));
//        Assert.Equal(
//            new HashSet<>(Collections.singletonList("five")),
//            toSet(windowStore.Fetch(
//                5,
//                ofEpochMilli(startTime + 5L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 5L))));

//        putSecondBatch(windowStore, startTime, context);

//        Assert.Equal(
//            new HashSet<>(Collections.emptyList()),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime - 1L - WINDOW_SIZE),
//                ofEpochMilli(startTime - 1L))));
//        Assert.Equal(
//            new HashSet<>(Collections.emptyList()),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime + 0L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 0L))));
//        Assert.Equal(
//            new HashSet<>(Collections.emptyList()),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime + 1L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 1L))));
//        Assert.Equal(
//            new HashSet<>(Collections.singletonList("two")),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime + 2L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 2L))));
//        Assert.Equal(
//            new HashSet<>(asList("two", "two+1")),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime + 3L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 3L))));
//        Assert.Equal(
//            new HashSet<>(asList("two", "two+1", "two+2")),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime + 4L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 4L))));
//        Assert.Equal(
//            new HashSet<>(asList("two", "two+1", "two+2", "two+3")),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime + 5L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 5L))));
//        Assert.Equal(
//            new HashSet<>(asList("two+1", "two+2", "two+3", "two+4")),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime + 6L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 6L))));
//        Assert.Equal(
//            new HashSet<>(asList("two+2", "two+3", "two+4", "two+5")),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime + 7L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 7L))));
//        Assert.Equal(
//            new HashSet<>(asList("two+3", "two+4", "two+5", "two+6")),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime + 8L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 8L))));
//        Assert.Equal(
//            new HashSet<>(asList("two+4", "two+5", "two+6")),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime + 9L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 9L))));
//        Assert.Equal(
//            new HashSet<>(asList("two+5", "two+6")),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime + 10L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 10L))));
//        Assert.Equal(
//            new HashSet<>(Collections.singletonList("two+6")),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime + 11L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 11L))));
//        Assert.Equal(
//            new HashSet<>(Collections.emptyList()),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime + 12L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 12L))));
//        Assert.Equal(
//            new HashSet<>(Collections.emptyList()),
//            toSet(windowStore.Fetch(
//                2,
//                ofEpochMilli(startTime + 13L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 13L))));

//        // Flush the store and verify All current entries were properly flushed ...
//        windowStore.Flush();

//        Dictionary<int, HashSet<string>> entriesByKey = entriesByKey(changeLog, startTime);
//        Assert.Equal(Utils.mkSet("zero@0"), entriesByKey.Get(0));
//        Assert.Equal(Utils.mkSet("one@1"), entriesByKey.Get(1));
//        Assert.Equal(
//            Utils.mkSet("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"),
//            entriesByKey.Get(2));
//        Assert.Null(entriesByKey.Get(3));
//        Assert.Equal(Utils.mkSet("four@4"), entriesByKey.Get(4));
//        Assert.Equal(Utils.mkSet("five@5"), entriesByKey.Get(5));
//        Assert.Null(entriesByKey.Get(6));
//    }

//    [Fact]
//    public void TestPutAndFetchAfter()
//    {
//        long startTime = SEGMENT_INTERVAL - 4L;

//        putFirstBatch(windowStore, startTime, context);

//        Assert.Equal(
//            new HashSet<>(Collections.singletonList("zero")),
//            toSet(windowStore.Fetch(0, ofEpochMilli(startTime + 0L),
//                ofEpochMilli(startTime + 0L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(Collections.singletonList("one")),
//            toSet(windowStore.Fetch(1, ofEpochMilli(startTime + 1L),
//                ofEpochMilli(startTime + 1L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(Collections.singletonList("two")),
//            toSet(windowStore.Fetch(2, ofEpochMilli(startTime + 2L),
//                ofEpochMilli(startTime + 2L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(Collections.emptyList()),
//            toSet(windowStore.Fetch(3, ofEpochMilli(startTime + 3L),
//                ofEpochMilli(startTime + 3L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(Collections.singletonList("four")),
//            toSet(windowStore.Fetch(4, ofEpochMilli(startTime + 4L),
//                ofEpochMilli(startTime + 4L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(Collections.singletonList("five")),
//            toSet(windowStore.Fetch(5, ofEpochMilli(startTime + 5L),
//                ofEpochMilli(startTime + 5L + WINDOW_SIZE))));

//        putSecondBatch(windowStore, startTime, context);

//        Assert.Equal(
//            new HashSet<>(Collections.emptyList()),
//            toSet(windowStore.Fetch(2, ofEpochMilli(startTime - 2L),
//                ofEpochMilli(startTime - 2L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(Collections.singletonList("two")),
//            toSet(windowStore.Fetch(2, ofEpochMilli(startTime - 1L),
//                ofEpochMilli(startTime - 1L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(asList("two", "two+1")),
//            toSet(windowStore
//                .Fetch(2, ofEpochMilli(startTime), ofEpochMilli(startTime + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(asList("two", "two+1", "two+2")),
//            toSet(windowStore.Fetch(2, ofEpochMilli(startTime + 1L),
//                ofEpochMilli(startTime + 1L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(asList("two", "two+1", "two+2", "two+3")),
//            toSet(windowStore.Fetch(2, ofEpochMilli(startTime + 2L),
//                ofEpochMilli(startTime + 2L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(asList("two+1", "two+2", "two+3", "two+4")),
//            toSet(windowStore.Fetch(2, ofEpochMilli(startTime + 3L),
//                ofEpochMilli(startTime + 3L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(asList("two+2", "two+3", "two+4", "two+5")),
//            toSet(windowStore.Fetch(2, ofEpochMilli(startTime + 4L),
//                ofEpochMilli(startTime + 4L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(asList("two+3", "two+4", "two+5", "two+6")),
//            toSet(windowStore.Fetch(2, ofEpochMilli(startTime + 5L),
//                ofEpochMilli(startTime + 5L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(asList("two+4", "two+5", "two+6")),
//            toSet(windowStore.Fetch(2, ofEpochMilli(startTime + 6L),
//                ofEpochMilli(startTime + 6L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(asList("two+5", "two+6")),
//            toSet(windowStore.Fetch(2, ofEpochMilli(startTime + 7L),
//                ofEpochMilli(startTime + 7L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(Collections.singletonList("two+6")),
//            toSet(windowStore.Fetch(2, ofEpochMilli(startTime + 8L),
//                ofEpochMilli(startTime + 8L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(Collections.emptyList()),
//            toSet(windowStore.Fetch(2, ofEpochMilli(startTime + 9L),
//                ofEpochMilli(startTime + 9L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(Collections.emptyList()),
//            toSet(windowStore.Fetch(2, ofEpochMilli(startTime + 10L),
//                ofEpochMilli(startTime + 10L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(Collections.emptyList()),
//            toSet(windowStore.Fetch(2, ofEpochMilli(startTime + 11L),
//                ofEpochMilli(startTime + 11L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(Collections.emptyList()),
//            toSet(windowStore.Fetch(2, ofEpochMilli(startTime + 12L),
//                ofEpochMilli(startTime + 12L + WINDOW_SIZE))));

//        // Flush the store and verify All current entries were properly flushed ...
//        windowStore.Flush();

//        Dictionary<int, HashSet<string>> entriesByKey = entriesByKey(changeLog, startTime);

//        Assert.Equal(Utils.mkSet("zero@0"), entriesByKey.Get(0));
//        Assert.Equal(Utils.mkSet("one@1"), entriesByKey.Get(1));
//        Assert.Equal(
//            Utils.mkSet("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"),
//            entriesByKey.Get(2));
//        Assert.Null(entriesByKey.Get(3));
//        Assert.Equal(Utils.mkSet("four@4"), entriesByKey.Get(4));
//        Assert.Equal(Utils.mkSet("five@5"), entriesByKey.Get(5));
//        Assert.Null(entriesByKey.Get(6));
//    }

//    [Fact]
//    public void TestPutSameKeyTimestamp()
//    {
//        windowStore = buildWindowStore(RETENTION_PERIOD, WINDOW_SIZE, true, Serdes.Int(), Serdes.String());
//        windowStore.Init(context, windowStore);

//        long startTime = SEGMENT_INTERVAL - 4L;

//        setCurrentTime(startTime);
//        windowStore.Put(0, "zero");

//        Assert.Equal(
//            new HashSet<>(Collections.singletonList("zero")),
//            toSet(windowStore.Fetch(0, ofEpochMilli(startTime - WINDOW_SIZE),
//                ofEpochMilli(startTime + WINDOW_SIZE))));

//        windowStore.Put(0, "zero");
//        windowStore.Put(0, "zero+");
//        windowStore.Put(0, "zero++");

//        Assert.Equal(
//            new HashSet<>(asList("zero", "zero", "zero+", "zero++")),
//            toSet(windowStore.Fetch(
//                0,
//                ofEpochMilli(startTime - WINDOW_SIZE),
//                ofEpochMilli(startTime + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(asList("zero", "zero", "zero+", "zero++")),
//            toSet(windowStore.Fetch(
//                0,
//                ofEpochMilli(startTime + 1L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 1L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(asList("zero", "zero", "zero+", "zero++")),
//            toSet(windowStore.Fetch(
//                0,
//                ofEpochMilli(startTime + 2L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 2L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(asList("zero", "zero", "zero+", "zero++")),
//            toSet(windowStore.Fetch(
//                0,
//                ofEpochMilli(startTime + 3L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 3L + WINDOW_SIZE))));
//        Assert.Equal(
//            new HashSet<>(Collections.emptyList()),
//            toSet(windowStore.Fetch(
//                0,
//                ofEpochMilli(startTime + 4L - WINDOW_SIZE),
//                ofEpochMilli(startTime + 4L + WINDOW_SIZE))));

//        // Flush the store and verify All current entries were properly flushed ...
//        windowStore.Flush();

//        Dictionary<int, HashSet<string>> entriesByKey = entriesByKey(changeLog, startTime);

//        Assert.Equal(Utils.mkSet("zero@0", "zero@0", "zero+@0", "zero++@0"), entriesByKey.Get(0));
//    }

//    [Fact]
//    public void ShouldCloseOpenIteratorsWhenStoreIsClosedAndNotThrowInvalidStateStoreExceptionOnHasNext()
//    {
//        setCurrentTime(0);
//        windowStore.Put(1, "one", 1L);
//        windowStore.Put(1, "two", 2L);
//        windowStore.Put(1, "three", 3L);

//        IWindowStoreIterator<string> iterator = windowStore.Fetch(1, ofEpochMilli(1L), ofEpochMilli(3L));
//        Assert.True(iterator.HasNext());
//        windowStore.Close();

//        Assert.False(iterator.HasNext());
//    }

//    [Fact]
//    public void ShouldFetchAndIterateOverExactKeys()
//    {
//        long windowSize = 0x7a00000000000000L;
//        long retentionPeriod = 0x7a00000000000000L;
//        IWindowStore<string, string> windowStore = buildWindowStore(retentionPeriod,
//                                                                         windowSize,
//                                                                         false,
//                                                                         Serdes.String(),
//                                                                         Serdes.String());

//        windowStore.Init(context, windowStore);

//        windowStore.Put("a", "0001", 0);
//        windowStore.Put("aa", "0002", 0);
//        windowStore.Put("a", "0003", 1);
//        windowStore.Put("aa", "0004", 1);
//        windowStore.Put("a", "0005", 0x7a00000000000000L - 1);

//        Set expected = new HashSet<>(asList("0001", "0003", "0005"));
//        Assert.Equal(toSet(windowStore.Fetch("a", ofEpochMilli(0), ofEpochMilli(long.MaxValue))), (expected));

//        HashSet<KeyValuePair<IWindowed<string>, string>> set =
//            toSet(windowStore.Fetch("a", "a", ofEpochMilli(0), ofEpochMilli(long.MaxValue)));
//        Assert.Equal(set, (new HashSet<>(asList(
//            windowedPair("a", "0001", 0, windowSize),
//            windowedPair("a", "0003", 1, windowSize),
//            windowedPair("a", "0005", 0x7a00000000000000L - 1, windowSize)
//        ))));

//        set = toSet(windowStore.Fetch("aa", "aa", ofEpochMilli(0), ofEpochMilli(long.MaxValue)));
//        Assert.Equal(set, (new HashSet<>(asList(
//            windowedPair("aa", "0002", 0, windowSize),
//            windowedPair("aa", "0004", 1, windowSize)
//        ))));
//    }

//    [Fact]
//    public void TestDeleteAndUpdate()
//    {

//        long currentTime = 0;
//        setCurrentTime(currentTime);
//        windowStore.Put(1, "one");
//        windowStore.Put(1, "one v2");

//        IWindowStoreIterator<string> iterator = windowStore.Fetch(1, 0, currentTime);
//        Assert.Equal(KeyValuePair.Create(currentTime, "one v2"), iterator.MoveNext());

//        windowStore.Put(1, null);
//        iterator = windowStore.Fetch(1, 0, currentTime);
//        Assert.False(iterator.HasNext());
//    }

//    [Fact]
//    public void ShouldReturnNullOnWindowNotFound()
//    {
//        Assert.Null(windowStore.Fetch(1, 0L));
//    }

//    [Fact]// (expected = NullPointerException)
//    public void ShouldThrowNullPointerExceptionOnPutNullKey()
//    {
//        windowStore.Put(null, "anyValue");
//    }

//    [Fact]// (expected = NullPointerException)
//    public void ShouldThrowNullPointerExceptionOnGetNullKey()
//    {
//        windowStore.Fetch(null, ofEpochMilli(1L), ofEpochMilli(2L));
//    }

//    [Fact]// (expected = NullPointerException)
//    public void ShouldThrowNullPointerExceptionOnRangeNullFromKey()
//    {
//        windowStore.Fetch(null, 2, ofEpochMilli(1L), ofEpochMilli(2L));
//    }

//    [Fact]// (expected = NullPointerException)
//    public void ShouldThrowNullPointerExceptionOnRangeNullToKey()
//    {
//        windowStore.Fetch(1, null, ofEpochMilli(1L), ofEpochMilli(2L));
//    }

//    [Fact]
//    public void ShouldFetchAndIterateOverExactBinaryKeys()
//    {
//        IWindowStore<Bytes, string> windowStore = buildWindowStore(RETENTION_PERIOD,
//                                                                        WINDOW_SIZE,
//                                                                        true,
//                                                                        Serdes.Bytes(),
//                                                                        Serdes.String());
//        windowStore.Init(context, windowStore);

//        Bytes key1 = Bytes.Wrap(new byte[] { 0 });
//        Bytes key2 = Bytes.Wrap(new byte[] { 0, 0 });
//        Bytes key3 = Bytes.Wrap(new byte[] { 0, 0, 0 });
//        windowStore.Put(key1, "1", 0);
//        windowStore.Put(key2, "2", 0);
//        windowStore.Put(key3, "3", 0);
//        windowStore.Put(key1, "4", 1);
//        windowStore.Put(key2, "5", 1);
//        windowStore.Put(key3, "6", 59999);
//        windowStore.Put(key1, "7", 59999);
//        windowStore.Put(key2, "8", 59999);
//        windowStore.Put(key3, "9", 59999);

//        Set expectedKey1 = new HashSet<>(asList("1", "4", "7"));
//        Assert.Equal(toSet(windowStore.Fetch(key1, ofEpochMilli(0), ofEpochMilli(long.MaxValue))),
//            equalTo(expectedKey1));
//        Set expectedKey2 = new HashSet<>(asList("2", "5", "8"));
//        Assert.Equal(toSet(windowStore.Fetch(key2, ofEpochMilli(0), ofEpochMilli(long.MaxValue))),
//            equalTo(expectedKey2));
//        Set expectedKey3 = new HashSet<>(asList("3", "6", "9"));
//        Assert.Equal(toSet(windowStore.Fetch(key3, ofEpochMilli(0), ofEpochMilli(long.MaxValue))),
//            equalTo(expectedKey3));
//    }

//    [Fact]
//    public void ShouldReturnSameResultsForSingleKeyFetchAndEqualKeyRangeFetch()
//    {
//        windowStore.Put(1, "one", 0L);
//        windowStore.Put(2, "two", 1L);
//        windowStore.Put(2, "two", 2L);
//        windowStore.Put(3, "three", 3L);

//        IWindowStoreIterator<string> singleKeyIterator = windowStore.Fetch(2, 0L, 5L);
//        IKeyValueIterator<IWindowed<int>, string> keyRangeIterator = windowStore.Fetch(2, 2, 0L, 5L);

//        Assert.Equal(singleKeyIterator.MoveNext().Value, keyRangeIterator.MoveNext().Value);
//        Assert.Equal(singleKeyIterator.MoveNext().Value, keyRangeIterator.MoveNext().Value);
//        Assert.False(singleKeyIterator.HasNext());
//        Assert.False(keyRangeIterator.HasNext());
//    }

//    [Fact]
//    public void ShouldNotThrowInvalidRangeExceptionWithNegativeFromKey()
//    {
//        setClassLoggerToDebug();
//        LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();

//        IKeyValueIterator iterator = windowStore.Fetch(-1, 1, 0L, 10L);
//        Assert.False(iterator.HasNext());

//        List<string> messages = appender.getMessages();
//        Assert.Equal(messages,
//            hasItem("Returning empty iterator for Fetch with invalid key range: from > to. "
//                + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. "
//                + "Note that the built-in numerical serdes do not follow this for negative numbers"));
//    }

//    [Fact]
//    public void ShouldLogAndMeasureExpiredRecords()
//    {
//        setClassLoggerToDebug();
//        LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();

//        // Advance stream time by inserting record with large enough timestamp that records with timestamp 0 are expired
//        windowStore.Put(1, "initial record", 2 * RETENTION_PERIOD);

//        // Try inserting a record with timestamp 0 -- should be dropped
//        windowStore.Put(1, "late record", 0L);
//        windowStore.Put(1, "another on-time record", RETENTION_PERIOD + 1);

//        LogCaptureAppender.Unregister(appender);

//        Dictionary < MetricName, ? : Metric > metrics = context.metrics().metrics();

//        string metricScope = getMetricsScope();

//        Metric dropTotal = metrics.Get(new MetricName(
//            "expired-window-record-drop-total",
//            "stream-" + metricScope + "-metrics",
//            "The total number of occurrence of expired-window-record-drop operations.",
//            mkMap(
//                mkEntry("client-id", "mock"),
//                mkEntry("task-id", "0_0"),
//                mkEntry(metricScope + "-id", windowStore.Name())
//            )
//        ));

//        Metric dropRate = metrics.Get(new MetricName(
//            "expired-window-record-drop-rate",
//            "stream-" + metricScope + "-metrics",
//            "The average number of occurrence of expired-window-record-drop operation per second.",
//            mkMap(
//                mkEntry("client-id", "mock"),
//                mkEntry("task-id", "0_0"),
//                mkEntry(metricScope + "-id", windowStore.Name())
//            )
//        ));

//        Assert.Equal(1.0, dropTotal.metricValue());
//        Assert.NotEqual(0.0, dropRate.metricValue());
//        List<string> messages = appender.getMessages();
//        Assert.Equal(messages, hasItem("Skipping record for expired segment."));
//    }

//    [Fact]
//    public void ShouldNotThrowExceptionWhenFetchRangeIsExpired()
//    {
//        windowStore.Put(1, "one", 0L);
//        windowStore.Put(1, "two", 4 * RETENTION_PERIOD);

//        IWindowStoreIterator<string> iterator = windowStore.Fetch(1, 0L, 10L);

//        Assert.False(iterator.HasNext());
//    }

//    [Fact]
//    public void TestWindowIteratorPeek()
//    {
//        long currentTime = 0;
//        setCurrentTime(currentTime);
//        windowStore.Put(1, "one");

//        IKeyValueIterator<IWindowed<int>, string> iterator = windowStore.FetchAll(0L, currentTime);

//        Assert.True(iterator.HasNext());
//        IWindowed<int> nextKey = iterator.PeekNextKey();

//        Assert.Equal(iterator.PeekNextKey(), nextKey);
//        Assert.Equal(iterator.PeekNextKey(), iterator.MoveNext().Key);
//        Assert.False(iterator.HasNext());
//    }

//    [Fact]
//    public void TestValueIteratorPeek()
//    {
//        windowStore.Put(1, "one", 0L);

//        IWindowStoreIterator<string> iterator = windowStore.Fetch(1, 0L, 10L);

//        Assert.True(iterator.HasNext());
//        long nextKey = iterator.PeekNextKey();

//        Assert.Equal(iterator.PeekNextKey(), nextKey);
//        Assert.Equal(iterator.PeekNextKey(), iterator.MoveNext().Key);
//        Assert.False(iterator.HasNext());
//    }

//    [Fact]
//    public void ShouldNotThrowConcurrentModificationException()
//    {
//        long currentTime = 0;
//        setCurrentTime(currentTime);
//        windowStore.Put(1, "one");

//        currentTime += WINDOW_SIZE * 10;
//        setCurrentTime(currentTime);
//        windowStore.Put(1, "two");

//        IKeyValueIterator<IWindowed<int>, string> iterator = windowStore.All();

//        currentTime += WINDOW_SIZE * 10;
//        setCurrentTime(currentTime);
//        windowStore.Put(1, "three");

//        currentTime += WINDOW_SIZE * 10;
//        setCurrentTime(currentTime);
//        windowStore.Put(2, "four");

//        // Iterator should return All records in store and not throw exception b/c some were added after Fetch
//        Assert.Equal(windowedPair(1, "one", 0), iterator.MoveNext());
//        Assert.Equal(windowedPair(1, "two", WINDOW_SIZE * 10), iterator.MoveNext());
//        Assert.Equal(windowedPair(1, "three", WINDOW_SIZE * 20), iterator.MoveNext());
//        Assert.Equal(windowedPair(2, "four", WINDOW_SIZE * 30), iterator.MoveNext());
//        Assert.False(iterator.HasNext());
//    }

//    [Fact]
//    public void TestFetchDuplicates()
//    {
//        windowStore = buildWindowStore(RETENTION_PERIOD, WINDOW_SIZE, true, Serdes.Int(), Serdes.String());
//        windowStore.Init(context, windowStore);

//        long currentTime = 0;
//        setCurrentTime(currentTime);
//        windowStore.Put(1, "one");
//        windowStore.Put(1, "one-2");

//        currentTime += WINDOW_SIZE * 10;
//        setCurrentTime(currentTime);
//        windowStore.Put(1, "two");
//        windowStore.Put(1, "two-2");

//        currentTime += WINDOW_SIZE * 10;
//        setCurrentTime(currentTime);
//        windowStore.Put(1, "three");
//        windowStore.Put(1, "three-2");

//        IWindowStoreIterator<string> iterator = windowStore.Fetch(1, 0, WINDOW_SIZE * 10);

//        Assert.Equal(new KeyValuePair<long, string>(0L, "one"), iterator.MoveNext());
//        Assert.Equal(new KeyValuePair<long, string>(0L, "one-2"), iterator.MoveNext());
//        Assert.Equal(new KeyValuePair<long, string>(WINDOW_SIZE * 10, "two"), iterator.MoveNext());
//        Assert.Equal(new KeyValuePair<long, string>(WINDOW_SIZE * 10, "two-2"), iterator.MoveNext());
//        Assert.False(iterator.HasNext());
//    }


//    private void PutFirstBatch(IWindowStore<int, string> store,
//         long startTime,
//        InternalMockProcessorContext context)
//    {
//        context.setRecordContext(createRecordContext(startTime));
//        store.Put(0, "zero");
//        context.setRecordContext(createRecordContext(startTime + 1L));
//        store.Put(1, "one");
//        context.setRecordContext(createRecordContext(startTime + 2L));
//        store.Put(2, "two");
//        context.setRecordContext(createRecordContext(startTime + 4L));
//        store.Put(4, "four");
//        context.setRecordContext(createRecordContext(startTime + 5L));
//        store.Put(5, "five");
//    }

//    private void PutSecondBatch(IWindowStore<int, string> store,
//         long startTime,
//        InternalMockProcessorContext context)
//    {
//        context.setRecordContext(createRecordContext(startTime + 3L));
//        store.Put(2, "two+1");
//        context.setRecordContext(createRecordContext(startTime + 4L));
//        store.Put(2, "two+2");
//        context.setRecordContext(createRecordContext(startTime + 5L));
//        store.Put(2, "two+3");
//        context.setRecordContext(createRecordContext(startTime + 6L));
//        store.Put(2, "two+4");
//        context.setRecordContext(createRecordContext(startTime + 7L));
//        store.Put(2, "two+5");
//        context.setRecordContext(createRecordContext(startTime + 8L));
//        store.Put(2, "two+6");
//    }

//    protected static HashSet<E> ToSet<E>(IWindowStoreIterator<E> iterator)
//    {
//        HashSet<E> set = new HashSet<E>();
//        while (iterator.MoveNext())
//        {
//            set.Add(iterator.Current.Value);
//        }
//        return set;
//    }

//    protected static HashSet<KeyValuePair<K, V>> ToSet<K, V>(Iterator<KeyValuePair<K, V>> iterator)
//    {
//        HashSet<KeyValuePair<K, V>> results = new HashSet<KeyValuePair<K, V>>();

//        while (iterator.HasNext())
//        {
//            results.Add(iterator.MoveNext());
//        }
//        return results;
//    }

//    private Dictionary<int, HashSet<string>> EntriesByKey(List<KeyValuePair<byte[], byte[]>> changeLog,
//         {
//        Dictionary<int, HashSet<string>> entriesByKey = new Dictionary<int, HashSet<string>>();

//        foreach (KeyValuePair<byte[], byte[]> entry in changeLog)
//        {
//            long timestamp = WindowKeySchema.extractStoreTimestamp(entry.Key);

//            int key = WindowKeySchema.extractStoreKey(entry.Key, serdes);
//            string value = entry.Value == null ? null : serdes.valueFrom(entry.Value);

//            HashSet<string> entries = entriesByKey.computeIfAbsent(key, k => new HashSet<>());
//            entries.Add(value + "@" + (timestamp - startTime));
//        }

//        return entriesByKey;
//    }

//    protected static KeyValuePair<IWindowed<K>, V> WindowedPair<K, V>(K key, V value, long timestamp)
//    {
//        return windowedPair(key, value, timestamp, WINDOW_SIZE);
//    }

//    private static KeyValuePair<IWindowed<K>, V> WindowedPair<K, V>(K key, V value, long timestamp, long windowSize)
//    {
//        return KeyValuePair.Create(new Windowed<>(key, WindowKeySchema.timeWindowForSize(timestamp, windowSize)), value);
//    }

//    protected void SetCurrentTime(long currentTime)
//    {
//        context.setRecordContext(createRecordContext(currentTime));
//    }

//    private ProcessorRecordContext CreateRecordContext(long time)
//    {
//        return new ProcessorRecordContext(time, 0, 0, "topic", null);
//    }

//}
//}
///*






//*

//*





//*/





























































































