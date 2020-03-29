/*






 *

 *





 */























































public abstract class WindowBytesStoreTest {

    static long WINDOW_SIZE = 3L;
    static long SEGMENT_INTERVAL = 60_000L;
    static long RETENTION_PERIOD = 2 * SEGMENT_INTERVAL;

    WindowStore<int, string> windowStore;
    InternalMockProcessorContext context;
    File baseDir = TestUtils.tempDirectory("test");

    private StateSerdes<int, string> serdes = new StateSerdes<>("", Serdes.Int(), Serdes.String());

    List<KeyValuePair<byte[], byte[]>> changeLog = new ArrayList<>();

    private Producer<byte[], byte[]> producer = new MockProducer<>(true,
        Serdes.ByteArray().Serializer,
        Serdes.ByteArray().Serializer);

    abstract WindowStore<K, V> BuildWindowStore<K, V>(long retentionPeriod,
                                                               long windowSize,
                                                               bool retainDuplicates,
                                                               Serde<K> keySerde,
                                                               Serde<V> valueSerde);

    abstract string GetMetricsScope();

    abstract void SetClassLoggerToDebug();

    private RecordCollectorImpl CreateRecordCollector(string name) {
        return new RecordCollectorImpl(name,
            new LogContext(name),
            new DefaultProductionExceptionHandler(),
            new Metrics().sensor("skipped-records")) {
            
            public void send<K1, V1>(string topic,
                K1 key,
                V1 value,
                Headers headers,
                int partition,
                long timestamp,
                Serializer<K1> keySerializer,
                Serializer<V1> valueSerializer) {
                changeLog.add(new KeyValuePair<>(
                    keySerializer.serialize(topic, headers, key),
                    valueSerializer.serialize(topic, headers, value))
                );
            }
        };
    }

    
    public void Setup() {
        windowStore = buildWindowStore(RETENTION_PERIOD, WINDOW_SIZE, false, Serdes.Int(), Serdes.String());

        RecordCollector recordCollector = createRecordCollector(windowStore.name());
        recordCollector.init(producer);

        context = new InternalMockProcessorContext(
            baseDir,
            Serdes.String(),
            Serdes.Int(),
            recordCollector,
            new ThreadCache(
                new LogContext("testCache"),
                0,
                new MockStreamsMetrics(new Metrics())));

        windowStore.init(context, windowStore);
    }

    
    public void After() {
        windowStore.close();
    }

    [Xunit.Fact]
    public void TestRangeAndSinglePointFetch() {
        long startTime = SEGMENT_INTERVAL - 4L;

        putFirstBatch(windowStore, startTime, context);

        Assert.Equal("zero", windowStore.fetch(0, startTime));
        Assert.Equal("one", windowStore.fetch(1, startTime + 1L));
        Assert.Equal("two", windowStore.fetch(2, startTime + 2L));
        Assert.Equal("four", windowStore.fetch(4, startTime + 4L));
        Assert.Equal("five", windowStore.fetch(5, startTime + 5L));

        Assert.Equal(
            new HashSet<>(Collections.singletonList("zero")),
            toSet(windowStore.fetch(
                0,
                ofEpochMilli(startTime + 0 - WINDOW_SIZE),
                ofEpochMilli(startTime + 0 + WINDOW_SIZE))));

        putSecondBatch(windowStore, startTime, context);

        Assert.Equal("two+1", windowStore.fetch(2, startTime + 3L));
        Assert.Equal("two+2", windowStore.fetch(2, startTime + 4L));
        Assert.Equal("two+3", windowStore.fetch(2, startTime + 5L));
        Assert.Equal("two+4", windowStore.fetch(2, startTime + 6L));
        Assert.Equal("two+5", windowStore.fetch(2, startTime + 7L));
        Assert.Equal("two+6", windowStore.fetch(2, startTime + 8L));

        Assert.Equal(
            new HashSet<>(Collections.emptyList()),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime - 2L - WINDOW_SIZE),
                ofEpochMilli(startTime - 2L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(Collections.singletonList("two")),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime - 1L - WINDOW_SIZE),
                ofEpochMilli(startTime - 1L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(asList("two", "two+1")),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime - WINDOW_SIZE),
                ofEpochMilli(startTime + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(asList("two", "two+1", "two+2")),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 1L - WINDOW_SIZE),
                ofEpochMilli(startTime + 1L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(asList("two", "two+1", "two+2", "two+3")),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 2L - WINDOW_SIZE),
                ofEpochMilli(startTime + 2L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(asList("two", "two+1", "two+2", "two+3", "two+4")),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 3L - WINDOW_SIZE),
                ofEpochMilli(startTime + 3L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(asList("two", "two+1", "two+2", "two+3", "two+4", "two+5")),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 4L - WINDOW_SIZE),
                ofEpochMilli(startTime + 4L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(asList("two", "two+1", "two+2", "two+3", "two+4", "two+5", "two+6")),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 5L - WINDOW_SIZE),
                ofEpochMilli(startTime + 5L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(asList("two+1", "two+2", "two+3", "two+4", "two+5", "two+6")),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 6L - WINDOW_SIZE),
                ofEpochMilli(startTime + 6L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(asList("two+2", "two+3", "two+4", "two+5", "two+6")),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 7L - WINDOW_SIZE),
                ofEpochMilli(startTime + 7L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(asList("two+3", "two+4", "two+5", "two+6")),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 8L - WINDOW_SIZE),
                ofEpochMilli(startTime + 8L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(asList("two+4", "two+5", "two+6")),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 9L - WINDOW_SIZE),
                ofEpochMilli(startTime + 9L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(asList("two+5", "two+6")),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 10L - WINDOW_SIZE),
                ofEpochMilli(startTime + 10L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(Collections.singletonList("two+6")),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 11L - WINDOW_SIZE),
                ofEpochMilli(startTime + 11L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(Collections.emptyList()),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 12L - WINDOW_SIZE),
                ofEpochMilli(startTime + 12L + WINDOW_SIZE))));

        // Flush the store and verify all current entries were properly flushed ...
        windowStore.flush();

        Dictionary<int, HashSet<string>> entriesByKey = entriesByKey(changeLog, startTime);

        Assert.Equal(Utils.mkSet("zero@0"), entriesByKey.get(0));
        Assert.Equal(Utils.mkSet("one@1"), entriesByKey.get(1));
        Assert.Equal(
            Utils.mkSet("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"),
            entriesByKey.get(2));
        assertNull(entriesByKey.get(3));
        Assert.Equal(Utils.mkSet("four@4"), entriesByKey.get(4));
        Assert.Equal(Utils.mkSet("five@5"), entriesByKey.get(5));
        assertNull(entriesByKey.get(6));
    }

    [Xunit.Fact]
    public void ShouldGetAll() {
        long startTime = SEGMENT_INTERVAL - 4L;

        putFirstBatch(windowStore, startTime, context);

        KeyValuePair<Windowed<int>, string> zero = windowedPair(0, "zero", startTime + 0);
        KeyValuePair<Windowed<int>, string> one = windowedPair(1, "one", startTime + 1);
        KeyValuePair<Windowed<int>, string> two = windowedPair(2, "two", startTime + 2);
        KeyValuePair<Windowed<int>, string> four = windowedPair(4, "four", startTime + 4);
        KeyValuePair<Windowed<int>, string> five = windowedPair(5, "five", startTime + 5);

        Assert.Equal(
            new HashSet<>(asList(zero, one, two, four, five)),
            toSet(windowStore.all())
        );
    }

    [Xunit.Fact]
    public void ShouldFetchAllInTimeRange() {
        long startTime = SEGMENT_INTERVAL - 4L;

        putFirstBatch(windowStore, startTime, context);

        KeyValuePair<Windowed<int>, string> zero = windowedPair(0, "zero", startTime + 0);
        KeyValuePair<Windowed<int>, string> one = windowedPair(1, "one", startTime + 1);
        KeyValuePair<Windowed<int>, string> two = windowedPair(2, "two", startTime + 2);
        KeyValuePair<Windowed<int>, string> four = windowedPair(4, "four", startTime + 4);
        KeyValuePair<Windowed<int>, string> five = windowedPair(5, "five", startTime + 5);

        Assert.Equal(
            new HashSet<>(asList(one, two, four)),
            toSet(windowStore.fetchAll(ofEpochMilli(startTime + 1), ofEpochMilli(startTime + 4)))
        );
        Assert.Equal(
            new HashSet<>(asList(zero, one, two)),
            toSet(windowStore.fetchAll(ofEpochMilli(startTime + 0), ofEpochMilli(startTime + 3)))
        );
        Assert.Equal(
            new HashSet<>(asList(one, two, four, five)),
            toSet(windowStore.fetchAll(ofEpochMilli(startTime + 1), ofEpochMilli(startTime + 5)))
        );
    }

    [Xunit.Fact]
    public void TestFetchRange() {
        long startTime = SEGMENT_INTERVAL - 4L;

        putFirstBatch(windowStore, startTime, context);

        KeyValuePair<Windowed<int>, string> zero = windowedPair(0, "zero", startTime + 0);
        KeyValuePair<Windowed<int>, string> one = windowedPair(1, "one", startTime + 1);
        KeyValuePair<Windowed<int>, string> two = windowedPair(2, "two", startTime + 2);
        KeyValuePair<Windowed<int>, string> four = windowedPair(4, "four", startTime + 4);
        KeyValuePair<Windowed<int>, string> five = windowedPair(5, "five", startTime + 5);

        Assert.Equal(
            new HashSet<>(asList(zero, one)),
            toSet(windowStore.fetch(
                0,
                1,
                ofEpochMilli(startTime + 0L - WINDOW_SIZE),
                ofEpochMilli(startTime + 0L + WINDOW_SIZE)))
        );
        Assert.Equal(
            new HashSet<>(Collections.singletonList(one)),
            toSet(windowStore.fetch(
                1,
                1,
                ofEpochMilli(startTime + 0L - WINDOW_SIZE),
                ofEpochMilli(startTime + 0L + WINDOW_SIZE)))
        );
        Assert.Equal(
            new HashSet<>(asList(one, two)),
            toSet(windowStore.fetch(
                1,
                3,
                ofEpochMilli(startTime + 0L - WINDOW_SIZE),
                ofEpochMilli(startTime + 0L + WINDOW_SIZE)))
        );
        Assert.Equal(
            new HashSet<>(asList(zero, one, two)),
            toSet(windowStore.fetch(
                0,
                5,
                ofEpochMilli(startTime + 0L - WINDOW_SIZE),
                ofEpochMilli(startTime + 0L + WINDOW_SIZE)))
        );
        Assert.Equal(
            new HashSet<>(asList(zero, one, two, four, five)),
            toSet(windowStore.fetch(
                0,
                5,
                ofEpochMilli(startTime + 0L - WINDOW_SIZE),
                ofEpochMilli(startTime + 0L + WINDOW_SIZE + 5L)))
        );
        Assert.Equal(
            new HashSet<>(asList(two, four, five)),
            toSet(windowStore.fetch(
                0,
                5,
                ofEpochMilli(startTime + 2L),
                ofEpochMilli(startTime + 0L + WINDOW_SIZE + 5L)))
        );
        Assert.Equal(
            new HashSet<>(Collections.emptyList()),
            toSet(windowStore.fetch(
                4,
                5,
                ofEpochMilli(startTime + 2L),
                ofEpochMilli(startTime + WINDOW_SIZE)))
        );
        Assert.Equal(
            new HashSet<>(Collections.emptyList()),
            toSet(windowStore.fetch(
                0,
                3,
                ofEpochMilli(startTime + 3L),
                ofEpochMilli(startTime + WINDOW_SIZE + 5)))
        );
    }

    [Xunit.Fact]
    public void TestPutAndFetchBefore() {
        long startTime = SEGMENT_INTERVAL - 4L;

        putFirstBatch(windowStore, startTime, context);

        Assert.Equal(
            new HashSet<>(Collections.singletonList("zero")),
            toSet(windowStore.fetch(
                0,
                ofEpochMilli(startTime + 0L - WINDOW_SIZE),
                ofEpochMilli(startTime + 0L))));
        Assert.Equal(
            new HashSet<>(Collections.singletonList("one")),
            toSet(windowStore.fetch(
                1,
                ofEpochMilli(startTime + 1L - WINDOW_SIZE),
                ofEpochMilli(startTime + 1L))));
        Assert.Equal(
            new HashSet<>(Collections.singletonList("two")),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 2L - WINDOW_SIZE),
                ofEpochMilli(startTime + 2L))));
        Assert.Equal(
            new HashSet<>(Collections.emptyList()),
            toSet(windowStore.fetch(
                3,
                ofEpochMilli(startTime + 3L - WINDOW_SIZE),
                ofEpochMilli(startTime + 3L))));
        Assert.Equal(
            new HashSet<>(Collections.singletonList("four")),
            toSet(windowStore.fetch(
                4,
                ofEpochMilli(startTime + 4L - WINDOW_SIZE),
                ofEpochMilli(startTime + 4L))));
        Assert.Equal(
            new HashSet<>(Collections.singletonList("five")),
            toSet(windowStore.fetch(
                5,
                ofEpochMilli(startTime + 5L - WINDOW_SIZE),
                ofEpochMilli(startTime + 5L))));

        putSecondBatch(windowStore, startTime, context);

        Assert.Equal(
            new HashSet<>(Collections.emptyList()),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime - 1L - WINDOW_SIZE),
                ofEpochMilli(startTime - 1L))));
        Assert.Equal(
            new HashSet<>(Collections.emptyList()),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 0L - WINDOW_SIZE),
                ofEpochMilli(startTime + 0L))));
        Assert.Equal(
            new HashSet<>(Collections.emptyList()),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 1L - WINDOW_SIZE),
                ofEpochMilli(startTime + 1L))));
        Assert.Equal(
            new HashSet<>(Collections.singletonList("two")),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 2L - WINDOW_SIZE),
                ofEpochMilli(startTime + 2L))));
        Assert.Equal(
            new HashSet<>(asList("two", "two+1")),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 3L - WINDOW_SIZE),
                ofEpochMilli(startTime + 3L))));
        Assert.Equal(
            new HashSet<>(asList("two", "two+1", "two+2")),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 4L - WINDOW_SIZE),
                ofEpochMilli(startTime + 4L))));
        Assert.Equal(
            new HashSet<>(asList("two", "two+1", "two+2", "two+3")),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 5L - WINDOW_SIZE),
                ofEpochMilli(startTime + 5L))));
        Assert.Equal(
            new HashSet<>(asList("two+1", "two+2", "two+3", "two+4")),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 6L - WINDOW_SIZE),
                ofEpochMilli(startTime + 6L))));
        Assert.Equal(
            new HashSet<>(asList("two+2", "two+3", "two+4", "two+5")),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 7L - WINDOW_SIZE),
                ofEpochMilli(startTime + 7L))));
        Assert.Equal(
            new HashSet<>(asList("two+3", "two+4", "two+5", "two+6")),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 8L - WINDOW_SIZE),
                ofEpochMilli(startTime + 8L))));
        Assert.Equal(
            new HashSet<>(asList("two+4", "two+5", "two+6")),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 9L - WINDOW_SIZE),
                ofEpochMilli(startTime + 9L))));
        Assert.Equal(
            new HashSet<>(asList("two+5", "two+6")),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 10L - WINDOW_SIZE),
                ofEpochMilli(startTime + 10L))));
        Assert.Equal(
            new HashSet<>(Collections.singletonList("two+6")),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 11L - WINDOW_SIZE),
                ofEpochMilli(startTime + 11L))));
        Assert.Equal(
            new HashSet<>(Collections.emptyList()),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 12L - WINDOW_SIZE),
                ofEpochMilli(startTime + 12L))));
        Assert.Equal(
            new HashSet<>(Collections.emptyList()),
            toSet(windowStore.fetch(
                2,
                ofEpochMilli(startTime + 13L - WINDOW_SIZE),
                ofEpochMilli(startTime + 13L))));

        // Flush the store and verify all current entries were properly flushed ...
        windowStore.flush();

        Dictionary<int, HashSet<string>> entriesByKey = entriesByKey(changeLog, startTime);
        Assert.Equal(Utils.mkSet("zero@0"), entriesByKey.get(0));
        Assert.Equal(Utils.mkSet("one@1"), entriesByKey.get(1));
        Assert.Equal(
            Utils.mkSet("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"),
            entriesByKey.get(2));
        assertNull(entriesByKey.get(3));
        Assert.Equal(Utils.mkSet("four@4"), entriesByKey.get(4));
        Assert.Equal(Utils.mkSet("five@5"), entriesByKey.get(5));
        assertNull(entriesByKey.get(6));
    }

    [Xunit.Fact]
    public void TestPutAndFetchAfter() {
        long startTime = SEGMENT_INTERVAL - 4L;

        putFirstBatch(windowStore, startTime, context);

        Assert.Equal(
            new HashSet<>(Collections.singletonList("zero")),
            toSet(windowStore.fetch(0, ofEpochMilli(startTime + 0L),
                ofEpochMilli(startTime + 0L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(Collections.singletonList("one")),
            toSet(windowStore.fetch(1, ofEpochMilli(startTime + 1L),
                ofEpochMilli(startTime + 1L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(Collections.singletonList("two")),
            toSet(windowStore.fetch(2, ofEpochMilli(startTime + 2L),
                ofEpochMilli(startTime + 2L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(Collections.emptyList()),
            toSet(windowStore.fetch(3, ofEpochMilli(startTime + 3L),
                ofEpochMilli(startTime + 3L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(Collections.singletonList("four")),
            toSet(windowStore.fetch(4, ofEpochMilli(startTime + 4L),
                ofEpochMilli(startTime + 4L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(Collections.singletonList("five")),
            toSet(windowStore.fetch(5, ofEpochMilli(startTime + 5L),
                ofEpochMilli(startTime + 5L + WINDOW_SIZE))));

        putSecondBatch(windowStore, startTime, context);

        Assert.Equal(
            new HashSet<>(Collections.emptyList()),
            toSet(windowStore.fetch(2, ofEpochMilli(startTime - 2L),
                ofEpochMilli(startTime - 2L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(Collections.singletonList("two")),
            toSet(windowStore.fetch(2, ofEpochMilli(startTime - 1L),
                ofEpochMilli(startTime - 1L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(asList("two", "two+1")),
            toSet(windowStore
                .fetch(2, ofEpochMilli(startTime), ofEpochMilli(startTime + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(asList("two", "two+1", "two+2")),
            toSet(windowStore.fetch(2, ofEpochMilli(startTime + 1L),
                ofEpochMilli(startTime + 1L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(asList("two", "two+1", "two+2", "two+3")),
            toSet(windowStore.fetch(2, ofEpochMilli(startTime + 2L),
                ofEpochMilli(startTime + 2L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(asList("two+1", "two+2", "two+3", "two+4")),
            toSet(windowStore.fetch(2, ofEpochMilli(startTime + 3L),
                ofEpochMilli(startTime + 3L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(asList("two+2", "two+3", "two+4", "two+5")),
            toSet(windowStore.fetch(2, ofEpochMilli(startTime + 4L),
                ofEpochMilli(startTime + 4L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(asList("two+3", "two+4", "two+5", "two+6")),
            toSet(windowStore.fetch(2, ofEpochMilli(startTime + 5L),
                ofEpochMilli(startTime + 5L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(asList("two+4", "two+5", "two+6")),
            toSet(windowStore.fetch(2, ofEpochMilli(startTime + 6L),
                ofEpochMilli(startTime + 6L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(asList("two+5", "two+6")),
            toSet(windowStore.fetch(2, ofEpochMilli(startTime + 7L),
                ofEpochMilli(startTime + 7L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(Collections.singletonList("two+6")),
            toSet(windowStore.fetch(2, ofEpochMilli(startTime + 8L),
                ofEpochMilli(startTime + 8L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(Collections.emptyList()),
            toSet(windowStore.fetch(2, ofEpochMilli(startTime + 9L),
                ofEpochMilli(startTime + 9L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(Collections.emptyList()),
            toSet(windowStore.fetch(2, ofEpochMilli(startTime + 10L),
                ofEpochMilli(startTime + 10L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(Collections.emptyList()),
            toSet(windowStore.fetch(2, ofEpochMilli(startTime + 11L),
                ofEpochMilli(startTime + 11L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(Collections.emptyList()),
            toSet(windowStore.fetch(2, ofEpochMilli(startTime + 12L),
                ofEpochMilli(startTime + 12L + WINDOW_SIZE))));

        // Flush the store and verify all current entries were properly flushed ...
        windowStore.flush();

        Dictionary<int, HashSet<string>> entriesByKey = entriesByKey(changeLog, startTime);

        Assert.Equal(Utils.mkSet("zero@0"), entriesByKey.get(0));
        Assert.Equal(Utils.mkSet("one@1"), entriesByKey.get(1));
        Assert.Equal(
            Utils.mkSet("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"),
            entriesByKey.get(2));
        assertNull(entriesByKey.get(3));
        Assert.Equal(Utils.mkSet("four@4"), entriesByKey.get(4));
        Assert.Equal(Utils.mkSet("five@5"), entriesByKey.get(5));
        assertNull(entriesByKey.get(6));
    }

    [Xunit.Fact]
    public void TestPutSameKeyTimestamp() {
        windowStore = buildWindowStore(RETENTION_PERIOD, WINDOW_SIZE, true, Serdes.Int(), Serdes.String());
        windowStore.init(context, windowStore);

        long startTime = SEGMENT_INTERVAL - 4L;

        setCurrentTime(startTime);
        windowStore.put(0, "zero");

        Assert.Equal(
            new HashSet<>(Collections.singletonList("zero")),
            toSet(windowStore.fetch(0, ofEpochMilli(startTime - WINDOW_SIZE),
                ofEpochMilli(startTime + WINDOW_SIZE))));

        windowStore.put(0, "zero");
        windowStore.put(0, "zero+");
        windowStore.put(0, "zero++");

        Assert.Equal(
            new HashSet<>(asList("zero", "zero", "zero+", "zero++")),
            toSet(windowStore.fetch(
                0,
                ofEpochMilli(startTime - WINDOW_SIZE),
                ofEpochMilli(startTime + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(asList("zero", "zero", "zero+", "zero++")),
            toSet(windowStore.fetch(
                0,
                ofEpochMilli(startTime + 1L - WINDOW_SIZE),
                ofEpochMilli(startTime + 1L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(asList("zero", "zero", "zero+", "zero++")),
            toSet(windowStore.fetch(
                0,
                ofEpochMilli(startTime + 2L - WINDOW_SIZE),
                ofEpochMilli(startTime + 2L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(asList("zero", "zero", "zero+", "zero++")),
            toSet(windowStore.fetch(
                0,
                ofEpochMilli(startTime + 3L - WINDOW_SIZE),
                ofEpochMilli(startTime + 3L + WINDOW_SIZE))));
        Assert.Equal(
            new HashSet<>(Collections.emptyList()),
            toSet(windowStore.fetch(
                0,
                ofEpochMilli(startTime + 4L - WINDOW_SIZE),
                ofEpochMilli(startTime + 4L + WINDOW_SIZE))));

        // Flush the store and verify all current entries were properly flushed ...
        windowStore.flush();

        Dictionary<int, HashSet<string>> entriesByKey = entriesByKey(changeLog, startTime);

        Assert.Equal(Utils.mkSet("zero@0", "zero@0", "zero+@0", "zero++@0"), entriesByKey.get(0));
    }

    [Xunit.Fact]
    public void ShouldCloseOpenIteratorsWhenStoreIsClosedAndNotThrowInvalidStateStoreExceptionOnHasNext() {
        setCurrentTime(0);
        windowStore.put(1, "one", 1L);
        windowStore.put(1, "two", 2L);
        windowStore.put(1, "three", 3L);

        WindowStoreIterator<string> iterator = windowStore.fetch(1, ofEpochMilli(1L), ofEpochMilli(3L));
        Assert.True(iterator.hasNext());
        windowStore.close();

        Assert.False(iterator.hasNext());
    }

    [Xunit.Fact]
    public void ShouldFetchAndIterateOverExactKeys() {
        long windowSize = 0x7a00000000000000L;
        long retentionPeriod = 0x7a00000000000000L;
        WindowStore<string, string> windowStore = buildWindowStore(retentionPeriod,
                                                                         windowSize,
                                                                         false,
                                                                         Serdes.String(),
                                                                         Serdes.String());

        windowStore.init(context, windowStore);

        windowStore.put("a", "0001", 0);
        windowStore.put("aa", "0002", 0);
        windowStore.put("a", "0003", 1);
        windowStore.put("aa", "0004", 1);
        windowStore.put("a", "0005", 0x7a00000000000000L - 1);

        Set expected = new HashSet<>(asList("0001", "0003", "0005"));
        Assert.Equal(toSet(windowStore.fetch("a", ofEpochMilli(0), ofEpochMilli(long.MaxValue))), (expected));

        HashSet<KeyValuePair<Windowed<string>, string>> set =
            toSet(windowStore.fetch("a", "a", ofEpochMilli(0), ofEpochMilli(long.MaxValue)));
        Assert.Equal(set, (new HashSet<>(asList(
            windowedPair("a", "0001", 0, windowSize),
            windowedPair("a", "0003", 1, windowSize),
            windowedPair("a", "0005", 0x7a00000000000000L - 1, windowSize)
        ))));

        set = toSet(windowStore.fetch("aa", "aa", ofEpochMilli(0), ofEpochMilli(long.MaxValue)));
        Assert.Equal(set, (new HashSet<>(asList(
            windowedPair("aa", "0002", 0, windowSize),
            windowedPair("aa", "0004", 1, windowSize)
        ))));
    }

    [Xunit.Fact]
    public void TestDeleteAndUpdate() {

        long currentTime = 0;
        setCurrentTime(currentTime);
        windowStore.put(1, "one");
        windowStore.put(1, "one v2");

        WindowStoreIterator<string> iterator = windowStore.fetch(1, 0, currentTime);
        Assert.Equal(new KeyValuePair<>(currentTime, "one v2"), iterator.next());

        windowStore.put(1, null);
        iterator = windowStore.fetch(1, 0, currentTime);
        Assert.False(iterator.hasNext());
    }

    [Xunit.Fact]
    public void ShouldReturnNullOnWindowNotFound() {
        assertNull(windowStore.fetch(1, 0L));
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerExceptionOnPutNullKey() {
        windowStore.put(null, "anyValue");
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerExceptionOnGetNullKey() {
        windowStore.fetch(null, ofEpochMilli(1L), ofEpochMilli(2L));
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerExceptionOnRangeNullFromKey() {
        windowStore.fetch(null, 2, ofEpochMilli(1L), ofEpochMilli(2L));
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerExceptionOnRangeNullToKey() {
        windowStore.fetch(1, null, ofEpochMilli(1L), ofEpochMilli(2L));
    }

    [Xunit.Fact]
    public void ShouldFetchAndIterateOverExactBinaryKeys() {
        WindowStore<Bytes, string> windowStore = buildWindowStore(RETENTION_PERIOD,
                                                                        WINDOW_SIZE,
                                                                        true,
                                                                        Serdes.Bytes(),
                                                                        Serdes.String());
        windowStore.init(context, windowStore);

        Bytes key1 = Bytes.wrap(new byte[]{0});
        Bytes key2 = Bytes.wrap(new byte[]{0, 0});
        Bytes key3 = Bytes.wrap(new byte[]{0, 0, 0});
        windowStore.put(key1, "1", 0);
        windowStore.put(key2, "2", 0);
        windowStore.put(key3, "3", 0);
        windowStore.put(key1, "4", 1);
        windowStore.put(key2, "5", 1);
        windowStore.put(key3, "6", 59999);
        windowStore.put(key1, "7", 59999);
        windowStore.put(key2, "8", 59999);
        windowStore.put(key3, "9", 59999);

        Set expectedKey1 = new HashSet<>(asList("1", "4", "7"));
        Assert.Equal(toSet(windowStore.fetch(key1, ofEpochMilli(0), ofEpochMilli(long.MaxValue))),
            equalTo(expectedKey1));
        Set expectedKey2 = new HashSet<>(asList("2", "5", "8"));
        Assert.Equal(toSet(windowStore.fetch(key2, ofEpochMilli(0), ofEpochMilli(long.MaxValue))),
            equalTo(expectedKey2));
        Set expectedKey3 = new HashSet<>(asList("3", "6", "9"));
        Assert.Equal(toSet(windowStore.fetch(key3, ofEpochMilli(0), ofEpochMilli(long.MaxValue))),
            equalTo(expectedKey3));
    }

    [Xunit.Fact]
    public void ShouldReturnSameResultsForSingleKeyFetchAndEqualKeyRangeFetch() {
        windowStore.put(1, "one", 0L);
        windowStore.put(2, "two", 1L);
        windowStore.put(2, "two", 2L);
        windowStore.put(3, "three", 3L);

        WindowStoreIterator<string> singleKeyIterator = windowStore.fetch(2, 0L, 5L);
        KeyValueIterator<Windowed<int>, string> keyRangeIterator = windowStore.fetch(2, 2, 0L, 5L);

        Assert.Equal(singleKeyIterator.next().value, keyRangeIterator.next().value);
        Assert.Equal(singleKeyIterator.next().value, keyRangeIterator.next().value);
        Assert.False(singleKeyIterator.hasNext());
        Assert.False(keyRangeIterator.hasNext());
    }

    [Xunit.Fact]
    public void ShouldNotThrowInvalidRangeExceptionWithNegativeFromKey() {
        setClassLoggerToDebug();
        LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();

        KeyValueIterator iterator = windowStore.fetch(-1, 1, 0L, 10L);
        Assert.False(iterator.hasNext());

        List<string> messages = appender.getMessages();
        Assert.Equal(messages,
            hasItem("Returning empty iterator for fetch with invalid key range: from > to. "
                + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. "
                + "Note that the built-in numerical serdes do not follow this for negative numbers"));
    }

    [Xunit.Fact]
    public void ShouldLogAndMeasureExpiredRecords() {
        setClassLoggerToDebug();
        LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();

        // Advance stream time by inserting record with large enough timestamp that records with timestamp 0 are expired
        windowStore.put(1, "initial record", 2 * RETENTION_PERIOD);

        // Try inserting a record with timestamp 0 -- should be dropped
        windowStore.put(1, "late record", 0L);
        windowStore.put(1, "another on-time record", RETENTION_PERIOD + 1);

        LogCaptureAppender.Unregister(appender);

        Dictionary<MetricName, ? : Metric> metrics = context.metrics().metrics();

        string metricScope = getMetricsScope();

        Metric dropTotal = metrics.get(new MetricName(
            "expired-window-record-drop-total",
            "stream-" + metricScope + "-metrics",
            "The total number of occurrence of expired-window-record-drop operations.",
            mkMap(
                mkEntry("client-id", "mock"),
                mkEntry("task-id", "0_0"),
                mkEntry(metricScope + "-id", windowStore.name())
            )
        ));

        Metric dropRate = metrics.get(new MetricName(
            "expired-window-record-drop-rate",
            "stream-" + metricScope + "-metrics",
            "The average number of occurrence of expired-window-record-drop operation per second.",
            mkMap(
                mkEntry("client-id", "mock"),
                mkEntry("task-id", "0_0"),
                mkEntry(metricScope + "-id", windowStore.name())
            )
        ));

        Assert.Equal(1.0, dropTotal.metricValue());
        Assert.NotEqual(0.0, dropRate.metricValue());
        List<string> messages = appender.getMessages();
        Assert.Equal(messages, hasItem("Skipping record for expired segment."));
    }

    [Xunit.Fact]
    public void ShouldNotThrowExceptionWhenFetchRangeIsExpired() {
        windowStore.put(1, "one", 0L);
        windowStore.put(1, "two", 4 * RETENTION_PERIOD);

        WindowStoreIterator<string> iterator = windowStore.fetch(1, 0L, 10L);

        Assert.False(iterator.hasNext());
    }

    [Xunit.Fact]
    public void TestWindowIteratorPeek() {
        long currentTime = 0;
        setCurrentTime(currentTime);
        windowStore.put(1, "one");

        KeyValueIterator<Windowed<int>, string> iterator = windowStore.fetchAll(0L, currentTime);

        Assert.True(iterator.hasNext());
        Windowed<int> nextKey = iterator.peekNextKey();

        Assert.Equal(iterator.peekNextKey(), nextKey);
        Assert.Equal(iterator.peekNextKey(), iterator.next().key);
        Assert.False(iterator.hasNext());
    }

    [Xunit.Fact]
    public void TestValueIteratorPeek() {
        windowStore.put(1, "one", 0L);

        WindowStoreIterator<string> iterator = windowStore.fetch(1, 0L, 10L);

        Assert.True(iterator.hasNext());
        long nextKey = iterator.peekNextKey();

        Assert.Equal(iterator.peekNextKey(), nextKey);
        Assert.Equal(iterator.peekNextKey(), iterator.next().key);
        Assert.False(iterator.hasNext());
    }

    [Xunit.Fact]
    public void ShouldNotThrowConcurrentModificationException() {
        long currentTime = 0;
        setCurrentTime(currentTime);
        windowStore.put(1, "one");

        currentTime += WINDOW_SIZE * 10;
        setCurrentTime(currentTime);
        windowStore.put(1, "two");

        KeyValueIterator<Windowed<int>, string> iterator = windowStore.all();

        currentTime += WINDOW_SIZE * 10;
        setCurrentTime(currentTime);
        windowStore.put(1, "three");

        currentTime += WINDOW_SIZE * 10;
        setCurrentTime(currentTime);
        windowStore.put(2, "four");

        // Iterator should return all records in store and not throw exception b/c some were added after fetch
        Assert.Equal(windowedPair(1, "one", 0), iterator.next());
        Assert.Equal(windowedPair(1, "two", WINDOW_SIZE * 10), iterator.next());
        Assert.Equal(windowedPair(1, "three", WINDOW_SIZE * 20), iterator.next());
        Assert.Equal(windowedPair(2, "four", WINDOW_SIZE * 30), iterator.next());
        Assert.False(iterator.hasNext());
    }

    [Xunit.Fact]
    public void TestFetchDuplicates() {
        windowStore = buildWindowStore(RETENTION_PERIOD, WINDOW_SIZE, true, Serdes.Int(), Serdes.String());
        windowStore.init(context, windowStore);

        long currentTime = 0;
        setCurrentTime(currentTime);
        windowStore.put(1, "one");
        windowStore.put(1, "one-2");

        currentTime += WINDOW_SIZE * 10;
        setCurrentTime(currentTime);
        windowStore.put(1, "two");
        windowStore.put(1, "two-2");

        currentTime += WINDOW_SIZE * 10;
        setCurrentTime(currentTime);
        windowStore.put(1, "three");
        windowStore.put(1, "three-2");

        WindowStoreIterator<string> iterator = windowStore.fetch(1, 0, WINDOW_SIZE * 10);

        Assert.Equal(new KeyValuePair<>(0L, "one"), iterator.next());
        Assert.Equal(new KeyValuePair<>(0L, "one-2"), iterator.next());
        Assert.Equal(new KeyValuePair<>(WINDOW_SIZE * 10, "two"), iterator.next());
        Assert.Equal(new KeyValuePair<>(WINDOW_SIZE * 10, "two-2"), iterator.next());
        Assert.False(iterator.hasNext());
    }


    private void PutFirstBatch(WindowStore<int, string> store,
         long startTime,
        InternalMockProcessorContext context) {
        context.setRecordContext(createRecordContext(startTime));
        store.put(0, "zero");
        context.setRecordContext(createRecordContext(startTime + 1L));
        store.put(1, "one");
        context.setRecordContext(createRecordContext(startTime + 2L));
        store.put(2, "two");
        context.setRecordContext(createRecordContext(startTime + 4L));
        store.put(4, "four");
        context.setRecordContext(createRecordContext(startTime + 5L));
        store.put(5, "five");
    }

    private void PutSecondBatch(WindowStore<int, string> store,
         long startTime,
        InternalMockProcessorContext context) {
        context.setRecordContext(createRecordContext(startTime + 3L));
        store.put(2, "two+1");
        context.setRecordContext(createRecordContext(startTime + 4L));
        store.put(2, "two+2");
        context.setRecordContext(createRecordContext(startTime + 5L));
        store.put(2, "two+3");
        context.setRecordContext(createRecordContext(startTime + 6L));
        store.put(2, "two+4");
        context.setRecordContext(createRecordContext(startTime + 7L));
        store.put(2, "two+5");
        context.setRecordContext(createRecordContext(startTime + 8L));
        store.put(2, "two+6");
    }

    protected static <E> HashSet<E> ToSet(WindowStoreIterator<E> iterator) {
        HashSet<E> set = new HashSet<>();
        while (iterator.hasNext()) {
            set.add(iterator.next().value);
        }
        return set;
    }

    protected static HashSet<KeyValuePair<K, V>> ToSet<K, V>(Iterator<KeyValuePair<K, V>> iterator) {
        HashSet<KeyValuePair<K, V>> results = new HashSet<>();

        while (iterator.hasNext()) {
            results.add(iterator.next());
        }
        return results;
    }

    private Dictionary<int, HashSet<string>> EntriesByKey(List<KeyValuePair<byte[], byte[]>> changeLog,
         {
        HashDictionary<int, HashSet<string>> entriesByKey = new HashMap<>();

        foreach (KeyValuePair<byte[], byte[]> entry in changeLog) {
            long timestamp = WindowKeySchema.extractStoreTimestamp(entry.key);

            int key = WindowKeySchema.extractStoreKey(entry.key, serdes);
            string value = entry.value == null ? null : serdes.valueFrom(entry.value);

            HashSet<string> entries = entriesByKey.computeIfAbsent(key, k => new HashSet<>());
            entries.add(value + "@" + (timestamp - startTime));
        }

        return entriesByKey;
    }

    protected static KeyValuePair<Windowed<K>, V> WindowedPair<K, V>(K key, V value, long timestamp) {
        return windowedPair(key, value, timestamp, WINDOW_SIZE);
    }

    private static KeyValuePair<Windowed<K>, V> WindowedPair<K, V>(K key, V value, long timestamp, long windowSize) {
        return KeyValuePair.Create(new Windowed<>(key, WindowKeySchema.timeWindowForSize(timestamp, windowSize)), value);
    }

    protected void SetCurrentTime(long currentTime) {
        context.setRecordContext(createRecordContext(currentTime));
    }

    private ProcessorRecordContext CreateRecordContext(long time) {
        return new ProcessorRecordContext(time, 0, 0, "topic", null);
    }

}
