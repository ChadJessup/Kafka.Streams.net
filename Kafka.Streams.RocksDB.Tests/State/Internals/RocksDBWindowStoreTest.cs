//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */



























//    public class RocksDBWindowStoreTest : WindowBytesStoreTest
//    {

//        private const string STORE_NAME = "rocksDB window store";

//        private KeyValueSegments segments = new KeyValueSegments(STORE_NAME, RETENTION_PERIOD, SEGMENT_INTERVAL);


//        IWindowStore<K, V> BuildWindowStore<K, V>(long retentionPeriod,
//                                                  long windowSize,
//                                                  bool retainDuplicates,
//                                                  Serde<K> keySerde,
//                                                  Serde<V> valueSerde)
//        {
//            return Stores.windowStoreBuilder(
//                Stores.PersistentWindowStore(
//                    STORE_NAME,
//                    FromMilliseconds(retentionPeriod),
//                    FromMilliseconds(windowSize),
//                    retainDuplicates),
//                keySerde,
//                valueSerde)
//                .Build();
//        }


//        string GetMetricsScope()
//        {
//            return new RocksDbWindowBytesStoreSupplier(null, 0, 0, 0, false, false).metricsScope();
//        }


//        void SetClassLoggerToDebug()
//        {
//            LogCaptureAppender.setClassLoggerToDebug(AbstractRocksDBSegmentedBytesStore);
//        }

//        [Fact]
//        public void ShouldOnlyIterateOpenSegments()
//        {
//            long currentTime = 0;
//            setCurrentTime(currentTime);
//            windowStore.Put(1, "one");

//            currentTime = currentTime + SEGMENT_INTERVAL;
//            setCurrentTime(currentTime);
//            windowStore.Put(1, "two");
//            currentTime = currentTime + SEGMENT_INTERVAL;

//            setCurrentTime(currentTime);
//            windowStore.Put(1, "three");

//            IWindowStoreIterator<string> iterator = windowStore.Fetch(1, 0L, currentTime);

//            // roll to the next segment that will Close the first
//            currentTime = currentTime + SEGMENT_INTERVAL;
//            setCurrentTime(currentTime);
//            windowStore.Put(1, "four");

//            // should only have 2 values as the first segment is no longer open
//            Assert.Equal(KeyValuePair.Create(SEGMENT_INTERVAL, "two"), iterator.MoveNext());
//            Assert.Equal(KeyValuePair.Create(2 * SEGMENT_INTERVAL, "three"), iterator.MoveNext());
//            Assert.False(iterator.MoveNext());
//        }

//        [Fact]
//        public void TestRolling()
//        {

//            // to validate segments
//            long startTime = SEGMENT_INTERVAL * 2;
//            long increment = SEGMENT_INTERVAL / 2;
//            setCurrentTime(startTime);
//            windowStore.Put(0, "zero");
//            Assert.Equal(Utils.mkSet(segments.segmentName(2)), SegmentDirs(baseDir));

//            setCurrentTime(startTime + increment);
//            windowStore.Put(1, "one");
//            Assert.Equal(Utils.mkSet(segments.segmentName(2)), SegmentDirs(baseDir));

//            setCurrentTime(startTime + increment * 2);
//            windowStore.Put(2, "two");
//            Assert.Equal(
//                Utils.mkSet(
//                    segments.segmentName(2),
//                    segments.segmentName(3)
//                ),
//                SegmentDirs(baseDir)
//            );

//            setCurrentTime(startTime + increment * 4);
//            windowStore.Put(4, "four");
//            Assert.Equal(
//                Utils.mkSet(
//                    segments.segmentName(2),
//                    segments.segmentName(3),
//                    segments.segmentName(4)
//                ),
//                SegmentDirs(baseDir)
//            );

//            setCurrentTime(startTime + increment * 5);
//            windowStore.Put(5, "five");
//            Assert.Equal(
//                Utils.mkSet(
//                    segments.segmentName(2),
//                    segments.segmentName(3),
//                    segments.segmentName(4)
//                ),
//                SegmentDirs(baseDir)
//            );

//            Assert.Equal(
//                new HashSet<>(Collections.singletonList("zero")),
//                toSet(windowStore.Fetch(
//                    0,
//                    ofEpochMilli(startTime - WINDOW_SIZE),
//                    ofEpochMilli(startTime + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.singletonList("one")),
//                toSet(windowStore.Fetch(
//                    1,
//                    ofEpochMilli(startTime + increment - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.singletonList("two")),
//                toSet(windowStore.Fetch(
//                    2,
//                    ofEpochMilli(startTime + increment * 2 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 2 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.emptyList()),
//                toSet(windowStore.Fetch(
//                    3,
//                    ofEpochMilli(startTime + increment * 3 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 3 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.singletonList("four")),
//                toSet(windowStore.Fetch(
//                    4,
//                    ofEpochMilli(startTime + increment * 4 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 4 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.singletonList("five")),
//                toSet(windowStore.Fetch(
//                    5,
//                    ofEpochMilli(startTime + increment * 5 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 5 + WINDOW_SIZE))));

//            setCurrentTime(startTime + increment * 6);
//            windowStore.Put(6, "six");
//            Assert.Equal(
//                Utils.mkSet(
//                    segments.segmentName(3),
//                    segments.segmentName(4),
//                    segments.segmentName(5)
//                ),
//                SegmentDirs(baseDir)
//            );

//            Assert.Equal(
//                new HashSet<>(Collections.emptyList()),
//                toSet(windowStore.Fetch(
//                    0,
//                    ofEpochMilli(startTime - WINDOW_SIZE),
//                    ofEpochMilli(startTime + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.emptyList()),
//                toSet(windowStore.Fetch(
//                    1,
//                    ofEpochMilli(startTime + increment - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.singletonList("two")),
//                toSet(windowStore.Fetch(
//                    2,
//                    ofEpochMilli(startTime + increment * 2 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 2 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.emptyList()),
//                toSet(windowStore.Fetch(
//                    3,
//                    ofEpochMilli(startTime + increment * 3 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 3 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.singletonList("four")),
//                toSet(windowStore.Fetch(
//                    4,
//                    ofEpochMilli(startTime + increment * 4 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 4 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.singletonList("five")),
//                toSet(windowStore.Fetch(
//                    5,
//                    ofEpochMilli(startTime + increment * 5 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 5 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.singletonList("six")),
//                toSet(windowStore.Fetch(
//                    6,
//                    ofEpochMilli(startTime + increment * 6 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 6 + WINDOW_SIZE))));

//            setCurrentTime(startTime + increment * 7);
//            windowStore.Put(7, "seven");
//            Assert.Equal(
//                Utils.mkSet(
//                    segments.segmentName(3),
//                    segments.segmentName(4),
//                    segments.segmentName(5)
//                ),
//                SegmentDirs(baseDir)
//            );

//            Assert.Equal(
//                new HashSet<>(Collections.emptyList()),
//                toSet(windowStore.Fetch(
//                    0,
//                    ofEpochMilli(startTime - WINDOW_SIZE),
//                    ofEpochMilli(startTime + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.emptyList()),
//                toSet(windowStore.Fetch(
//                    1,
//                    ofEpochMilli(startTime + increment - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.singletonList("two")),
//                toSet(windowStore.Fetch(
//                    2,
//                    ofEpochMilli(startTime + increment * 2 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 2 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.emptyList()),
//                toSet(windowStore.Fetch(
//                    3,
//                    ofEpochMilli(startTime + increment * 3 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 3 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.singletonList("four")),
//                toSet(windowStore.Fetch(
//                    4,
//                    ofEpochMilli(startTime + increment * 4 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 4 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.singletonList("five")),
//                toSet(windowStore.Fetch(
//                    5,
//                    ofEpochMilli(startTime + increment * 5 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 5 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.singletonList("six")),
//                toSet(windowStore.Fetch(
//                    6,
//                    ofEpochMilli(startTime + increment * 6 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 6 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.singletonList("seven")),
//                toSet(windowStore.Fetch(
//                    7,
//                    ofEpochMilli(startTime + increment * 7 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 7 + WINDOW_SIZE))));

//            setCurrentTime(startTime + increment * 8);
//            windowStore.Put(8, "eight");
//            Assert.Equal(
//                Utils.mkSet(
//                    segments.segmentName(4),
//                    segments.segmentName(5),
//                    segments.segmentName(6)
//                ),
//                SegmentDirs(baseDir)
//            );

//            Assert.Equal(
//                new HashSet<>(Collections.emptyList()),
//                toSet(windowStore.Fetch(
//                    0,
//                    ofEpochMilli(startTime - WINDOW_SIZE),
//                    ofEpochMilli(startTime + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.emptyList()),
//                toSet(windowStore.Fetch(
//                    1,
//                    ofEpochMilli(startTime + increment - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.emptyList()),
//                toSet(windowStore.Fetch(
//                    2,
//                    ofEpochMilli(startTime + increment * 2 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 2 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.emptyList()),
//                toSet(windowStore.Fetch(
//                    3,
//                    ofEpochMilli(startTime + increment * 3 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 3 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.singletonList("four")),
//                toSet(windowStore.Fetch(
//                    4,
//                    ofEpochMilli(startTime + increment * 4 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 4 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.singletonList("five")),
//                toSet(windowStore.Fetch(
//                    5,
//                    ofEpochMilli(startTime + increment * 5 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 5 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.singletonList("six")),
//                toSet(windowStore.Fetch(
//                    6,
//                    ofEpochMilli(startTime + increment * 6 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 6 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.singletonList("seven")),
//                toSet(windowStore.Fetch(
//                    7,
//                    ofEpochMilli(startTime + increment * 7 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 7 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.singletonList("eight")),
//                toSet(windowStore.Fetch(
//                    8,
//                    ofEpochMilli(startTime + increment * 8 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 8 + WINDOW_SIZE))));

//            // check segment directories
//            windowStore.Flush();
//            Assert.Equal(
//                Utils.mkSet(
//                    segments.segmentName(4),
//                    segments.segmentName(5),
//                    segments.segmentName(6)
//                ),
//                SegmentDirs(baseDir)
//            );
//        }

//        [Fact]
//        public void TestSegmentMaintenance()
//        {

//            windowStore = buildWindowStore(RETENTION_PERIOD, WINDOW_SIZE, true, Serdes.Int(),
//                Serdes.String());
//            windowStore.Init(context, windowStore);

//            context.setTime(0L);
//            setCurrentTime(0);
//            windowStore.Put(0, "v");
//            Assert.Equal(
//                Utils.mkSet(segments.segmentName(0L)),
//                SegmentDirs(baseDir)
//            );

//            setCurrentTime(SEGMENT_INTERVAL - 1);
//            windowStore.Put(0, "v");
//            windowStore.Put(0, "v");
//            Assert.Equal(
//                Utils.mkSet(segments.segmentName(0L)),
//                SegmentDirs(baseDir)
//            );

//            setCurrentTime(SEGMENT_INTERVAL);
//            windowStore.Put(0, "v");
//            Assert.Equal(
//                Utils.mkSet(segments.segmentName(0L), segments.segmentName(1L)),
//                SegmentDirs(baseDir)
//            );

//            IWindowStoreIterator iter;
//            int fetchedCount;

//            iter = windowStore.Fetch(0, ofEpochMilli(0L), ofEpochMilli(SEGMENT_INTERVAL * 4));
//            fetchedCount = 0;
//            while (iter.MoveNext())
//            {
//                iter.MoveNext();
//                fetchedCount++;
//            }
//            Assert.Equal(4, fetchedCount);

//            Assert.Equal(
//                Utils.mkSet(segments.segmentName(0L), segments.segmentName(1L)),
//                SegmentDirs(baseDir)
//            );

//            setCurrentTime(SEGMENT_INTERVAL * 3);
//            windowStore.Put(0, "v");

//            iter = windowStore.Fetch(0, ofEpochMilli(0L), ofEpochMilli(SEGMENT_INTERVAL * 4));
//            fetchedCount = 0;
//            while (iter.MoveNext())
//            {
//                iter.MoveNext();
//                fetchedCount++;
//            }
//            Assert.Equal(2, fetchedCount);

//            Assert.Equal(
//                Utils.mkSet(segments.segmentName(1L), segments.segmentName(3L)),
//                SegmentDirs(baseDir)
//            );

//            setCurrentTime(SEGMENT_INTERVAL * 5);
//            windowStore.Put(0, "v");

//            iter = windowStore.Fetch(0, ofEpochMilli(SEGMENT_INTERVAL * 4), ofEpochMilli(SEGMENT_INTERVAL * 10));
//            fetchedCount = 0;
//            while (iter.MoveNext())
//            {
//                iter.MoveNext();
//                fetchedCount++;
//            }
//            Assert.Equal(1, fetchedCount);

//            Assert.Equal(
//                Utils.mkSet(segments.segmentName(3L), segments.segmentName(5L)),
//                SegmentDirs(baseDir)
//            );

//        }


//        [Fact]
//        public void TestInitialLoading()
//        {
//            File storeDir = new File(baseDir, STORE_NAME);

//            new File(storeDir, segments.segmentName(0L)).mkdir();
//            new File(storeDir, segments.segmentName(1L)).mkdir();
//            new File(storeDir, segments.segmentName(2L)).mkdir();
//            new File(storeDir, segments.segmentName(3L)).mkdir();
//            new File(storeDir, segments.segmentName(4L)).mkdir();
//            new File(storeDir, segments.segmentName(5L)).mkdir();
//            new File(storeDir, segments.segmentName(6L)).mkdir();
//            windowStore.Close();

//            windowStore = buildWindowStore(RETENTION_PERIOD, WINDOW_SIZE, false, Serdes.Int(), Serdes.String());
//            windowStore.Init(context, windowStore);

//            // Put something in the store to advance its stream time and expire the old segments
//            windowStore.Put(1, "v", 6L * SEGMENT_INTERVAL);

//            List<string> expected = Arrays.asList(
//                segments.segmentName(4L),
//                segments.segmentName(5L),
//                segments.segmentName(6L));
//            expected.sort(string::compareTo);

//            List<string> actual = Utils.toList(SegmentDirs(baseDir).iterator());
//            actual.sort(string::compareTo);

//            Assert.Equal(expected, actual);

//            try
//            {
//                (IWindowStoreIterator iter = windowStore.Fetch(0, ofEpochMilli(0L), ofEpochMilli(1000000L)));
//                while (iter.MoveNext())
//                {
//                    iter.MoveNext();
//                }
//            }

//        Assert.Equal(
//            Utils.mkSet(
//                segments.segmentName(4L),
//                segments.segmentName(5L),
//                segments.segmentName(6L)),
//            SegmentDirs(baseDir)
//        );
//        }

//        [Fact]
//        public void TestRestore()
//        {// throws Exception
//            long startTime = SEGMENT_INTERVAL * 2;
//            long increment = SEGMENT_INTERVAL / 2;

//            setCurrentTime(startTime);
//            windowStore.Put(0, "zero");
//            setCurrentTime(startTime + increment);
//            windowStore.Put(1, "one");
//            setCurrentTime(startTime + increment * 2);
//            windowStore.Put(2, "two");
//            setCurrentTime(startTime + increment * 3);
//            windowStore.Put(3, "three");
//            setCurrentTime(startTime + increment * 4);
//            windowStore.Put(4, "four");
//            setCurrentTime(startTime + increment * 5);
//            windowStore.Put(5, "five");
//            setCurrentTime(startTime + increment * 6);
//            windowStore.Put(6, "six");
//            setCurrentTime(startTime + increment * 7);
//            windowStore.Put(7, "seven");
//            setCurrentTime(startTime + increment * 8);
//            windowStore.Put(8, "eight");
//            windowStore.Flush();

//            windowStore.Close();

//            // remove local store image
//            Utils.delete(baseDir);

//            windowStore = buildWindowStore(RETENTION_PERIOD,
//                                           WINDOW_SIZE,
//                                           false,
//                                           Serdes.Int(),
//                                           Serdes.String());
//            windowStore.Init(context, windowStore);

//            Assert.Equal(
//                new HashSet<>(Collections.emptyList()),
//                toSet(windowStore.Fetch(
//                    0,
//                    ofEpochMilli(startTime - WINDOW_SIZE),
//                    ofEpochMilli(startTime + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.emptyList()),
//                toSet(windowStore.Fetch(
//                    1,
//                    ofEpochMilli(startTime + increment - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.emptyList()),
//                toSet(windowStore.Fetch(
//                    2,
//                    ofEpochMilli(startTime + increment * 2 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 2 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.emptyList()),
//                toSet(windowStore.Fetch(
//                    3,
//                    ofEpochMilli(startTime + increment * 3 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 3 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.emptyList()),
//                toSet(windowStore.Fetch(
//                    4,
//                    ofEpochMilli(startTime + increment * 4 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 4 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.emptyList()),
//                toSet(windowStore.Fetch(
//                    5,
//                    ofEpochMilli(startTime + increment * 5 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 5 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.emptyList()),
//                toSet(windowStore.Fetch(
//                    6,
//                    ofEpochMilli(startTime + increment * 6 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 6 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.emptyList()),
//                toSet(windowStore.Fetch(
//                    7,
//                    ofEpochMilli(startTime + increment * 7 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 7 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.emptyList()),
//                toSet(windowStore.Fetch(
//                    8,
//                    ofEpochMilli(startTime + increment * 8 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 8 + WINDOW_SIZE))));

//            context.restore(STORE_NAME, changeLog);

//            Assert.Equal(
//                new HashSet<>(Collections.emptyList()),
//                toSet(windowStore.Fetch(
//                    0,
//                    ofEpochMilli(startTime - WINDOW_SIZE),
//                    ofEpochMilli(startTime + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.emptyList()),
//                toSet(windowStore.Fetch(
//                    1,
//                    ofEpochMilli(startTime + increment - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.emptyList()),
//                toSet(windowStore.Fetch(
//                    2,
//                    ofEpochMilli(startTime + increment * 2 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 2 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.emptyList()),
//                toSet(windowStore.Fetch(
//                    3,
//                    ofEpochMilli(startTime + increment * 3 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 3 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.singletonList("four")),
//                toSet(windowStore.Fetch(
//                    4,
//                    ofEpochMilli(startTime + increment * 4 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 4 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.singletonList("five")),
//                toSet(windowStore.Fetch(
//                    5,
//                    ofEpochMilli(startTime + increment * 5 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 5 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.singletonList("six")),
//                toSet(windowStore.Fetch(
//                    6,
//                    ofEpochMilli(startTime + increment * 6 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 6 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.singletonList("seven")),
//                toSet(windowStore.Fetch(
//                    7,
//                    ofEpochMilli(startTime + increment * 7 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 7 + WINDOW_SIZE))));
//            Assert.Equal(
//                new HashSet<>(Collections.singletonList("eight")),
//                toSet(windowStore.Fetch(
//                    8,
//                    ofEpochMilli(startTime + increment * 8 - WINDOW_SIZE),
//                    ofEpochMilli(startTime + increment * 8 + WINDOW_SIZE))));

//            // check segment directories
//            windowStore.Flush();
//            Assert.Equal(
//                Utils.mkSet(
//                    segments.segmentName(4L),
//                    segments.segmentName(5L),
//                    segments.segmentName(6L)),
//                SegmentDirs(baseDir)
//            );
//        }

//        private HashSet<string> SegmentDirs(File baseDir)
//        {
//            File windowDir = new File(baseDir, windowStore.Name());

//            return new HashSet<>(asList(requireNonNull(windowDir.list())));
//        }

//    }
//}
///*






//*

//*





//*/



























