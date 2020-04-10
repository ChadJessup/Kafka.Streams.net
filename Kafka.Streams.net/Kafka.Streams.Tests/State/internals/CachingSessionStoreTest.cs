//using Confluent.Kafka;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.State.KeyValues;
//using System;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    public class CachingSessionStoreTest
//    {
//        private const int MAX_CACHE_SIZE_BYTES = 600;
//        private const long DEFAULT_TIMESTAMP = 10L;
//        private const long SEGMENT_INTERVAL = 100L;
//        private Bytes keyA = Bytes.Wrap("a".getBytes());
//        private Bytes keyAA = Bytes.Wrap("aa".getBytes());
//        private Bytes keyB = Bytes.Wrap("b".getBytes());

//        private CachingSessionStore cachingStore;
//        private ThreadCache cache;

//        public CachingSessionStoreTest()
//        {
//            SessionKeySchema schema = new SessionKeySchema();
//            RocksDBSegmentedBytesStore root =
//                new RocksDBSegmentedBytesStore("test", "metrics-scope", 0L, SEGMENT_INTERVAL, schema);
//            RocksDBSessionStore sessionStore = new RocksDBSessionStore(root);
//            cachingStore = new CachingSessionStore(sessionStore, SEGMENT_INTERVAL);
//            cache = new ThreadCache(new LogContext("testCache "), MAX_CACHE_SIZE_BYTES, new MockStreamsMetrics(new Metrics()));
//            InternalMockProcessorContext context = new InternalMockProcessorContext(TestUtils.GetTempDirectory(), null, null, null, cache);
//            context.setRecordContext(new ProcessorRecordContext(DEFAULT_TIMESTAMP, 0, 0, "topic", null));
//            cachingStore.Init(context, cachingStore);
//        }


//        public void Close()
//        {
//            cachingStore.Close();
//        }

//        [Fact]
//        public void ShouldPutFetchFromCache()
//        {
//            cachingStore.Put(new IWindowed<>(keyA, new SessionWindow(0, 0)), "1".getBytes());
//            cachingStore.Put(new IWindowed<>(keyAA, new SessionWindow(0, 0)), "1".getBytes());
//            cachingStore.Put(new IWindowed<>(keyB, new SessionWindow(0, 0)), "1".getBytes());

//            Assert.Equal(3, cache.Count);

//            IKeyValueIterator<IWindowed<Bytes>, byte[]> a = cachingStore.findSessions(keyA, 0, 0);
//            IKeyValueIterator<IWindowed<Bytes>, byte[]> b = cachingStore.findSessions(keyB, 0, 0);

//            verifyWindowedKeyValue(a.MoveNext(), new IWindowed<>(keyA, new SessionWindow(0, 0)), "1");
//            verifyWindowedKeyValue(b.MoveNext(), new IWindowed<>(keyB, new SessionWindow(0, 0)), "1");
//            Assert.False(a.HasNext());
//            Assert.False(b.HasNext());
//        }

//        [Fact]
//        public void ShouldPutFetchAllKeysFromCache()
//        {
//            cachingStore.Put(new IWindowed<>(keyA, new SessionWindow(0, 0)), "1".getBytes());
//            cachingStore.Put(new IWindowed<>(keyAA, new SessionWindow(0, 0)), "1".getBytes());
//            cachingStore.Put(new IWindowed<>(keyB, new SessionWindow(0, 0)), "1".getBytes());

//            Assert.Equal(3, cache.Count);

//            IKeyValueIterator<IWindowed<Bytes>, byte[]> All = cachingStore.findSessions(keyA, keyB, 0, 0);
//            verifyWindowedKeyValue(All.MoveNext(), new IWindowed<>(keyA, new SessionWindow(0, 0)), "1");
//            verifyWindowedKeyValue(All.MoveNext(), new IWindowed<>(keyAA, new SessionWindow(0, 0)), "1");
//            verifyWindowedKeyValue(All.MoveNext(), new IWindowed<>(keyB, new SessionWindow(0, 0)), "1");
//            Assert.False(All.HasNext());
//        }

//        [Fact]
//        public void ShouldPutFetchRangeFromCache()
//        {
//            cachingStore.Put(new IWindowed<>(keyA, new SessionWindow(0, 0)), "1".getBytes());
//            cachingStore.Put(new IWindowed<>(keyAA, new SessionWindow(0, 0)), "1".getBytes());
//            cachingStore.Put(new IWindowed<>(keyB, new SessionWindow(0, 0)), "1".getBytes());

//            Assert.Equal(3, cache.Count);

//            IKeyValueIterator<IWindowed<Bytes>, byte[]> some = cachingStore.findSessions(keyAA, keyB, 0, 0);
//            verifyWindowedKeyValue(some.MoveNext(), new IWindowed<>(keyAA, new SessionWindow(0, 0)), "1");
//            verifyWindowedKeyValue(some.MoveNext(), new IWindowed<>(keyB, new SessionWindow(0, 0)), "1");
//            Assert.False(some.HasNext());
//        }

//        [Fact]
//        public void ShouldFetchAllSessionsWithSameRecordKey()
//        {
//            List<KeyValuePair<IWindowed<Bytes>, byte[]>> expected = asList(
//                KeyValuePair.Create(new IWindowed<Bytes>(keyA, new SessionWindow(0, 0)), "1".getBytes()),
//                KeyValuePair.Create(new IWindowed<Bytes>(keyA, new SessionWindow(10, 10)), "2".getBytes()),
//                KeyValuePair.Create(new IWindowed<Bytes>(keyA, new SessionWindow(100, 100)), "3".getBytes()),
//                KeyValuePair.Create(new IWindowed<Bytes>(keyA, new SessionWindow(1000, 1000)), "4".getBytes())
//            );

//            foreach (KeyValuePair<IWindowed<Bytes>, byte[]> kv in expected)
//            {
//                cachingStore.Put(kv.Key, kv.Value);
//            }

//            // add one that shouldn't appear in the results
//            cachingStore.Put(new IWindowed<>(keyAA, new SessionWindow(0, 0)), "5".getBytes());

//            List<KeyValuePair<IWindowed<Bytes>, byte[]>> results = toList(cachingStore.Fetch(keyA));
//            verifyKeyValueList(expected, results);
//        }

//        [Fact]
//        public void ShouldFlushItemsToStoreOnEviction()
//        {
//            List<KeyValuePair<IWindowed<Bytes>, byte[]>> added = addSessionsUntilOverflow("a", "b", "c", "d");
//            Assert.Equal(added.Count - 1, cache.Count);
//            IKeyValueIterator<IWindowed<Bytes>, byte[]> iterator = cachingStore.findSessions(added.Get(0).key.Key, 0, 0);
//            KeyValuePair<IWindowed<Bytes>, byte[]> next = iterator.MoveNext();
//            Assert.Equal(added.Get(0).key, next.key);
//            assertArrayEquals(added.Get(0).value, next.value);
//        }

//        [Fact]
//        public void ShouldQueryItemsInCacheAndStore()
//        {
//            List<KeyValuePair<IWindowed<Bytes>, byte[]>> added = addSessionsUntilOverflow("a");
//            IKeyValueIterator<IWindowed<Bytes>, byte[]> iterator = cachingStore.findSessions(
//                Bytes.Wrap("a".getBytes(StandardCharsets.UTF_8)),
//                0,
//                added.Count * 10);
//            List<KeyValuePair<IWindowed<Bytes>, byte[]>> actual = toList(iterator);
//            verifyKeyValueList(added, actual);
//        }

//        [Fact]
//        public void ShouldRemove()
//        {
//            IWindowed<Bytes> a = new IWindowed<>(keyA, new SessionWindow(0, 0));
//            IWindowed<Bytes> b = new IWindowed<>(keyB, new SessionWindow(0, 0));
//            cachingStore.Put(a, "2".getBytes());
//            cachingStore.Put(b, "2".getBytes());
//            cachingStore.remove(a);

//            IKeyValueIterator<IWindowed<Bytes>, byte[]> rangeIter =
//                cachingStore.findSessions(keyA, 0, 0);
//            Assert.False(rangeIter.HasNext());

//            Assert.Null(cachingStore.FetchSession(keyA, 0, 0));
//            Assert.Equal(cachingStore.FetchSession(keyB, 0, 0), ("2".getBytes()));

//        }

//        [Fact]
//        public void ShouldFetchCorrectlyAcrossSegments()
//        {
//            IWindowed<Bytes> a1 = new IWindowed<>(keyA, new SessionWindow(SEGMENT_INTERVAL * 0, SEGMENT_INTERVAL * 0));
//            IWindowed<Bytes> a2 = new IWindowed<>(keyA, new SessionWindow(SEGMENT_INTERVAL * 1, SEGMENT_INTERVAL * 1));
//            IWindowed<Bytes> a3 = new IWindowed<>(keyA, new SessionWindow(SEGMENT_INTERVAL * 2, SEGMENT_INTERVAL * 2));
//            cachingStore.Put(a1, "1".getBytes());
//            cachingStore.Put(a2, "2".getBytes());
//            cachingStore.Put(a3, "3".getBytes());
//            cachingStore.Flush();
//            IKeyValueIterator<IWindowed<Bytes>, byte[]> results =
//                cachingStore.findSessions(keyA, 0, SEGMENT_INTERVAL * 2);
//            Assert.Equal(a1, results.MoveNext().key);
//            Assert.Equal(a2, results.MoveNext().key);
//            Assert.Equal(a3, results.MoveNext().key);
//            Assert.False(results.HasNext());
//        }

//        [Fact]
//        public void ShouldFetchRangeCorrectlyAcrossSegments()
//        {
//            IWindowed<Bytes> a1 = new IWindowed<>(keyA, new SessionWindow(SEGMENT_INTERVAL * 0, SEGMENT_INTERVAL * 0));
//            IWindowed<Bytes> aa1 = new IWindowed<>(keyAA, new SessionWindow(SEGMENT_INTERVAL * 0, SEGMENT_INTERVAL * 0));
//            IWindowed<Bytes> a2 = new IWindowed<>(keyA, new SessionWindow(SEGMENT_INTERVAL * 1, SEGMENT_INTERVAL * 1));
//            IWindowed<Bytes> a3 = new IWindowed<>(keyA, new SessionWindow(SEGMENT_INTERVAL * 2, SEGMENT_INTERVAL * 2));
//            IWindowed<Bytes> aa3 = new IWindowed<>(keyAA, new SessionWindow(SEGMENT_INTERVAL * 2, SEGMENT_INTERVAL * 2));
//            cachingStore.Put(a1, "1".getBytes());
//            cachingStore.Put(aa1, "1".getBytes());
//            cachingStore.Put(a2, "2".getBytes());
//            cachingStore.Put(a3, "3".getBytes());
//            cachingStore.Put(aa3, "3".getBytes());

//            IKeyValueIterator<IWindowed<Bytes>, byte[]> rangeResults =
//                cachingStore.findSessions(keyA, keyAA, 0, SEGMENT_INTERVAL * 2);
//            HashSet<IWindowed<Bytes>> keys = new HashSet<>();
//            while (rangeResults.HasNext())
//            {
//                keys.Add(rangeResults.MoveNext().key);
//            }
//            rangeResults.Close();
//            Assert.Equal(mkSet(a1, a2, a3, aa1, aa3), keys);
//        }

//        [Fact]
//        public void ShouldSetFlushListener()
//        {
//            Assert.True(cachingStore.setFlushListener(null, true));
//            Assert.True(cachingStore.setFlushListener(null, false));
//        }

//        [Fact]
//        public void ShouldForwardChangedValuesDuringFlush()
//        {
//            IWindowed<Bytes> a = new IWindowed<>(keyA, new SessionWindow(2, 4));
//            IWindowed<Bytes> b = new IWindowed<>(keyA, new SessionWindow(1, 2));
//            IWindowed<string> aDeserialized = new IWindowed<>("a", new SessionWindow(2, 4));
//            IWindowed<string> bDeserialized = new IWindowed<>("a", new SessionWindow(1, 2));
//            CacheFlushListenerStub<IWindowed<string>, string> flushListener =
//                new CacheFlushListenerStub<>(
//                    new SessionWindowedDeserializer<>(new Serdes.String().Deserializer()),
//                    new Serdes.String().Deserializer());
//            cachingStore.setFlushListener(flushListener, true);

//            cachingStore.Put(b, "1".getBytes());
//            cachingStore.Flush();

//            Assert.Equal(
//                Collections.singletonList(
//                    new KeyValueTimestamp<>(
//                        bDeserialized,
//                        new Change<>("1", null),
//                        DEFAULT_TIMESTAMP)),
//                flushListener.forwarded
//            );
//            flushListener.forwarded.Clear();

//            cachingStore.Put(a, "1".getBytes());
//            cachingStore.Flush();

//            Assert.Equal(
//                Collections.singletonList(
//                    new KeyValueTimestamp<>(
//                        aDeserialized,
//                        new Change<>("1", null),
//                        DEFAULT_TIMESTAMP)),
//                flushListener.forwarded
//            );
//            flushListener.forwarded.Clear();

//            cachingStore.Put(a, "2".getBytes());
//            cachingStore.Flush();

//            Assert.Equal(
//                Collections.singletonList(
//                    new KeyValueTimestamp<>(
//                        aDeserialized,
//                        new Change<>("2", "1"),
//                        DEFAULT_TIMESTAMP)),
//                flushListener.forwarded
//            );
//            flushListener.forwarded.Clear();

//            cachingStore.remove(a);
//            cachingStore.Flush();

//            Assert.Equal(
//                Collections.singletonList(
//                    new KeyValueTimestamp<>(
//                        aDeserialized,
//                        new Change<>(null, "2"),
//                        DEFAULT_TIMESTAMP)),
//                flushListener.forwarded
//            );
//            flushListener.forwarded.Clear();

//            cachingStore.Put(a, "1".getBytes());
//            cachingStore.Put(a, "2".getBytes());
//            cachingStore.remove(a);
//            cachingStore.Flush();

//            Assert.Equal(
//                Collections.emptyList(),
//                flushListener.forwarded
//            );
//            flushListener.forwarded.Clear();
//        }

//        [Fact]
//        public void ShouldNotForwardChangedValuesDuringFlushWhenSendOldValuesDisabled()
//        {
//            IWindowed<Bytes> a = new IWindowed<>(keyA, new SessionWindow(0, 0));
//            IWindowed<string> aDeserialized = new IWindowed<>("a", new SessionWindow(0, 0));
//            CacheFlushListenerStub<IWindowed<string>, string> flushListener =
//                new CacheFlushListenerStub<>(
//                    new SessionWindowedDeserializer<>(new Serdes.String().Deserializer()),
//                    new Serdes.String().Deserializer());
//            cachingStore.setFlushListener(flushListener, false);

//            cachingStore.Put(a, "1".getBytes());
//            cachingStore.Flush();

//            cachingStore.Put(a, "2".getBytes());
//            cachingStore.Flush();

//            cachingStore.remove(a);
//            cachingStore.Flush();

//            Assert.Equal(
//                asList(new KeyValueTimestamp<>(
//                        aDeserialized,
//                        new Change<>("1", null),
//                        DEFAULT_TIMESTAMP),
//                    new KeyValueTimestamp<>(
//                        aDeserialized,
//                        new Change<>("2", null),
//                        DEFAULT_TIMESTAMP),
//                    new KeyValueTimestamp<>(
//                        aDeserialized,
//                        new Change<>(null, null),
//                        DEFAULT_TIMESTAMP)),
//                flushListener.forwarded
//            );
//            flushListener.forwarded.Clear();

//            cachingStore.Put(a, "1".getBytes());
//            cachingStore.Put(a, "2".getBytes());
//            cachingStore.remove(a);
//            cachingStore.Flush();

//            Assert.Equal(
//                Collections.emptyList(),
//                flushListener.forwarded
//            );
//            flushListener.forwarded.Clear();
//        }

//        [Fact]
//        public void ShouldReturnSameResultsForSingleKeyFindSessionsAndEqualKeyRangeFindSessions()
//        {
//            cachingStore.Put(new IWindowed<>(keyA, new SessionWindow(0, 1)), "1".getBytes());
//            cachingStore.Put(new IWindowed<>(keyAA, new SessionWindow(2, 3)), "2".getBytes());
//            cachingStore.Put(new IWindowed<>(keyAA, new SessionWindow(4, 5)), "3".getBytes());
//            cachingStore.Put(new IWindowed<>(keyB, new SessionWindow(6, 7)), "4".getBytes());

//            IKeyValueIterator<IWindowed<Bytes>, byte[]> singleKeyIterator = cachingStore.findSessions(keyAA, 0L, 10L);
//            IKeyValueIterator<IWindowed<Bytes>, byte[]> keyRangeIterator = cachingStore.findSessions(keyAA, keyAA, 0L, 10L);

//            Assert.Equal(singleKeyIterator.MoveNext(), keyRangeIterator.MoveNext());
//            Assert.Equal(singleKeyIterator.MoveNext(), keyRangeIterator.MoveNext());
//            Assert.False(singleKeyIterator.HasNext());
//            Assert.False(keyRangeIterator.HasNext());
//        }

//        [Fact]
//        public void ShouldClearNamespaceCacheOnClose()
//        {
//            IWindowed<Bytes> a1 = new IWindowed<>(keyA, new SessionWindow(0, 0));
//            cachingStore.Put(a1, "1".getBytes());
//            Assert.Equal(1, cache.Count);
//            cachingStore.Close();
//            Assert.Equal(0, cache.Count);
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowIfTryingToFetchFromClosedCachingStore()
//        {
//            cachingStore.Close();
//            cachingStore.Fetch(keyA);
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowIfTryingToFindMergeSessionFromClosedCachingStore()
//        {
//            cachingStore.Close();
//            cachingStore.findSessions(keyA, 0, long.MaxValue);
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowIfTryingToRemoveFromClosedCachingStore()
//        {
//            cachingStore.Close();
//            cachingStore.remove(new IWindowed<>(keyA, new SessionWindow(0, 0)));
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowIfTryingToPutIntoClosedCachingStore()
//        {
//            cachingStore.Close();
//            cachingStore.Put(new IWindowed<>(keyA, new SessionWindow(0, 0)), "1".getBytes());
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerExceptionOnFindSessionsNullKey()
//        {
//            cachingStore.findSessions(null, 1L, 2L);
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerExceptionOnFindSessionsNullFromKey()
//        {
//            cachingStore.findSessions(null, keyA, 1L, 2L);
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerExceptionOnFindSessionsNullToKey()
//        {
//            cachingStore.findSessions(keyA, null, 1L, 2L);
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerExceptionOnFetchNullFromKey()
//        {
//            cachingStore.Fetch(null, keyA);
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerExceptionOnFetchNullToKey()
//        {
//            cachingStore.Fetch(keyA, null);
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerExceptionOnFetchNullKey()
//        {
//            cachingStore.Fetch(null);
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerExceptionOnRemoveNullKey()
//        {
//            cachingStore.remove(null);
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerExceptionOnPutNullKey()
//        {
//            cachingStore.Put(null, "1".getBytes());
//        }

//        [Fact]
//        public void ShouldNotThrowInvalidRangeExceptionWithNegativeFromKey()
//        {
//            LogCaptureAppender.setClassLoggerToDebug(InMemoryWindowStore);
//            LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();

//            Bytes keyFrom = Bytes.Wrap(Serdes.Int().Serializer.Serialize("", -1));
//            Bytes keyTo = Bytes.Wrap(Serdes.Int().Serializer.Serialize("", 1));

//            IKeyValueIterator<IWindowed<Bytes>, byte[]> iterator = cachingStore.findSessions(keyFrom, keyTo, 0L, 10L);
//            Assert.False(iterator.HasNext());

//            List<string> messages = appender.getMessages();
//            Assert.Equal(messages, hasItem("Returning empty iterator for Fetch with invalid key range: from > to. "
//                + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. "
//                + "Note that the built-in numerical serdes do not follow this for negative numbers"));
//        }

//        private List<KeyValuePair<IWindowed<Bytes>, byte[]>> AddSessionsUntilOverflow(params string[] sessionIds)
//        {
//            Random random = new Random();
//            List<KeyValuePair<IWindowed<Bytes>, byte[]>> results = new ArrayList<>();
//            while (cache.Count == results.Count)
//            {
//                string sessionId = sessionIds[random.nextInt(sessionIds.Length)];
//                AddSingleSession(sessionId, results);
//            }
//            return results;
//        }

//        private void AddSingleSession(string sessionId, List<KeyValuePair<IWindowed<Bytes>, byte[]>> allSessions)
//        {
//            int timestamp = allSessions.Count * 10;
//            IWindowed<Bytes> key = new IWindowed<Bytes>(Bytes.Wrap(sessionId.getBytes()), new SessionWindow(timestamp, timestamp));
//            byte[] value = "1".getBytes();
//            cachingStore.Put(key, value);
//            allSessions.Add(KeyValuePair.Create(key, value));
//        }

//        public static class CacheFlushListenerStub<K, V> : CacheFlushListener<byte[], byte[]>
//        {
//            IDeserializer<K> keyDeserializer;
//            IDeserializer<V> valueDesializer;
//            List<KeyValueTimestamp<K, Change<V>>> forwarded = new LinkedList<>();

//            CacheFlushListenerStub(IDeserializer<K> keyDeserializer,
//                                   IDeserializer<V> valueDesializer)
//            {
//                this.keyDeserializer = keyDeserializer;
//                this.valueDesializer = valueDesializer;
//            }


//            public void Apply(byte[] key,
//                              byte[] newValue,
//                              byte[] oldValue,
//                              long timestamp)
//            {
//                forwarded.Add(
//                    new KeyValueTimestamp<>(
//                        keyDeserializer.deserialize(null, key),
//                        new Change<V>(
//                            valueDesializer.deserialize(null, newValue),
//                            valueDesializer.deserialize(null, oldValue)),
//                        timestamp));
//            }
//        }
//    }
//}
