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
//            cachingStore.close();
//        }

//        [Fact]
//        public void ShouldPutFetchFromCache()
//        {
//            cachingStore.put(new Windowed<>(keyA, new SessionWindow(0, 0)), "1".getBytes());
//            cachingStore.put(new Windowed<>(keyAA, new SessionWindow(0, 0)), "1".getBytes());
//            cachingStore.put(new Windowed<>(keyB, new SessionWindow(0, 0)), "1".getBytes());

//            Assert.Equal(3, cache.Count);

//            IKeyValueIterator<Windowed<Bytes>, byte[]> a = cachingStore.findSessions(keyA, 0, 0);
//            IKeyValueIterator<Windowed<Bytes>, byte[]> b = cachingStore.findSessions(keyB, 0, 0);

//            verifyWindowedKeyValue(a.MoveNext(), new Windowed<>(keyA, new SessionWindow(0, 0)), "1");
//            verifyWindowedKeyValue(b.MoveNext(), new Windowed<>(keyB, new SessionWindow(0, 0)), "1");
//            Assert.False(a.HasNext());
//            Assert.False(b.HasNext());
//        }

//        [Fact]
//        public void ShouldPutFetchAllKeysFromCache()
//        {
//            cachingStore.put(new Windowed<>(keyA, new SessionWindow(0, 0)), "1".getBytes());
//            cachingStore.put(new Windowed<>(keyAA, new SessionWindow(0, 0)), "1".getBytes());
//            cachingStore.put(new Windowed<>(keyB, new SessionWindow(0, 0)), "1".getBytes());

//            Assert.Equal(3, cache.Count);

//            IKeyValueIterator<Windowed<Bytes>, byte[]> all = cachingStore.findSessions(keyA, keyB, 0, 0);
//            verifyWindowedKeyValue(all.MoveNext(), new Windowed<>(keyA, new SessionWindow(0, 0)), "1");
//            verifyWindowedKeyValue(all.MoveNext(), new Windowed<>(keyAA, new SessionWindow(0, 0)), "1");
//            verifyWindowedKeyValue(all.MoveNext(), new Windowed<>(keyB, new SessionWindow(0, 0)), "1");
//            Assert.False(all.HasNext());
//        }

//        [Fact]
//        public void ShouldPutFetchRangeFromCache()
//        {
//            cachingStore.put(new Windowed<>(keyA, new SessionWindow(0, 0)), "1".getBytes());
//            cachingStore.put(new Windowed<>(keyAA, new SessionWindow(0, 0)), "1".getBytes());
//            cachingStore.put(new Windowed<>(keyB, new SessionWindow(0, 0)), "1".getBytes());

//            Assert.Equal(3, cache.Count);

//            IKeyValueIterator<Windowed<Bytes>, byte[]> some = cachingStore.findSessions(keyAA, keyB, 0, 0);
//            verifyWindowedKeyValue(some.MoveNext(), new Windowed<>(keyAA, new SessionWindow(0, 0)), "1");
//            verifyWindowedKeyValue(some.MoveNext(), new Windowed<>(keyB, new SessionWindow(0, 0)), "1");
//            Assert.False(some.HasNext());
//        }

//        [Fact]
//        public void ShouldFetchAllSessionsWithSameRecordKey()
//        {
//            List<KeyValuePair<Windowed<Bytes>, byte[]>> expected = asList(
//                KeyValuePair.Create(new Windowed<Bytes>(keyA, new SessionWindow(0, 0)), "1".getBytes()),
//                KeyValuePair.Create(new Windowed<Bytes>(keyA, new SessionWindow(10, 10)), "2".getBytes()),
//                KeyValuePair.Create(new Windowed<Bytes>(keyA, new SessionWindow(100, 100)), "3".getBytes()),
//                KeyValuePair.Create(new Windowed<Bytes>(keyA, new SessionWindow(1000, 1000)), "4".getBytes())
//            );

//            foreach (KeyValuePair<Windowed<Bytes>, byte[]> kv in expected)
//            {
//                cachingStore.put(kv.Key, kv.Value);
//            }

//            // add one that shouldn't appear in the results
//            cachingStore.put(new Windowed<>(keyAA, new SessionWindow(0, 0)), "5".getBytes());

//            List<KeyValuePair<Windowed<Bytes>, byte[]>> results = toList(cachingStore.Fetch(keyA));
//            verifyKeyValueList(expected, results);
//        }

//        [Fact]
//        public void ShouldFlushItemsToStoreOnEviction()
//        {
//            List<KeyValuePair<Windowed<Bytes>, byte[]>> added = addSessionsUntilOverflow("a", "b", "c", "d");
//            Assert.Equal(added.Count - 1, cache.Count);
//            IKeyValueIterator<Windowed<Bytes>, byte[]> iterator = cachingStore.findSessions(added.Get(0).key.Key, 0, 0);
//            KeyValuePair<Windowed<Bytes>, byte[]> next = iterator.MoveNext();
//            Assert.Equal(added.Get(0).key, next.key);
//            assertArrayEquals(added.Get(0).value, next.value);
//        }

//        [Fact]
//        public void ShouldQueryItemsInCacheAndStore()
//        {
//            List<KeyValuePair<Windowed<Bytes>, byte[]>> added = addSessionsUntilOverflow("a");
//            IKeyValueIterator<Windowed<Bytes>, byte[]> iterator = cachingStore.findSessions(
//                Bytes.Wrap("a".getBytes(StandardCharsets.UTF_8)),
//                0,
//                added.Count * 10);
//            List<KeyValuePair<Windowed<Bytes>, byte[]>> actual = toList(iterator);
//            verifyKeyValueList(added, actual);
//        }

//        [Fact]
//        public void ShouldRemove()
//        {
//            Windowed<Bytes> a = new Windowed<>(keyA, new SessionWindow(0, 0));
//            Windowed<Bytes> b = new Windowed<>(keyB, new SessionWindow(0, 0));
//            cachingStore.put(a, "2".getBytes());
//            cachingStore.put(b, "2".getBytes());
//            cachingStore.remove(a);

//            IKeyValueIterator<Windowed<Bytes>, byte[]> rangeIter =
//                cachingStore.findSessions(keyA, 0, 0);
//            Assert.False(rangeIter.HasNext());

//            Assert.Null(cachingStore.FetchSession(keyA, 0, 0));
//            Assert.Equal(cachingStore.FetchSession(keyB, 0, 0), ("2".getBytes()));

//        }

//        [Fact]
//        public void ShouldFetchCorrectlyAcrossSegments()
//        {
//            Windowed<Bytes> a1 = new Windowed<>(keyA, new SessionWindow(SEGMENT_INTERVAL * 0, SEGMENT_INTERVAL * 0));
//            Windowed<Bytes> a2 = new Windowed<>(keyA, new SessionWindow(SEGMENT_INTERVAL * 1, SEGMENT_INTERVAL * 1));
//            Windowed<Bytes> a3 = new Windowed<>(keyA, new SessionWindow(SEGMENT_INTERVAL * 2, SEGMENT_INTERVAL * 2));
//            cachingStore.put(a1, "1".getBytes());
//            cachingStore.put(a2, "2".getBytes());
//            cachingStore.put(a3, "3".getBytes());
//            cachingStore.flush();
//            IKeyValueIterator<Windowed<Bytes>, byte[]> results =
//                cachingStore.findSessions(keyA, 0, SEGMENT_INTERVAL * 2);
//            Assert.Equal(a1, results.MoveNext().key);
//            Assert.Equal(a2, results.MoveNext().key);
//            Assert.Equal(a3, results.MoveNext().key);
//            Assert.False(results.HasNext());
//        }

//        [Fact]
//        public void ShouldFetchRangeCorrectlyAcrossSegments()
//        {
//            Windowed<Bytes> a1 = new Windowed<>(keyA, new SessionWindow(SEGMENT_INTERVAL * 0, SEGMENT_INTERVAL * 0));
//            Windowed<Bytes> aa1 = new Windowed<>(keyAA, new SessionWindow(SEGMENT_INTERVAL * 0, SEGMENT_INTERVAL * 0));
//            Windowed<Bytes> a2 = new Windowed<>(keyA, new SessionWindow(SEGMENT_INTERVAL * 1, SEGMENT_INTERVAL * 1));
//            Windowed<Bytes> a3 = new Windowed<>(keyA, new SessionWindow(SEGMENT_INTERVAL * 2, SEGMENT_INTERVAL * 2));
//            Windowed<Bytes> aa3 = new Windowed<>(keyAA, new SessionWindow(SEGMENT_INTERVAL * 2, SEGMENT_INTERVAL * 2));
//            cachingStore.put(a1, "1".getBytes());
//            cachingStore.put(aa1, "1".getBytes());
//            cachingStore.put(a2, "2".getBytes());
//            cachingStore.put(a3, "3".getBytes());
//            cachingStore.put(aa3, "3".getBytes());

//            IKeyValueIterator<Windowed<Bytes>, byte[]> rangeResults =
//                cachingStore.findSessions(keyA, keyAA, 0, SEGMENT_INTERVAL * 2);
//            HashSet<Windowed<Bytes>> keys = new HashSet<>();
//            while (rangeResults.HasNext())
//            {
//                keys.Add(rangeResults.MoveNext().key);
//            }
//            rangeResults.close();
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
//            Windowed<Bytes> a = new Windowed<>(keyA, new SessionWindow(2, 4));
//            Windowed<Bytes> b = new Windowed<>(keyA, new SessionWindow(1, 2));
//            Windowed<string> aDeserialized = new Windowed<>("a", new SessionWindow(2, 4));
//            Windowed<string> bDeserialized = new Windowed<>("a", new SessionWindow(1, 2));
//            CacheFlushListenerStub<Windowed<string>, string> flushListener =
//                new CacheFlushListenerStub<>(
//                    new SessionWindowedDeserializer<>(new Serdes.String().Deserializer()),
//                    new Serdes.String().Deserializer());
//            cachingStore.setFlushListener(flushListener, true);

//            cachingStore.put(b, "1".getBytes());
//            cachingStore.flush();

//            Assert.Equal(
//                Collections.singletonList(
//                    new KeyValueTimestamp<>(
//                        bDeserialized,
//                        new Change<>("1", null),
//                        DEFAULT_TIMESTAMP)),
//                flushListener.forwarded
//            );
//            flushListener.forwarded.Clear();

//            cachingStore.put(a, "1".getBytes());
//            cachingStore.flush();

//            Assert.Equal(
//                Collections.singletonList(
//                    new KeyValueTimestamp<>(
//                        aDeserialized,
//                        new Change<>("1", null),
//                        DEFAULT_TIMESTAMP)),
//                flushListener.forwarded
//            );
//            flushListener.forwarded.Clear();

//            cachingStore.put(a, "2".getBytes());
//            cachingStore.flush();

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
//            cachingStore.flush();

//            Assert.Equal(
//                Collections.singletonList(
//                    new KeyValueTimestamp<>(
//                        aDeserialized,
//                        new Change<>(null, "2"),
//                        DEFAULT_TIMESTAMP)),
//                flushListener.forwarded
//            );
//            flushListener.forwarded.Clear();

//            cachingStore.put(a, "1".getBytes());
//            cachingStore.put(a, "2".getBytes());
//            cachingStore.remove(a);
//            cachingStore.flush();

//            Assert.Equal(
//                Collections.emptyList(),
//                flushListener.forwarded
//            );
//            flushListener.forwarded.Clear();
//        }

//        [Fact]
//        public void ShouldNotForwardChangedValuesDuringFlushWhenSendOldValuesDisabled()
//        {
//            Windowed<Bytes> a = new Windowed<>(keyA, new SessionWindow(0, 0));
//            Windowed<string> aDeserialized = new Windowed<>("a", new SessionWindow(0, 0));
//            CacheFlushListenerStub<Windowed<string>, string> flushListener =
//                new CacheFlushListenerStub<>(
//                    new SessionWindowedDeserializer<>(new Serdes.String().Deserializer()),
//                    new Serdes.String().Deserializer());
//            cachingStore.setFlushListener(flushListener, false);

//            cachingStore.put(a, "1".getBytes());
//            cachingStore.flush();

//            cachingStore.put(a, "2".getBytes());
//            cachingStore.flush();

//            cachingStore.remove(a);
//            cachingStore.flush();

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

//            cachingStore.put(a, "1".getBytes());
//            cachingStore.put(a, "2".getBytes());
//            cachingStore.remove(a);
//            cachingStore.flush();

//            Assert.Equal(
//                Collections.emptyList(),
//                flushListener.forwarded
//            );
//            flushListener.forwarded.Clear();
//        }

//        [Fact]
//        public void ShouldReturnSameResultsForSingleKeyFindSessionsAndEqualKeyRangeFindSessions()
//        {
//            cachingStore.put(new Windowed<>(keyA, new SessionWindow(0, 1)), "1".getBytes());
//            cachingStore.put(new Windowed<>(keyAA, new SessionWindow(2, 3)), "2".getBytes());
//            cachingStore.put(new Windowed<>(keyAA, new SessionWindow(4, 5)), "3".getBytes());
//            cachingStore.put(new Windowed<>(keyB, new SessionWindow(6, 7)), "4".getBytes());

//            IKeyValueIterator<Windowed<Bytes>, byte[]> singleKeyIterator = cachingStore.findSessions(keyAA, 0L, 10L);
//            IKeyValueIterator<Windowed<Bytes>, byte[]> keyRangeIterator = cachingStore.findSessions(keyAA, keyAA, 0L, 10L);

//            Assert.Equal(singleKeyIterator.MoveNext(), keyRangeIterator.MoveNext());
//            Assert.Equal(singleKeyIterator.MoveNext(), keyRangeIterator.MoveNext());
//            Assert.False(singleKeyIterator.HasNext());
//            Assert.False(keyRangeIterator.HasNext());
//        }

//        [Fact]
//        public void ShouldClearNamespaceCacheOnClose()
//        {
//            Windowed<Bytes> a1 = new Windowed<>(keyA, new SessionWindow(0, 0));
//            cachingStore.put(a1, "1".getBytes());
//            Assert.Equal(1, cache.Count);
//            cachingStore.close();
//            Assert.Equal(0, cache.Count);
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowIfTryingToFetchFromClosedCachingStore()
//        {
//            cachingStore.close();
//            cachingStore.Fetch(keyA);
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowIfTryingToFindMergeSessionFromClosedCachingStore()
//        {
//            cachingStore.close();
//            cachingStore.findSessions(keyA, 0, long.MaxValue);
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowIfTryingToRemoveFromClosedCachingStore()
//        {
//            cachingStore.close();
//            cachingStore.remove(new Windowed<>(keyA, new SessionWindow(0, 0)));
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowIfTryingToPutIntoClosedCachingStore()
//        {
//            cachingStore.close();
//            cachingStore.put(new Windowed<>(keyA, new SessionWindow(0, 0)), "1".getBytes());
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
//            cachingStore.put(null, "1".getBytes());
//        }

//        [Fact]
//        public void ShouldNotThrowInvalidRangeExceptionWithNegativeFromKey()
//        {
//            LogCaptureAppender.setClassLoggerToDebug(InMemoryWindowStore);
//            LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();

//            Bytes keyFrom = Bytes.Wrap(Serdes.Int().Serializer.Serialize("", -1));
//            Bytes keyTo = Bytes.Wrap(Serdes.Int().Serializer.Serialize("", 1));

//            IKeyValueIterator<Windowed<Bytes>, byte[]> iterator = cachingStore.findSessions(keyFrom, keyTo, 0L, 10L);
//            Assert.False(iterator.HasNext());

//            List<string> messages = appender.getMessages();
//            Assert.Equal(messages, hasItem("Returning empty iterator for fetch with invalid key range: from > to. "
//                + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. "
//                + "Note that the built-in numerical serdes do not follow this for negative numbers"));
//        }

//        private List<KeyValuePair<Windowed<Bytes>, byte[]>> AddSessionsUntilOverflow(params string[] sessionIds)
//        {
//            Random random = new Random();
//            List<KeyValuePair<Windowed<Bytes>, byte[]>> results = new ArrayList<>();
//            while (cache.Count == results.Count)
//            {
//                string sessionId = sessionIds[random.nextInt(sessionIds.Length)];
//                AddSingleSession(sessionId, results);
//            }
//            return results;
//        }

//        private void AddSingleSession(string sessionId, List<KeyValuePair<Windowed<Bytes>, byte[]>> allSessions)
//        {
//            int timestamp = allSessions.Count * 10;
//            Windowed<Bytes> key = new Windowed<Bytes>(Bytes.Wrap(sessionId.getBytes()), new SessionWindow(timestamp, timestamp));
//            byte[] value = "1".getBytes();
//            cachingStore.put(key, value);
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
