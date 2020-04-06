namespace Kafka.Streams.Tests.Kstream.Internals.Suppress
{
}
//using Kafka.Streams.KStream;
//
//using System.Collections.ObjectModel;
//using Xunit;

//namespace Kafka.Streams.KStream.Internals.suppress
//{
//    public class KTableSuppressProcessorTest
//    {
//        private static long ARBITRARY_LONG = 5L;

//        private static Change<long> ARBITRARY_CHANGE = new Change<>(7L, 14L);

//        public static class Harness<K, V>
//        {
//            public Processor<K, Change<V>> processor;
//            public MockInternalProcessorContext context;


//            Harness(Suppressed<K> suppressed,
//                    ISerde<K> keySerde,
//                    ISerde<V> valueSerde)
//            {

//                var storeName = "test-store";

//                IStateStore buffer = new InMemoryTimeOrderedKeyValueBuffer.Builder<>(storeName, keySerde, valueSerde)
//                    .withLoggingDisabled()
//                    .Build();

//                var parent = EasyMock.mock(KTable));
//                Processor<K, Change<V>> processor =
//                    new KTableSuppressProcessorSupplier<>((SuppressedInternal<K>)suppressed, storeName, parent).Get();

//                var context = new MockInternalProcessorContext();
//                context.setCurrentNode(new ProcessorNode("testNode"));

//                buffer.Init(context, buffer);
//                processor.Init(context);

//                this.processor = processor;
//                this.context = context;
//            }

//            [Fact]
//            public void zeroTimeLimitShouldImmediatelyEmit()
//            {
//                Harness<string, long> harness =
//                    new Harness<>(untilTimeLimit(TimeSpan.Zero, unbounded()), string(), long());
//                MockInternalProcessorContext context = harness.context;

//                var timestamp = ARBITRARY_LONG;
//                context.setRecordMetadata("", 0, 0L, null, timestamp);
//                var key = "hey";
//                Change<long> value = ARBITRARY_CHANGE;
//                harness.processor.process(key, value);

//                Assert.Equal(context.forwarded(), asSize(1));
//                MockProcessorContext.CapturedForward capturedForward = context.forwarded().Get(0);
//                Assert.Equal(capturedForward.keyValue(), (KeyValuePair.Create(key, value)));
//                Assert.Equal(capturedForward.Timestamp, (timestamp));
//            }

//            [Fact]
//            public void windowedZeroTimeLimitShouldImmediatelyEmit()
//            {
//                Harness<Windowed<string>, long> harness =
//                    new Harness<>(untilTimeLimit(TimeSpan.Zero, unbounded()), timeWindowedSerdeFrom<string>(), 100L), long());
//                MockInternalProcessorContext context = harness.context;

//                var timestamp = ARBITRARY_LONG;
//                context.setRecordMetadata("", 0, 0L, null, timestamp);
//                Windowed<string> key = new Windowed<>("hey", new TimeWindow(0L, 100L));
//                Change<long> value = ARBITRARY_CHANGE;
//                harness.processor.process(key, value);

//                Assert.Equal(context.forwarded(), asSize(1));
//                MockProcessorContext.CapturedForward capturedForward = context.forwarded().Get(0);
//                Assert.Equal(capturedForward.keyValue(), (KeyValuePair.Create(key, value)));
//                Assert.Equal(capturedForward.Timestamp, (timestamp));
//            }

//            [Fact]
//            public void intermediateSuppressionShouldBufferAndEmitLater()
//            {
//                Harness<string, long> harness =
//                    new Harness<>(untilTimeLimit(TimeSpan.FromMilliseconds(1), unbounded()), string(), long());
//                MockInternalProcessorContext context = harness.context;

//                var timestamp = 0L;
//                context.setRecordMetadata("topic", 0, 0, null, timestamp);
//                var key = "hey";
//                Change<long> value = new Change<>(null, 1L);
//                harness.processor.process(key, value);
//                Assert.Equal(context.forwarded(), asSize(0));

//                context.setRecordMetadata("topic", 0, 1, null, 1L);
//                harness.processor.process("tick", new Change<>(null, null));

//                Assert.Equal(context.forwarded(), asSize(1));
//                MockProcessorContext.CapturedForward capturedForward = context.forwarded().Get(0);
//                Assert.Equal(capturedForward.keyValue(), (KeyValuePair.Create(key, value)));
//                Assert.Equal(capturedForward.Timestamp, (timestamp));
//            }

//            [Fact]
//            public void finalResultsSuppressionShouldBufferAndEmitAtGraceExpiration()
//            {
//                Harness<Windowed<string>, long> harness =
//                    new Harness<>(finalResults(TimeSpan.FromMilliseconds(1L)), timeWindowedSerdeFrom<string>(), 1L), long());
//                MockInternalProcessorContext context = harness.context;

//                var windowStart = 99L;
//                var recordTime = 99L;
//                var windowEnd = 100L;
//                context.setRecordMetadata("topic", 0, 0, null, recordTime);
//                Windowed<string> key = new Windowed<>("hey", new TimeWindow(windowStart, windowEnd));
//                Change<long> value = ARBITRARY_CHANGE;
//                harness.processor.process(key, value);
//                Assert.Equal(context.forwarded(), asSize(0));

//                // although the stream time is now 100, we have to wait 1 ms after the window *end* before we
//                // emit "hey", so we don't emit yet.
//                var windowStart2 = 100L;
//                var recordTime2 = 100L;
//                var windowEnd2 = 101L;
//                context.setRecordMetadata("topic", 0, 1, null, recordTime2);
//                harness.processor.process(new Windowed<>("dummyKey1", new TimeWindow(windowStart2, windowEnd2)), ARBITRARY_CHANGE);
//                Assert.Equal(context.forwarded(), asSize(0));

//                // ok, now it's time to emit "hey"
//                var windowStart3 = 101L;
//                var recordTime3 = 101L;
//                var windowEnd3 = 102L;
//                context.setRecordMetadata("topic", 0, 1, null, recordTime3);
//                harness.processor.process(new Windowed<>("dummyKey2", new TimeWindow(windowStart3, windowEnd3)), ARBITRARY_CHANGE);

//                Assert.Equal(context.forwarded(), asSize(1));
//                MockProcessorContext.CapturedForward capturedForward = context.forwarded().Get(0);
//                Assert.Equal(capturedForward.keyValue(), (KeyValuePair.Create(key, value)));
//                Assert.Equal(capturedForward.Timestamp, (recordTime));
//            }

//            /**
//             * Testing a special case of results: that even with a grace period of 0,
//             * it will still buffer events and emit only after the end of the window.
//             * As opposed to emitting immediately the way regular suppression would with a time limit of 0.
//             */
//            [Fact]
//            public void finalResultsWithZeroGraceShouldStillBufferUntilTheWindowEnd()
//            {
//                Harness<Windowed<string>, long> harness =
//                    new Harness<>(finalResults(TimeSpan.FromMilliseconds(0L)), timeWindowedSerdeFrom(string), 100L), long());
//                MockInternalProcessorContext context = harness.context;

//                // note the record is in the .Ast, but the window end is in the future, so we still have to buffer,
//                // even though the grace period is 0.
//                var timestamp = 5L;
//                var windowEnd = 100L;
//                context.setRecordMetadata("", 0, 0L, null, timestamp);
//                Windowed<string> key = new Windowed<>("hey", new TimeWindow(0, windowEnd));
//                Change<long> value = ARBITRARY_CHANGE;
//                harness.processor.process(key, value);
//                Assert.Equal(context.forwarded(), asSize(0));

//                context.setRecordMetadata("", 0, 1L, null, windowEnd);
//                harness.processor.process(new Windowed<>("dummyKey", new TimeWindow(windowEnd, windowEnd + 100L)), ARBITRARY_CHANGE);

//                Assert.Equal(context.forwarded(), asSize(1));
//                MockProcessorContext.CapturedForward capturedForward = context.forwarded().Get(0);
//                Assert.Equal(capturedForward.keyValue(), (KeyValuePair.Create(key, value)));
//                Assert.Equal(capturedForward.Timestamp, (timestamp));
//            }

//            [Fact]
//            public void finalResultsWithZeroGraceAtWindowEndShouldImmediatelyEmit()
//            {
//                Harness<Windowed<string>, long> harness =
//        new Harness<>(finalResults(TimeSpan.FromMilliseconds(0L)), timeWindowedSerdeFrom(string), 100L), long());
//                MockInternalProcessorContext context = harness.context;

//                var timestamp = 100L;
//                context.setRecordMetadata("", 0, 0L, null, timestamp);
//                Windowed<string> key = new Windowed<>("hey", new TimeWindow(0, 100L));
//                Change<long> value = ARBITRARY_CHANGE;
//                harness.processor.process(key, value);

//                Assert.Equal(context.forwarded(), asSize(1));
//                MockProcessorContext.CapturedForward capturedForward = context.forwarded().Get(0);
//                Assert.Equal(capturedForward.keyValue(), (KeyValuePair.Create(key, value)));
//                Assert.Equal(capturedForward.Timestamp, (timestamp));
//            }

//            /**
//             * It's desirable to drop tombstones for final-results windowed streams, since .As described in the
//             * {@link SuppressedInternal} javadoc), they are unnecessary to emit.
//             */
//            [Fact]
//            public void finalResultsShouldDropTombstonesForTimeWindows()
//            {
//                Harness<Windowed<string>, long> harness =
//        new Harness<>(finalResults(TimeSpan.FromMilliseconds(0L)), timeWindowedSerdeFrom<string>(), 100L), long());
//                MockInternalProcessorContext context = harness.context;

//                var timestamp = 100L;
//                context.setRecordMetadata("", 0, 0L, null, timestamp);
//                Windowed<string> key = new Windowed<>("hey", new TimeWindow(0, 100L));
//                Change<long> value = new Change<>(null, ARBITRARY_LONG);
//                harness.processor.process(key, value);

//                Assert.Equal(context.forwarded(), asSize(0));
//            }


//            /**
//             * It's desirable to drop tombstones for final-results windowed streams, since .As described in the
//             * {@link SuppressedInternal} javadoc), they are unnecessary to emit.
//             */
//            [Fact]
//            public void finalResultsShouldDropTombstonesForSessionWindows()
//            {
//                Harness<Windowed<string>, long> harness =
//        new Harness<>(finalResults(TimeSpan.FromMilliseconds(0L)), sessionWindowedSerdeFrom(string)), long());
//                MockInternalProcessorContext context = harness.context;

//                var timestamp = 100L;
//                context.setRecordMetadata("", 0, 0L, null, timestamp);
//                Windowed<string> key = new Windowed<>("hey", new SessionWindow(0L, 0L));
//                Change<long> value = new Change<>(null, ARBITRARY_LONG);
//                harness.processor.process(key, value);

//                Assert.Equal(context.forwarded(), asSize(0));
//            }

//            /**
//             * It's NOT OK to drop tombstones for non-final-results windowed streams, since we may have emitted some results for
//             * the window before getting the tombstone (see the {@link SuppressedInternal} javadoc).
//             */
//            [Fact]
//            public void suppressShouldNotDropTombstonesForTimeWindows()
//            {
//                Harness<Windowed<string>, long> harness =
//        new Harness<>(untilTimeLimit(TimeSpan.FromMilliseconds(0), maxRecords(0)), timeWindowedSerdeFrom(string), 100L), long());
//                MockInternalProcessorContext context = harness.context;

//                var timestamp = 100L;
//                context.setRecordMetadata("", 0, 0L, null, timestamp);
//                Windowed<string> key = new Windowed<>("hey", new TimeWindow(0L, 100L));
//                Change<long> value = new Change<>(null, ARBITRARY_LONG);
//                harness.processor.process(key, value);

//                Assert.Equal(context.forwarded(), asSize(1));
//                MockProcessorContext.CapturedForward capturedForward = context.forwarded().Get(0);
//                Assert.Equal(capturedForward.keyValue(), (KeyValuePair.Create(key, value)));
//                Assert.Equal(capturedForward.Timestamp, (timestamp));
//            }


//            /**
//             * It's NOT OK to drop tombstones for non-final-results windowed streams, since we may have emitted some results for
//             * the window before getting the tombstone (see the {@link SuppressedInternal} javadoc).
//             */
//            [Fact]
//            public void suppressShouldNotDropTombstonesForSessionWindows()
//            {
//                Harness<Windowed<string>, long> harness =
//        new Harness<>(untilTimeLimit(TimeSpan.FromMilliseconds(0), maxRecords(0)), sessionWindowedSerdeFrom(string)), long());
//                MockInternalProcessorContext context = harness.context;

//                var timestamp = 100L;
//                context.setRecordMetadata("", 0, 0L, null, timestamp);
//                Windowed<string> key = new Windowed<>("hey", new SessionWindow(0L, 0L));
//                Change<long> value = new Change<>(null, ARBITRARY_LONG);
//                harness.processor.process(key, value);

//                Assert.Equal(context.forwarded(), asSize(1));
//                MockProcessorContext.CapturedForward capturedForward = context.forwarded().Get(0);
//                Assert.Equal(capturedForward.keyValue(), (KeyValuePair.Create(key, value)));
//                Assert.Equal(capturedForward.Timestamp, (timestamp));
//            }


//            /**
//             * It's SUPER NOT OK to drop tombstones for non-windowed streams, since we may have emitted some results for
//             * the key before getting the tombstone (see the {@link SuppressedInternal} javadoc).
//             */
//            [Fact]
//            public void suppressShouldNotDropTombstonesForKTable()
//            {
//                Harness<string, long> harness =
//                    new Harness<>(untilTimeLimit(TimeSpan.FromMilliseconds(0), maxRecords(0)), string(), long());
//                MockInternalProcessorContext context = harness.context;

//                var timestamp = 100L;
//                context.setRecordMetadata("", 0, 0L, null, timestamp);
//                var key = "hey";
//                Change<long> value = new Change<>(null, ARBITRARY_LONG);
//                harness.processor.process(key, value);

//                Assert.Equal(context.forwarded(), asSize(1));
//                MockProcessorContext.CapturedForward capturedForward = context.forwarded().Get(0);
//                Assert.Equal(capturedForward.keyValue(), (KeyValuePair.Create(key, value)));
//                Assert.Equal(capturedForward.Timestamp, (timestamp));
//            }

//            [Fact]
//            public void suppressShouldEmitWhenOverRecordCapacity()
//            {
//                Harness<string, long> harness =
//                    new Harness<>(untilTimeLimit(TimeSpan.FromDays(100), maxRecords(1)), string(), long());
//                MockInternalProcessorContext context = harness.context;

//                var timestamp = 100L;
//                context.setRecordMetadata("", 0, 0L, null, timestamp);
//                var key = "hey";
//                Change<long> value = new Change<>(null, ARBITRARY_LONG);
//                harness.processor.process(key, value);

//                context.setRecordMetadata("", 0, 1L, null, timestamp + 1);
//                harness.processor.process("dummyKey", value);

//                Assert.Equal(context.forwarded(), asSize(1));
//                MockProcessorContext.CapturedForward capturedForward = context.forwarded().Get(0);
//                Assert.Equal(capturedForward.keyValue(), (KeyValuePair.Create(key, value)));
//                Assert.Equal(capturedForward.Timestamp, (timestamp));
//            }

//            [Fact]
//            public void suppressShouldEmitWhenOverByteCapacity()
//            {
//                Harness<string, long> harness =
//                    new Harness<>(untilTimeLimit(TimeSpan.FromDays(100), maxBytes(60L)), string(), long());
//                MockInternalProcessorContext context = harness.context;

//                var timestamp = 100L;
//                context.setRecordMetadata("", 0, 0L, null, timestamp);
//                var key = "hey";
//                Change<long> value = new Change<>(null, ARBITRARY_LONG);
//                harness.processor.process(key, value);

//                context.setRecordMetadata("", 0, 1L, null, timestamp + 1);
//                harness.processor.process("dummyKey", value);

//                Assert.Equal(context.forwarded(), asSize(1));
//                MockProcessorContext.CapturedForward capturedForward = context.forwarded().Get(0);
//                Assert.Equal(capturedForward.keyValue(), (KeyValuePair.Create(key, value)));
//                Assert.Equal(capturedForward.Timestamp, (timestamp));
//            }

//            [Fact]
//            public void suppressShouldShutDownWhenOverRecordCapacity()
//            {
//                Harness<string, long> harness =
//                    new Harness<>(untilTimeLimit(TimeSpan.FromDays(100), maxRecords(1).shutDownWhenFull()), string(), long());
//                MockInternalProcessorContext context = harness.context;

//                var timestamp = 100L;
//                context.setRecordMetadata("", 0, 0L, null, timestamp);
//                context.setCurrentNode(new ProcessorNode("testNode"));
//                var key = "hey";
//                Change<long> value = new Change<>(null, ARBITRARY_LONG);
//                harness.processor.process(key, value);

//                context.setRecordMetadata("", 0, 1L, null, timestamp);
//                try
//                {
//                    harness.processor.process("dummyKey", value);
//                    Assert.False(true, "expected an exception");
//                }
//                catch (StreamsException e)
//                {
//                    Assert.Equal(e.ToString(),.ContainsString("buffer exceeded its max capacity"));
//                }
//            }

//            [Fact]
//            public void suppressShouldShutDownWhenOverByteCapacity()
//            {
//                Harness<string, long> harness =
//                    new Harness<>(untilTimeLimit(TimeSpan.FromDays(100), maxBytes(60L).shutDownWhenFull()), string(), long());
//                MockInternalProcessorContext context = harness.context;

//                var timestamp = 100L;
//                context.setRecordMetadata("", 0, 0L, null, timestamp);
//                context.setCurrentNode(new ProcessorNode("testNode"));
//                var key = "hey";
//                Change<long> value = new Change<>(null, ARBITRARY_LONG);
//                harness.processor.process(key, value);

//                context.setRecordMetadata("", 0, 1L, null, timestamp);
//                try
//                {
//                    harness.processor.process("dummyKey", value);
//                    Assert.False(true, "expected an exception");
//                }
//                catch (StreamsException e)
//                {
//                    Assert.Equal(e.ToString(),.ContainsString("buffer exceeded its max capacity"));
//                }
//            }


//            private static SuppressedInternal<K> finalResults<K>(TimeSpan grace)
//                where K : Windowed
//            {
//                return ((FinalResultsSuppressionBuilder)untilWindowCloses(unbounded())).buildFinalResultsSuppression(grace);
//            }

//            private static Matcher<Collection<E>> asSize(int i)
//            {
//                return new BaseMatcher<Collection<E>>()
//                {


//            public void describeTo(Description description)
//                {
//                    description.appendText("a collection of size " + i);
//                }



//                public bool matches(object item)
//                {
//                    if (item == null)
//                    {
//                        return false;
//                    }
//                    else
//                    {
//                        return ((Collection<E>)item).Count == i;
//                    }
//                }

//            };
//        }

//        private static <K> ISerde<Windowed<K>> timeWindowedSerdeFrom(Class<K> rawType, long windowSize)
//        {
//            ISerde<K> kSerde = Serdes.SerdeFrom(rawType);
//            return new Serdes.WrapperSerde<>(
//                new TimeWindowedSerializer<>(kSerde.Serializer),
//                new TimeWindowedDeserializer<>(kSerde.deserializer(), windowSize)
//            );
//        }
//    }