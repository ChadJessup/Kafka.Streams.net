//using Kafka.Streams.Errors;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.KStream.Internals.Suppress;
//using Kafka.Streams.Nodes;
//using Kafka.Streams.Processors;
//using Kafka.Streams.State;
//using Kafka.Streams.Temporary;
//using Kafka.Streams.Tests.Mocks;
//using Moq;
//using System;
//using System.Collections.Generic;
//using System.Collections.ObjectModel;
//using System.Linq;
//using Xunit;

//namespace Kafka.Streams.Tests.Kstream.Internals.Suppress
//{
//    public class KTableSuppressProcessorTest
//    {
//        private static long ARBITRARY_LONG = 5L;

//        private static IChange<long> ARBITRARY_CHANGE = new Change<long>(7L, 14L);

//        public class Harness<K, V>
//        {
//            public IKeyValueProcessor<K, IChange<V>> processor;
//            internal MockInternalProcessorContext context;

//            public Harness(
//                ISuppressed<K> suppressed,
//                ISerde<K> keySerde,
//                ISerde<V> valueSerde)
//            {

//                var storeName = "test-store";

//                IStateStore buffer = null;
//                //new InMemoryTimeOrderedKeyValueBufferBuilder<>(storeName, keySerde, valueSerde)
//                //    .WithLoggingDisabled()
//                //    .Build();

//                var parent = Mock.Of<IKTable<K, IChange<V>>>();
//                var processor = new KTableSuppressProcessorSupplier<K, V>(suppressed, storeName, parent).Get();

//                var context = new MockInternalProcessorContext();
//                context.setCurrentNode(new ProcessorNode("testNode"));

//                buffer.Init(context, buffer);
//                processor.Init(context);

//                this.processor = processor;
//                this.context = context;
//            }
//        }

//        [Fact]
//        public void zeroTimeLimitShouldImmediatelyEmit()
//        {
//            Harness<string, long> harness =
//                new Harness<string, long>(UntilTimeLimit(TimeSpan.Zero, unbounded()), Serdes.String(), Serdes.Long());
//            MockInternalProcessorContext context = harness.context;

//            var timestamp = 0938420L;
//            context.SetRecordMetadata("", 0, 0L, null, timestamp);
//            var key = "hey";
//            IChange<long> value = ARBITRARY_CHANGE;
//            harness.processor.Process(key, value);

//            Assert.Single(context.Forwarded());
//            MockProcessorContext.CapturedForward capturedForward = context.Forwarded().ElementAt(0);
//            Assert.Equal(capturedForward.KeyValue, KeyValuePair.Create(key, value));
//            Assert.Equal(capturedForward.Timestamp, timestamp);
//        }

//        [Fact]
//        public void windowedZeroTimeLimitShouldImmediatelyEmit()
//        {
//            Harness<IWindowed<string>, long> harness =
//                new Harness<IWindowed<string>, long>(untilTimeLimit(TimeSpan.Zero, unbounded()), TimeWindowedSerdeFrom<string>(typeof(string), 100L), Serdes.Long());
//            MockInternalProcessorContext context = harness.context;

//            var timestamp = 0938420L;
//            context.SetRecordMetadata("", 0, 0L, null, timestamp);
//            IWindowed<string> key = new Windowed<string>("hey", new TimeWindow(0L, 100L));
//            IChange<long> value = ARBITRARY_CHANGE;
//            harness.processor.Process(key, value);

//            Assert.Single(context.Forwarded());
//            MockProcessorContext.CapturedForward capturedForward = context.Forwarded().ElementAt(0);
//            Assert.Equal(capturedForward.keyValue(), KeyValuePair.Create(key, value));
//            Assert.Equal(capturedForward.Timestamp, timestamp);
//        }

//        [Fact]
//        public void IntermediateSuppressionShouldBufferAndEmitLater()
//        {
//            Harness<string, long> harness =
//                new Harness<string, long>(untilTimeLimit(TimeSpan.FromMilliseconds(1), unbounded()), Serdes.String(), Serdes.Long());
//            MockInternalProcessorContext context = harness.context;

//            var timestamp = 0L;
//            context.SetRecordMetadata("topic", 0, 0, null, timestamp);
//            var key = "hey";
//            IChange<long> value = new Change<long>(default, 1L);
//            harness.processor.Process(key, value);
//            Assert.Empty(context.Forwarded());

//            context.SetRecordMetadata("topic", 0, 1, null, 1L);
//            harness.processor.Process("tick", new Change<long>(default, default));

//            Assert.Single(context.Forwarded());
//            MockProcessorContext.CapturedForward capturedForward = context.Forwarded().ElementAt(0);
//            Assert.Equal(capturedForward.keyValue(), KeyValuePair.Create(key, value));
//            Assert.Equal(capturedForward.Timestamp, timestamp);
//        }

//        [Fact]
//        public void finalResultsSuppressionShouldBufferAndEmitAtGraceExpiration()
//        {
//            Harness<IWindowed<string>, long> harness =
//                new Harness<IWindowed<string>, long>(finalResults<string>(TimeSpan.FromMilliseconds(1L)), TimeWindowedSerdeFrom<string>(typeof(string), 1L), Serdes.Long());
//            MockInternalProcessorContext context = harness.context;

//            var windowStart = 99L;
//            var recordTime = 99L;
//            var windowEnd = 100L;
//            context.SetRecordMetadata("topic", 0, 0, null, recordTime);
//            IWindowed<string> key = new Windowed<string>("hey", new TimeWindow(windowStart, windowEnd));
//            IChange<long> value = ARBITRARY_CHANGE;
//            harness.processor.Process(key, value);
//            Assert.Empty(context.Forwarded());

//            // although the stream time is now 100, we have to wait 1 ms after the window *end* before we
//            // emit "hey", so we don't emit yet.
//            var windowStart2 = 100L;
//            var recordTime2 = 100L;
//            var windowEnd2 = 101L;
//            context.SetRecordMetadata("topic", 0, 1, null, recordTime2);
//            harness.processor.Process(new Windowed<string>("dummyKey1", new TimeWindow(windowStart2, windowEnd2)), ARBITRARY_CHANGE);
//            Assert.Empty(context.Forwarded());

//            // ok, now it's time to emit "hey"
//            var windowStart3 = 101L;
//            var recordTime3 = 101L;
//            var windowEnd3 = 102L;
//            context.SetRecordMetadata("topic", 0, 1, null, recordTime3);
//            harness.processor.Process(new Windowed<string>("dummyKey2", new TimeWindow(windowStart3, windowEnd3)), ARBITRARY_CHANGE);

//            Assert.Single(context.Forwarded());
//            MockProcessorContext.CapturedForward capturedForward = context.Forwarded().ElementAt(0);
//            Assert.Equal(capturedForward.keyValue(), KeyValuePair.Create(key, value));
//            Assert.Equal(capturedForward.Timestamp, recordTime);
//        }

//        /**
//         * Testing a special case of results: that even with a grace period of 0,
//         * it will still buffer events and emit only after the end of the window.
//         * As opposed to emitting immediately the way regular suppression would with a time limit of 0.
//         */
//        [Fact]
//        public void finalResultsWithZeroGraceShouldStillBufferUntilTheWindowEnd()
//        {
//            Harness<IWindowed<string>, long> harness =
//                new Harness<IWindowed<string>, long>(
//                    finalResults(TimeSpan.FromMilliseconds(0L)),
//                    TimeWindowedSerdeFrom<string>(typeof(string), 100L),
//                    Serdes.Long());

//            MockInternalProcessorContext context = harness.context;

//            // note the record is in the .Ast, but the window end is in the future, so we still have to buffer,
//            // even though the grace period is 0.
//            var timestamp = 5L;
//            var windowEnd = 100L;
//            context.SetRecordMetadata("", 0, 0L, null, timestamp);
//            IWindowed<string> key = new Windowed<string>("hey", new TimeWindow(0, windowEnd));
//            IChange<long> value = ARBITRARY_CHANGE;
//            harness.processor.Process(key, value);
//            Assert.Empty(context.Forwarded());

//            context.SetRecordMetadata("", 0, 1L, null, windowEnd);
//            harness.processor.Process(new Windowed<string>("dummyKey", new TimeWindow(windowEnd, windowEnd + 100L)), ARBITRARY_CHANGE);

//            Assert.Single(context.Forwarded());
//            MockProcessorContext.CapturedForward capturedForward = context.Forwarded().ElementAt(0);
//            Assert.Equal(capturedForward.keyValue(), KeyValuePair.Create(key, value));
//            Assert.Equal(capturedForward.Timestamp, timestamp);
//        }

//        [Fact]
//        public void finalResultsWithZeroGraceAtWindowEndShouldImmediatelyEmit()
//        {
//            Harness<IWindowed<string>, long> harness =
//                new Harness<IWindowed<string>, long>(
//                    finalResults<string>(TimeSpan.FromMilliseconds(0L)), TimeWindowedSerdeFrom<string>(typeof(string), 100L), Serdes.Long());

//            MockInternalProcessorContext context = harness.context;

//            var timestamp = 100L;
//            context.SetRecordMetadata("", 0, 0L, null, timestamp);
//            IWindowed<string> key = new Windowed<string>("hey", new TimeWindow(0, 100L));
//            IChange<long> value = ARBITRARY_CHANGE;
//            harness.processor.Process(key, value);

//            Assert.Single(context.Forwarded());
//            MockProcessorContext.CapturedForward capturedForward = context.Forwarded().ElementAt(0);
//            Assert.Equal(capturedForward.keyValue(), KeyValuePair.Create(key, value));
//            Assert.Equal(capturedForward.Timestamp, timestamp);
//        }

//        /**
//         * It's desirable to drop tombstones for final-results windowed streams, since .As described in the
//         * {@link SuppressedInternal} javadoc), they are unnecessary to emit.
//         */
//        [Fact]
//        public void finalResultsShouldDropTombstonesForTimeWindows()
//        {
//            Harness<IWindowed<string>, long> harness =
//                new Harness<IWindowed<string>, long>(finalResults<string>(TimeSpan.FromMilliseconds(0L)), TimeWindowedSerdeFrom<string>(typeof(string), 100L), Serdes.Long());
//            MockInternalProcessorContext context = harness.context;

//            var timestamp = 100L;
//            context.SetRecordMetadata("", 0, 0L, null, timestamp);
//            IWindowed<string> key = new Windowed<string>("hey", new TimeWindow(0, 100L));
//            IChange<long> value = new Change<long>(default, 1098L);
//            harness.processor.Process(key, value);

//            Assert.Empty(context.Forwarded());
//        }

//        /**
//         * It's desirable to drop tombstones for final-results windowed streams, since .As described in the
//         * {@link SuppressedInternal} javadoc), they are unnecessary to emit.
//         */
//        [Fact]
//        public void FinalResultsShouldDropTombstonesForSessionWindows()
//        {
//            Harness<IWindowed<string>, long> harness =
//                new Harness<IWindowed<string>, long>(
//                    finalResults(TimeSpan.FromMilliseconds(0L)),
//                    WindowedSerdes.SessionWindowedSerdeFrom<string>(),
//                    Serdes.Long());

//            MockInternalProcessorContext context = harness.context;

//            var timestamp = 100L;
//            context.SetRecordMetadata("", 0, 0L, null, timestamp);
//            IWindowed<string> key = new Windowed<string>("hey", new SessionWindow(0L, 0L));
//            Change<long> value = new Change<long>(default, 02948L);
//            harness.processor.Process(key, value);

//            Assert.Empty(context.Forwarded());
//        }

//        /**
//         * It's NOT OK to drop tombstones for non-final-results windowed streams, since we may have emitted some results for
//         * the window before getting the tombstone (see the {@link SuppressedInternal} javadoc).
//         */
//        [Fact]
//        public void suppressShouldNotDropTombstonesForTimeWindows()
//        {
//            Harness<IWindowed<string>, long> harness =
//                new Harness<IWindowed<string>, long>(
//                    UntilTimeLimit(TimeSpan.FromMilliseconds(0), maxRecords(0)),
//                    TimeWindowedSerdeFrom<string>(Serdes.String().GetType(), 100L),
//                    Serdes.Long());

//            MockInternalProcessorContext context = harness.context;

//            var timestamp = 100L;
//            context.SetRecordMetadata("", 0, 0L, null, timestamp);
//            IWindowed<string> key = new Windowed<string>("hey", new TimeWindow(0L, 100L));
//            IChange<long> value = new Change<long>(default, 098098L);
//            harness.processor.Process(key, value);

//            Assert.Single(context.Forwarded());
//            MockProcessorContext.CapturedForward capturedForward = context.Forwarded().ElementAt(0);
//            Assert.Equal(capturedForward.keyValue(), KeyValuePair.Create(key, value));
//            Assert.Equal(capturedForward.Timestamp, timestamp);
//        }


//        /**
//         * It's NOT OK to drop tombstones for non-final-results windowed streams, since we may have emitted some results for
//         * the window before getting the tombstone (see the {@link SuppressedInternal} javadoc).
//         */
//        [Fact]
//        public void suppressShouldNotDropTombstonesForSessionWindows()
//        {
//            Harness<IWindowed<string>, long> harness =
//                new Harness<IWindowed<string>, long>(
//                    UntilTimeLimit(TimeSpan.FromMilliseconds(0), maxRecords(0)),
//                    WindowedSerdes.SessionWindowedSerdeFrom<string>(), Serdes.Long());

//            MockInternalProcessorContext context = harness.context;

//            var timestamp = 100L;
//            context.SetRecordMetadata("", 0, 0L, null, timestamp);
//            IWindowed<string> key = new Windowed<string>("hey", new SessionWindow(0L, 0L));
//            IChange<long> value = new Change<long>(default, 0938420L);
//            harness.processor.Process(key, value);

//            Assert.Single(context.Forwarded());
//            MockProcessorContext.CapturedForward capturedForward = context.Forwarded().ElementAt(0);
//            Assert.Equal(capturedForward.keyValue(), KeyValuePair.Create(key, value));
//            Assert.Equal(capturedForward.Timestamp, timestamp);
//        }


//        /**
//         * It's SUPER NOT OK to drop tombstones for non-windowed streams, since we may have emitted some results for
//         * the key before getting the tombstone (see the {@link SuppressedInternal} javadoc).
//         */
//        [Fact]
//        public void suppressShouldNotDropTombstonesForKTable()
//        {
//            Harness<string, long> harness =
//                new Harness<string, long>(untilTimeLimit(TimeSpan.FromMilliseconds(0), maxRecords(0)), Serdes.String(), Serdes.Long());
//            MockInternalProcessorContext context = harness.context;

//            var timestamp = 100L;
//            context.SetRecordMetadata("", 0, 0L, null, timestamp);
//            var key = "hey";
//            IChange<long> value = new Change<long>(default, 0938420L);
//            harness.processor.Process(key, value);

//            Assert.Single(context.Forwarded());
//            MockProcessorContext.CapturedForward capturedForward = context.Forwarded().ElementAt(0);
//            Assert.Equal(capturedForward.keyValue(), KeyValuePair.Create(key, value));
//            Assert.Equal(capturedForward.Timestamp, timestamp);
//        }

//        [Fact]
//        public void suppressShouldEmitWhenOverRecordCapacity()
//        {
//            Harness<string, long> harness =
//                new Harness<string, long>(untilTimeLimit(TimeSpan.FromDays(100), maxRecords(1)), Serdes.String(), Serdes.Long());
//            MockInternalProcessorContext context = harness.context;

//            var timestamp = 100L;
//            context.SetRecordMetadata("", 0, 0L, null, timestamp);
//            var key = "hey";
//            IChange<long> value = new Change<long>(default, 0938420L);
//            harness.processor.Process(key, value);

//            context.SetRecordMetadata("", 0, 1L, null, timestamp + 1);
//            harness.processor.Process("dummyKey", value);

//            Assert.Single(context.Forwarded());
//            MockProcessorContext.CapturedForward capturedForward = context.Forwarded().ElementAt(0);
//            Assert.Equal(capturedForward.keyValue(), KeyValuePair.Create(key, value));
//            Assert.Equal(capturedForward.Timestamp, timestamp);
//        }

//        [Fact]
//        public void suppressShouldEmitWhenOverByteCapacity()
//        {
//            Harness<string, long> harness =
//                new Harness<string, long>(untilTimeLimit(TimeSpan.FromDays(100), maxBytes(60L)), Serdes.String(), Serdes.Long());
//            MockInternalProcessorContext context = harness.context;

//            var timestamp = 100L;
//            context.SetRecordMetadata("", 0, 0L, null, timestamp);
//            var key = "hey";
//            IChange<long> value = new Change<long>(default, 0938420L);
//            harness.processor.Process(key, value);

//            context.SetRecordMetadata("", 0, 1L, null, timestamp + 1);
//            harness.processor.Process("dummyKey", value);

//            Assert.Single(context.Forwarded());
//            MockProcessorContext.CapturedForward capturedForward = context.Forwarded().ElementAt(0);
//            Assert.Equal(capturedForward.keyValue(), KeyValuePair.Create(key, value));
//            Assert.Equal(capturedForward.Timestamp, timestamp);
//        }

//        [Fact]
//        public void SuppressShouldShutDownWhenOverRecordCapacity()
//        {
//            Harness<string, long> harness =
//                new Harness<string, long>(untilTimeLimit(TimeSpan.FromDays(100), maxRecords(1).shutDownWhenFull()), Serdes.String(), Serdes.Long());
//            MockInternalProcessorContext context = harness.context;

//            var timestamp = 100L;
//            context.SetRecordMetadata("", 0, 0L, null, timestamp);
//            context.setCurrentNode(new ProcessorNode("testNode"));
//            var key = "hey";
//            IChange<long> value = new Change<long>(default, 0938420L);
//            harness.processor.Process(key, value);

//            context.SetRecordMetadata("", 0, 1L, null, timestamp);
//            try
//            {
//                harness.processor.Process("dummyKey", value);
//                Assert.False(true, "expected an exception");
//            }
//            catch (StreamsException e)
//            {
//                Assert.Equal("buffer exceeded its max capacity", e.ToString());
//            }
//        }

//        [Fact]
//        public void suppressShouldShutDownWhenOverByteCapacity()
//        {
//            Harness<string, long> harness =
//                new Harness<string, long>(untilTimeLimit(TimeSpan.FromDays(100), maxBytes(60L).shutDownWhenFull()), Serdes.String(), Serdes.Long());
//            MockInternalProcessorContext context = harness.context;

//            var timestamp = 100L;
//            context.SetRecordMetadata("", 0, 0L, null, timestamp);
//            context.setCurrentNode(new ProcessorNode("testNode"));
//            var key = "hey";
//            IChange<long> value = new Change<long>(default, 0938420L);
//            harness.processor.Process(key, value);

//            context.SetRecordMetadata("", 0, 1L, null, timestamp);
//            try
//            {
//                harness.processor.Process("dummyKey", value);
//                Assert.False(true, "expected an exception");
//            }
//            catch (StreamsException e)
//            {
//                Assert.Contains("buffer exceeded its max capacity", e.ToString());
//            }
//        }

//        private static ISuppressed<K> finalResults<K>(TimeSpan grace)
//            where K : IWindowed<K>
//        {
//            return ((FinalResultsSuppressionBuilder<K>)UntilWindowCloses(unbounded())).buildFinalResultsSuppression(grace);
//        }

//        private static Match<Collection<E>> asSize(int i)
//        {
//            return null;
//            //                return new BaseMatcher<Collection<E>>()
//            //                {
//            //
//            //
//            //            public void describeTo(Description description)
//            //                {
//            //                    description.appendText("a collection of size " + i);
//            //                }
//            //
//            //
//            //
//            //                public bool matches(object item)
//            //                {
//            //                    if (item == null)
//            //                    {
//            //                        return false;
//            //                    }
//            //                    else
//            //                    {
//            //                        return ((Collection<E>)item).Count == i;
//            //                    }
//            //                }
//            //
//            //            };
//        }

//        private static ISerde<IWindowed<K>> TimeWindowedSerdeFrom<K>(Type rawType, long windowSize)
//        {
//            ISerde<K> kSerde = (ISerde<K>)Serdes.SerdeFrom(rawType);
//            return new Serdes.WrapperSerde<IWindowed<K>>(
//                new TimeWindowedSerializer<K>(services, kSerde.Serializer),
//                new TimeWindowedDeserializer<K>(services, kSerde.Deserializer, windowSize)
//            );
//        }
//    }
//}
