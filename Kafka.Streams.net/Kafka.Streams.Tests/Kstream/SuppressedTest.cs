namespace Kafka.Streams.Tests.Kstream
{
}
//namespace Kafka.Streams.Tests
//{
//    public class SuppressedTest
//    {

//        [Fact]
//        public void bufferBuilderShouldBeConsistent()
//        {
//            Assert.Equal(
//                "noBound should remove bounds",
//                maxBytes(2L).withMaxRecords(4L).withNoBound(),
//                (unbounded())
//            );

//            Assert.Equal(
//                "keys alone should be set",
//                maxRecords(2L),
//                (new EagerBufferConfigImpl(2L, MAX_VALUE))
//            );

//            Assert.Equal(
//                "size alone should be set",
//                maxBytes(2L),
//                (new EagerBufferConfigImpl(MAX_VALUE, 2L))
//            );
//        }

//        [Fact]
//        public void intermediateEventsShouldAcceptAnyBufferAndSetBounds()
//        {
//            Assert.Equal(
//                "Name should be set",
//                untilTimeLimit(TimeSpan.FromMilliseconds(2), unbounded()).withName("myname"),
//                (new SuppressedInternal<>("myname", FromMilliseconds(2), unbounded(), null, false))
//            );

//            Assert.Equal(
//                "time alone should be set",
//                untilTimeLimit(TimeSpan.FromMilliseconds(2), unbounded()),
//                (new SuppressedInternal<>(null, FromMilliseconds(2), unbounded(), null, false))
//            );

//            Assert.Equal(
//                "time and unbounded buffer should be set",
//                untilTimeLimit(TimeSpan.FromMilliseconds(2), unbounded()),
//                (new SuppressedInternal<>(null, FromMilliseconds(2), unbounded(), null, false))
//            );

//            Assert.Equal(
//                "time and keys buffer should be set",
//                untilTimeLimit(TimeSpan.FromMilliseconds(2), maxRecords(2)),
//                (new SuppressedInternal<>(null, FromMilliseconds(2), maxRecords(2), null, false))
//            );

//            Assert.Equal(
//                "time and size buffer should be set",
//                untilTimeLimit(TimeSpan.FromMilliseconds(2), maxBytes(2)),
//                (new SuppressedInternal<>(null, FromMilliseconds(2), maxBytes(2), null, false))
//            );

//            Assert.Equal(
//                "All constraints should be set",
//                untilTimeLimit(TimeSpan.FromMilliseconds(2L), maxRecords(3L).withMaxBytes(2L)),
//                (new SuppressedInternal<>(null, FromMilliseconds(2), new EagerBufferConfigImpl(3L, 2L), null, false))
//            );
//        }

//        [Fact]
//        public void finalEventsShouldAcceptStrictBuffersAndSetBounds()
//        {

//            Assert.Equal(
//                untilWindowCloses(unbounded()),
//                (new FinalResultsSuppressionBuilder<>(null, unbounded()))
//            );

//            Assert.Equal(
//                untilWindowCloses(maxRecords(2L).shutDownWhenFull()),
//                (new FinalResultsSuppressionBuilder<>(null, new StrictBufferConfigImpl(2L, MAX_VALUE, SHUT_DOWN))
//                )
//            );

//            Assert.Equal(
//                untilWindowCloses(maxBytes(2L).shutDownWhenFull()),
//                (new FinalResultsSuppressionBuilder<>(null, new StrictBufferConfigImpl(MAX_VALUE, 2L, SHUT_DOWN))
//                )
//            );

//            Assert.Equal(
//                untilWindowCloses(unbounded()).withName("Name"),
//                (new FinalResultsSuppressionBuilder<>("Name", unbounded()))
//            );

//            Assert.Equal(
//                untilWindowCloses(maxRecords(2L).shutDownWhenFull()).withName("Name"),
//                (new FinalResultsSuppressionBuilder<>("Name", new StrictBufferConfigImpl(2L, MAX_VALUE, SHUT_DOWN))
//                )
//            );

//            Assert.Equal(
//                untilWindowCloses(maxBytes(2L).shutDownWhenFull()).withName("Name"),
//                (new FinalResultsSuppressionBuilder<>("Name", new StrictBufferConfigImpl(MAX_VALUE, 2L, SHUT_DOWN))
//                )
//            );
//        }
//    }
//}
