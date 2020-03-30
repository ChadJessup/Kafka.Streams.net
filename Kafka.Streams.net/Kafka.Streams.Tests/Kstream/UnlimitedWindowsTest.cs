namespace Kafka.Streams.Tests.Kstream
{
}
//using Kafka.Streams.KStream.Internals;
//using System;
//using System.Collections.Generic;
//using System.Linq;
//using Xunit;

//namespace Kafka.Streams.Tests
//{
//    public class UnlimitedWindowsTest
//    {

//        private static long anyStartTime = 10L;

//        [Fact]
//        public void shouldSetWindowStartTime()
//        {
//            Assert.Equal(anyStartTime, UnlimitedWindows.of().startOn(ofEpochMilli(anyStartTime)).startMs);
//        }

//        [Fact]
//        public void startTimeMustNotBeNegative()
//        {
//            UnlimitedWindows.of().startOn(ofEpochMilli(-1));
//        }


//        [Fact]
//        public void shouldThrowOnUntil()
//        {
//            UnlimitedWindows windowSpec = UnlimitedWindows.of();
//            try
//            {
//                windowSpec.until(42);
//                Assert.False(true, "should not allow to set window retention time");
//            }
//            catch (ArgumentException e)
//            {
//                // expected
//            }
//        }

//        [Fact]
//        public void shouldIncludeRecordsThatHappenedOnWindowStart()
//        {
//            UnlimitedWindows w = UnlimitedWindows.of().startOn(ofEpochMilli(anyStartTime));
//            Dictionary<long, UnlimitedWindow> matchedWindows = w.windowsFor(w.startMs);
//            Assert.Single(matchedWindows);
//            Assert.Equal(new UnlimitedWindow(anyStartTime), matchedWindows[anyStartTime]);
//        }

//        [Fact]
//        public void shouldIncludeRecordsThatHappenedAfterWindowStart()
//        {
//            UnlimitedWindows w = UnlimitedWindows.of().startOn(ofEpochMilli(anyStartTime));
//            long timestamp = w.startMs + 1;
//            Dictionary<long, UnlimitedWindow> matchedWindows = w.windowsFor(timestamp);
//            Assert.Equal(1, matchedWindows.Count);
//            Assert.Equal(new UnlimitedWindow(anyStartTime), matchedWindows[anyStartTime]);
//        }

//        [Fact]
//        public void shouldExcludeRecordsThatHappenedBeforeWindowStart()
//        {
//            UnlimitedWindows w = UnlimitedWindows.of().startOn(ofEpochMilli(anyStartTime));
//            long timestamp = w.startMs - 1;
//            Dictionary<long, UnlimitedWindow> matchedWindows = w.windowsFor(timestamp);
//            Assert.False(matchedWindows.Any());
//        }

//        [Fact]
//        public void equalsAndHashcodeShouldBeValidForPositiveCases()
//        {
//            VerifyEquality(UnlimitedWindows.of(), UnlimitedWindows.of());

//            VerifyEquality(UnlimitedWindows.of().startOn(ofEpochMilli(1)), UnlimitedWindows.of().startOn(ofEpochMilli(1)));

//        }

//        [Fact]
//        public void equalsAndHashcodeShouldBeValidForNegativeCases()
//        {
//            EqualityCheck.VerifyInEquality(UnlimitedWindows.of().startOn(ofEpochMilli(9)), UnlimitedWindows.of().startOn(ofEpochMilli(1)));
//        }

//    }
//}
