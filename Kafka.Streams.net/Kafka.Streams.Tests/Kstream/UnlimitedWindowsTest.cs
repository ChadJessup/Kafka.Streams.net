using Kafka.Streams.KStream.Internals;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Kafka.Streams.Tests.Kstream
{
    public class UnlimitedWindowsTest
    {

        private static long anyStartTime = 10L;

        [Fact]
        public void shouldSetWindowStartTime()
        {
            Assert.Equal(anyStartTime, UnlimitedWindows.Of().startOn(TimeSpan.FromMilliseconds(anyStartTime)).startMs);
        }

        [Fact]
        public void startTimeMustNotBeNegative()
        {
            UnlimitedWindows.Of().startOn(TimeSpan.FromMilliseconds(-1));
        }


        [Fact]
        public void shouldThrowOnUntil()
        {
            UnlimitedWindows windowSpec = UnlimitedWindows.Of();
            try
            {
                windowSpec.until(42);
                Assert.False(true, "should not allow to set window retention time");
            }
            catch (ArgumentException e)
            {
                // expected
            }
        }

        [Fact]
        public void shouldIncludeRecordsThatHappenedOnWindowStart()
        {
            UnlimitedWindows w = UnlimitedWindows.Of().startOn(TimeSpan.FromMilliseconds(anyStartTime));
            Dictionary<long, UnlimitedWindow> matchedWindows = w.windowsFor(w.startMs);
            Assert.Single(matchedWindows);
            Assert.Equal(new UnlimitedWindow(anyStartTime), matchedWindows[anyStartTime]);
        }

        [Fact]
        public void shouldIncludeRecordsThatHappenedAfterWindowStart()
        {
            UnlimitedWindows w = UnlimitedWindows.Of().startOn(TimeSpan.FromMilliseconds(anyStartTime));
            long timestamp = w.startMs + 1;
            Dictionary<long, UnlimitedWindow> matchedWindows = w.windowsFor(timestamp);
            Assert.Single(matchedWindows);
            Assert.Equal(new UnlimitedWindow(anyStartTime), matchedWindows[anyStartTime]);
        }

        [Fact]
        public void shouldExcludeRecordsThatHappenedBeforeWindowStart()
        {
            UnlimitedWindows w = UnlimitedWindows.Of().startOn(TimeSpan.FromMilliseconds(anyStartTime));
            long timestamp = w.startMs - 1;
            Dictionary<long, UnlimitedWindow> matchedWindows = w.windowsFor(timestamp);
            Assert.False(matchedWindows.Any());
        }

        [Fact]
        public void EqualsAndHashcodeShouldBeValidForPositiveCases()
        {
            VerifyEquality(UnlimitedWindows.Of(), UnlimitedWindows.Of());

            VerifyEquality(UnlimitedWindows.Of().startOn(TimeSpan.FromMilliseconds(1)), UnlimitedWindows.Of().startOn(TimeSpan.FromMilliseconds(1)));

        }

        [Fact]
        public void EqualsAndHashcodeShouldBeValidForNegativeCases()
        {
            EqualityCheck.VerifyInEquality(UnlimitedWindows.Of().startOn(TimeSpan.FromMilliseconds(9)), UnlimitedWindows.Of().startOn(TimeSpan.FromMilliseconds(1)));
        }
    }
}
