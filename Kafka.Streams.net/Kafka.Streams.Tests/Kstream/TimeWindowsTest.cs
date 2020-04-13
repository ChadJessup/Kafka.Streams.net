using Kafka.Streams.KStream.Internals;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Kstream
{
    public class TimeWindowsTest
    {

        private static long ANY_SIZE = 123L;

        [Fact]
        public void shouldSetWindowSize()
        {
            Assert.Equal(ANY_SIZE, TimeWindows.of(TimeSpan.FromMilliseconds(ANY_SIZE)).sizeMs);
        }

        [Fact]
        public void shouldSetWindowAdvance()
        {
            long anyAdvance = 4;
            Assert.Equal(anyAdvance, TimeWindows.of(TimeSpan.FromMilliseconds(ANY_SIZE)).advanceBy(TimeSpan.FromMilliseconds(anyAdvance)).advanceMs);
        }

        // specifically testing deprecated APIs
        [Fact]
        public void shouldSetWindowRetentionTime()
        {
            Assert.Equal(ANY_SIZE, TimeWindows.of(TimeSpan.FromMilliseconds(ANY_SIZE)).until(ANY_SIZE).maintainMs());
        }

        // specifically testing deprecated APIs
        [Fact]
        public void shouldUseWindowSizeAsRentitionTimeIfWindowSizeIsLargerThanDefaultRetentionTime()
        {
            long windowSize = 2 * TimeWindows.of(TimeSpan.FromMilliseconds(1)).maintainMs();
            Assert.Equal(windowSize, TimeWindows.of(TimeSpan.FromMilliseconds(windowSize)).maintainMs());
        }

        [Fact]
        public void windowSizeMustNotBeZero()
        {
            TimeWindows.of(TimeSpan.FromMilliseconds(0));
        }

        [Fact]
        public void windowSizeMustNotBeNegative()
        {
            TimeWindows.of(TimeSpan.FromMilliseconds(-1));
        }

        [Fact]
        public void advanceIntervalMustNotBeZero()
        {
            TimeWindows windowSpec = TimeWindows.of(TimeSpan.FromMilliseconds(ANY_SIZE));
            try
            {
                windowSpec.advanceBy(TimeSpan.FromMilliseconds(0));
                Assert.False(true, "should not accept zero advance parameter");
            }
            catch (ArgumentException e)
            {
                // expected
            }
        }

        [Fact]
        public void advanceIntervalMustNotBeNegative()
        {
            TimeWindows windowSpec = TimeWindows.of(TimeSpan.FromMilliseconds(ANY_SIZE));
            try
            {
                windowSpec.advanceBy(TimeSpan.FromMilliseconds(-1));
                Assert.False(true, "should not accept negative advance parameter");
            }
            catch (ArgumentException e)
            {
                // expected
            }
        }


        [Fact]
        public void advanceIntervalMustNotBeLargerThanWindowSize()
        {
            TimeWindows windowSpec = TimeWindows.of(TimeSpan.FromMilliseconds(ANY_SIZE));
            try
            {
                windowSpec.advanceBy(TimeSpan.FromMilliseconds(ANY_SIZE + 1));
                Assert.False(true, "should not accept advance greater than window size");
            }
            catch (ArgumentException e)
            {
                // expected
            }
        }


        [Fact]
        public void retentionTimeMustNoBeSmallerThanWindowSize()
        {
            TimeWindows windowSpec = TimeWindows.of(TimeSpan.FromMilliseconds(ANY_SIZE));
            try
            {
                windowSpec.until(ANY_SIZE - 1);
                Assert.False(true, "should not accept retention time smaller than window size");
            }
            catch (ArgumentException e)
            {
                // expected
            }
        }

        [Fact]
        public void gracePeriodShouldEnforceBoundaries()
        {
            TimeWindows.of(TimeSpan.FromMilliseconds(3L)).grace(TimeSpan.FromMilliseconds(0L));

            try
            {
                TimeWindows.of(TimeSpan.FromMilliseconds(3L)).grace(TimeSpan.FromMilliseconds(-1L));
                Assert.False(true, "should not accept negatives");
            }
            catch (ArgumentException e)
            {
                //expected
            }
        }

        [Fact]
        public void shouldComputeWindowsForHoppingWindows()
        {
            TimeWindows windows = TimeWindows.of(TimeSpan.FromMilliseconds(12L)).advanceBy(TimeSpan.FromMilliseconds(5L));
            Dictionary<long, TimeWindow> matched = windows.windowsFor(21L);
            Assert.Equal(12L / 5L + 1, matched.Count);
            Assert.Equal(new TimeWindow(10L, 22L), matched.Get(10L));
            Assert.Equal(new TimeWindow(15L, 27L), matched.Get(15L));
            Assert.Equal(new TimeWindow(20L, 32L), matched.Get(20L));
        }

        [Fact]
        public void shouldComputeWindowsForBarelyOverlappingHoppingWindows()
        {
            TimeWindows windows = TimeWindows.of(TimeSpan.FromMilliseconds(6L)).advanceBy(TimeSpan.FromMilliseconds(5L));
            Dictionary<long, TimeWindow> matched = windows.windowsFor(7L);
            Assert.Single(matched);
            Assert.Equal(new TimeWindow(5L, 11L), matched.Get(5L));
        }

        [Fact]
        public void shouldComputeWindowsForTumblingWindows()
        {
            TimeWindows windows = TimeWindows.of(TimeSpan.FromMilliseconds(12L));
            Dictionary<long, TimeWindow> matched = windows.windowsFor(21L);
            Assert.Single(matched);
            Assert.Equal(new TimeWindow(12L, 24L), matched.Get(12L));
        }


        [Fact]
        public void.EqualsAndHashcodeShouldBeValidForPositiveCases()
        {
            VerifyEquality(TimeWindows.of(TimeSpan.FromMilliseconds(3)), TimeWindows.of(TimeSpan.FromMilliseconds(3)));

            VerifyEquality(TimeWindows.of(TimeSpan.FromMilliseconds(3)).advanceBy(TimeSpan.FromMilliseconds(1)), TimeWindows.of(TimeSpan.FromMilliseconds(3)).advanceBy(TimeSpan.FromMilliseconds(1)));

            VerifyEquality(TimeWindows.of(TimeSpan.FromMilliseconds(3)).grace(TimeSpan.FromMilliseconds(1)), TimeWindows.of(TimeSpan.FromMilliseconds(3)).grace(TimeSpan.FromMilliseconds(1)));

            VerifyEquality(TimeWindows.of(TimeSpan.FromMilliseconds(3)).grace(TimeSpan.FromMilliseconds(4)), TimeWindows.of(TimeSpan.FromMilliseconds(3)).grace(TimeSpan.FromMilliseconds(4)));

            VerifyEquality(
                TimeWindows.of(TimeSpan.FromMilliseconds(3)).advanceBy(TimeSpan.FromMilliseconds(1)).grace(TimeSpan.FromMilliseconds(1)).grace(TimeSpan.FromMilliseconds(4)),
                TimeWindows.of(TimeSpan.FromMilliseconds(3)).advanceBy(TimeSpan.FromMilliseconds(1)).grace(TimeSpan.FromMilliseconds(1)).grace(TimeSpan.FromMilliseconds(4))
            );
        }

        [Fact]
        public void.EqualsAndHashcodeShouldBeValidForNegativeCases()
        {
            EqualityCheck.VerifyInEquality(TimeWindows.of(TimeSpan.FromMilliseconds(9)), TimeWindows.of(TimeSpan.FromMilliseconds(3)));

            EqualityCheck.VerifyInEquality(TimeWindows.of(TimeSpan.FromMilliseconds(3)).advanceBy(TimeSpan.FromMilliseconds(2)), TimeWindows.of(TimeSpan.FromMilliseconds(3)).advanceBy(TimeSpan.FromMilliseconds(1)));

            EqualityCheck.VerifyInEquality(TimeWindows.of(TimeSpan.FromMilliseconds(3)).grace(TimeSpan.FromMilliseconds(2)), TimeWindows.of(TimeSpan.FromMilliseconds(3)).grace(TimeSpan.FromMilliseconds(1)));

            EqualityCheck.VerifyInEquality(TimeWindows.of(TimeSpan.FromMilliseconds(3)).grace(TimeSpan.FromMilliseconds(9)), TimeWindows.of(TimeSpan.FromMilliseconds(3)).grace(TimeSpan.FromMilliseconds(4)));


            EqualityCheck.VerifyInEquality(
                TimeWindows.of(TimeSpan.FromMilliseconds(4)).advanceBy(TimeSpan.FromMilliseconds(2)).grace(TimeSpan.FromMilliseconds(2)),
                TimeWindows.of(TimeSpan.FromMilliseconds(3)).advanceBy(TimeSpan.FromMilliseconds(2)).grace(TimeSpan.FromMilliseconds(2))
            );

            EqualityCheck.VerifyInEquality(
                TimeWindows.of(TimeSpan.FromMilliseconds(3)).advanceBy(TimeSpan.FromMilliseconds(1)).grace(TimeSpan.FromMilliseconds(2)),
                TimeWindows.of(TimeSpan.FromMilliseconds(3)).advanceBy(TimeSpan.FromMilliseconds(2)).grace(TimeSpan.FromMilliseconds(2))
            );

            Assert.NotEqual(
                 TimeWindows.of(TimeSpan.FromMilliseconds(3)).advanceBy(TimeSpan.FromMilliseconds(2)).grace(TimeSpan.FromMilliseconds(1)),
                 TimeWindows.of(TimeSpan.FromMilliseconds(3)).advanceBy(TimeSpan.FromMilliseconds(2)).grace(TimeSpan.FromMilliseconds(2))
             );
        }
    }
}
