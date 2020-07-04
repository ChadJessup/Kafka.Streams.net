//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Internals;
//using System;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.Kstream
//{
//    public class TimeWindowsTest
//    {

//        private static long ANY_SIZE = 123L;

//        [Fact]
//        public void shouldSetWindowSize()
//        {
//            Assert.Equal(ANY_SIZE, TimeWindows.Of(TimeSpan.FromMilliseconds(ANY_SIZE)).sizeMs);
//        }

//        [Fact]
//        public void shouldSetWindowAdvance()
//        {
//            long anyAdvance = 4;
//            Assert.Equal(anyAdvance, TimeWindows.Of(TimeSpan.FromMilliseconds(ANY_SIZE)).AdvanceBy(TimeSpan.FromMilliseconds(anyAdvance)).advanceMs);
//        }

//        // specifically testing deprecated APIs
//        [Fact]
//        public void shouldSetWindowRetentionTime()
//        {
//            Assert.Equal(ANY_SIZE, TimeWindows.Of(TimeSpan.FromMilliseconds(ANY_SIZE)).until(ANY_SIZE).maintainMs());
//        }

//        // specifically testing deprecated APIs
//        [Fact]
//        public void shouldUseWindowSizeAsRentitionTimeIfWindowSizeIsLargerThanDefaultRetentionTime()
//        {
//            long windowSize = 2 * TimeWindows.Of(TimeSpan.FromMilliseconds(1)).maintainMs();
//            Assert.Equal(windowSize, TimeWindows.Of(TimeSpan.FromMilliseconds(windowSize)).maintainMs());
//        }

//        [Fact]
//        public void windowSizeMustNotBeZero()
//        {
//            TimeWindows.Of(TimeSpan.FromMilliseconds(0));
//        }

//        [Fact]
//        public void windowSizeMustNotBeNegative()
//        {
//            TimeWindows.Of(TimeSpan.FromMilliseconds(-1));
//        }

//        [Fact]
//        public void advanceIntervalMustNotBeZero()
//        {
//            TimeWindows windowSpec = TimeWindows.Of(TimeSpan.FromMilliseconds(ANY_SIZE));
//            try
//            {
//                windowSpec.AdvanceBy(TimeSpan.FromMilliseconds(0));
//                Assert.False(true, "should not accept zero advance parameter");
//            }
//            catch (ArgumentException e)
//            {
//                // expected
//            }
//        }

//        [Fact]
//        public void advanceIntervalMustNotBeNegative()
//        {
//            TimeWindows windowSpec = TimeWindows.Of(TimeSpan.FromMilliseconds(ANY_SIZE));
//            try
//            {
//                windowSpec.AdvanceBy(TimeSpan.FromMilliseconds(-1));
//                Assert.False(true, "should not accept negative advance parameter");
//            }
//            catch (ArgumentException e)
//            {
//                // expected
//            }
//        }


//        [Fact]
//        public void advanceIntervalMustNotBeLargerThanWindowSize()
//        {
//            TimeWindows windowSpec = TimeWindows.Of(TimeSpan.FromMilliseconds(ANY_SIZE));
//            try
//            {
//                windowSpec.AdvanceBy(TimeSpan.FromMilliseconds(ANY_SIZE + 1));
//                Assert.False(true, "should not accept advance greater than window size");
//            }
//            catch (ArgumentException e)
//            {
//                // expected
//            }
//        }


//        [Fact]
//        public void retentionTimeMustNoBeSmallerThanWindowSize()
//        {
//            TimeWindows windowSpec = TimeWindows.Of(TimeSpan.FromMilliseconds(ANY_SIZE));
//            try
//            {
//                windowSpec.Until(ANY_SIZE - 1);
//                Assert.False(true, "should not accept retention time smaller than window size");
//            }
//            catch (ArgumentException e)
//            {
//                // expected
//            }
//        }

//        [Fact]
//        public void gracePeriodShouldEnforceBoundaries()
//        {
//            TimeWindows.Of(TimeSpan.FromMilliseconds(3L)).Grace(TimeSpan.FromMilliseconds(0L));

//            try
//            {
//                TimeWindows.Of(TimeSpan.FromMilliseconds(3L)).Grace(TimeSpan.FromMilliseconds(-1L));
//                Assert.False(true, "should not accept negatives");
//            }
//            catch (ArgumentException e)
//            {
//                //expected
//            }
//        }

//        [Fact]
//        public void shouldComputeWindowsForHoppingWindows()
//        {
//            TimeWindows windows = TimeWindows.Of(TimeSpan.FromMilliseconds(12L)).AdvanceBy(TimeSpan.FromMilliseconds(5L));
//            Dictionary<long, TimeWindow> matched = windows.WindowsFor(21L);
//            Assert.Equal(12L / 5L + 1, matched.Count);
//            Assert.Equal(new TimeWindow(10L, 22L), matched[10L]);
//            Assert.Equal(new TimeWindow(15L, 27L), matched[15L]);
//            Assert.Equal(new TimeWindow(20L, 32L), matched[20L]);
//        }

//        [Fact]
//        public void shouldComputeWindowsForBarelyOverlappingHoppingWindows()
//        {
//            TimeWindows windows = TimeWindows.Of(TimeSpan.FromMilliseconds(6L)).AdvanceBy(TimeSpan.FromMilliseconds(5L));
//            Dictionary<long, TimeWindow> matched = windows.WindowsFor(7L);
//            Assert.Single(matched);
//            Assert.Equal(new TimeWindow(5L, 11L), matched[5L]);
//        }

//        [Fact]
//        public void shouldComputeWindowsForTumblingWindows()
//        {
//            TimeWindows windows = TimeWindows.Of(TimeSpan.FromMilliseconds(12L));
//            Dictionary<long, TimeWindow> matched = windows.WindowsFor(21L);
//            Assert.Single(matched);
//            Assert.Equal(new TimeWindow(12L, 24L), matched[12L]);
//        }


//        [Fact]
//        public void EqualsAndHashcodeShouldBeValidForPositiveCases()
//        {
//            VerifyEquality(TimeWindows.Of(TimeSpan.FromMilliseconds(3)), TimeWindows.Of(TimeSpan.FromMilliseconds(3)));

//            VerifyEquality(TimeWindows.Of(TimeSpan.FromMilliseconds(3)).AdvanceBy(TimeSpan.FromMilliseconds(1)), TimeWindows.Of(TimeSpan.FromMilliseconds(3)).AdvanceBy(TimeSpan.FromMilliseconds(1)));

//            VerifyEquality(TimeWindows.Of(TimeSpan.FromMilliseconds(3)).Grace(TimeSpan.FromMilliseconds(1)), TimeWindows.Of(TimeSpan.FromMilliseconds(3)).Grace(TimeSpan.FromMilliseconds(1)));

//            VerifyEquality(TimeWindows.Of(TimeSpan.FromMilliseconds(3)).Grace(TimeSpan.FromMilliseconds(4)), TimeWindows.Of(TimeSpan.FromMilliseconds(3)).Grace(TimeSpan.FromMilliseconds(4)));

//            VerifyEquality(
//                TimeWindows.Of(TimeSpan.FromMilliseconds(3)).AdvanceBy(TimeSpan.FromMilliseconds(1)).Grace(TimeSpan.FromMilliseconds(1)).Grace(TimeSpan.FromMilliseconds(4)),
//                TimeWindows.Of(TimeSpan.FromMilliseconds(3)).AdvanceBy(TimeSpan.FromMilliseconds(1)).Grace(TimeSpan.FromMilliseconds(1)).Grace(TimeSpan.FromMilliseconds(4))
//            );
//        }

//        [Fact]
//        public void EqualsAndHashcodeShouldBeValidForNegativeCases()
//        {
//            EqualityCheck.VerifyInEquality(TimeWindows.Of(TimeSpan.FromMilliseconds(9)), TimeWindows.Of(TimeSpan.FromMilliseconds(3)));
//            EqualityCheck.VerifyInEquality(TimeWindows.Of(TimeSpan.FromMilliseconds(3)).AdvanceBy(TimeSpan.FromMilliseconds(2)), TimeWindows.Of(TimeSpan.FromMilliseconds(3)).AdvanceBy(TimeSpan.FromMilliseconds(1)));
//            EqualityCheck.VerifyInEquality(TimeWindows.Of(TimeSpan.FromMilliseconds(3)).Grace(TimeSpan.FromMilliseconds(2)), TimeWindows.Of(TimeSpan.FromMilliseconds(3)).Grace(TimeSpan.FromMilliseconds(1)));
//            EqualityCheck.VerifyInEquality(TimeWindows.Of(TimeSpan.FromMilliseconds(3)).Grace(TimeSpan.FromMilliseconds(9)), TimeWindows.Of(TimeSpan.FromMilliseconds(3)).Grace(TimeSpan.FromMilliseconds(4)));

//            EqualityCheck.VerifyInEquality(
//                TimeWindows.Of(TimeSpan.FromMilliseconds(4)).AdvanceBy(TimeSpan.FromMilliseconds(2)).Grace(TimeSpan.FromMilliseconds(2)),
//                TimeWindows.Of(TimeSpan.FromMilliseconds(3)).AdvanceBy(TimeSpan.FromMilliseconds(2)).Grace(TimeSpan.FromMilliseconds(2)));

//            EqualityCheck.VerifyInEquality(
//                TimeWindows.Of(TimeSpan.FromMilliseconds(3)).AdvanceBy(TimeSpan.FromMilliseconds(1)).Grace(TimeSpan.FromMilliseconds(2)),
//                TimeWindows.Of(TimeSpan.FromMilliseconds(3)).AdvanceBy(TimeSpan.FromMilliseconds(2)).Grace(TimeSpan.FromMilliseconds(2)));

//            Assert.NotEqual(
//                 TimeWindows.Of(TimeSpan.FromMilliseconds(3)).AdvanceBy(TimeSpan.FromMilliseconds(2)).Grace(TimeSpan.FromMilliseconds(1)),
//                 TimeWindows.Of(TimeSpan.FromMilliseconds(3)).AdvanceBy(TimeSpan.FromMilliseconds(2)).Grace(TimeSpan.FromMilliseconds(2)));
//        }
//    }
//}
