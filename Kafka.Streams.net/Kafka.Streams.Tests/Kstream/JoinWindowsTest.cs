namespace Kafka.Streams.Tests.Kstream
{
}
//using Kafka.Streams.KStream;

//namespace Kafka.Streams.Tests
//{
//    public class JoinWindowsTest
//    {

//        private static long ANY_SIZE = 123L;
//        private static long ANY_OTHER_SIZE = 456L; // should be larger than anySize

//        [Fact]
//        public void validWindows()
//        {
//            JoinWindows.of(TimeSpan.FromMilliseconds(ANY_OTHER_SIZE))   // [ -anyOtherSize ; anyOtherSize ]
//                       .before(TimeSpan.FromMilliseconds(ANY_SIZE))                    // [ -anySize ; anyOtherSize ]
//                       .before(TimeSpan.FromMilliseconds(0))                          // [ 0 ; anyOtherSize ]
//                       .before(TimeSpan.FromMilliseconds(-ANY_SIZE))                   // [ anySize ; anyOtherSize ]
//                       .before(TimeSpan.FromMilliseconds(-ANY_OTHER_SIZE));             // [ anyOtherSize ; anyOtherSize ]

//            JoinWindows.of(TimeSpan.FromMilliseconds(ANY_OTHER_SIZE))   // [ -anyOtherSize ; anyOtherSize ]
//                       .after(TimeSpan.FromMilliseconds(ANY_SIZE))                     // [ -anyOtherSize ; anySize ]
//                       .after(TimeSpan.FromMilliseconds(0))                           // [ -anyOtherSize ; 0 ]
//                       .after(TimeSpan.FromMilliseconds(-ANY_SIZE))                    // [ -anyOtherSize ; -anySize ]
//                       .after(TimeSpan.FromMilliseconds(-ANY_OTHER_SIZE));              // [ -anyOtherSize ; -anyOtherSize ]
//        }

//        [Fact]
//        public void timeDifferenceMustNotBeNegative()
//        {
//            JoinWindows.of(TimeSpan.FromMilliseconds(-1));
//        }

//        [Fact]
//        public void endTimeShouldNotBeBeforeStart()
//        {
//            JoinWindows windowSpec = JoinWindows.of(TimeSpan.FromMilliseconds(ANY_SIZE));
//            try
//            {
//                windowSpec.after(TimeSpan.FromMilliseconds(-ANY_SIZE - 1));
//                Assert.False(true, "window end time should not be before window start time");
//            }
//            catch (ArgumentException e)
//            {
//                // expected
//            }
//        }

//        [Fact]
//        public void startTimeShouldNotBeAfterEnd()
//        {
//            JoinWindows windowSpec = JoinWindows.of(TimeSpan.FromMilliseconds(ANY_SIZE));
//            try
//            {
//                windowSpec.before(TimeSpan.FromMilliseconds(-ANY_SIZE - 1));
//                Assert.False(true, "window start time should not be after window end time");
//            }
//            catch (ArgumentException e)
//            {
//                // expected
//            }
//        }

//        [Fact]
//        public void untilShouldSetGraceDuration()
//        {
//            JoinWindows windowSpec = JoinWindows.of(TimeSpan.FromMilliseconds(ANY_SIZE));
//            long windowSize = windowSpec.Count;
//            Assert.Equal(windowSize, windowSpec.grace(TimeSpan.FromMilliseconds(windowSize)).gracePeriodMs());
//        }


//        [Fact]
//        public void retentionTimeMustNoBeSmallerThanWindowSize()
//        {
//            JoinWindows windowSpec = JoinWindows.of(TimeSpan.FromMilliseconds(ANY_SIZE));
//            long windowSize = windowSpec.Count;
//            try
//            {
//                windowSpec.until(windowSize - 1);
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
//            JoinWindows.of(TimeSpan.FromMilliseconds(3L)).grace(TimeSpan.FromMilliseconds(0L));

//            try
//            {
//                JoinWindows.of(TimeSpan.FromMilliseconds(3L)).grace(TimeSpan.FromMilliseconds(-1L));
//                Assert.False(true, "should not accept negatives");
//            }
//            catch (ArgumentException e)
//            {
//                //expected
//            }
//        }

//        [Fact]
//        public void equalsAndHashcodeShouldBeValidForPositiveCases()
//        {
//            VerifyEquality(JoinWindows.of(TimeSpan.FromMilliseconds(3)), JoinWindows.of(TimeSpan.FromMilliseconds(3)));

//            VerifyEquality(JoinWindows.of(TimeSpan.FromMilliseconds(3)).after(TimeSpan.FromMilliseconds(2)), JoinWindows.of(TimeSpan.FromMilliseconds(3)).after(TimeSpan.FromMilliseconds(2)));

//            VerifyEquality(JoinWindows.of(TimeSpan.FromMilliseconds(3)).before(TimeSpan.FromMilliseconds(2)), JoinWindows.of(TimeSpan.FromMilliseconds(3)).before(TimeSpan.FromMilliseconds(2)));

//            VerifyEquality(JoinWindows.of(TimeSpan.FromMilliseconds(3)).grace(TimeSpan.FromMilliseconds(2)), JoinWindows.of(TimeSpan.FromMilliseconds(3)).grace(TimeSpan.FromMilliseconds(2)));

//            VerifyEquality(JoinWindows.of(TimeSpan.FromMilliseconds(3)).grace(TimeSpan.FromMilliseconds(60)), JoinWindows.of(TimeSpan.FromMilliseconds(3)).grace(TimeSpan.FromMilliseconds(60)));

//            VerifyEquality(
//                JoinWindows.of(TimeSpan.FromMilliseconds(3)).before(TimeSpan.FromMilliseconds(1)).after(TimeSpan.FromMilliseconds(2)).grace(TimeSpan.FromMilliseconds(3)).grace(TimeSpan.FromMilliseconds(60)),
//                JoinWindows.of(TimeSpan.FromMilliseconds(3)).before(TimeSpan.FromMilliseconds(1)).after(TimeSpan.FromMilliseconds(2)).grace(TimeSpan.FromMilliseconds(3)).grace(TimeSpan.FromMilliseconds(60))
//            );
//            // JoinWindows is a little weird in that before and after set the same fields.As of.
//            VerifyEquality(
//                JoinWindows.of(TimeSpan.FromMilliseconds(9)).before(TimeSpan.FromMilliseconds(1)).after(TimeSpan.FromMilliseconds(2)).grace(TimeSpan.FromMilliseconds(3)).grace(TimeSpan.FromMilliseconds(60)),
//                JoinWindows.of(TimeSpan.FromMilliseconds(3)).before(TimeSpan.FromMilliseconds(1)).after(TimeSpan.FromMilliseconds(2)).grace(TimeSpan.FromMilliseconds(3)).grace(TimeSpan.FromMilliseconds(60))
//            );
//        }

//        [Fact]
//        public void equalsAndHashcodeShouldBeValidForNegativeCases()
//        {
//            EqualityCheck.VerifyInEquality(JoinWindows.of(TimeSpan.FromMilliseconds(9)), JoinWindows.of(TimeSpan.FromMilliseconds(3)));

//            EqualityCheck.VerifyInEquality(JoinWindows.of(TimeSpan.FromMilliseconds(3)).after(TimeSpan.FromMilliseconds(9)), JoinWindows.of(TimeSpan.FromMilliseconds(3)).after(TimeSpan.FromMilliseconds(2)));

//            EqualityCheck.VerifyInEquality(JoinWindows.of(TimeSpan.FromMilliseconds(3)).before(TimeSpan.FromMilliseconds(9)), JoinWindows.of(TimeSpan.FromMilliseconds(3)).before(TimeSpan.FromMilliseconds(2)));

//            EqualityCheck.VerifyInEquality(JoinWindows.of(TimeSpan.FromMilliseconds(3)).grace(TimeSpan.FromMilliseconds(9)), JoinWindows.of(TimeSpan.FromMilliseconds(3)).grace(TimeSpan.FromMilliseconds(2)));

//            EqualityCheck.VerifyInEquality(JoinWindows.of(TimeSpan.FromMilliseconds(3)).grace(TimeSpan.FromMilliseconds(90)), JoinWindows.of(TimeSpan.FromMilliseconds(3)).grace(TimeSpan.FromMilliseconds(60)));


//            EqualityCheck.VerifyInEquality(
//                JoinWindows.of(TimeSpan.FromMilliseconds(3)).before(TimeSpan.FromMilliseconds(9)).after(TimeSpan.FromMilliseconds(2)).grace(TimeSpan.FromMilliseconds(3)),
//                JoinWindows.of(TimeSpan.FromMilliseconds(3)).before(TimeSpan.FromMilliseconds(1)).after(TimeSpan.FromMilliseconds(2)).grace(TimeSpan.FromMilliseconds(3))
//            );

//            EqualityCheck.VerifyInEquality(
//                JoinWindows.of(TimeSpan.FromMilliseconds(3)).before(TimeSpan.FromMilliseconds(1)).after(TimeSpan.FromMilliseconds(9)).grace(TimeSpan.FromMilliseconds(3)),
//                JoinWindows.of(TimeSpan.FromMilliseconds(3)).before(TimeSpan.FromMilliseconds(1)).after(TimeSpan.FromMilliseconds(2)).grace(TimeSpan.FromMilliseconds(3))
//            );

//            EqualityCheck.VerifyInEquality(
//                JoinWindows.of(TimeSpan.FromMilliseconds(3)).before(TimeSpan.FromMilliseconds(1)).after(TimeSpan.FromMilliseconds(2)).grace(TimeSpan.FromMilliseconds(9)),
//                JoinWindows.of(TimeSpan.FromMilliseconds(3)).before(TimeSpan.FromMilliseconds(1)).after(TimeSpan.FromMilliseconds(2)).grace(TimeSpan.FromMilliseconds(3))
//            );
//        }
//    }
