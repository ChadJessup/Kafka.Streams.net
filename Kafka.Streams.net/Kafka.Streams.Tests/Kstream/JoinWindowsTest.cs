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
//            JoinWindows.of(Duration.FromMilliseconds(ANY_OTHER_SIZE))   // [ -anyOtherSize ; anyOtherSize ]
//                       .before(Duration.FromMilliseconds(ANY_SIZE))                    // [ -anySize ; anyOtherSize ]
//                       .before(Duration.FromMilliseconds(0))                          // [ 0 ; anyOtherSize ]
//                       .before(Duration.FromMilliseconds(-ANY_SIZE))                   // [ anySize ; anyOtherSize ]
//                       .before(Duration.FromMilliseconds(-ANY_OTHER_SIZE));             // [ anyOtherSize ; anyOtherSize ]

//            JoinWindows.of(Duration.FromMilliseconds(ANY_OTHER_SIZE))   // [ -anyOtherSize ; anyOtherSize ]
//                       .after(Duration.FromMilliseconds(ANY_SIZE))                     // [ -anyOtherSize ; anySize ]
//                       .after(Duration.FromMilliseconds(0))                           // [ -anyOtherSize ; 0 ]
//                       .after(Duration.FromMilliseconds(-ANY_SIZE))                    // [ -anyOtherSize ; -anySize ]
//                       .after(Duration.FromMilliseconds(-ANY_OTHER_SIZE));              // [ -anyOtherSize ; -anyOtherSize ]
//        }

//        [Fact]
//        public void timeDifferenceMustNotBeNegative()
//        {
//            JoinWindows.of(Duration.FromMilliseconds(-1));
//        }

//        [Fact]
//        public void endTimeShouldNotBeBeforeStart()
//        {
//            JoinWindows windowSpec = JoinWindows.of(Duration.FromMilliseconds(ANY_SIZE));
//            try
//            {
//                windowSpec.after(Duration.FromMilliseconds(-ANY_SIZE - 1));
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
//            JoinWindows windowSpec = JoinWindows.of(Duration.FromMilliseconds(ANY_SIZE));
//            try
//            {
//                windowSpec.before(Duration.FromMilliseconds(-ANY_SIZE - 1));
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
//            JoinWindows windowSpec = JoinWindows.of(Duration.FromMilliseconds(ANY_SIZE));
//            long windowSize = windowSpec.Count;
//            Assert.Equal(windowSize, windowSpec.grace(Duration.FromMilliseconds(windowSize)).gracePeriodMs());
//        }


//        [Fact]
//        public void retentionTimeMustNoBeSmallerThanWindowSize()
//        {
//            JoinWindows windowSpec = JoinWindows.of(Duration.FromMilliseconds(ANY_SIZE));
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
//            JoinWindows.of(Duration.FromMilliseconds(3L)).grace(Duration.FromMilliseconds(0L));

//            try
//            {
//                JoinWindows.of(Duration.FromMilliseconds(3L)).grace(Duration.FromMilliseconds(-1L));
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
//            VerifyEquality(JoinWindows.of(Duration.FromMilliseconds(3)), JoinWindows.of(Duration.FromMilliseconds(3)));

//            VerifyEquality(JoinWindows.of(Duration.FromMilliseconds(3)).after(Duration.FromMilliseconds(2)), JoinWindows.of(Duration.FromMilliseconds(3)).after(Duration.FromMilliseconds(2)));

//            VerifyEquality(JoinWindows.of(Duration.FromMilliseconds(3)).before(Duration.FromMilliseconds(2)), JoinWindows.of(Duration.FromMilliseconds(3)).before(Duration.FromMilliseconds(2)));

//            VerifyEquality(JoinWindows.of(Duration.FromMilliseconds(3)).grace(Duration.FromMilliseconds(2)), JoinWindows.of(Duration.FromMilliseconds(3)).grace(Duration.FromMilliseconds(2)));

//            VerifyEquality(JoinWindows.of(Duration.FromMilliseconds(3)).grace(Duration.FromMilliseconds(60)), JoinWindows.of(Duration.FromMilliseconds(3)).grace(Duration.FromMilliseconds(60)));

//            VerifyEquality(
//                JoinWindows.of(Duration.FromMilliseconds(3)).before(Duration.FromMilliseconds(1)).after(Duration.FromMilliseconds(2)).grace(Duration.FromMilliseconds(3)).grace(Duration.FromMilliseconds(60)),
//                JoinWindows.of(Duration.FromMilliseconds(3)).before(Duration.FromMilliseconds(1)).after(Duration.FromMilliseconds(2)).grace(Duration.FromMilliseconds(3)).grace(Duration.FromMilliseconds(60))
//            );
//            // JoinWindows is a little weird in that before and after set the same fields.As of.
//            VerifyEquality(
//                JoinWindows.of(Duration.FromMilliseconds(9)).before(Duration.FromMilliseconds(1)).after(Duration.FromMilliseconds(2)).grace(Duration.FromMilliseconds(3)).grace(Duration.FromMilliseconds(60)),
//                JoinWindows.of(Duration.FromMilliseconds(3)).before(Duration.FromMilliseconds(1)).after(Duration.FromMilliseconds(2)).grace(Duration.FromMilliseconds(3)).grace(Duration.FromMilliseconds(60))
//            );
//        }

//        [Fact]
//        public void equalsAndHashcodeShouldBeValidForNegativeCases()
//        {
//            EqualityCheck.VerifyInEquality(JoinWindows.of(Duration.FromMilliseconds(9)), JoinWindows.of(Duration.FromMilliseconds(3)));

//            EqualityCheck.VerifyInEquality(JoinWindows.of(Duration.FromMilliseconds(3)).after(Duration.FromMilliseconds(9)), JoinWindows.of(Duration.FromMilliseconds(3)).after(Duration.FromMilliseconds(2)));

//            EqualityCheck.VerifyInEquality(JoinWindows.of(Duration.FromMilliseconds(3)).before(Duration.FromMilliseconds(9)), JoinWindows.of(Duration.FromMilliseconds(3)).before(Duration.FromMilliseconds(2)));

//            EqualityCheck.VerifyInEquality(JoinWindows.of(Duration.FromMilliseconds(3)).grace(Duration.FromMilliseconds(9)), JoinWindows.of(Duration.FromMilliseconds(3)).grace(Duration.FromMilliseconds(2)));

//            EqualityCheck.VerifyInEquality(JoinWindows.of(Duration.FromMilliseconds(3)).grace(Duration.FromMilliseconds(90)), JoinWindows.of(Duration.FromMilliseconds(3)).grace(Duration.FromMilliseconds(60)));


//            EqualityCheck.VerifyInEquality(
//                JoinWindows.of(Duration.FromMilliseconds(3)).before(Duration.FromMilliseconds(9)).after(Duration.FromMilliseconds(2)).grace(Duration.FromMilliseconds(3)),
//                JoinWindows.of(Duration.FromMilliseconds(3)).before(Duration.FromMilliseconds(1)).after(Duration.FromMilliseconds(2)).grace(Duration.FromMilliseconds(3))
//            );

//            EqualityCheck.VerifyInEquality(
//                JoinWindows.of(Duration.FromMilliseconds(3)).before(Duration.FromMilliseconds(1)).after(Duration.FromMilliseconds(9)).grace(Duration.FromMilliseconds(3)),
//                JoinWindows.of(Duration.FromMilliseconds(3)).before(Duration.FromMilliseconds(1)).after(Duration.FromMilliseconds(2)).grace(Duration.FromMilliseconds(3))
//            );

//            EqualityCheck.VerifyInEquality(
//                JoinWindows.of(Duration.FromMilliseconds(3)).before(Duration.FromMilliseconds(1)).after(Duration.FromMilliseconds(2)).grace(Duration.FromMilliseconds(9)),
//                JoinWindows.of(Duration.FromMilliseconds(3)).before(Duration.FromMilliseconds(1)).after(Duration.FromMilliseconds(2)).grace(Duration.FromMilliseconds(3))
//            );
//        }
//    }
