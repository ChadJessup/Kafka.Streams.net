using Kafka.Streams.KStream;
using Moq;
using System;
using Xunit;

namespace Kafka.Streams.Tests.Kstream
{
    public class JoinWindowsTest
    {

        private static readonly long ANY_SIZE = 123L;
        private static readonly long ANY_OTHER_SIZE = 456L; // should be larger than anySize

        [Fact]
        public void validWindows()
        {
            JoinWindows.Of(TimeSpan.FromMilliseconds(ANY_OTHER_SIZE))   // [ -anyOtherSize ; anyOtherSize ]
                       .Before(TimeSpan.FromMilliseconds(ANY_SIZE))                    // [ -anySize ; anyOtherSize ]
                       .Before(TimeSpan.FromMilliseconds(0))                          // [ 0 ; anyOtherSize ]
                       .Before(TimeSpan.FromMilliseconds(-ANY_SIZE))                   // [ anySize ; anyOtherSize ]
                       .Before(TimeSpan.FromMilliseconds(-ANY_OTHER_SIZE));             // [ anyOtherSize ; anyOtherSize ]

            JoinWindows.Of(TimeSpan.FromMilliseconds(ANY_OTHER_SIZE))   // [ -anyOtherSize ; anyOtherSize ]
                       .After(TimeSpan.FromMilliseconds(ANY_SIZE))                     // [ -anyOtherSize ; anySize ]
                       .After(TimeSpan.FromMilliseconds(0))                           // [ -anyOtherSize ; 0 ]
                       .After(TimeSpan.FromMilliseconds(-ANY_SIZE))                    // [ -anyOtherSize ; -anySize ]
                       .After(TimeSpan.FromMilliseconds(-ANY_OTHER_SIZE));              // [ -anyOtherSize ; -anyOtherSize ]
        }

        [Fact]
        public void TimeDifferenceMustNotBeNegative()
        {
            Assert.Throws<ArgumentException>(() => JoinWindows.Of(TimeSpan.FromMilliseconds(-1)));
        }

        [Fact]
        public void endTimeShouldNotBeBeforeStart()
        {
            JoinWindows windowSpec = JoinWindows.Of(TimeSpan.FromMilliseconds(ANY_SIZE));
            try
            {
                windowSpec.After(TimeSpan.FromMilliseconds(-ANY_SIZE - 1));
                Assert.False(true, "window end time should not be before window start time");
            }
            catch (ArgumentException e)
            {
                // expected
            }
        }

        [Fact]
        public void startTimeShouldNotBeAfterEnd()
        {
            JoinWindows windowSpec = JoinWindows.Of(TimeSpan.FromMilliseconds(ANY_SIZE));
            try
            {
                windowSpec.Before(TimeSpan.FromMilliseconds(-ANY_SIZE - 1));
                Assert.False(true, "window start time should not be after window end time");
            }
            catch (ArgumentException e)
            {
                // expected
            }
        }

        [Fact]
        public void UntilShouldSetGraceDuration()
        {
            JoinWindows windowSpec = JoinWindows.Of(TimeSpan.FromMilliseconds(ANY_SIZE));
            var windowSize = windowSpec.Size();
            Assert.Equal(windowSize, windowSpec.Grace(windowSize).GracePeriod());
        }


        [Fact]
        public void RetentionTimeMustNoBeSmallerThanWindowSize()
        {
            JoinWindows windowSpec = JoinWindows.Of(TimeSpan.FromMilliseconds(ANY_SIZE));
            var windowSize = windowSpec.Size();
            try
            {
                windowSpec.Until(windowSize.Add(TimeSpan.FromMilliseconds(-1)));
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
            JoinWindows.Of(TimeSpan.FromMilliseconds(3L)).Grace(TimeSpan.FromMilliseconds(0L));

            try
            {
                JoinWindows.Of(TimeSpan.FromMilliseconds(3L)).Grace(TimeSpan.FromMilliseconds(-1L));
                Assert.False(true, "should not accept negatives");
            }
            catch (ArgumentException e)
            {
                //expected
            }
        }

        [Fact]
        public void EqualsAndHashcodeShouldBeValidForPositiveCases()
        {
            EqualityCheck.VerifyEquality(JoinWindows.Of(TimeSpan.FromMilliseconds(3)), JoinWindows.Of(TimeSpan.FromMilliseconds(3)));

            EqualityCheck.VerifyEquality(JoinWindows.Of(TimeSpan.FromMilliseconds(3)).After(TimeSpan.FromMilliseconds(2)), JoinWindows.Of(TimeSpan.FromMilliseconds(3)).After(TimeSpan.FromMilliseconds(2)));

            EqualityCheck.VerifyEquality(JoinWindows.Of(TimeSpan.FromMilliseconds(3)).Before(TimeSpan.FromMilliseconds(2)), JoinWindows.Of(TimeSpan.FromMilliseconds(3)).Before(TimeSpan.FromMilliseconds(2)));

            EqualityCheck.VerifyEquality(JoinWindows.Of(TimeSpan.FromMilliseconds(3)).Grace(TimeSpan.FromMilliseconds(2)), JoinWindows.Of(TimeSpan.FromMilliseconds(3)).Grace(TimeSpan.FromMilliseconds(2)));

            EqualityCheck.VerifyEquality(JoinWindows.Of(TimeSpan.FromMilliseconds(3)).Grace(TimeSpan.FromMilliseconds(60)), JoinWindows.Of(TimeSpan.FromMilliseconds(3)).Grace(TimeSpan.FromMilliseconds(60)));

            EqualityCheck.VerifyEquality(
                JoinWindows.Of(TimeSpan.FromMilliseconds(3)).Before(TimeSpan.FromMilliseconds(1)).After(TimeSpan.FromMilliseconds(2)).Grace(TimeSpan.FromMilliseconds(3)).Grace(TimeSpan.FromMilliseconds(60)),
                JoinWindows.Of(TimeSpan.FromMilliseconds(3)).Before(TimeSpan.FromMilliseconds(1)).After(TimeSpan.FromMilliseconds(2)).Grace(TimeSpan.FromMilliseconds(3)).Grace(TimeSpan.FromMilliseconds(60))
            );

            // JoinWindows is a little weird in that before and after set the same fields.As of.
            EqualityCheck.VerifyEquality(
                JoinWindows.Of(TimeSpan.FromMilliseconds(9)).Before(TimeSpan.FromMilliseconds(1)).After(TimeSpan.FromMilliseconds(2)).Grace(TimeSpan.FromMilliseconds(3)).Grace(TimeSpan.FromMilliseconds(60)),
                JoinWindows.Of(TimeSpan.FromMilliseconds(3)).Before(TimeSpan.FromMilliseconds(1)).After(TimeSpan.FromMilliseconds(2)).Grace(TimeSpan.FromMilliseconds(3)).Grace(TimeSpan.FromMilliseconds(60))
            );
        }

        [Fact]
        public void EqualsAndHashcodeShouldBeValidForNegativeCases()
        {
            EqualityCheck.VerifyInEquality(JoinWindows.Of(TimeSpan.FromMilliseconds(9)), JoinWindows.Of(TimeSpan.FromMilliseconds(3)));

            EqualityCheck.VerifyInEquality(JoinWindows.Of(TimeSpan.FromMilliseconds(3)).After(TimeSpan.FromMilliseconds(9)), JoinWindows.Of(TimeSpan.FromMilliseconds(3)).After(TimeSpan.FromMilliseconds(2)));

            EqualityCheck.VerifyInEquality(JoinWindows.Of(TimeSpan.FromMilliseconds(3)).Before(TimeSpan.FromMilliseconds(9)), JoinWindows.Of(TimeSpan.FromMilliseconds(3)).Before(TimeSpan.FromMilliseconds(2)));

            EqualityCheck.VerifyInEquality(JoinWindows.Of(TimeSpan.FromMilliseconds(3)).Grace(TimeSpan.FromMilliseconds(9)), JoinWindows.Of(TimeSpan.FromMilliseconds(3)).Grace(TimeSpan.FromMilliseconds(2)));

            EqualityCheck.VerifyInEquality(JoinWindows.Of(TimeSpan.FromMilliseconds(3)).Grace(TimeSpan.FromMilliseconds(90)), JoinWindows.Of(TimeSpan.FromMilliseconds(3)).Grace(TimeSpan.FromMilliseconds(60)));


            EqualityCheck.VerifyInEquality(
                JoinWindows.Of(TimeSpan.FromMilliseconds(3)).Before(TimeSpan.FromMilliseconds(9)).After(TimeSpan.FromMilliseconds(2)).Grace(TimeSpan.FromMilliseconds(3)),
                JoinWindows.Of(TimeSpan.FromMilliseconds(3)).Before(TimeSpan.FromMilliseconds(1)).After(TimeSpan.FromMilliseconds(2)).Grace(TimeSpan.FromMilliseconds(3))
            );

            EqualityCheck.VerifyInEquality(
                JoinWindows.Of(TimeSpan.FromMilliseconds(3)).Before(TimeSpan.FromMilliseconds(1)).After(TimeSpan.FromMilliseconds(9)).Grace(TimeSpan.FromMilliseconds(3)),
                JoinWindows.Of(TimeSpan.FromMilliseconds(3)).Before(TimeSpan.FromMilliseconds(1)).After(TimeSpan.FromMilliseconds(2)).Grace(TimeSpan.FromMilliseconds(3))
            );

            EqualityCheck.VerifyInEquality(
                JoinWindows.Of(TimeSpan.FromMilliseconds(3)).Before(TimeSpan.FromMilliseconds(1)).After(TimeSpan.FromMilliseconds(2)).Grace(TimeSpan.FromMilliseconds(9)),
                JoinWindows.Of(TimeSpan.FromMilliseconds(3)).Before(TimeSpan.FromMilliseconds(1)).After(TimeSpan.FromMilliseconds(2)).Grace(TimeSpan.FromMilliseconds(3))
            );
        }
    }
}
