using Kafka.Streams.KStream;

using System;
using Xunit;

namespace Kafka.Streams.Tests
{
    public class SessionWindowsTest
    {
        [Fact]
        public void ShouldSetWindowGap()
        {
            var anyGap = TimeSpan.FromMilliseconds(42L);
            Assert.Equal(anyGap, SessionWindows.With(anyGap).InactivityGap());
        }

        [Fact]
        public void ShouldSetWindowGraceTime()
        {
            var anyRetentionTime = TimeSpan.FromMilliseconds(42L);
            Assert.Equal(anyRetentionTime, SessionWindows
                .With(TimeSpan.FromMilliseconds(1))
                .Grace(anyRetentionTime)
                .GracePeriod());
        }

        [Fact]
        public void GracePeriodShouldEnforceBoundaries()
        {
            SessionWindows.With(TimeSpan.FromMilliseconds(3L))
                .Grace(TimeSpan.FromMilliseconds(0));
            Assert.Throws<ArgumentException>(() => SessionWindows.With(TimeSpan.FromMilliseconds(3L)).Grace(TimeSpan.FromMilliseconds(-1L)));
        }

        [Fact]
        public void WindowSizeMustNotBeNegative()
        {
            Assert.Throws<ArgumentException>(() => SessionWindows.With(TimeSpan.FromMilliseconds(-1)));
        }

        [Fact]
        public void WindowSizeMustNotBeZero()
        {
            Assert.Throws<ArgumentException>(() => SessionWindows.With(TimeSpan.FromMilliseconds(0)));
        }

        // specifically testing deprecated apis
        [Fact]
        public void RetentionTimeShouldBeGapIfGapIsLargerThanDefaultRetentionTime()
        {
            var windowGap = 2 * SessionWindows.With(TimeSpan.FromMilliseconds(1)).Maintain();

            Assert.Equal(windowGap, SessionWindows.With(windowGap).Maintain());
        }


        [Fact]
        public void RetentionTimeMustNotBeNegative()
        {
            var windowSpec = SessionWindows.With(TimeSpan.FromMilliseconds(42));

            Assert.Throws<ArgumentException>(() => windowSpec.Until(TimeSpan.FromMilliseconds(41)));
        }

        [Fact]
        public void EqualsAndHashcodeShouldBeValidForPositiveCases()
        {
            EqualityCheck.VerifyEquality(SessionWindows.With(TimeSpan.FromMilliseconds(1)), SessionWindows.With(TimeSpan.FromMilliseconds(1)));
            EqualityCheck.VerifyEquality(SessionWindows.With(TimeSpan.FromMilliseconds(1)).Grace(TimeSpan.FromMilliseconds(6)), SessionWindows.With(TimeSpan.FromMilliseconds(1)).Grace(TimeSpan.FromMilliseconds(6)));
            EqualityCheck.VerifyEquality(SessionWindows.With(TimeSpan.FromMilliseconds(1)).Grace(TimeSpan.FromMilliseconds(7)), SessionWindows.With(TimeSpan.FromMilliseconds(1)).Grace(TimeSpan.FromMilliseconds(7)));
            EqualityCheck.VerifyEquality(SessionWindows.With(TimeSpan.FromMilliseconds(1)).Grace(TimeSpan.FromMilliseconds(6)).Grace(TimeSpan.FromMilliseconds(7)), SessionWindows.With(TimeSpan.FromMilliseconds(1)).Grace(TimeSpan.FromMilliseconds(6)).Grace(TimeSpan.FromMilliseconds(7)));
        }

        [Fact]
        public void EqualsAndHshcodeShouldBeValidForNegativeCases()
        {
            EqualityCheck.VerifyInEquality(SessionWindows.With(TimeSpan.FromMilliseconds(9)), SessionWindows.With(TimeSpan.FromMilliseconds(1)));
            EqualityCheck.VerifyInEquality(SessionWindows.With(TimeSpan.FromMilliseconds(1)).Grace(TimeSpan.FromMilliseconds(9)), SessionWindows.With(TimeSpan.FromMilliseconds(1)).Grace(TimeSpan.FromMilliseconds(6)));
            EqualityCheck.VerifyInEquality(SessionWindows.With(TimeSpan.FromMilliseconds(1)).Grace(TimeSpan.FromMilliseconds(9)), SessionWindows.With(TimeSpan.FromMilliseconds(1)).Grace(TimeSpan.FromMilliseconds(7)));
            EqualityCheck.VerifyInEquality(SessionWindows.With(TimeSpan.FromMilliseconds(2)).Grace(TimeSpan.FromMilliseconds(6)).Grace(TimeSpan.FromMilliseconds(7)), SessionWindows.With(TimeSpan.FromMilliseconds(1)).Grace(TimeSpan.FromMilliseconds(6)));
            EqualityCheck.VerifyInEquality(SessionWindows.With(TimeSpan.FromMilliseconds(1)).Grace(TimeSpan.FromMilliseconds(0)).Grace(TimeSpan.FromMilliseconds(7)), SessionWindows.With(TimeSpan.FromMilliseconds(1)).Grace(TimeSpan.FromMilliseconds(6)));
        }
    }
}
