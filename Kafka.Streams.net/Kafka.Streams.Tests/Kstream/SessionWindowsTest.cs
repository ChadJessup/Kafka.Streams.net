using Kafka.Streams.KStream;
using NodaTime;
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
            Assert.Equal(anyGap, SessionWindows.With(Duration.FromTimeSpan(anyGap)).InactivityGap());
        }

        [Fact]
        public void ShouldSetWindowGraceTime()
        {
            var anyRetentionTime = TimeSpan.FromMilliseconds(42L);
            Assert.Equal(anyRetentionTime, SessionWindows.With(Duration.FromMilliseconds(1)).Grace(Duration.FromTimeSpan(anyRetentionTime)).GracePeriod());
        }

        [Fact]
        public void GracePeriodShouldEnforceBoundaries()
        {
            SessionWindows.With(Duration.FromMilliseconds(3L)).Grace(Duration.FromMilliseconds(0));
            Assert.Throws<ArgumentException>(() => SessionWindows.With(Duration.FromMilliseconds(3L)).Grace(Duration.FromMilliseconds(-1L)));
        }

        [Fact]
        public void WindowSizeMustNotBeNegative()
        {
            Assert.Throws<ArgumentException>(() => SessionWindows.With(Duration.FromMilliseconds(-1)));
        }

        [Fact]
        public void WindowSizeMustNotBeZero()
        {
            Assert.Throws<ArgumentException>(() => SessionWindows.With(Duration.FromMilliseconds(0)));
        }

        // specifically testing deprecated apis
        [Fact]
        public void RetentionTimeShouldBeGapIfGapIsLargerThanDefaultRetentionTime()
        {
            var windowGap = 2 * SessionWindows.With(Duration.FromMilliseconds(1)).Maintain();

            Assert.Equal(windowGap, SessionWindows.With(Duration.FromTimeSpan(windowGap)).Maintain());
        }


        [Fact]
        public void RetentionTimeMustNotBeNegative()
        {
            var windowSpec = SessionWindows.With(Duration.FromMilliseconds(42));

            Assert.Throws<ArgumentException>(() => windowSpec.Until(TimeSpan.FromMilliseconds(41)));
        }

        [Fact]
        public void EqualsAndHashcodeShouldBeValidForPositiveCases()
        {
            EqualityCheck.VerifyEquality(SessionWindows.With(Duration.FromMilliseconds(1)), SessionWindows.With(Duration.FromMilliseconds(1)));
            EqualityCheck.VerifyEquality(SessionWindows.With(Duration.FromMilliseconds(1)).Grace(Duration.FromMilliseconds(6)), SessionWindows.With(Duration.FromMilliseconds(1)).Grace(Duration.FromMilliseconds(6)));
            EqualityCheck.VerifyEquality(SessionWindows.With(Duration.FromMilliseconds(1)).Grace(Duration.FromMilliseconds(7)), SessionWindows.With(Duration.FromMilliseconds(1)).Grace(Duration.FromMilliseconds(7)));
            EqualityCheck.VerifyEquality(SessionWindows.With(Duration.FromMilliseconds(1)).Grace(Duration.FromMilliseconds(6)).Grace(Duration.FromMilliseconds(7)), SessionWindows.With(Duration.FromMilliseconds(1)).Grace(Duration.FromMilliseconds(6)).Grace(Duration.FromMilliseconds(7)));
        }

        [Fact]
        public void EqualsAndHshcodeShouldBeValidForNegativeCases()
        {
            EqualityCheck.VerifyInEquality(SessionWindows.With(Duration.FromMilliseconds(9)), SessionWindows.With(Duration.FromMilliseconds(1)));
            EqualityCheck.VerifyInEquality(SessionWindows.With(Duration.FromMilliseconds(1)).Grace(Duration.FromMilliseconds(9)), SessionWindows.With(Duration.FromMilliseconds(1)).Grace(Duration.FromMilliseconds(6)));
            EqualityCheck.VerifyInEquality(SessionWindows.With(Duration.FromMilliseconds(1)).Grace(Duration.FromMilliseconds(9)), SessionWindows.With(Duration.FromMilliseconds(1)).Grace(Duration.FromMilliseconds(7)));
            EqualityCheck.VerifyInEquality(SessionWindows.With(Duration.FromMilliseconds(2)).Grace(Duration.FromMilliseconds(6)).Grace(Duration.FromMilliseconds(7)), SessionWindows.With(Duration.FromMilliseconds(1)).Grace(Duration.FromMilliseconds(6)));
            EqualityCheck.VerifyInEquality(SessionWindows.With(Duration.FromMilliseconds(1)).Grace(Duration.FromMilliseconds(0)).Grace(Duration.FromMilliseconds(7)), SessionWindows.With(Duration.FromMilliseconds(1)).Grace(Duration.FromMilliseconds(6)));
        }
    }
}
