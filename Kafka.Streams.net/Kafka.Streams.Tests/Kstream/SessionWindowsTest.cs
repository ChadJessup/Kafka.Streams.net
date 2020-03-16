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
            Assert.Equal(anyGap, SessionWindows.with(Duration.FromTimeSpan(anyGap)).inactivityGap());
        }

        [Fact]
        public void ShouldSetWindowGraceTime()
        {
            var anyRetentionTime = TimeSpan.FromMilliseconds(42L);
            Assert.Equal(anyRetentionTime, SessionWindows.with(Duration.FromMilliseconds(1)).grace(Duration.FromTimeSpan(anyRetentionTime)).gracePeriod());
        }

        [Fact]
        public void GracePeriodShouldEnforceBoundaries()
        {
            SessionWindows.with(Duration.FromMilliseconds(3L)).grace(Duration.FromMilliseconds(0));
            Assert.Throws<ArgumentException>(() => SessionWindows.with(Duration.FromMilliseconds(3L)).grace(Duration.FromMilliseconds(-1L)));
        }

        [Fact]
        public void WindowSizeMustNotBeNegative()
        {
            Assert.Throws<ArgumentException>(() => SessionWindows.with(Duration.FromMilliseconds(-1)));
        }

        [Fact]
        public void WindowSizeMustNotBeZero()
        {
            Assert.Throws<ArgumentException>(() => SessionWindows.with(Duration.FromMilliseconds(0)));
        }

        // specifically testing deprecated apis
        [Fact]
        public void RetentionTimeShouldBeGapIfGapIsLargerThanDefaultRetentionTime()
        {
            var windowGap = 2 * SessionWindows.with(Duration.FromMilliseconds(1)).maintain();

            Assert.Equal(windowGap, SessionWindows.with(Duration.FromTimeSpan(windowGap)).maintain());
        }


        [Fact]
        public void RetentionTimeMustNotBeNegative()
        {
            var windowSpec = SessionWindows.with(Duration.FromMilliseconds(42));

            Assert.Throws<ArgumentException>(() => windowSpec.until(TimeSpan.FromMilliseconds(41)));
        }

        [Fact]
        public void EqualsAndHashcodeShouldBeValidForPositiveCases()
        {
            EqualityCheck.VerifyEquality(SessionWindows.with(Duration.FromMilliseconds(1)), SessionWindows.with(Duration.FromMilliseconds(1)));
            EqualityCheck.VerifyEquality(SessionWindows.with(Duration.FromMilliseconds(1)).grace(Duration.FromMilliseconds(6)), SessionWindows.with(Duration.FromMilliseconds(1)).grace(Duration.FromMilliseconds(6)));
            EqualityCheck.VerifyEquality(SessionWindows.with(Duration.FromMilliseconds(1)).grace(Duration.FromMilliseconds(7)), SessionWindows.with(Duration.FromMilliseconds(1)).grace(Duration.FromMilliseconds(7)));
            EqualityCheck.VerifyEquality(SessionWindows.with(Duration.FromMilliseconds(1)).grace(Duration.FromMilliseconds(6)).grace(Duration.FromMilliseconds(7)), SessionWindows.with(Duration.FromMilliseconds(1)).grace(Duration.FromMilliseconds(6)).grace(Duration.FromMilliseconds(7)));
        }

        [Fact]
        public void EqualsAndHshcodeShouldBeValidForNegativeCases()
        {
            EqualityCheck.VerifyInEquality(SessionWindows.with(Duration.FromMilliseconds(9)), SessionWindows.with(Duration.FromMilliseconds(1)));
            EqualityCheck.VerifyInEquality(SessionWindows.with(Duration.FromMilliseconds(1)).grace(Duration.FromMilliseconds(9)), SessionWindows.with(Duration.FromMilliseconds(1)).grace(Duration.FromMilliseconds(6)));
            EqualityCheck.VerifyInEquality(SessionWindows.with(Duration.FromMilliseconds(1)).grace(Duration.FromMilliseconds(9)), SessionWindows.with(Duration.FromMilliseconds(1)).grace(Duration.FromMilliseconds(7)));
            EqualityCheck.VerifyInEquality(SessionWindows.with(Duration.FromMilliseconds(2)).grace(Duration.FromMilliseconds(6)).grace(Duration.FromMilliseconds(7)), SessionWindows.with(Duration.FromMilliseconds(1)).grace(Duration.FromMilliseconds(6)));
            EqualityCheck.VerifyInEquality(SessionWindows.with(Duration.FromMilliseconds(1)).grace(Duration.FromMilliseconds(0)).grace(Duration.FromMilliseconds(7)), SessionWindows.with(Duration.FromMilliseconds(1)).grace(Duration.FromMilliseconds(6)));
        }
    }
}
