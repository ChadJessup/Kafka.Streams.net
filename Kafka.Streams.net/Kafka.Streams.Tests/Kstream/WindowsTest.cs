using Kafka.Streams.KStream;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests
{
    public class WindowsTest
    {
        private class TestWindows<W> : Windows<W>
             where W : Window
        {

            public override Dictionary<long, W>? WindowsFor(TimeSpan timestamp)
            {
                return null;
            }

            public override TimeSpan Size()
            {
                return TimeSpan.Zero;
            }

            public override TimeSpan GracePeriod()
            {
                return TimeSpan.Zero;
            }
        }

        // specifically testing deprecated APIs
#pragma warning disable CS0612 // Type or member is obsolete

        [Fact]
        public void ShouldSetWindowRetentionTime()
        {
            var anyNotNegativeRetentionTime = TimeSpan.FromMilliseconds(42);
            Assert.Equal(anyNotNegativeRetentionTime, new TestWindows<Window>()
                .Until(anyNotNegativeRetentionTime)
                .MaintainDuration());
        }

        [Fact]
        public void NumberOfSegmentsMustBeAtLeastTwo()
        {
            Assert.Throws<ArgumentException>(() => new TestWindows<Window>().Segments(1));
        }

        [Fact]
        public void RetentionTimeMustNotBeNegative()
        {
            Assert.Throws<ArgumentException>(() => new TestWindows<Window>().Until(TimeSpan.FromMilliseconds(-1)));
        }
    }
#pragma warning restore CS0612 // Type or member is obsolete
}
