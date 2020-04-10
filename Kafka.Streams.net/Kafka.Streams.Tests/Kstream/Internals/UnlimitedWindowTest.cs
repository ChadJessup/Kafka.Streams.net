using System;
using Xunit;

namespace Kafka.Streams.KStream.Internals
{
    public class UnlimitedWindowTest
    {
        private readonly long start = 50;
        private readonly UnlimitedWindow window;
        private readonly SessionWindow sessionWindow;

        public UnlimitedWindowTest()
        {
            this.window = new UnlimitedWindow(this.start);
            this.sessionWindow = new SessionWindow(this.start, this.start);
        }

        [Fact]
        public void ShouldAlwaysOverlap()
        {
            Assert.True(this.window.Overlap(new UnlimitedWindow(this.start - 1)));
            Assert.True(this.window.Overlap(new UnlimitedWindow(this.start)));
            Assert.True(this.window.Overlap(new UnlimitedWindow(this.start + 1)));
        }

        [Fact]
        public void CannotCompareUnlimitedWindowWithDifferentWindowType()
        {
            var e = Assert.Throws<ArgumentException>(() => this.window.Overlap(this.sessionWindow));
            Assert.Equal("Cannot compare windows of different type. Other window has type Kafka.Streams.KStream.Internals.SessionWindow.", e.Message);
        }
    }
}
