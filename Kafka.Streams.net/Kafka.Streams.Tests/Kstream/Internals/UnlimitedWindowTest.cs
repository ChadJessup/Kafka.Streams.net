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
            this.window = new UnlimitedWindow(start);
            this.sessionWindow = new SessionWindow(start, start);
        }

        [Fact]
        public void ShouldAlwaysOverlap()
        {
            Assert.True(window.Overlap(new UnlimitedWindow(start - 1)));
            Assert.True(window.Overlap(new UnlimitedWindow(start)));
            Assert.True(window.Overlap(new UnlimitedWindow(start + 1)));
        }

        [Fact]
        public void CannotCompareUnlimitedWindowWithDifferentWindowType()
        {
            window.Overlap(sessionWindow);
        }
    }
}
