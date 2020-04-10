using System;
using Xunit;

namespace Kafka.Streams.KStream.Internals
{
    public class SessionWindowTest
    {
        private readonly long start = 50;
        private readonly long end = 100;
        private readonly SessionWindow window;
        private readonly TimeWindow timeWindow;

        public SessionWindowTest()
        {
            this.window = new SessionWindow(this.start, this.end);
            this.timeWindow = new TimeWindow(this.start, this.end);
        }

        [Fact]
        public void ShouldNotOverlapIfOtherWindowIsBeforeThisWindow()
        {
            /*
             * This:        [-------]
             * Other: [---]
             */
            Assert.False(this.window.Overlap(new SessionWindow(0, 25)));
            Assert.False(this.window.Overlap(new SessionWindow(0, this.start - 1)));
            Assert.False(this.window.Overlap(new SessionWindow(this.start - 1, this.start - 1)));
        }

        [Fact]
        public void ShouldOverlapIfOtherWindowEndIsWithinThisWindow()
        {
            /*
             * This:        [-------]
             * Other: [---------]
             */
            Assert.True(this.window.Overlap(new SessionWindow(0, this.start)));
            Assert.True(this.window.Overlap(new SessionWindow(0, this.start + 1)));
            Assert.True(this.window.Overlap(new SessionWindow(0, 75)));
            Assert.True(this.window.Overlap(new SessionWindow(0, this.end - 1)));
            Assert.True(this.window.Overlap(new SessionWindow(0, this.end)));

            Assert.True(this.window.Overlap(new SessionWindow(this.start - 1, this.start)));
            Assert.True(this.window.Overlap(new SessionWindow(this.start - 1, this.start + 1)));
            Assert.True(this.window.Overlap(new SessionWindow(this.start - 1, 75)));
            Assert.True(this.window.Overlap(new SessionWindow(this.start - 1, this.end - 1)));
            Assert.True(this.window.Overlap(new SessionWindow(this.start - 1, this.end)));
        }

        [Fact]
        public void ShouldOverlapIfOtherWindowContainsThisWindow()
        {
            /*
             * This:        [-------]
             * Other: [------------------]
             */
            Assert.True(this.window.Overlap(new SessionWindow(0, this.end)));
            Assert.True(this.window.Overlap(new SessionWindow(0, this.end + 1)));
            Assert.True(this.window.Overlap(new SessionWindow(0, 150)));

            Assert.True(this.window.Overlap(new SessionWindow(this.start - 1, this.end)));
            Assert.True(this.window.Overlap(new SessionWindow(this.start - 1, this.end + 1)));
            Assert.True(this.window.Overlap(new SessionWindow(this.start - 1, 150)));

            Assert.True(this.window.Overlap(new SessionWindow(this.start, this.end)));
            Assert.True(this.window.Overlap(new SessionWindow(this.start, this.end + 1)));
            Assert.True(this.window.Overlap(new SessionWindow(this.start, 150)));
        }

        [Fact]
        public void ShouldOverlapIfOtherWindowIsWithinThisWindow()
        {
            /*
             * This:        [-------]
             * Other:         [---]
             */
            Assert.True(this.window.Overlap(new SessionWindow(this.start, this.start)));
            Assert.True(this.window.Overlap(new SessionWindow(this.start, 75)));
            Assert.True(this.window.Overlap(new SessionWindow(this.start, this.end)));
            Assert.True(this.window.Overlap(new SessionWindow(75, this.end)));
            Assert.True(this.window.Overlap(new SessionWindow(this.end, this.end)));
        }

        [Fact]
        public void ShouldOverlapIfOtherWindowStartIsWithinThisWindow()
        {
            /*
             * This:        [-------]
             * Other:           [-------]
             */
            Assert.True(this.window.Overlap(new SessionWindow(this.start, this.end + 1)));
            Assert.True(this.window.Overlap(new SessionWindow(this.start, 150)));
            Assert.True(this.window.Overlap(new SessionWindow(75, this.end + 1)));
            Assert.True(this.window.Overlap(new SessionWindow(75, 150)));
            Assert.True(this.window.Overlap(new SessionWindow(this.end, this.end + 1)));
            Assert.True(this.window.Overlap(new SessionWindow(this.end, 150)));
        }

        [Fact]
        public void ShouldNotOverlapIsOtherWindowIsAfterThisWindow()
        {
            /*
             * This:        [-------]
             * Other:                  [---]
             */
            Assert.False(this.window.Overlap(new SessionWindow(this.end + 1, this.end + 1)));
            Assert.False(this.window.Overlap(new SessionWindow(this.end + 1, 150)));
            Assert.False(this.window.Overlap(new SessionWindow(125, 150)));
        }

        [Fact]
        public void CannotCompareSessionWindowWithDifferentWindowType()
        {
            Assert.Throws<ArgumentException>(() => this.window.Overlap(this.timeWindow));
        }
    }
}
