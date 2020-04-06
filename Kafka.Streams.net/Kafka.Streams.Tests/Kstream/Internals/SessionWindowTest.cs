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
            this.window = new SessionWindow(start, end);
            this.timeWindow = new TimeWindow(start, end);
        }

        [Fact]
        public void ShouldNotOverlapIfOtherWindowIsBeforeThisWindow()
        {
            /*
             * This:        [-------]
             * Other: [---]
             */
            Assert.False(window.Overlap(new SessionWindow(0, 25)));
            Assert.False(window.Overlap(new SessionWindow(0, start - 1)));
            Assert.False(window.Overlap(new SessionWindow(start - 1, start - 1)));
        }

        [Fact]
        public void ShouldOverlapIfOtherWindowEndIsWithinThisWindow()
        {
            /*
             * This:        [-------]
             * Other: [---------]
             */
            Assert.True(window.Overlap(new SessionWindow(0, start)));
            Assert.True(window.Overlap(new SessionWindow(0, start + 1)));
            Assert.True(window.Overlap(new SessionWindow(0, 75)));
            Assert.True(window.Overlap(new SessionWindow(0, end - 1)));
            Assert.True(window.Overlap(new SessionWindow(0, end)));

            Assert.True(window.Overlap(new SessionWindow(start - 1, start)));
            Assert.True(window.Overlap(new SessionWindow(start - 1, start + 1)));
            Assert.True(window.Overlap(new SessionWindow(start - 1, 75)));
            Assert.True(window.Overlap(new SessionWindow(start - 1, end - 1)));
            Assert.True(window.Overlap(new SessionWindow(start - 1, end)));
        }

        [Fact]
        public void ShouldOverlapIfOtherWindowContainsThisWindow()
        {
            /*
             * This:        [-------]
             * Other: [------------------]
             */
            Assert.True(window.Overlap(new SessionWindow(0, end)));
            Assert.True(window.Overlap(new SessionWindow(0, end + 1)));
            Assert.True(window.Overlap(new SessionWindow(0, 150)));

            Assert.True(window.Overlap(new SessionWindow(start - 1, end)));
            Assert.True(window.Overlap(new SessionWindow(start - 1, end + 1)));
            Assert.True(window.Overlap(new SessionWindow(start - 1, 150)));

            Assert.True(window.Overlap(new SessionWindow(start, end)));
            Assert.True(window.Overlap(new SessionWindow(start, end + 1)));
            Assert.True(window.Overlap(new SessionWindow(start, 150)));
        }

        [Fact]
        public void ShouldOverlapIfOtherWindowIsWithinThisWindow()
        {
            /*
             * This:        [-------]
             * Other:         [---]
             */
            Assert.True(window.Overlap(new SessionWindow(start, start)));
            Assert.True(window.Overlap(new SessionWindow(start, 75)));
            Assert.True(window.Overlap(new SessionWindow(start, end)));
            Assert.True(window.Overlap(new SessionWindow(75, end)));
            Assert.True(window.Overlap(new SessionWindow(end, end)));
        }

        [Fact]
        public void ShouldOverlapIfOtherWindowStartIsWithinThisWindow()
        {
            /*
             * This:        [-------]
             * Other:           [-------]
             */
            Assert.True(window.Overlap(new SessionWindow(start, end + 1)));
            Assert.True(window.Overlap(new SessionWindow(start, 150)));
            Assert.True(window.Overlap(new SessionWindow(75, end + 1)));
            Assert.True(window.Overlap(new SessionWindow(75, 150)));
            Assert.True(window.Overlap(new SessionWindow(end, end + 1)));
            Assert.True(window.Overlap(new SessionWindow(end, 150)));
        }

        [Fact]
        public void ShouldNotOverlapIsOtherWindowIsAfterThisWindow()
        {
            /*
             * This:        [-------]
             * Other:                  [---]
             */
            Assert.False(window.Overlap(new SessionWindow(end + 1, end + 1)));
            Assert.False(window.Overlap(new SessionWindow(end + 1, 150)));
            Assert.False(window.Overlap(new SessionWindow(125, 150)));
        }

        [Fact]
        public void CannotCompareSessionWindowWithDifferentWindowType()
        {
            Assert.Throws<ArgumentException>(() => window.Overlap(timeWindow));
        }
    }
}
