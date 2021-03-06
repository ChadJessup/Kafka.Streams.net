using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Internals;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{
    public class TimeWindowTest
    {
        private readonly long start = 50;
        private readonly long end = 100;
        private TimeWindow window => new TimeWindow(start, end);
        private SessionWindow sessionWindow => new SessionWindow(start, end);

        [Fact]
        public void endMustBeLargerThanStart()
        {
            new TimeWindow(start, start);
        }

        [Fact]
        public void shouldNotOverlapIfOtherWindowIsBeforeThisWindow()
        {
            /*
             * This:        [-------)
             * Other: [-----)
             */
            Assert.False(window.Overlap(new TimeWindow(0, 25)));
            Assert.False(window.Overlap(new TimeWindow(0, start - 1)));
            Assert.False(window.Overlap(new TimeWindow(0, start)));
        }

        [Fact]
        public void shouldOverlapIfOtherWindowEndIsWithinThisWindow()
        {
            /*
             * This:        [-------)
             * Other: [---------)
             */
            Assert.True(window.Overlap(new TimeWindow(0, start + 1)));
            Assert.True(window.Overlap(new TimeWindow(0, 75)));
            Assert.True(window.Overlap(new TimeWindow(0, end - 1)));

            Assert.True(window.Overlap(new TimeWindow(start - 1, start + 1)));
            Assert.True(window.Overlap(new TimeWindow(start - 1, 75)));
            Assert.True(window.Overlap(new TimeWindow(start - 1, end - 1)));
        }

        [Fact]
        public void shouldOverlapIfOtherWindowContainsThisWindow()
        {
            /*
             * This:        [-------)
             * Other: [------------------)
             */
            Assert.True(window.Overlap(new TimeWindow(0, end)));
            Assert.True(window.Overlap(new TimeWindow(0, end + 1)));
            Assert.True(window.Overlap(new TimeWindow(0, 150)));

            Assert.True(window.Overlap(new TimeWindow(start - 1, end)));
            Assert.True(window.Overlap(new TimeWindow(start - 1, end + 1)));
            Assert.True(window.Overlap(new TimeWindow(start - 1, 150)));

            Assert.True(window.Overlap(new TimeWindow(start, end)));
            Assert.True(window.Overlap(new TimeWindow(start, end + 1)));
            Assert.True(window.Overlap(new TimeWindow(start, 150)));
        }

        [Fact]
        public void shouldOverlapIfOtherWindowIsWithinThisWindow()
        {
            /*
             * This:        [-------)
             * Other:         [---)
             */
            Assert.True(window.Overlap(new TimeWindow(start, 75)));
            Assert.True(window.Overlap(new TimeWindow(start, end)));
            Assert.True(window.Overlap(new TimeWindow(75, end)));
        }

        [Fact]
        public void shouldOverlapIfOtherWindowStartIsWithinThisWindow()
        {
            /*
             * This:        [-------)
             * Other:           [-------)
             */
            Assert.True(window.Overlap(new TimeWindow(start, end + 1)));
            Assert.True(window.Overlap(new TimeWindow(start, 150)));
            Assert.True(window.Overlap(new TimeWindow(75, end + 1)));
            Assert.True(window.Overlap(new TimeWindow(75, 150)));
        }

        [Fact]
        public void shouldNotOverlapIsOtherWindowIsAfterThisWindow()
        {
            /*
             * This:        [-------)
             * Other:               [------)
             */
            Assert.False(window.Overlap(new TimeWindow(end, end + 1)));
            Assert.False(window.Overlap(new TimeWindow(end, 150)));
            Assert.False(window.Overlap(new TimeWindow(end + 1, 150)));
            Assert.False(window.Overlap(new TimeWindow(125, 150)));
        }

        [Fact]
        public void cannotCompareTimeWindowWithDifferentWindowType()
        {
            Assert.Throws<ArgumentException>(() => window.Overlap(sessionWindow));
        }

        [Fact]
        public void shouldReturnMatchedWindowsOrderedByTimestamp()
        {
            TimeWindows windows = TimeWindows
                .Of(TimeSpan.FromMilliseconds(12L))
                .AdvanceBy(TimeSpan.FromMilliseconds(5L));

            var matched = windows.WindowsFor(DateTime.MinValue.AddMilliseconds(21L));

            var expected = matched.Keys.ToArray();
            //Assert.Equal(expected[0].longValue(), 10L);
            //Assert.Equal(expected[1].longValue(), 15L);
            //Assert.Equal(expected[2].longValue(), 20L);
        }
    }
}
