using Kafka.Streams.KStream;
using Xunit;

namespace Kafka.Streams.Tests.Kstream
{
    public class WindowTest
    {

        internal class TestWindow : Window
        {
            public TestWindow(long startMs, long endMs)
                : base(startMs, endMs)
            {
            }


            public bool overlap(Window other)
            {
                return false;
            }

            public override bool Overlap(Window other)
            {
                throw new System.NotImplementedException();
            }
        }

        internal class TestWindow2 : Window
        {
            public TestWindow2(long startMs, long endMs)
                : base(startMs, endMs)
            {
            }


            public bool overlap(Window other)
            {
                return false;
            }

            public override bool Overlap(Window other)
            {
                throw new System.NotImplementedException();
            }
        }

        private readonly TestWindow window = new TestWindow(5, 10);

        [Fact]
        public void shouldThrowIfStartIsNegative()
        {
            new TestWindow(-1, 0);
        }

        [Fact]
        public void shouldThrowIfEndIsSmallerThanStart()
        {
            new TestWindow(1, 0);
        }

        [Fact]
        public void shouldBeEqualIfStartAndEndSame()
        {
            var window2 = new TestWindow(window.Start(), window.End());

            Assert.Equal(window, window);
            Assert.Equal(window, window2);
            Assert.Equal(window2, window);
        }

        [Fact]
        public void shouldNotBeEqualIfNull()
        {
            Assert.NotEqual(null, window);
        }

        [Fact]
        public void shouldNotBeEqualIfStartOrEndIsDifferent()
        {
            Assert.NotEqual(window, new TestWindow(0, window.End()));
            Assert.NotEqual(window, new TestWindow(7, window.End()));
            Assert.NotEqual(window, new TestWindow(window.Start(), 7));
            Assert.NotEqual(window, new TestWindow(window.Start(), 15));
            Assert.NotEqual(window, new TestWindow(7, 8));
            Assert.NotEqual(window, new TestWindow(0, 15));
        }

        //[Fact]
        //public void shouldNotBeEqualIfDifferentWindowType()
        //{
        //    Assert.NotEqual<TestWindow>(window, new TestWindow2(window.Start(), window.End()));
        //}
    }
}
