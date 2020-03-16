using NodaTime;
using System.Threading;

namespace Kafka.Streams.Tests
{
    public class MockTime : IClock
    {
        private Instant currentInstant;
        private long autoTickMs;

        public MockTime()
            : this(0)
        {
        }

        public MockTime(long autoTickMs)
            : this(autoTickMs, SystemClock.Instance.GetCurrentInstant())
        {
        }

        public MockTime(long autoTickMs, Instant currentInstant)
        {
            this.currentInstant = currentInstant;
            this.autoTickMs = autoTickMs;
        }

        public Instant GetCurrentInstant()
        {
            return this.currentInstant;
        }

        public void Sleep(long ms)
        {
            this.currentInstant = this.currentInstant.Plus(Duration.FromMilliseconds(ms));
        }
    }
}