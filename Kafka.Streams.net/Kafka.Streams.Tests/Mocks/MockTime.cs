using Confluent.Kafka;
using Kafka.Common;
using System;

namespace Kafka.Streams.Tests
{
    public class MockTime : IClock
    {
        private DateTime currentInstant;
        private readonly long autoTickMs;

        public MockTime()
            : this(0)
        {
        }

        public MockTime(long autoTickMs)
            : this(autoTickMs, DateTime.UtcNow)
        {
        }

        public MockTime(long autoTickMs, DateTime currentInstant)
        {
            this.currentInstant = currentInstant;
            this.autoTickMs = autoTickMs;
        }

        public DateTime UtcNow
            => this.currentInstant.ToUniversalTime();

        public long NowAsEpochMilliseconds
            => this.currentInstant.ToEpochMilliseconds();

        public long NowAsEpochNanoseconds
            => this.currentInstant.ToEpochMilliseconds() * 1000000;

        public void Sleep(long ms)
        {
            this.currentInstant += TimeSpan.FromMilliseconds(ms);
        }
    }
}