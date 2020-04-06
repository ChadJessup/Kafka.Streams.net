using Confluent.Kafka;
using Kafka.Common;
using System;

namespace Kafka.Streams.Tests
{
    public class MockTime : IClock
    {
        private DateTime currentInstant;
        private readonly long autoTickMs;
        private readonly long timeMs;
        private readonly long highResTimeNs;

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

        //public MockTime(long startTimestampMs)
        //{
        //    this.timeMs = startTimestampMs;
        //    this.highResTimeNs = startTimestampMs * 1000L * 1000L;
        //}

        public DateTime UtcNow
            => Timestamp.UnixTimestampMsToDateTime(this.timeMs);

        public long NowAsEpochMilliseconds
            => this.timeMs;

        public long NowAsEpochNanoseconds
            => this.highResTimeNs;

        public void Sleep(long ms)
        {
            this.currentInstant += TimeSpan.FromMilliseconds(ms);
        }
    }
}