using Kafka.Common;
using System;

namespace Kafka.Streams.Kafka.Streams
{
    public class SystemClock : IClock
    {
        public static DateTime EpochStart { get; } = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        public static DateTime AsUtcNow => DateTime.UtcNow;
        public static long AsEpochMilliseconds
            => (long)(SystemClock.AsUtcNow - SystemClock.EpochStart).TotalMilliseconds;
        public static long AsEpochNanoseconds
            => (SystemClock.AsUtcNow - SystemClock.EpochStart).Ticks * 100;

        public DateTime UtcNow => SystemClock.AsUtcNow;
        public long NowAsEpochMilliseconds => SystemClock.AsEpochNanoseconds;
        public long NowAsEpochNanoseconds => SystemClock.AsEpochNanoseconds;
    }
}
