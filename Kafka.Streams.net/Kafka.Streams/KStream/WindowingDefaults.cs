using System;

namespace Kafka.Streams.KStream
{
    public static class WindowingDefaults
    {
        public static TimeSpan DefaultRetention { get; } = TimeSpan.FromMilliseconds(24 * 60 * 60 * 1000L); // one day
    }
}
