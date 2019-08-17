using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.KStream
{
    public static class WindowingDefaults
    {
        public static TimeSpan DefaultRetention = TimeSpan.FromMilliseconds(24 * 60 * 60 * 1000L); // one day
    }
}
