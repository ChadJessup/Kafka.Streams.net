using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Threads.Stream
{
    public enum ProcessingMode
    {
        AT_LEAST_ONCE, //("AT_LEAST_ONCE"),

        EXACTLY_ONCE_ALPHA, //("EXACTLY_ONCE_ALPHA"),

        EXACTLY_ONCE_BETA, //("EXACTLY_ONCE_BETA");
    }
}
