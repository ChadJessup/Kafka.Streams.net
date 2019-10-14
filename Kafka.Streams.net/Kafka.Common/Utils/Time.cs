using System;
using Kafka.Common.Utils.Interfaces;

namespace Kafka.Common.Utils
{
    /**
     * An interface abstracting the clock to use in unit testinges that make use of clock time.
     *
     * Implementations of this should be thread-safe.
     */

    public abstract class Time : ITime
    {
        public static SystemTime SYSTEM { get; } = new SystemTime();
        public abstract long milliseconds();
        public abstract long nanoseconds();
        public abstract void sleep(long ms);
        public abstract void waitObject(object obj, Func<bool> condition, long timeoutMs);
    }
}