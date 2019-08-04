using System;
using Kafka.Common.Utils.Interfaces;

namespace Kafka.Common.Utils
{
    /**
     * An interface abstracting the clock to use in unit testing classes that make use of clock time.
     *
     * Implementations of this class should be thread-safe.
     */

    public abstract class Time : ITime
    {
        public static SystemTime SYSTEM { get; } = new SystemTime();

        public long milliseconds()
        {
            throw new NotImplementedException();
        }

        public long nanoseconds()
        {
            throw new NotImplementedException();
        }

        public void sleep(long ms)
        {
            throw new NotImplementedException();
        }

        public void waitObject(object obj, Func<bool> condition, long timeoutMs)
        {
            throw new NotImplementedException();
        }
    }
}