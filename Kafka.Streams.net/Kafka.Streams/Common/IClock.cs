using System;

namespace Kafka.Common
{
    public interface IClock
    {
        DateTime UtcNow { get; }
        long NowAsEpochMilliseconds { get; }
        long NowAsEpochNanoseconds { get; }
    }
}
