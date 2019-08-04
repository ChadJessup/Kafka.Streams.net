using System;

namespace Kafka.Common.Metrics.Stats
{
    /**
     * A {@link SampledStat} that maintains a simple count of what it has seen.
     * This is a special kind of {@link WindowedSum} that always records a value of {@code 1} instead of the provided value.
     *
     * See also {@link CumulativeCount} for a non-sampled version of this metric.
     *
     * @deprecated since 2.4 . Use {@link WindowedCount} instead
     */
    [Obsolete]
    public class Count : WindowedCount
    {
    }
}