using Kafka.Common.Metrics.Stats;
using System.Collections.Generic;

namespace Kafka.Common.Metrics
{
    /**
     * A compound stat is a stat where a single measurement and associated data structure feeds many metrics. This is the
     * example for a histogram which has many associated percentiles.
     */
    public interface ICompoundStat : IStat
    {
        List<NamedMeasurable> stats();
    }
}