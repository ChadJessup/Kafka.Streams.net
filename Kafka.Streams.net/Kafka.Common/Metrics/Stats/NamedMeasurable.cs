
namespace Kafka.Common.Metrics.Stats
{
    public class NamedMeasurable
    {
        public NamedMeasurable(MetricName name, IMeasurable stat)
            : base()
        {
            this.name = name;
            this.stat = stat;
        }

        public MetricName name { get; }
        public IMeasurable stat { get; }
    }
}
