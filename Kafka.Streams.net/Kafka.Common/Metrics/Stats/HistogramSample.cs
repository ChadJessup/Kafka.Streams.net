using Kafka.Common.Metrics.Stats.Interfaces;

namespace Kafka.Common.Metrics.Stats
{
    public HistogramSample : Sample
    {
        public Histogram histogram { get; }

        public HistogramSample(IBinScheme scheme, long now)
            : base(0.0, now)
        {
            this.histogram = new Histogram(scheme);
        }

        public void reset(long now)
        {
            base.reset(now);
            this.histogram.clear();
        }
    }
}
