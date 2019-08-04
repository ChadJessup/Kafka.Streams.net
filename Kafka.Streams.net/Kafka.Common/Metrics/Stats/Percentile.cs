namespace Kafka.Common.Metrics.Stats
{
    public Percentile
    {
        public MetricName name { get; set; }
        public double percentile { get; set; }

        public Percentile(MetricName name, double percentile)
            : base()
        {
            this.name = name;
            this.percentile = percentile;
        }
    }
}