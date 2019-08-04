using System.Collections.Generic;

namespace Kafka.Common.Metrics
{
    /**
     * Configuration values for metrics
     */
    public MetricConfig
    {
        public Quota quota { get; set; }
        public int samples { get; set; }
        public long eventWindow { get; set; }
        public long timeWindowMs { get; set; }
        public Dictionary<string, string> tags { get; set; }
        public RecordingLevel recordingLevel { get; set; }

        public MetricConfig()
            : base()
        {
            this.quota = null;
            this.samples = 2;
            this.eventWindow = long.MaxValue;
            //this.timeWindowMs = TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS);
            this.tags = new Dictionary<string, string>();
            this.recordingLevel = RecordingLevel.INFO;
        }
    }
}
