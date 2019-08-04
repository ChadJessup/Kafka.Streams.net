﻿namespace Kafka.Common.Metrics.Stats
{
    public class Sample
    {
        public double initialValue;
        public long eventCount;
        public long lastWindowMs;
        public double value;

        public Sample(double initialValue, long now)
        {
            this.initialValue = initialValue;
            this.eventCount = 0;
            this.lastWindowMs = now;
            this.value = initialValue;
        }

        public void reset(long now)
        {
            this.eventCount = 0;
            this.lastWindowMs = now;
            this.value = initialValue;
        }

        public bool isComplete(long timeMs, MetricConfig config)
        {
            return timeMs - lastWindowMs >= config.timeWindowMs || eventCount >= config.eventWindow;
        }
    }
}