namespace Kafka.Streams.Metrics
{
    internal class JmxReporter
    {
        private readonly string jMX_PREFIX;

        public JmxReporter(string jMX_PREFIX)
        {
            this.jMX_PREFIX = jMX_PREFIX;
        }
    }
}