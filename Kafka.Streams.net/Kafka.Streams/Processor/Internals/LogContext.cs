namespace Kafka.Streams.Processor.Internals
{
    internal class LogContext
    {
        private string logPrefix;

        public LogContext(string logPrefix)
        {
            this.logPrefix = logPrefix;
        }
    }
}