using System;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.Processor.Internals
{
    public class LogContext
    {
        private string logPrefix;

        public LogContext(string logPrefix)
        {
            this.logPrefix = logPrefix;
        }

        internal ILogger logger(Type type)
        {
            return null;
        }

        internal ILogger logger<T>()
        {
            throw new NotImplementedException();
        }
    }
}