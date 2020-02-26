using System;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.Processors.Internals
{
    public class LogContext
    {
        private readonly string logPrefix;

        public LogContext(string logPrefix)
        {
            this.logPrefix = logPrefix;
        }

        public ILogger logger(Type type)
        {
            return null;
        }

        internal ILogger logger<T>()
        {
            throw new NotImplementedException();
        }
    }
}