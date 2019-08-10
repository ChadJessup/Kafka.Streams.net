using System;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.IProcessor.Internals
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
            throw new NotImplementedException();
        }

        internal ILogger logger<T>()
        {
            throw new NotImplementedException();
        }
    }
}