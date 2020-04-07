
using System.IO;

namespace Kafka.Streams.Processors.Internals
{
    internal class PrintWriter
    {
        private readonly StringWriter stringWriter;

        public PrintWriter(StringWriter stringWriter)
        {
            this.stringWriter = stringWriter;
        }
    }
}
