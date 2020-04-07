
using System.IO;
using System.Text;

namespace Kafka.Streams.KStream.Internals
{
    internal class OutputStreamWriter : StringWriter
    {
        private readonly Stream outputStream;
        private readonly Encoding uTF8;

        public OutputStreamWriter(Stream outputStream, Encoding uTF8)
        {
            this.outputStream = outputStream;
            this.uTF8 = uTF8;
        }
    }
}
