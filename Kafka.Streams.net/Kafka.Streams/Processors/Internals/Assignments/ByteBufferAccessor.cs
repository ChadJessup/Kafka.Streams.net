using Kafka.Streams.KStream.Internals;

namespace Kafka.Streams.Processors.Internals.Assignments
{
    internal class ByteBufferAccessor
    {
        private ByteBuffer data;

        public ByteBufferAccessor(ByteBuffer data)
        {
            this.data = data;
        }
    }
}