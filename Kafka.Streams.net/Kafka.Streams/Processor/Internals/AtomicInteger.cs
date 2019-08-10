using System;

namespace Kafka.Streams.IProcessor.Internals
{
    public class AtomicInteger
    {
        private int v;

        public AtomicInteger(int v)
        {
            this.v = v;
        }

        internal int getAndIncrement()
        {
            throw new NotImplementedException();
        }
    }
}