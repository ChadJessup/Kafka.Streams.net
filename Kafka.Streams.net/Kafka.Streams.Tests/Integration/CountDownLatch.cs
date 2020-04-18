using Kafka.Common;
using System;

namespace Kafka.Streams.Tests.Integration
{
    internal class CountDownLatch
    {
        private int v;

        public CountDownLatch(int v)
        {
            this.v = v;
        }

        internal void countDown()
        {
            throw new NotImplementedException();
        }

        internal bool wait(int v, TimeUnit sECONDS)
        {
            throw new NotImplementedException();
        }
    }
}