
using System;

namespace Kafka.Streams.State.Internals
{
    internal class SimpleDateFormat
    {
        private readonly string v;

        public SimpleDateFormat(string v)
        {
            this.v = v;
        }

        internal void SetTimeZone(SimpleTimeZone simpleTimeZone)
        {
            throw new NotImplementedException();
        }

        internal object Parse(string segmentIdString)
        {
            throw new NotImplementedException();
        }
    }
}