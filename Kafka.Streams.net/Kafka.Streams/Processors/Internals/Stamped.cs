using System;

namespace Kafka.Streams.Processor.Internals
{
    public class Stamped<V> : IComparable
    {
        public V value;
        public long timestamp;

        public Stamped(V value, long timestamp)
        {
            this.value = value;
            this.timestamp = timestamp;
        }

        public int CompareTo(object other)
        {
            long otherTimestamp = ((Stamped<object>)other).timestamp;

            if (timestamp < otherTimestamp)
            {
                return -1;
            }
            else if (timestamp > otherTimestamp)
            {
                return 1;
            }
            return 0;
        }

        public override bool Equals(object other)
        {
            if (other == null || this.GetType() != other.GetType())
            {
                return false;
            }

            long otherTimestamp = ((Stamped<object>)other).timestamp;

            return timestamp == otherTimestamp;
        }

        public override int GetHashCode()
        {
            return (timestamp).GetHashCode();
        }
    }
}