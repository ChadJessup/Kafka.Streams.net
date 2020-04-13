using System;

namespace Kafka.Streams.Processors.Internals
{
    public class Stamped<V> : IComparable
    {
        public V value { get; }
        public DateTime timestamp { get; }

        public Stamped(V value, DateTime timestamp)
        {
            this.value = value;
            this.timestamp = timestamp;
        }

        public int CompareTo(object other)
        {
            var otherTimestamp = ((Stamped<object>)other).timestamp;

            if (this.timestamp < otherTimestamp)
            {
                return -1;
            }
            else if (this.timestamp > otherTimestamp)
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

            var otherTimestamp = ((Stamped<object>)other).timestamp;

            return this.timestamp == otherTimestamp;
        }

        public override int GetHashCode()
        {
            return (this.timestamp).GetHashCode();
        }
    }
}
