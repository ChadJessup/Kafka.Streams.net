using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.RocksDbState;
using System;

namespace Kafka.Streams.State.TimeStamped
{
    public class TimestampedSegment : RocksDbTimestampedStore, IComparable<TimestampedSegment>, ISegment
    {
        public long id;

        public TimestampedSegment(
            string segmentName,
            string windowName,
            long id)
                : base(segmentName, windowName)
        {
            this.id = id;
        }

        public void Destroy()
        {
            //Utils.delete(this.DbDir);
        }

        public int CompareTo(TimestampedSegment segment)
        {
            return id.CompareTo(segment.id);
        }

        public override void OpenDB(IProcessorContext context)
        {
            base.OpenDB(context);
            // skip the registering step
            //internalProcessorContext = context;
        }

        public override string ToString()
        {
            return "TimestampedSegment(id=" + id + ", name=" + Name + ")";
        }

        public override bool Equals(object obj)
        {
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }
            TimestampedSegment segment = (TimestampedSegment)obj;
            return id == segment.id;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(id);
        }
    }
}
