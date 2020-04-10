using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.RocksDbState;
using Kafka.Streams.State.Interfaces;
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
            return this.id.CompareTo(segment.id);
        }

        public override void OpenDB(IProcessorContext context)
        {
            base.OpenDB(context);
            // skip the registering step
            //internalProcessorContext = context;
        }

        public override string ToString()
        {
            return "TimestampedSegment(id=" + this.id + ", Name=" + this.Name + ")";
        }

        public override bool Equals(object obj)
        {
            if (obj == null || this.GetType() != obj.GetType())
            {
                return false;
            }
            TimestampedSegment segment = (TimestampedSegment)obj;
            return this.id == segment.id;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(this.id);
        }
    }
}
