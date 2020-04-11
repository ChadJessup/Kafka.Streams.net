using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.RocksDbState;
using Kafka.Streams.State.Interfaces;
using System;

namespace Kafka.Streams.State.TimeStamped
{
    public class TimestampedSegment : RocksDbTimestampedStore, IComparable<TimestampedSegment>, ISegment, IDisposable
    {
        public long Id { get; }

        public TimestampedSegment(
            string segmentName,
            string windowName,
            long id)
                : base(segmentName, windowName)
        {
            this.Id = id;
        }

        public void Destroy()
        {
            try
            {
                this.DbDir.Delete(recursive: true);
            }
            catch (Exception e)
            {

            }
        }

        public int CompareTo(TimestampedSegment segment)
        {
            return this.Id.CompareTo(segment.Id);
        }

        public override IDisposable OpenDB(IProcessorContext context)
        {
            var disposable = base.OpenDB(context);
            // skip the registering step
            this.InternalProcessorContext = context;

            return disposable;
        }

        public override string ToString()
        {
            return $"TimestampedSegment(id={this.Id}, Name={this.Name})";
        }

        public override bool Equals(object obj)
        {
            if (obj == null || this.GetType() != obj.GetType())
            {
                return false;
            }

            TimestampedSegment segment = (TimestampedSegment)obj;
            return this.Id == segment.Id;
        }

        public override int GetHashCode()
            => HashCode.Combine(this.Id);
    }
}
