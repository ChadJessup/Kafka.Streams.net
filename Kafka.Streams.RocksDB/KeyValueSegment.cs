using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.RocksDbState;
using Kafka.Streams.State.Interfaces;
using System;

namespace Kafka.Streams.State.KeyValues
{
    public class KeyValueSegment : RocksDbStore, IComparable<KeyValueSegment>, ISegment
    {
        public long id;

        public KeyValueSegment(
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

        public int CompareTo(KeyValueSegment segment)
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
            return "KeyValueSegment(id=" + this.id + ", Name=" + this.Name + ")";
        }

        public override bool Equals(object obj)
        {
            if (obj == null || this.GetType() != obj.GetType())
            {
                return false;
            }
            KeyValueSegment segment = (KeyValueSegment)obj;
            return this.id == segment.id;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(this.id);
        }
    }
}
