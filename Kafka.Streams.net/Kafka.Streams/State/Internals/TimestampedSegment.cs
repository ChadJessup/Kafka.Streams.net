
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.State.Interfaces;
//using System;

//namespace Kafka.Streams.State.Internals
//{
//    public class TimestampedSegment : RocksDbTimestampedStore, IComparable<TimestampedSegment>, ISegment
//    {
//        public long id;

//        TimestampedSegment(string segmentName,
//                           string windowName,
//                           long id)
//            : base(segmentName, windowName)
//        {
//            this.id = id;
//        }

//        public override void destroy()
//        {
//            Utils.delete(dbDir);
//        }

//        public override int CompareTo(TimestampedSegment segment)
//        {
//            return id.CompareTo(segment.id);
//        }

//        public override void openDB(IProcessorContext<K, V> context)
//        {
//            base.OpenDb(context);
//            // skip the registering step
//            internalProcessorContext = context;
//        }

//        public override string ToString()
//        {
//            return "TimestampedSegment(id=" + id + ", name=" + name + ")";
//        }

//        public override bool Equals(object obj)
//        {
//            if (obj == null || GetType() != obj.GetType())
//            {
//                return false;
//            }
//            TimestampedSegment segment = (TimestampedSegment)obj;
//            return id == segment.id;
//        }

//        public override int GetHashCode()
//        {
//            return Objects.hash(id);
//        }
//    }
//}