
//namespace Kafka.Streams.State.Internals
//{


//    using Kafka.Common.Utils.Utils;
//    using Kafka.Streams.Processors.IProcessorContext;




//    class KeyValueSegment : RocksDbStore : Comparable<KeyValueSegment>, Segment
//    {
//        public long id;

//        KeyValueSegment(string segmentName,
//                        string windowName,
//                        long id)
//        {
//            base(segmentName, windowName);
//            this.id = id;
//        }

//        public override void destroy()
//        {
//            Utils.delete(dbDir);
//        }

//        public override int CompareTo(KeyValueSegment segment)
//        {
//            return long.compare(id, segment.id);
//        }

//        public override void openDB(IProcessorContext<K, V> context)
//        {
//            base.openDB(context);
//            // skip the registering step
//            internalProcessorContext = context;
//        }

//        public override string ToString()
//        {
//            return "KeyValueSegment(id=" + id + ", name=" + name + ")";
//        }

//        public override bool Equals(object obj)
//        {
//            if (obj == null || GetType() != obj.GetType())
//            {
//                return false;
//            }
//            KeyValueSegment segment = (KeyValueSegment)obj;
//            return id == segment.id;
//        }

//        public override int GetHashCode()
//        {
//            return Objects.hash(id);
//        }
//    }
//}