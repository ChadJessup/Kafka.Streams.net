
//using Kafka.Common.Utils;
//using System;

//namespace Kafka.Streams.State.Internals
//{
//    public class BufferKey : IComparable<BufferKey>
//    {
//        public long time { get; }
//        public Bytes key { get; }

//        public BufferKey(long time, Bytes key)
//        {
//            this.time = time;
//            this.key = key;
//        }

//        public override bool Equals(object o)
//        {
//            if (this == o)
//            {
//                return true;
//            }
//            if (o == null || GetType() != o.GetType())
//            {
//                return false;
//            }
//            BufferKey bufferKey = (BufferKey)o;
//            return time == bufferKey.time &&
//                key.Equals(bufferKey.key);
//        }

//        public override int GetHashCode()
//        {
//            return (time, key).GetHashCode();
//        }

//        public override int CompareTo(BufferKey o)
//        {
//            // ordering of keys within a time uses GetHashCode().
//            int timeComparison = time.CompareTo(o.time);
//            return timeComparison == 0 ? key.CompareTo(o.key) : timeComparison;
//        }

//        public override string ToString()
//        {
//            return "BufferKey{" +
//                "key=" + key +
//                ", time=" + time +
//                '}';
//        }
//    }
//}