
//namespace Kafka.Streams.KStream.Internals.Suppress
//{
//    public class EagerBufferConfigImpl : BufferConfigInternal<IEagerBufferConfig>, IEagerBufferConfig
//    {
//        public long maxRecords { get; }
//        public long maxBytes { get; }

//        public EagerBufferConfigImpl(long maxRecords, long maxBytes)
//        {
//            this.maxRecords = maxRecords;
//            this.maxBytes = maxBytes;
//        }

//        public IEagerBufferConfig withMaxRecords(long recordLimit)
//        {
//            return new EagerBufferConfigImpl(recordLimit, maxBytes);
//        }


//        public IEagerBufferConfig withMaxBytes(long byteLimit)
//        {
//            return new EagerBufferConfigImpl(maxRecords, byteLimit);
//        }

//        public override BufferFullStrategy bufferFullStrategy()
//        {
//            return BufferFullStrategy.EMIT;
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

//            EagerBufferConfigImpl that = (EagerBufferConfigImpl)o;
//            return maxRecords == that.maxRecords &&
//                maxBytes == that.maxBytes;
//        }


//        public override int GetHashCode()
//        {
//            return (maxRecords, maxBytes).GetHashCode();
//        }


//        public override string ToString()
//        {
//            return "EagerBufferConfigImpl{maxRecords=" + maxRecords + ", maxBytes=" + maxBytes + '}';
//        }
//    }
//}