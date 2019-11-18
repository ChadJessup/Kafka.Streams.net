
//namespace Kafka.Streams.KStream.Internals.Suppress
//{
//    public class StrictBufferConfigImpl : BufferConfigInternal<IStrictBufferConfig>, IStrictBufferConfig
//    {
//        private long maxRecords;
//        private long maxBytes;
//        private BufferFullStrategy bufferFullStrategy;

//        public StrictBufferConfigImpl(
//            long maxRecords,
//            long maxBytes,
//            BufferFullStrategy bufferFullStrategy)
//        {
//            this.maxRecords = maxRecords;
//            this.maxBytes = maxBytes;
//            this.bufferFullStrategy = bufferFullStrategy;
//        }

//        public StrictBufferConfigImpl()
//        {
//            this.maxRecords = long.MaxValue;
//            this.maxBytes = long.MaxValue;
//            this.bufferFullStrategy = SHUT_DOWN;
//        }


//        public IStrictBufferConfig withMaxRecords(long recordLimit)
//        {
//            return new StrictBufferConfigImpl(recordLimit, maxBytes, bufferFullStrategy);
//        }


//        public IStrictBufferConfig withMaxBytes(long byteLimit)
//        {
//            return new StrictBufferConfigImpl(maxRecords, byteLimit, bufferFullStrategy);
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
//            StrictBufferConfigImpl that = (StrictBufferConfigImpl)o;
//            return maxRecords == that.maxRecords &&
//                maxBytes == that.maxBytes &&
//                bufferFullStrategy == that.bufferFullStrategy;
//        }


//        public override int GetHashCode()
//        {
//            return (maxRecords, maxBytes, bufferFullStrategy).GetHashCode();
//        }


//        public override string ToString()
//        {
//            return "StrictBufferConfigImpl{maxKeys=" + maxRecords +
//                ", maxBytes=" + maxBytes +
//                ", bufferFullStrategy=" + bufferFullStrategy + '}';
//        }
//    }
//}