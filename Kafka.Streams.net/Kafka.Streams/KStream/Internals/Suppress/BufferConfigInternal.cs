
//namespace Kafka.Streams.KStream.Internals.Suppress
//{
//    public abstract class BufferConfigInternal<BC> : IBufferConfig<BC>
//        where BC : IBufferConfig<BC>
//    {
//        public abstract long maxRecords();

//        public abstract long maxBytes();


//        public abstract BufferFullStrategy bufferFullStrategy();


//        public IStrictBufferConfig withNoBound()
//        {
//            return new StrictBufferConfigImpl(
//                long.MaxValue,
//                long.MaxValue,
//                BufferFullStrategy.SHUT_DOWN // doesn't matter, given the bounds
//            );
//        }


//        public IStrictBufferConfig shutDownWhenFull()
//        {
//            return new StrictBufferConfigImpl(maxRecords(), maxBytes(), SHUT_DOWN);
//        }


//        public IEagerBufferConfig emitEarlyWhenFull()
//        {
//            return new EagerBufferConfigImpl(maxRecords(), maxBytes());
//        }
//    }
//}