
//namespace Kafka.Streams.State.Internals
//{
//    public class WrappedSessionStoreIterator : IKeyValueIterator<Windowed<Bytes>, byte[]>
//    {

//        private IKeyValueIterator<Bytes, byte[]> bytesIterator;

//        WrappedSessionStoreIterator(IKeyValueIterator<Bytes, byte[]> bytesIterator)
//        {
//            this.bytesIterator = bytesIterator;
//        }

//        public override void close()
//        {
//            bytesIterator.close();
//        }

//        public override Windowed<Bytes> peekNextKey()
//        {
//            return SessionKeySchema.from(bytesIterator.peekNextKey());
//        }

//        public override bool hasNext()
//        {
//            return bytesIterator.hasNext();
//        }

//        public override KeyValuePair<Windowed<Bytes>, byte[]> next()
//        {
//            KeyValuePair<Bytes, byte[]> next = bytesIterator.next();
//            return KeyValuePair.pair(SessionKeySchema.from(next.key), next.value);
//        }

//        public override void Remove()
//        {
//            throw new InvalidOperationException("Remove() is not supported in " + GetType().getName());
//        }
//    }
//}