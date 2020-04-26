
//namespace Kafka.Streams.State.Internals
//{
//    public class WrappedSessionStoreIterator : IKeyValueIterator<IWindowed<Bytes>, byte[]>
//    {

//        private IKeyValueIterator<Bytes, byte[]> bytesIterator;

//        WrappedSessionStoreIterator(IKeyValueIterator<Bytes, byte[]> bytesIterator)
//        {
//            this.bytesIterator = bytesIterator;
//        }

//        public override void Close()
//        {
//            bytesIterator.Close();
//        }

//        public override IWindowed<Bytes> PeekNextKey()
//        {
//            return SessionKeySchema.from(bytesIterator.PeekNextKey());
//        }

//        public override bool HasNext()
//        {
//            return bytesIterator.MoveNext();
//        }

//        public override KeyValuePair<IWindowed<Bytes>, byte[]> next()
//        {
//            KeyValuePair<Bytes, byte[]> next = bytesIterator.MoveNext();
//            return KeyValuePair.pair(SessionKeySchema.from(next.key), next.value);
//        }

//        public override void Remove()
//        {
//            throw new InvalidOperationException("Remove() is not supported in " + GetType().getName());
//        }
//    }
//}