
//using Kafka.Common.Utils;
//using Kafka.Streams.State.Interfaces;

//namespace Kafka.Streams.State.Internals
//{
//    public class RocksDbSessionStore
//        : WrappedStateStore<ISegmentedBytesStore, object, object>, ISessionStore<Bytes, byte[]>
//    {
//        public RocksDbSessionStore(ISegmentedBytesStore bytesStore)
//            : base(bytesStore)
//        {
//        }

//        public override IKeyValueIterator<Windowed<Bytes>, byte[]> findSessions(Bytes key,
//                                                                      long earliestSessionEndTime,
//                                                                      long latestSessionStartTime)
//        {
//            IKeyValueIterator<Bytes, byte[]> bytesIterator = wrapped.fetch(
//                key,
//                earliestSessionEndTime,
//                latestSessionStartTime
//            );
//            return new WrappedSessionStoreIterator(bytesIterator);
//        }

//        public override IKeyValueIterator<Windowed<Bytes>, byte[]> findSessions(Bytes keyFrom,
//                                                                      Bytes keyTo,
//                                                                      long earliestSessionEndTime,
//                                                                      long latestSessionStartTime)
//        {
//            IKeyValueIterator<Bytes, byte[]> bytesIterator = wrapped.fetch(
//                keyFrom,
//                keyTo,
//                earliestSessionEndTime,
//                latestSessionStartTime
//            );
//            return new WrappedSessionStoreIterator(bytesIterator);
//        }

//        public override byte[] fetchSession(Bytes key, long startTime, long endTime)
//        {
//            return wrapped[SessionKeySchema.toBinary(key, startTime, endTime)];
//        }

//        public override IKeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes key)
//        {
//            return findSessions(key, 0, long.MaxValue);
//        }

//        public override IKeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from, Bytes to)
//        {
//            return findSessions(from, to, 0, long.MaxValue);
//        }

//        public override void Remove(Windowed<Bytes> key)
//        {
//            wrapped.Remove(SessionKeySchema.toBinary(key));
//        }

//        public override void put(Windowed<Bytes> sessionKey, byte[] aggregate)
//        {
//            wrapped.Add(SessionKeySchema.toBinary(sessionKey), aggregate);
//        }
//    }
//}