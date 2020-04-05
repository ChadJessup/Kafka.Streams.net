
//    public class ChangeLoggingSessionBytesStore
//        : WrappedStateStore<ISessionStore<Bytes, byte[]>, byte[], byte[]>,
//        ISessionStore<Bytes, byte[]>
//    {
//        private StoreChangeLogger<Bytes, byte[]> changeLogger;

//        public ChangeLoggingSessionBytesStore(ISessionStore<Bytes, byte[]> bytesStore)
//            : base(bytesStore)
//        {
//        }

//        public override void init(IProcessorContext<Bytes, byte[]> context, IStateStore root)
//        {
//            base.Init(context, root);
//            string topic = ProcessorStateManager.storeChangelogTopic(
//                    context.applicationId(),
//                    name);

//            changeLogger = new StoreChangeLogger<>(
//                    name,
//                    context,
//                    new StateSerdes<>(topic, Serdes.Bytes(), Serdes.ByteArray()));
//        }


//        public override IKeyValueIterator<Windowed<Bytes>, byte[]> findSessions(Bytes key, long earliestSessionEndTime, long latestSessionStartTime)
//        {
//            return wrapped.findSessions(key, earliestSessionEndTime, latestSessionStartTime);
//        }

//        public override IKeyValueIterator<Windowed<Bytes>, byte[]> findSessions(Bytes keyFrom, Bytes keyTo, long earliestSessionEndTime, long latestSessionStartTime)
//        {
//            return wrapped.findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
//        }

//        public override void Remove(Windowed<Bytes> sessionKey)
//        {
//            wrapped.Remove(sessionKey);
//            changeLogger.logChange(SessionKeySchema.toBinary(sessionKey), null);
//        }

//        public override void put(Windowed<Bytes> sessionKey, byte[] aggregate)
//        {
//            wrapped.Add(sessionKey, aggregate);
//            changeLogger.logChange(SessionKeySchema.toBinary(sessionKey), aggregate);

//        }

//        public override byte[] fetchSession(Bytes key, long startTime, long endTime)
//        {
//            return wrapped.FetchSession(key, startTime, endTime);
//        }

//        public override IKeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes key)
//        {
//            return wrapped.Fetch(key);
//        }

//        public override IKeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from, Bytes to)
//        {
//            return wrapped.Fetch(from, to);
//        }
//    }
//}