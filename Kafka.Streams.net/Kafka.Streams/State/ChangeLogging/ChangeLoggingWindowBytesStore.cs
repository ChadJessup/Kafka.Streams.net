
//    public class ChangeLoggingWindowBytesStore
//        : WrappedStateStore<IWindowStore<Bytes, byte[]>, byte[], byte[]>
//    , IWindowStore<Bytes, byte[]>
//    {
//        private bool retainDuplicates;
//        private IProcessorContext<Bytes, byte[]> context;
//        private int seqnum = 0;

//        StoreChangeLogger<Bytes, byte[]> changeLogger;

//        public ChangeLoggingWindowBytesStore(IWindowStore<Bytes, byte[]> bytesStore,
//                                      bool retainDuplicates)
//            : base(bytesStore)
//        {
//            this.retainDuplicates = retainDuplicates;
//        }

//        public override void init(IProcessorContext<Bytes, byte[]> context,
//                         IStateStore root)
//        {
//            this.context = context;
//            base.init(context, root);
//            string topic = ProcessorStateManager<Bytes, byte[]>.storeChangelogTopic(context.applicationId(), name);
//            changeLogger = new StoreChangeLogger<Bytes, byte[]>(
//                name,
//                context,
//                new StateSerdes<Bytes, byte[]>(topic, Serdes.Bytes(), Serdes.ByteArray()));
//        }

//        public override byte[] fetch(Bytes key,
//                            long timestamp)
//        {
//            return wrapped.fetch(key, timestamp);
//        }


//        public override IWindowStoreIterator<byte[]> fetch(Bytes key,
//                                                 long from,
//                                                 long to)
//        {
//            return wrapped.fetch(key, from, to);
//        }


//        public override IKeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes keyFrom,
//                                                               Bytes keyTo,
//                                                               long from,
//                                                               long to)
//        {
//            return wrapped.fetch(keyFrom, keyTo, from, to);
//        }

//        public override IKeyValueIterator<Windowed<Bytes>, byte[]> all()
//        {
//            return wrapped.all();
//        }


//        public override IKeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(long timeFrom,
//                                                                  long timeTo)
//        {
//            return wrapped.fetchAll(timeFrom, timeTo);
//        }

//        public override void put(Bytes key, byte[] value)
//        {
//            // Note: It's incorrect to bypass the wrapped store here by delegating to another method,
//            // but we have no alternative. We must send a timestamped key to the changelog, which means
//            // we need to know what timestamp gets used for the record. Hopefully, we can deprecate this
//            // method in the future to resolve the situation.
//            put(key, value, context.timestamp());
//        }

//        public override void put(Bytes key,
//                        byte[] value,
//                        long windowStartTimestamp)
//        {
//            wrapped.Add(key, value, windowStartTimestamp);
//            log(WindowKeySchema.toStoreKeyBinary(key, windowStartTimestamp, maybeUpdateSeqnumForDups()), value);
//        }

//        void log(Bytes key,
//                 byte[] value)
//        {
//            changeLogger.logChange(key, value);
//        }

//        private int maybeUpdateSeqnumForDups()
//        {
//            if (retainDuplicates)
//            {
//                seqnum = (seqnum + 1) & 0x7FFFFFFF;
//            }
//            return seqnum;
//        }
//    }
//}
