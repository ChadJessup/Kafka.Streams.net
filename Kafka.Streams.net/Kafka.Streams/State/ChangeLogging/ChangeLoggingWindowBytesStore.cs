
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

//        public override void Init(IProcessorContext<Bytes, byte[]> context,
//                         IStateStore root)
//        {
//            this.context = context;
//            base.Init(context, root);
//            string topic = ProcessorStateManager<Bytes, byte[]>.storeChangelogTopic(context.applicationId(), Name);
//            changeLogger = new StoreChangeLogger<Bytes, byte[]>(
//                Name,
//                context,
//                new StateSerdes<Bytes, byte[]>(topic, Serdes.Bytes(), Serdes.ByteArray()));
//        }

//        public override byte[] Fetch(Bytes key,
//                            long timestamp)
//        {
//            return wrapped.Fetch(key, timestamp);
//        }


//        public override IWindowStoreIterator<byte[]> Fetch(Bytes key,
//                                                 long from,
//                                                 long to)
//        {
//            return wrapped.Fetch(key, from, to);
//        }


//        public override IKeyValueIterator<IWindowed<Bytes>, byte[]> Fetch(Bytes keyFrom,
//                                                               Bytes keyTo,
//                                                               long from,
//                                                               long to)
//        {
//            return wrapped.Fetch(keyFrom, keyTo, from, to);
//        }

//        public override IKeyValueIterator<IWindowed<Bytes>, byte[]> All()
//        {
//            return wrapped.All();
//        }


//        public override IKeyValueIterator<IWindowed<Bytes>, byte[]> FetchAll(long timeFrom,
//                                                                  long timeTo)
//        {
//            return wrapped.FetchAll(timeFrom, timeTo);
//        }

//        public override void Put(Bytes key, byte[] value)
//        {
//            // Note: It's incorrect to bypass the wrapped store here by delegating to another method,
//            // but we have no alternative. We must send a timestamped key to the changelog, which means
//            // we need to know what timestamp gets used for the record. Hopefully, we can deprecate this
//            // method in the future to resolve the situation.
//            Put(key, value, context.timestamp());
//        }

//        public override void Put(Bytes key,
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
