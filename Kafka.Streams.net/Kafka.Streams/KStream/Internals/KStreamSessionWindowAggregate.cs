//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.Processors;
//using Microsoft.Extensions.Logging;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KStreamSessionWindowAggregate<K, V, Agg> : KStreamAggIProcessorSupplier<K, IWindowed<K>, V, Agg>
//    {
//        private static ILogger LOG = new LoggerFactory().CreateLogger<KStreamSessionWindowAggregate>();

//        private string storeName;
//        private SessionWindows windows;
//        private IInitializer<Agg> initializer;
//        private IAggregator<K, V, Agg> aggregator;
//        private IMerger<K, Agg> sessionMerger;

//        private bool sendOldValues = false;

//        public KStreamSessionWindowAggregate(SessionWindows windows,
//                                              string storeName,
//                                              IInitializer<Agg> initializer,
//                                              IAggregator<K, V, Agg> aggregator,
//                                              IMerger<K, Agg> sessionMerger)
//        {
//            this.windows = windows;
//            this.storeName = storeName;
//            this.initializer = initializer;
//            this.aggregator = aggregator;
//            this.sessionMerger = sessionMerger;
//        }


//        public IProcessor<K, V> get()
//        {
//            return new KStreamSessionWindowAggregateProcessor();
//        }

//        public SessionWindows windows()
//        {
//            return windows;
//        }


//        public void EnableSendingOldValues()
//        {
//            sendOldValues = true;
//        }

//        private SessionWindow mergeSessionWindow(SessionWindow one, SessionWindow two)
//        {
//            long start = one.Start() < two.Start() ? one.Start() : two.Start();
//            long end = one.end() > two.end() ? one.end() : two.end();
//            return new SessionWindow(start, end);
//        }


//        public IKTableValueGetterSupplier<IWindowed<K>, Agg> view()
//        {
//            //    return new IKTableValueGetterSupplier<IWindowed<K>, Agg>()
//            //    {

//            //        public IKTableValueGetter<IWindowed<K>, Agg> get()
//            //    {
//            //        return new KTableSessionWindowValueGetter();
//            //    }


//            //    public string[] storeNames()
//            //    {
//            //        return new string[] { storeName };
//            //    }
//            //};
//        }
//    }
//}