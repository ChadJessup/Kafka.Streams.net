
//using Kafka.Streams.Processors;
//using Microsoft.Extensions.Logging;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableKTableOuterJoin<K, R, V1, V2> : KTableKTableAbstractJoin<K, R, V1, V2>
//    {
//        private static ILogger LOG = new LoggerFactory().CreateLogger<KTableKTableOuterJoin<K, R, V1, V2>>();

//        KTableKTableOuterJoin(KTable<K, object, V1> table1,
//                               KTable<K, object, V2> table2,
//                               IValueJoiner<V1, V2, R> joiner)
//            : base(table1, table2, joiner)
//        {
//        }


//        public IProcessor<K, Change<V1>> get()
//        {
//            return null; // new KTableKTableOuterJoinProcessor(valueGetterSupplier2());
//        }


//        public IKTableValueGetterSupplier<K, R> view()
//        {
//            return null; // new KTableKTableOuterJoinValueGetterSupplier(valueGetterSupplier1, valueGetterSupplier2);
//        }
//    }
//}
