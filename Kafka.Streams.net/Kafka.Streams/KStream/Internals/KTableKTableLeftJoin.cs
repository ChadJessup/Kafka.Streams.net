
//using Microsoft.Extensions.Logging;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableKTableLeftJoin<K, R, V1, V2> : KTableKTableAbstractJoin<K, R, V1, V2>
//    {
//        private static ILogger LOG = new LoggerFactory().CreateLogger<KTableKTableLeftJoin>();

//        public KTableKTableLeftJoin(
//            KTable<K, object, V1> table1,
//            KTable<K, object, V2> table2,
//            IValueJoiner<V1, V2, R> joiner)
//            : base(table1, table2, joiner)
//        {
//        }


//        public IProcessor<K, Change<V1>> get()
//        {
//            return new KTableKTableLeftJoinProcessor(valueGetterSupplier2());
//        }


//        public IKTableValueGetterSupplier<K, R> view()
//        {
//            return new KTableKTableLeftJoinValueGetterSupplier(valueGetterSupplier1, valueGetterSupplier2);
//        }
//    }
//}