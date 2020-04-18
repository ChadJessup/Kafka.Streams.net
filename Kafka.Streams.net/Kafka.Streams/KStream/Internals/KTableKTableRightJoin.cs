
//using Kafka.Streams.Processors;
//using Microsoft.Extensions.Logging;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableKTableRightJoin<K, R, V1, V2> : KTableKTableAbstractJoin<K, R, V1, V2>
//    {
//        private static ILogger LOG = new LoggerFactory().CreateLogger<KTableKTableRightJoin<K, R, V1, V2>>();

//        public KTableKTableRightJoin(
//            KTable<K, object, V1> table1,
//            KTable<K, object, V2> table2,
//            ValueJoiner<V1, V2, R> joiner)
//            : base(table1, table2, joiner)
//        {
//        }

//        public IProcessor<K, Change<V1>> get()
//        {
//            return new KTableKTableRightJoinProcessor(valueGetterSupplier2());
//        }

//        public IKTableValueGetterSupplier<K, R> view()
//        {
//            return new KTableKTableRightJoinValueGetterSupplier(valueGetterSupplier1, valueGetterSupplier2);
//        }
//    }
//}