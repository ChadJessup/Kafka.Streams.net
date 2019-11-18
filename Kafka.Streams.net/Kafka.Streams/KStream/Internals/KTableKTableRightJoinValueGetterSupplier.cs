

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableKTableRightJoin<K, R, V1, V2>
//    {
//        public class KTableKTableRightJoinValueGetterSupplier : KTableKTableAbstractJoinValueGetterSupplier<K, R, V1, V2>
//        {
//            public KTableKTableRightJoinValueGetterSupplier(
//                IKTableValueGetterSupplier<K, V1> valueGetterSupplier1,
//                IKTableValueGetterSupplier<K, V2> valueGetterSupplier2)
//                : base(valueGetterSupplier1, valueGetterSupplier2)
//            {
//            }

//            public IKTableValueGetter<K, R> get()
//            {
//                return new KTableKTableRightJoinValueGetter(valueGetterSupplier1(), valueGetterSupplier2());
//            }
//        }
//    }
//}