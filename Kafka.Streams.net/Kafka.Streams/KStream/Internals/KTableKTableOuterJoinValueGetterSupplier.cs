
//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableKTableOuterJoinValueGetterSupplier : KTableKTableAbstractJoinValueGetterSupplier<K, R, V1, V2>
//    {
//        KTableKTableOuterJoinValueGetterSupplier(IKTableValueGetterSupplier<K, V1> valueGetterSupplier1,
//                                                  IKTableValueGetterSupplier<K, V2> valueGetterSupplier2)
//            : base(valueGetterSupplier1, valueGetterSupplier2)
//        {
//        }

//        public IKTableValueGetter<K, R> get()
//        {
//            return new KTableKTableOuterJoinValueGetter(valueGetterSupplier1(), valueGetterSupplier2());
//        }
//    }
//}
