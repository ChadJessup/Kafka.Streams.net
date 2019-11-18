

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableKTableInnerJoinValueGetterSupplier : KTableKTableAbstractJoinValueGetterSupplier<K, R, V1, V2>
//    {

//        KTableKTableInnerJoinValueGetterSupplier(IKTableValueGetterSupplier<K, V1> valueGetterSupplier1,
//                                                  IKTableValueGetterSupplier<K, V2> valueGetterSupplier2)
//        {
//            base(valueGetterSupplier1, valueGetterSupplier2);
//        }

//        public IKTableValueGetter<K, R> get()
//        {
//            return new KTableKTableInnerJoinValueGetter(valueGetterSupplier1(), valueGetterSupplier2());
//        }
//    }
//}
