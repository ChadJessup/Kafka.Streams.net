

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableKTableLeftJoinValueGetterSupplier : KTableKTableAbstractJoinValueGetterSupplier<K, R, V1, V2>
//    {

//        KTableKTableLeftJoinValueGetterSupplier(IKTableValueGetterSupplier<K, V1> valueGetterSupplier1,
//                                                 IKTableValueGetterSupplier<K, V2> valueGetterSupplier2)
//            : base(valueGetterSupplier1, valueGetterSupplier2)
//        {
//        }

//        public IKTableValueGetter<K, R> get()
//        {
//            return new KTableKTableLeftJoinValueGetter(valueGetterSupplier1(), valueGetterSupplier2());
//        }
//    }
//}
