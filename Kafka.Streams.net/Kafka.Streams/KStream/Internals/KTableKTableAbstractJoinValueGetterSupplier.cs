
//using System.Collections.Generic;
//using System.Linq;

//namespace Kafka.Streams.KStream.Internals
//{
//    public abstract class KTableKTableAbstractJoinValueGetterSupplier<K, R, V1, V2> : IKTableValueGetterSupplier<K, R>
//    {
//        IKTableValueGetterSupplier<K, V1> valueGetterSupplier1;
//        IKTableValueGetterSupplier<K, V2> valueGetterSupplier2;

//        public KTableKTableAbstractJoinValueGetterSupplier(
//            IKTableValueGetterSupplier<K, V1> valueGetterSupplier1,
//            IKTableValueGetterSupplier<K, V2> valueGetterSupplier2)
//        {
//            this.valueGetterSupplier1 = valueGetterSupplier1;
//            this.valueGetterSupplier2 = valueGetterSupplier2;
//        }

//        public string[] storeNames()
//        {
//            string[] storeNames1 = valueGetterSupplier1.storeNames();
//            string[] storeNames2 = valueGetterSupplier2.storeNames();
//            HashSet<string> stores = new HashSet<string>();

//            stores.UnionWith(storeNames1);
//            stores.UnionWith(storeNames2);

//            return stores.ToArray();
//        }
//    }
//}
