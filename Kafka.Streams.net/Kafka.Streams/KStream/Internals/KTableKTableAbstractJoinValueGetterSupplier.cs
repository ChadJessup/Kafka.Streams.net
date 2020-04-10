using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.KStream.Internals
{
    public abstract class KTableKTableAbstractJoinValueGetterSupplier<K, R, V1, V2> : IKTableValueGetterSupplier<K, R>
    {
        protected IKTableValueGetterSupplier<K, V1> valueGetterSupplier1 { get; }
        protected IKTableValueGetterSupplier<K, V2> valueGetterSupplier2 { get; }

        public KTableKTableAbstractJoinValueGetterSupplier(
            IKTableValueGetterSupplier<K, V1> valueGetterSupplier1,
            IKTableValueGetterSupplier<K, V2> valueGetterSupplier2)
        {
            this.valueGetterSupplier1 = valueGetterSupplier1;
            this.valueGetterSupplier2 = valueGetterSupplier2;
        }

        public abstract IKTableValueGetter<K, R> Get();

        public string[] StoreNames()
        {
            string[] storeNames1 = this.valueGetterSupplier1.StoreNames();
            string[] storeNames2 = this.valueGetterSupplier2.StoreNames();
            HashSet<string> stores = new HashSet<string>();

            stores.UnionWith(storeNames1);
            stores.UnionWith(storeNames2);

            return stores.ToArray();
        }
    }
}
