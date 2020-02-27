using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals.Suppress
{
    public class KTableSuppressProcessorSupplier<K, V, S> : IKTableProcessorSupplier<K, V, V>
    {
        //private SuppressedInternal<K, V> suppress;
        private readonly string storeName;
        private readonly KTable<K, V> parentKTable;

        public KTableSuppressProcessorSupplier(
//            SuppressedInternal<K, V> suppress,
            string storeName,
            KTable<K, V> parentKTable)
        {
  //          this.suppress = suppress;
            this.storeName = storeName;
            this.parentKTable = parentKTable;
            // The suppress buffer requires seeing the old values, to support the prior value view.
            parentKTable.enableSendingOldValues();
        }

        public IKeyValueProcessor<K, Change<V>> get()
        {
            return null;
            //return new KTableSuppressProcessor<K, Change<V>>(
            //    suppress, storeName);
        }


        public IKTableValueGetterSupplier<K, V> view()
        {
            //IKTableValueGetterSupplier<K, V> parentValueGetterSupplier = parentKTable.valueGetterSupplier();
            return null;// parentValueGetterSupplier;
        }

        //public IKTableValueGetter<K, V> get()
        //{
        //    //            IKTableValueGetter<K, V> parentGetter = parentValueGetterSupplier[];

        //    return null;
        //    //            return new KTableValueGetter<K, V>()
        //    //            {
        //    //                    private TimeOrderedKeyValueBuffer<K, V> buffer;
        //    //        public void init(IProcessorContext<K, V> context)
        //    //        {
        //    //            parentGetter.init(context);
        //    //            // the main processor is responsible for the buffer's lifecycle
        //    //            buffer = requireNonNull((TimeOrderedKeyValueBuffer<K, V>)context.getStateStore(storeName));
        //    //        }


        //    //        public ValueAndTimestamp<V> get(K key)
        //    //        {
        //    //            Maybe<ValueAndTimestamp<V>> maybeValue = buffer.priorValueForBuffered(key);
        //    //            if (maybeValue.isDefined())
        //    //            {
        //    //                return maybeValue.getNullableValue();
        //    //            }
        //    //            else
        //    //            {

        //    //                // not buffered, so the suppressed view is equal to the parent view
        //    //                return parentGetter[key];
        //    //            }
        //    //        }


        //    //        public void close()
        //    //        {
        //    //            parentGetter.close();
        //    //            // the main processor is responsible for the buffer's lifecycle
        //    //        }
        //    //    };
        //    //}


        //    //public string[] storeNames()
        //    //{
        //    //    string[] parentStores = parentValueGetterSupplier.storeNames();
        //    //    string[] stores = new string[1 + parentStores.Length];
        //    //    System.arraycopy(parentStores, 0, stores, 1, parentStores.Length);
        //    //    stores[0] = storeName;
        //    //    return stores;
        //    //}
        //    //        };
        //}


        public void enableSendingOldValues()
        {
            parentKTable.enableSendingOldValues();
        }
    }
}