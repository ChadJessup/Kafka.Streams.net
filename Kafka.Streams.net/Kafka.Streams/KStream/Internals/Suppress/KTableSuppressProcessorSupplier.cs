using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;
using System;

namespace Kafka.Streams.KStream.Internals.Suppress
{
    public class KTableSuppressProcessorSupplier<K, V, S> : IKTableProcessorSupplier<K, V, V>
    {
        private SuppressedInternal<K> suppress;
        private readonly string storeName;
        private readonly IKTable<K, V> parentKTable;

        public KTableSuppressProcessorSupplier(
            SuppressedInternal<K> suppress,
            string storeName,
            IKTable<K, V> parentKTable)
        {
            this.suppress = suppress;
            this.storeName = storeName;
            this.parentKTable = parentKTable ?? throw new ArgumentNullException(nameof(parentKTable));

            // The suppress buffer requires seeing the old values, to support the prior value view.
            parentKTable.EnableSendingOldValues();
        }

        public IKeyValueProcessor<K, Change<V>> Get()
        {
            return (IKeyValueProcessor<K, Change<V>>)new KTableSuppressProcessor<K, Change<V>>(
                suppress, storeName);
        }


        public IKTableValueGetterSupplier<K, V> View()
        {
            return parentKTable.ValueGetterSupplier<V>();
        }

        //public IKTableValueGetter<K, V> get()
        //{
        //    //            IKTableValueGetter<K, V> parentGetter = parentValueGetterSupplier[];

        //    return null;
        //    //            return new KTableValueGetter<K, V>()
        //    //            {
        //    //                    private ITimeOrderedKeyValueBuffer<K, V> buffer;
        //    //        public void init(IProcessorContext<K, V> context)
        //    //        {
        //    //            parentGetter.Init(context);
        //    //            // the main processor is responsible for the buffer's lifecycle
        //    //            buffer = requireNonNull((ITimeOrderedKeyValueBuffer<K, V>)context.getStateStore(storeName));
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


        public void EnableSendingOldValues()
        {
            parentKTable.EnableSendingOldValues();
        }
    }
}