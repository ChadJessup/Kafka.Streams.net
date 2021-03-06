using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;
using System;

namespace Kafka.Streams.KStream.Internals.Suppress
{
    public class KTableSuppressProcessorSupplier<K, V> : IKTableProcessorSupplier<K, V, V>
    {
        private readonly ISuppressed<K> suppress;
        private readonly IKTable<K, V> parentKTable;
        private readonly string storeName;

        public KTableSuppressProcessorSupplier(
            ISuppressed<K> suppress,
            string storeName,
            IKTable<K, V> parentKTable)
        {
            this.suppress = suppress;
            this.storeName = storeName;
            this.parentKTable = parentKTable ?? throw new ArgumentNullException(nameof(parentKTable));

            // The suppress buffer requires seeing the old values, to support the prior value view.
            parentKTable.EnableSendingOldValues();
        }

        public IKeyValueProcessor<K, IChange<V>> Get()
        {
            return (IKeyValueProcessor<K, IChange<V>>)new KTableSuppressProcessor<K, IChange<V>>(
                this.suppress, this.storeName);
        }

        IKeyValueProcessor IProcessorSupplier.Get()
            => this.Get();

        public IKTableValueGetterSupplier<K, V> View()
        {
            return this.parentKTable.ValueGetterSupplier<V>();
        }

        //public IKTableValueGetter<K, V> get()
        //{
        //    //            IKTableValueGetter<K, V> parentGetter = parentValueGetterSupplier[];

        //    return null;
        //    //            return new KTableValueGetter<K, V>()
        //    //            {
        //    //                    private ITimeOrderedKeyValueBuffer<K, V> buffer;
        //    //        public void Init(IProcessorContext context)
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


        //    //        public void Close()
        //    //        {
        //    //            parentGetter.Close();
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
            this.parentKTable.EnableSendingOldValues();
        }
    }
}
