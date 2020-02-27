using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;
using Kafka.Streams.State;
using System;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableFilter<K, S, V> : IKTableProcessorSupplier<K, V, V>
    {
        private readonly KTable<K, V> parent;
        private readonly IPredicate<K, V> predicate;
        private readonly bool filterNot;
        private readonly string queryableName;
        private bool sendOldValues = false;

        public KTableFilter(
            KTable<K, V> parent,
            IPredicate<K, V> predicate,
            bool filterNot,
            string queryableName)
        {
            this.parent = parent;
            this.predicate = predicate;
            this.filterNot = filterNot;
            this.queryableName = queryableName;
        }


        public IKeyValueProcessor<K, Change<V>> get()
        {
            return null; // new KTableFilterProcessor();
        }


        public void enableSendingOldValues()
        {
            parent.enableSendingOldValues();
            sendOldValues = true;
        }

        private V computeValue(K key, V value)
        {
            V newValue = default;

            if (value != null && (filterNot))// ^ predicate.test(key, value)))
            {
                newValue = value;
            }

            return newValue;
        }

        private ValueAndTimestamp<V> computeValue(K key, ValueAndTimestamp<V> valueAndTimestamp)
        {
            ValueAndTimestamp<V> newValueAndTimestamp = null;

            if (valueAndTimestamp != null)
            {
                V value = default;// valueAndTimestamp.value();
                if (filterNot)// ^ predicate.test(key, value))
                {
                    newValueAndTimestamp = valueAndTimestamp;
                }
            }

            return newValueAndTimestamp;
        }

        public IKTableValueGetterSupplier<K, V> view()
        {
            // if the KTable is materialized, use the materialized store to return getter value;
            // otherwise rely on the parent getter and apply filter on-the-fly
            if (queryableName != null)
            {
                return new KTableMaterializedValueGetterSupplier<K, V>(queryableName);
            }
            else
            {
                return null;// new KTableValueGetterSupplier<K, V>();
                //{
                //                 KTableValueGetterSupplier<K, V> parentValueGetterSupplier = parent.valueGetterSupplier();

                //            public KTableValueGetter<K, V> get()
                //            {
                //                return new KTableFilterValueGetter(parentValueGetterSupplier());
                //            }


                //            public string[] storeNames()
                //            {
                //                return parentValueGetterSupplier.storeNames();
                //            }
                //        };
            }
        }
    }
}