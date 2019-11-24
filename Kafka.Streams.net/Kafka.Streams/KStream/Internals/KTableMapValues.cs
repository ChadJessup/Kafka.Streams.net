using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;
using Kafka.Streams.State;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableMapValues<K, V, V1> : IKTableProcessorSupplier<K, V, V1>
    {
        private readonly KTable<K, V1, V> parent;
        private readonly IValueMapperWithKey<K, V, V1> mapper;
        private readonly string queryableName;
        private bool sendOldValues = false;

        public KTableMapValues(
            KTable<K, V1, V> parent,
            IValueMapperWithKey<K, V, V1> mapper,
            string queryableName)
        {
            this.parent = parent;
            this.mapper = mapper;
            this.queryableName = queryableName;
        }


        public IKeyValueProcessor<K, Change<V>> get()
        {
            return new KTableMapValuesProcessor<K, V, Change<V>>(null);// this.mapper);
        }


        public IKTableValueGetterSupplier<K, V1> view()
        {
            // if the KTable is materialized, use the materialized store to return getter value;
            // otherwise rely on the parent getter and apply map-values on-the-fly
            if (queryableName != null)
            {
                return new KTableMaterializedValueGetterSupplier<K, V1>(queryableName);
            }
            else
            {

                //                return new KTableValueGetterSupplier<K, V1>()
                //{
                //                 KTableValueGetterSupplier<K, V> parentValueGetterSupplier = parent.valueGetterSupplier();

                //                public KTableValueGetter<K, V1> get()
                //                {
                //                    return new KTableMapValuesValueGetter(parentValueGetterSupplier());
                //                }


                //                public string[] storeNames()
                //                {
                //                    return parentValueGetterSupplier.storeNames();
            }
            //            };
            return null;
        }

        public void enableSendingOldValues()
        {
            parent.enableSendingOldValues();
            sendOldValues = true;
        }

        private V1 computeValue(K key, V value)
        {
            V1 newValue = default;

            if (value != null)
            {
                newValue = mapper.apply(key, value);
            }

            return newValue;
        }

        private ValueAndTimestamp<V> computeValueAndTimestamp(
            K key,
            ValueAndTimestamp<V> valueAndTimestamp)
        {
            V newValue = default;
            long timestamp = 0;

            if (valueAndTimestamp != null)
            {
                newValue = default;// mapper.apply(key, valueAndTimestamp.value);
                timestamp = valueAndTimestamp.timestamp;
            }

            return ValueAndTimestamp<V>.make(newValue, timestamp);
        }
    }
}
