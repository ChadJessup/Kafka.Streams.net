using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    /**
    * KTable repartition map functions are not exposed to public APIs, but only used for keyed aggregations.
    * <p>
    * Given the input, it can output at most two records (one mapped from old value and one mapped from new value).
*/
    public class KTableRepartitionMap<K, V, K1, V1> : IKTableProcessorSupplier<K, V, KeyValuePair<K1, V1>>
    {
        private readonly IKTable<K, V> parent;
        private readonly KeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper;

        public KTableRepartitionMap(IKTable<K, V> parent, KeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper)
        {
            this.parent = parent;
            this.mapper = mapper;
        }

        public IKeyValueProcessor<K, IChange<V>> Get()
        {
            return new KTableMapProcessor(this.mapper);
        }

        public IKTableValueGetterSupplier<K, KeyValuePair<K1, V1>> View()
        {
            var parentValueGetterSupplier = this.parent.ValueGetterSupplier<KeyValuePair<K1, V1>>();

            return null; // new KTableValueGetterSupplier<K, KeyValuePair<K1, V1>>(this.parent);
            //{
            //
            //public KTableValueGetter<K, KeyValuePair<K1, V1>> get()
            //{
            //    return new KTableMapValueGetter(parentValueGetterSupplier.Get());
            //}
            //
            //    public string[] storeNames()
            //{
            //    throw new StreamsException("Underlying state store not accessible due to repartitioning.");
            //}
            //};
        }

        /**
         * @throws InvalidOperationException since this method should never be called
         */
        public void EnableSendingOldValues()
        {
            // this should never be called
            throw new InvalidOperationException("KTableRepartitionMap should always require sending old values.");
        }

        IKeyValueProcessor<K, IChange<V>> IProcessorSupplier<K, IChange<V>>.Get()
        {
            throw new NotImplementedException();
        }

        IKeyValueProcessor IProcessorSupplier.Get()
        {
            throw new NotImplementedException();
        }

        private class KTableMapProcessor : AbstractProcessor<K, IChange<V>>
        {
            private readonly KeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper;

            /**
            * @throws StreamsException if key is null
            */
            public KTableMapProcessor(KeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper)
                => this.mapper = mapper;

            public override void Process(K key, IChange<V> change)
            {
                // the original key should never be null
                if (key == null)
                {
                    throw new StreamsException("Record key for the grouping KTable should not be null.");
                }

                // if the value is null, we do not need to forward its selected key-value further
                KeyValuePair<K1, V1>? newPair = change.NewValue == null
                    ? (KeyValuePair<K1, V1>?)null
                    : this.mapper(key, change.NewValue);

                KeyValuePair<K1, V1>? oldPair = change.OldValue == null
                    ? (KeyValuePair<K1, V1>?)null
                    : this.mapper(key, change.OldValue);

                // if the selected repartition key or value is null, skip
                // forward oldPair first, to be consistent with reduce and aggregate
                if (oldPair != null && oldPair.Value.Key != null && oldPair.Value.Value != null)
                {
                    this.Context.Forward(oldPair.Value.Key, new Change<V1>(oldPair.Value.Value));
                }

                if (newPair != null && newPair.Value.Key != null && newPair.Value.Value != null)
                {
                    this.Context.Forward(newPair.Value.Key, new Change<V1>(newPair.Value.Value));
                }
            }
        }

        private class KTableMapValueGetter : IKTableValueGetter<K, KeyValuePair<K1, V1>>
        {
            private readonly KeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper;
            private readonly IKTableValueGetter<K, V> parentGetter;
            private IProcessorContext? context;

            public KTableMapValueGetter(
                KeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper,
                IKTableValueGetter<K, V> parentGetter)
            {
                this.mapper = mapper;
                this.parentGetter = parentGetter;
            }

            public void Init(IProcessorContext context, string? storeName)
            {
                this.context = context;
                this.parentGetter.Init(context, storeName);
            }

            public IValueAndTimestamp<KeyValuePair<K1, V1>> Get(K key)
            {
                IValueAndTimestamp<V>? valueAndTimestamp = this.parentGetter.Get(key);

                return ValueAndTimestamp.Make(
                    this.mapper(key, valueAndTimestamp.Value),
                    valueAndTimestamp == null
                    ? this.context.Timestamp
                    : valueAndTimestamp.Timestamp);
            }

            public void Close()
            {
                this.parentGetter.Close();
            }
        }
    }
}
