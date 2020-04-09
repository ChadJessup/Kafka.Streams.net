using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableFilter<K, V> : IKTableProcessorSupplier<K, V, V>
    {
        private readonly KafkaStreamsContext context;
        private readonly IKTable<K, V> parent;
        private readonly Func<K, V, bool> predicate;
        private readonly bool filterNot;
        private readonly string? queryableName;
        private bool sendOldValues = false;

        public KTableFilter(
            KafkaStreamsContext context,
            IKTable<K, V> parent,
            Func<K, V, bool> predicate,
            bool filterNot,
            string? queryableName)
        {
            this.context = context;
            this.parent = parent;
            this.predicate = predicate;
            this.filterNot = filterNot;
            this.queryableName = queryableName;
        }

        public IKeyValueProcessor<K, IChange<V>> Get()
        {
            return new KTableFilterProcessor<K, V>(
                this.context,
                this.queryableName,
                this.sendOldValues,
                this.filterNot,
                this.predicate);
        }

        IKeyValueProcessor IProcessorSupplier.Get()
            => this.Get();

        public void EnableSendingOldValues()
        {
            parent.EnableSendingOldValues();
            sendOldValues = true;
        }

        public IKTableValueGetterSupplier<K, V> View()
        {
            // if the KTable is materialized, use the materialized store to return getter value;
            // otherwise rely on the parent getter and apply filter on-the-fly
            if (queryableName != null)
            {
                return new KTableMaterializedValueGetterSupplier<K, V>(
                    this.context,
                    queryableName);
            }
            else
            {
                return new KTableValueGetterSupplier<K, V>(this.parent);
            }
        }
    }
}
