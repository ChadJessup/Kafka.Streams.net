using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;
using System;

namespace Kafka.Streams.KStream.Internals
{
    public abstract class KTableKTableAbstractJoin<K, R, V1, V2> : IKTableProcessorSupplier<K, V1, R>
    {
        private readonly IKTable<K, V1> table1;
        private readonly IKTable<K, V2> table2;
        protected IKTableValueGetterSupplier<K, V1> valueGetterSupplier1 { get; }
        protected IKTableValueGetterSupplier<K, V2> valueGetterSupplier2 { get; }
        protected ValueJoiner<V1, V2, R> joiner { get; }

        protected bool sendOldValues { get; set; } = false;

        public KTableKTableAbstractJoin(
            IKTable<K, V1> table1,
            IKTable<K, V2> table2,
            ValueJoiner<V1, V2, R> joiner)
        {
            this.table1 = table1 ?? throw new ArgumentNullException(nameof(table1));
            this.table2 = table2 ?? throw new ArgumentNullException(nameof(table2));
            this.joiner = joiner ?? throw new ArgumentNullException(nameof(joiner));

            this.valueGetterSupplier1 = table1.ValueGetterSupplier<V1>();
            this.valueGetterSupplier2 = table2.ValueGetterSupplier<V2>();
        }

        public void EnableSendingOldValues()
        {
            this.table1.EnableSendingOldValues();
            this.table2.EnableSendingOldValues();
            this.sendOldValues = true;
        }

        public abstract IKTableValueGetterSupplier<K, R> View();
        public abstract IKeyValueProcessor<K, IChange<V1>> Get();
        IKeyValueProcessor IProcessorSupplier.Get() => this.Get();
    }
}
