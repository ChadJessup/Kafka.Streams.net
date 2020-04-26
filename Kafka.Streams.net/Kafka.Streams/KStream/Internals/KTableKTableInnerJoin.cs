using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;
using Kafka.Streams.State;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableKTableInnerJoin<K, R, V1, V2> : KTableKTableAbstractJoin<K, R, V1, V2>
    {
        private readonly string storeName;
        private readonly KeyValueMapper<K, V1, K> keyValueMapper = new KeyValueMapper<K, V1, K>((key, value) => key);

        public KTableKTableInnerJoin(
            IKTable<K, V1> table1,
            IKTable<K, V2> table2,
            ValueJoiner<V1, V2, R> joiner,
            string storeName)
            : base(table1, table2, joiner)
        {
            this.storeName = storeName;
        }

        public override IKeyValueProcessor<K, IChange<V1>> Get()
        {
            return new KTableKTableJoinProcessor<K, R, V1, V2>(
                this.valueGetterSupplier2.Get(),
                this.joiner,
                this.storeName,
                this.sendOldValues);
        }

        public override IKTableValueGetterSupplier<K, R> View()
        {
            return new KTableKTableInnerJoinValueGetterSupplier<K, R, V1, V2>(
                this.valueGetterSupplier1,
                this.valueGetterSupplier2,
                this.keyValueMapper,
                this.joiner);
        }
    }
}
