using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Mappers;
using Kafka.Streams.Processors;
using Kafka.Streams.State;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableKTableInnerJoin<K, S, R, V1, V2> : KTableKTableAbstractJoin<K, S, R, V1, V2>
        where S : IStateStore
    {
        private readonly string storeName;
        private readonly IKeyValueMapper<K, V1, K> keyValueMapper = new KeyValueMapper<K, V1, K>((key, value) => key);

        public KTableKTableInnerJoin(
            KTable<K, S, V1> table1,
            KTable<K, S, V2> table2,
            IValueJoiner<V1, V2, R> joiner,
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
