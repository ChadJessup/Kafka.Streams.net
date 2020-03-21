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
        private static ILogger LOG = new LoggerFactory().CreateLogger<KTableKTableInnerJoin<K, S, R, V1, V2>>();
        private readonly string storeName;
        private IKeyValueMapper<K, V1, K> keyValueMapper = new KeyValueMapper<K, V1, K>((key, value) => key);

        public KTableKTableInnerJoin(
            KTable<K, S, V1> table1,
            KTable<K, S, V2> table2,
            IValueJoiner<V1, V2, R> joiner,
            string storeName)
            : base(table1, table2, joiner)
        {
            this.storeName = storeName;
        }

        public override IKeyValueProcessor<K, Change<V1>> get()
        {
            return new KTableKTableJoinProcessor<K, R, V1, V2>(
                valueGetterSupplier2.get(),
                this.joiner,
                this.storeName,
                this.sendOldValues);
        }

        public override IKTableValueGetterSupplier<K, R> view()
        {
            return new KTableKTableInnerJoinValueGetterSupplier<K, R, V1, V2>(
                valueGetterSupplier1,
                valueGetterSupplier2,
                this.keyValueMapper,
                this.joiner);
        }
    }
}
