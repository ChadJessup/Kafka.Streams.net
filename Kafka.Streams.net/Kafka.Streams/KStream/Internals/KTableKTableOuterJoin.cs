using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableKTableOuterJoin<K, R, V1, V2> : KTableKTableAbstractJoin<K, R, V1, V2>
    {
        private static readonly ILogger LOG = new LoggerFactory().CreateLogger<KTableKTableOuterJoin<K, R, V1, V2>>();

        public KTableKTableOuterJoin(
            IKTable<K, V1> table1,
            IKTable<K, V2> table2,
            ValueJoiner<V1, V2, R> joiner)
            : base(table1, table2, joiner)
        {
        }

        public override IKeyValueProcessor<K, IChange<V1>> Get()
        {
            return new KTableKTableOuterJoinProcessor<K, R, V1, V2>(
                this.valueGetterSupplier2.Get(),
                this.sendOldValues,
                this.joiner);
        }

        public override IKTableValueGetterSupplier<K, R> View()
        {
            return new KTableKTableOuterJoinValueGetterSupplier<K, R, V1, V2>(
                this.valueGetterSupplier1, 
                this.valueGetterSupplier2,
                this.joiner);
        }
    }
}
