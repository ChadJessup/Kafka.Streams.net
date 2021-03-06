using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamGlobalKTableJoin<K1, K2, R, V1, V2> : IProcessorSupplier<K1, V1>
    {
        private readonly IKTableValueGetterSupplier<K2, V2> valueGetterSupplier;
        private readonly ValueJoiner<V1, V2, R> joiner;
        private readonly KeyValueMapper<K1, V1, K2> mapper;
        private readonly bool leftJoin;

        public KStreamGlobalKTableJoin(
            IKTableValueGetterSupplier<K2, V2> valueGetterSupplier,
            ValueJoiner<V1, V2, R> joiner,
            KeyValueMapper<K1, V1, K2> mapper,
            bool leftJoin)
        {
            this.valueGetterSupplier = valueGetterSupplier;
            this.joiner = joiner;
            this.mapper = mapper;
            this.leftJoin = leftJoin;
        }


        public IKeyValueProcessor<K1, V1> Get()
        {
            return new KStreamKTableJoinProcessor<K1, K2, V1, V2, R>(
                null,
                string.Empty,
                this.valueGetterSupplier.Get(),
                this.mapper,
                this.joiner,
                this.leftJoin);
        }

        IKeyValueProcessor IProcessorSupplier.Get()
            => this.Get();
    }
}
