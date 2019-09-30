using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamGlobalKTableJoin<K1, K2, R, V1, V2> : IProcessorSupplier<K1, V1>
    {
        private readonly IKTableValueGetterSupplier<K2, V2> valueGetterSupplier;
        private readonly IValueJoiner<V1, V2, R> joiner;
        private readonly IKeyValueMapper<K1, V1, K2> mapper;
        private readonly bool leftJoin;

        public KStreamGlobalKTableJoin(
            IKTableValueGetterSupplier<K2, V2> valueGetterSupplier,
            IValueJoiner<V1, V2, R> joiner,
            IKeyValueMapper<K1, V1, K2> mapper,
            bool leftJoin)
        {
            this.valueGetterSupplier = valueGetterSupplier;
            this.joiner = joiner;
            this.mapper = mapper;
            this.leftJoin = leftJoin;
        }


        public IKeyValueProcessor<K1, V1> get()
        {
            return new KStreamKTableJoinProcessor<K1, K2, V1, V2, R>(
                null,
                string.Empty,
                valueGetterSupplier.get(),
                mapper,
                joiner,
                leftJoin);
        }
    }
}
