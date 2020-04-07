using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Mappers;
using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamKTableJoin<K, R, V1, V2> : IProcessorSupplier<K, V1>
    {
        private readonly IKeyValueMapper<K, V1, K> keyValueMapper = new KeyValueMapper<K, V1, K>((k, v) => k);

        private readonly IKTableValueGetterSupplier<K, V2> valueGetterSupplier;
        private readonly IValueJoiner<V1, V2, R> joiner;
        private readonly bool leftJoin;

        public KStreamKTableJoin(
            IKTableValueGetterSupplier<K, V2> valueGetterSupplier,
            IValueJoiner<V1, V2, R> joiner,
            bool leftJoin)
        {
            this.valueGetterSupplier = valueGetterSupplier;
            this.joiner = joiner;
            this.leftJoin = leftJoin;
        }


        public IKeyValueProcessor<K, V1> Get()
        {
            return new KStreamKTableJoinProcessor<K, K, V1, V2, R>(
                null, 
                null,
                valueGetterSupplier.Get(),
                this.keyValueMapper,
                joiner,
                leftJoin);
        }

        IKeyValueProcessor IProcessorSupplier.Get()
            => this.Get();
    }
}