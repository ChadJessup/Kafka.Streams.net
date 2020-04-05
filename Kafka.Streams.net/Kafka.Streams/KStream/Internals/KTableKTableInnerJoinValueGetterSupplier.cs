using Kafka.Streams.Interfaces;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableKTableInnerJoinValueGetterSupplier<K, R, V1, V2> : KTableKTableAbstractJoinValueGetterSupplier<K, R, V1, V2>
    {
        private readonly IKeyValueMapper<K, V1, K> keyValueMapper;
        private readonly IValueJoiner<V1, V2, R> joiner;

        public KTableKTableInnerJoinValueGetterSupplier(
            IKTableValueGetterSupplier<K, V1> valueGetterSupplier1,
            IKTableValueGetterSupplier<K, V2> valueGetterSupplier2,
            IKeyValueMapper<K, V1, K> keyValueMapper,
            IValueJoiner<V1, V2, R> joiner)
                : base(valueGetterSupplier1, valueGetterSupplier2)
        {
            this.keyValueMapper = keyValueMapper;
            this.joiner = joiner;
        }

        public override IKTableValueGetter<K, R> Get()
        {
            return new KTableKTableInnerJoinValueGetter<K, R, V1, V2>(
                this.valueGetterSupplier1.Get(),
                this.valueGetterSupplier2.Get(),
                this.keyValueMapper,
                this.joiner);
        }
    }
}
