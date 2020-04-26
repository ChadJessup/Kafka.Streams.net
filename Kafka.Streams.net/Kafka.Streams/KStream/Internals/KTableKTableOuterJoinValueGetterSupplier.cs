using Kafka.Streams.Interfaces;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableKTableOuterJoinValueGetterSupplier<K, R, V1, V2> : KTableKTableAbstractJoinValueGetterSupplier<K, R, V1, V2>
    {
        private readonly ValueJoiner<V1, V2, R> joiner;

        public KTableKTableOuterJoinValueGetterSupplier(
            IKTableValueGetterSupplier<K, V1> valueGetterSupplier1,
            IKTableValueGetterSupplier<K, V2> valueGetterSupplier2,
            ValueJoiner<V1, V2, R> joiner)
            : base(valueGetterSupplier1, valueGetterSupplier2)
        {
            this.joiner = joiner;
        }

        public override IKTableValueGetter<K, R> Get()
        {
            return new KTableKTableOuterJoinValueGetter<K, R, V1, V2>(
                valueGetterSupplier1.Get(),
                valueGetterSupplier2.Get(),
                this.joiner);
        }
    }
}
