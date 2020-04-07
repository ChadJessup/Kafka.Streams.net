using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamTransformValues<K, V, R> : IProcessorSupplier<K, V>
    {
        private readonly IValueTransformerWithKeySupplier<K, V, R> valueTransformerSupplier;

        public KStreamTransformValues(IValueTransformerWithKeySupplier<K, V, R> valueTransformerSupplier)
        {
            this.valueTransformerSupplier = valueTransformerSupplier;
        }

        IKeyValueProcessor IProcessorSupplier.Get()
            => this.Get();

        public IKeyValueProcessor<K, V> Get()
        {
            return null;// new KStreamTransformValuesProcessor<>(valueTransformerSupplier());
        }
    }
}
