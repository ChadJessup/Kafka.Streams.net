
using Kafka.Streams.Processors;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public partial class KStreamFlatTransformValues<KIn, VIn, VOut> : IProcessorSupplier<KIn, VIn>
    {
        private readonly IValueTransformerWithKeySupplier<KIn, VIn, IEnumerable<VOut>> valueTransformerSupplier;

        public KStreamFlatTransformValues(IValueTransformerWithKeySupplier<KIn, VIn, IEnumerable<VOut>> valueTransformerWithKeySupplier)
        {
            this.valueTransformerSupplier = valueTransformerWithKeySupplier;
        }

        public IKeyValueProcessor<KIn, VIn> Get()
        {
            return new KStreamFlatTransformValuesProcessor<KIn, VIn, VOut>(this.valueTransformerSupplier.Get());
        }

        IKeyValueProcessor IProcessorSupplier.Get()
            => this.Get();
    }
}
