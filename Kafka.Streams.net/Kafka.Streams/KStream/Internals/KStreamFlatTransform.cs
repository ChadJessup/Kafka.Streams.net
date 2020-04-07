using Kafka.Streams.Processors;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamFlatTransform<KIn, VIn, KOut, VOut> : IProcessorSupplier<KIn, VIn>
    {
        private readonly ITransformerSupplier<KIn, VIn, IEnumerable<KeyValuePair<KOut, VOut>>> transformerSupplier;

        public KStreamFlatTransform(ITransformerSupplier<KIn, VIn, IEnumerable<KeyValuePair<KOut, VOut>>> transformerSupplier)
        {
            this.transformerSupplier = transformerSupplier;
        }

        public IKeyValueProcessor<KIn, VIn> Get()
        {
            return null; // new KStreamFlatTransformProcessor<KIn, VIn, KOut, VOut>(transformerSupplier());
        }

        IKeyValueProcessor IProcessorSupplier.Get()
            => this.Get();
    }
}