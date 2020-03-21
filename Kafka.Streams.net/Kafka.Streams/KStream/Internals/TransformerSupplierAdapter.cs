using Kafka.Streams.Processors.Interfaces;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.KStream.Internals
{
    public class TransformerSupplierAdapter<KIn, VIn, KOut, VOut> : ITransformerSupplier<KIn, VIn, IEnumerable<KeyValuePair<KOut, VOut>>>
    {
        private readonly ITransformerSupplier<KIn, VIn, KeyValuePair<KOut, VOut>> transformerSupplier;
        private ITransformer<KIn, VIn, KeyValuePair<KOut, VOut>> transformer { get; }

        public TransformerSupplierAdapter(ITransformerSupplier<KIn, VIn, KeyValuePair<KOut, VOut>> transformerSupplier)
        {
            this.transformerSupplier = transformerSupplier;
            this.transformer = transformerSupplier.get();
        }

        public ITransformer<KIn, VIn, IEnumerable<KeyValuePair<KOut, VOut>>> get()
        {
            return null;// new ITransformer<KIn, VIn, IEnumerable<KeyValuePair<KOut, VOut>>>();
        }

        public void init(IProcessorContext context)
        {
            transformer.init(context);
        }


        public IEnumerable<KeyValuePair<KOut, VOut>> Transform(KIn key, VIn value)
        {
            var pair = transformer.Transform(key, value);

            // if (pair != null)
            // {
            //     return new List<KeyValuePair<KOut, VOut>> { pair };
            // }

            return Enumerable.Empty<KeyValuePair<KOut, VOut>>();
        }


        public void close()
        {
            transformer.close();
        }
    };
}
