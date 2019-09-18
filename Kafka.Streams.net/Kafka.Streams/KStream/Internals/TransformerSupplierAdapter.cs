using Kafka.Streams.Processor.Interfaces;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.KStream.Internals
{
    public class TransformerSupplierAdapter<KIn, VIn, KOut, VOut> : ITransformerSupplier<KIn, VIn, IEnumerable<KeyValue<KOut, VOut>>>
    {
        private readonly ITransformerSupplier<KIn, VIn, KeyValue<KOut, VOut>> transformerSupplier;
        private ITransformer<KIn, VIn, KeyValue<KOut, VOut>> transformer { get; }

        public TransformerSupplierAdapter(ITransformerSupplier<KIn, VIn, KeyValue<KOut, VOut>> transformerSupplier)
        {
            this.transformerSupplier = transformerSupplier;
            this.transformer = transformerSupplier.get();
        }

        public ITransformer<KIn, VIn, IEnumerable<KeyValue<KOut, VOut>>> get()
        {
            return null;// new ITransformer<KIn, VIn, IEnumerable<KeyValue<KOut, VOut>>>();
        }

        public void init(IProcessorContext context)
        {
            transformer.init(context);
        }


        public IEnumerable<KeyValue<KOut, VOut>> transform(KIn key, VIn value)
        {
            KeyValue<KOut, VOut> pair = transformer.transform(key, value);

            if (pair != null)
            {
                return new List<KeyValue<KOut, VOut>> { pair };
            }

            return Enumerable.Empty<KeyValue<KOut, VOut>>();
        }


        public void close()
        {
            transformer.close();
        }
    };
}
