using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamFlatTransformValuesProcessor<K, V, VR> : IKeyValueProcessor<K, V>
    {
        private readonly IValueTransformerWithKey<K, V, IEnumerable<VR>> valueTransformer;
        private IProcessorContext context;

        public KStreamFlatTransformValuesProcessor(IValueTransformerWithKey<K, V, IEnumerable<VR>> valueTransformer)
        {
            this.valueTransformer = valueTransformer;
        }

        public void Init(IProcessorContext context)
        {
            this.valueTransformer.Init(new ForwardingDisabledProcessorContext<K, V>(context));

            this.context = context;
        }

        public void Process(K key, V value)
        {
            IEnumerable<VR> transformedValues = this.valueTransformer.Transform(key, value);

            if (transformedValues != null)
            {
                foreach (VR transformedValue in transformedValues)
                {
                    this.context.Forward(key, transformedValue);
                }
            }
        }

        public void Close()
        {
            this.valueTransformer.Close();
        }
    }
}
