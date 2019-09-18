﻿using Kafka.Streams.Processor;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.Processor.Internals;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamFlatTransformValuesProcessor<K, V, VR> : IProcessor<K, V>
    {
        private readonly IValueTransformerWithKey<K, V, IEnumerable<VR>> valueTransformer;
        private IProcessorContext<K, V> context;

        public KStreamFlatTransformValuesProcessor(IValueTransformerWithKey<K, V, IEnumerable<VR>> valueTransformer)
        {
            this.valueTransformer = valueTransformer;
        }

        public void init(IProcessorContext<K, V> context)
        {
            valueTransformer.init(new ForwardingDisabledProcessorContext<K, V>(context));

            this.context = context;
        }

        public void process(K key, V value)
        {
            IEnumerable<VR> transformedValues = valueTransformer.transform(key, value);

            if (transformedValues != null)
            {
                foreach (VR transformedValue in transformedValues)
                {
                    context.forward(key, transformedValue);
                }
            }
        }

        public void close()
        {
            valueTransformer.close();
        }
    }
}
