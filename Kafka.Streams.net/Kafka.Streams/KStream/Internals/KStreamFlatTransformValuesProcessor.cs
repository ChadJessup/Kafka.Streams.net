﻿using Kafka.Streams.Processors;
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
            valueTransformer.init(new ForwardingDisabledProcessorContext<K, V>(context));

            this.context = context;
        }

        public void Process(K key, V value)
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

        public void Close()
        {
            valueTransformer.close();
        }

        public void Process<K1, V1>(K1 key, V1 value)
        {
            throw new System.NotImplementedException();
        }
    }
}
