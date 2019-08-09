using Kafka.Streams.Interfaces;
using Kafka.Streams.Processor;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamFlatMapValuesProcessor<K, V, V1> : AbstractProcessor<K, V>
    {
        private IValueMapperWithKey<K, V, V1> mapper;

        public KStreamFlatMapValuesProcessor(IValueMapperWithKey<K, V, V1> mapper)
            => this.mapper = mapper;

        public void process(K key, V value)
        {
            IEnumerable<V1> newValues = this.mapper.apply(key, value);
            foreach (V1 v in newValues)
            {
                context.forward(key, v);
            }
        }
    }
