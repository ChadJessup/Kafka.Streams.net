﻿using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamFlatMapProcessor<K, V> : AbstractProcessor<K, V>
    {
        public override void Process(K key, V value)
        {
            //foreach (var newPair in mapper.apply(key, value))
            //{
            //    context.Forward(newPair.key, newPair.value);
            //}
        }
    }
}