
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamTransformValuesProcessor<K, V, R> : IKeyValueProcessor<K, V>
    {
        private readonly IValueTransformerWithKey<K, V, R> valueTransformer;
        private IProcessorContext context;

        public KStreamTransformValuesProcessor(IValueTransformerWithKey<K, V, R> valueTransformer)
        {
            this.valueTransformer = valueTransformer;
        }


        public void Init(IProcessorContext context)
        {
            valueTransformer.Init(new ForwardingDisabledProcessorContext<K, V>(context));
            this.context = context;
        }


        public void Process(K key, V value)
        {
            //context.Forward(key, valueTransformer.transform(key, value));
        }

        public void Close()
        {
            valueTransformer.Close();
        }

        public void Process<K1, V1>(K1 key, V1 value)
        {
            throw new System.NotImplementedException();
        }
    }
}
