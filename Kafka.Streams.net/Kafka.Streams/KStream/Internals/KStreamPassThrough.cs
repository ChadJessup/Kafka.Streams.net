using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamPassThrough<K, V> : IProcessorSupplier<K, V>
    {
        public IKeyValueProcessor<K, V> Get()
        {
            return new KStreamPassThroughProcessor<K, V>();
        }

        IKeyValueProcessor IProcessorSupplier.Get()
            => this.Get();
    }
}