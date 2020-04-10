
using Kafka.Streams.Processors.Interfaces;

namespace Kafka.Streams.KStream.Internals
{






    public class ProducedInternal<K, V> : Produced<K, V>
    {
        public ProducedInternal(Produced<K, V> produced)
            : base(produced)
        {
        }

        public IStreamPartitioner<K, V> StreamPartitioner()
        {
            return this.Partitioner;
        }

        public string Name => this.ProcessorName;
    }
}
