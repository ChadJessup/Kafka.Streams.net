using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamJoinWindow<K, V> : IProcessorSupplier<K, V>
    {
        private readonly string windowName;

        public KStreamJoinWindow(string windowName)
        {
            this.windowName = windowName;
        }

        public IKeyValueProcessor<K, V> Get()
        {
            return null;//new KStreamJoinWindowProcessor<K, V>(this.windowName);
        }

        IKeyValueProcessor IProcessorSupplier.Get()
            => this.Get();
    }
}
