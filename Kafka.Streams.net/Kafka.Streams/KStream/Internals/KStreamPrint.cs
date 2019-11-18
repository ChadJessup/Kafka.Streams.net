using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamPrint<K, V> : IProcessorSupplier<K, V>
    {

        private readonly IForeachAction<K, V> action;

        public KStreamPrint(IForeachAction<K, V> action)
        {
            this.action = action;
        }


        public IKeyValueProcessor<K, V> get()
        {
            return null;// new KStreamPrintProcessor();
        }
    }
}