using Kafka.Streams.Processors;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamAggregate<K, V, T> : IKStreamAggProcessorSupplier<K, K, V, T>
    {
        private static readonly ILogger LOG = new LoggerFactory().CreateLogger<KStreamAggregate<K, V, T>>();
        private readonly string storeName;
        private readonly IInitializer<T> initializer;
        private readonly IAggregator<K, V, T> aggregator;

        private bool sendOldValues = false;

        public KStreamAggregate(
            string storeName,
            IInitializer<T> initializer,
            IAggregator<K, V, T> aggregator)
        {
            this.storeName = storeName;
            this.initializer = initializer;
            this.aggregator = aggregator;
        }

        public IProcessor<K, V> get()
        {
            return null; // new KStreamAggregateProcessor();
        }

        public void enableSendingOldValues()
        {
            sendOldValues = true;
        }

        public IKTableValueGetterSupplier<K, T> view()
        {
            return null;
            //    return new KTableValueGetterSupplier<K, T>()
            //    {

            //        public KTableValueGetter<K, T> get()
            //    {
            //        return new KStreamAggregateValueGetter();
            //    }


            //    public string[] storeNames()
            //    {
            //        return new string[] { storeName };
            //    }
            //};
        }

        public IProcessorSupplier<K, T> GetSwappedProcessorSupplier()
        {
            return null;
        }
    }
}