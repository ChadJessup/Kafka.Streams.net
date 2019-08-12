using Kafka.Streams.Processor;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamAggregate<K, V, T> : IKStreamAggProcessorSupplier<K, K, V, T>
    {
        private static ILogger LOG = new LoggerFactory().CreateLogger<KStreamAggregate<K, V, T>>();
        private string storeName;
        private IInitializer<T> initializer;
        private IAggregator<K, V, T> aggregator;

        private bool sendOldValues = false;

        KStreamAggregate(string storeName, IInitializer<T> initializer, IAggregator<K, V, T> aggregator)
        {
            this.storeName = storeName;
            this.initializer = initializer;
            this.aggregator = aggregator;
        }


        public IProcessor<K, V> get()
        {
            return new KStreamAggregateProcessor();
        }


        public void enableSendingOldValues()
        {
            sendOldValues = true;
        }


        public IKTableValueGetterSupplier<K, T> view()
        {
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
    }
}