using Kafka.Streams.Processors;
using Kafka.Streams.KStream.Interfaces;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamAggregate<K, V, T> : IKStreamAggProcessorSupplier<K, K, V, T>
    {
        private static readonly ILogger LOG = new LoggerFactory().CreateLogger<KStreamAggregate<K, V, T>>();
        private readonly KafkaStreamsContext context;
        private readonly string storeName;
        private readonly IInitializer<T> initializer;
        private readonly IAggregator<K, V, T> aggregator;

        private bool sendOldValues = false;

        public KStreamAggregate(
            KafkaStreamsContext context,
            string storeName,
            IInitializer<T> initializer,
            IAggregator<K, V, T> aggregator)
        {
            this.context = context;
            this.storeName = storeName;
            this.initializer = initializer;
            this.aggregator = aggregator;
        }

        public IKeyValueProcessor<K, V> Get()
        {
            return new KStreamAggregateProcessor<K, V, T>(
                this.context,
                this.storeName,
                this.sendOldValues,
                this.initializer,
                this.aggregator);
        }

        IKeyValueProcessor IProcessorSupplier.Get()
            => this.Get();

        public void EnableSendingOldValues()
        {
            this.sendOldValues = true;
        }

        public IKTableValueGetterSupplier<K, T> View()
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
    }
}
