using Kafka.Streams.Processor;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableSource<K, V> : IProcessorSupplier<K, V>
    {
        private static ILogger LOG = new LoggerFactory().CreateLogger<KTableSource<K, V>>();

        private string storeName;
        private string queryableName;
        private bool sendOldValues;

        public KTableSource(string storeName, string queryableName)
        {
            storeName = storeName ?? throw new System.ArgumentNullException("storeName can't be null", nameof(storeName));

            this.storeName = storeName;
            this.queryableName = queryableName;
            this.sendOldValues = false;
        }

        public string queryableName()
        {
            return queryableName;
        }

        public Processor<K, V> get()
        {
            return new KTableSourceProcessor();
        }

        // when source ktable requires sending old values, we just
        // need to set the queryable name as the store name to enforce materialization
        public void enableSendingOldValues()
        {
            this.sendOldValues = true;
            this.queryableName = storeName;
        }

        // when the source ktable requires materialization from downstream, we just
        // need to set the queryable name as the store name to enforce materialization
        public void materialize()
        {
            this.queryableName = storeName;
        }
    }
}