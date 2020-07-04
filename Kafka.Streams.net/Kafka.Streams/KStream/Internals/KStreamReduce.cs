using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamReduce<K, V> : IKStreamAggProcessorSupplier<K, K, V, V>
    {
        private static readonly ILogger LOG = new LoggerFactory().CreateLogger<KStreamReduce<K, V>>();

        private readonly string? storeName;
        private readonly Reducer<V> reducer;

        private bool sendOldValues = false;

        public KStreamReduce(string? storeName, Reducer<V> reducer1)
        {
            this.storeName = storeName;
        }

        public IKeyValueProcessor<K, V> Get()
        {
            return null;// new KStreamReduceProcessor();
        }

        IKeyValueProcessor IProcessorSupplier.Get()
            => this.Get();

        public void EnableSendingOldValues()
        {
            this.sendOldValues = true;
        }


        public IKTableValueGetterSupplier<K, V> View()
        {
            return null;
            //    return new KTableValueGetterSupplier<K, V>()
            //    {

            //        public KTableValueGetter<K, V> get()
            //    {
            //        return new KStreamReduceValueGetter();
            //    }


            //    public string[] storeNames()
            //    {
            //        return new string[] { storeName };
            //    }
            //};
        }
    }
}
