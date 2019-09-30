using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamReduce<K, V> : IKStreamAggProcessorSupplier<K, K, V, V>
    {
        private static readonly ILogger LOG = new LoggerFactory().CreateLogger<KStreamReduce<K, V>>();

        private readonly string storeName;
        private readonly IReducer<V> reducer;

        private bool sendOldValues = false;

        KStreamReduce(string storeName, IReducer<V> reducer)
        {
            this.storeName = storeName;
            this.reducer = reducer;
        }


        public IKeyValueProcessor<K, V> get()
        {
            return null;// new KStreamReduceProcessor();
        }


        public void enableSendingOldValues()
        {
            sendOldValues = true;
        }


        public IKTableValueGetterSupplier<K, V> view()
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

        public IProcessorSupplier<K, V> GetSwappedProcessorSupplier()
        {
            throw new System.NotImplementedException();
        }
    }
}