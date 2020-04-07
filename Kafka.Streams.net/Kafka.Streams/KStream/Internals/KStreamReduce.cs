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

        public IKeyValueProcessor<K, V> Get()
        {
            return null;// new KStreamReduceProcessor();
        }

        IKeyValueProcessor IProcessorSupplier.Get()
            => this.Get();

        public void EnableSendingOldValues()
        {
            sendOldValues = true;
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

        public IProcessorSupplier<K, V> GetSwappedProcessorSupplier()
        {
            throw new System.NotImplementedException();
        }
    }
}