using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamWindowAggregate<K, V, Agg, W> : IKStreamAggProcessorSupplier<K, Windowed<K>, V, Agg>
        where W : Window
    {
        private readonly ILogger log = new LoggerFactory().CreateLogger<KStreamWindowAggregate<K, V, Agg, W>>();

        private readonly string storeName;
        private readonly Windows<W> windows;
        private readonly IInitializer<Agg> initializer;
        private readonly IAggregator<K, V, Agg> aggregator;

        private bool sendOldValues = false;

        public KStreamWindowAggregate(
            Windows<W> windows,
            string storeName,
            IInitializer<Agg> initializer,
            IAggregator<K, V, Agg> aggregator)
        {
            this.windows = windows;
            this.storeName = storeName;
            this.initializer = initializer;
            this.aggregator = aggregator;
        }

        public IKeyValueProcessor<K, V> Get()
        {
            return new KStreamWindowAggregateProcessor<K, V, Agg, W>(
                this.windows,
                this.storeName,
                this.sendOldValues,
                this.initializer,
                this.aggregator);
        }

        public void EnableSendingOldValues()
        {
            sendOldValues = true;
        }

        public IKTableValueGetterSupplier<Windowed<K>, Agg> View()
        {
            return null;
            //return new KTableValueGetterSupplier<Windowed<K>, Agg>()
            //{

            //    public KTableValueGetter<Windowed<K>, Agg> get()
            //{
            //                return new KStreamWindowAggregateValueGetter();
            //            }


            //            public string[] storeNames()
            //{
            //                return new string[] {storeName};
            //            }
            //        };
        }

        public IProcessorSupplier<Windowed<K>, Agg> GetSwappedProcessorSupplier()
        {
            throw new System.NotImplementedException();
        }
    }
}