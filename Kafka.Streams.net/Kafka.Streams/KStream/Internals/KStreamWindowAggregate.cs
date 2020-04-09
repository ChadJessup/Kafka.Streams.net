using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamWindowAggregate<K, V, Agg, W> : IKStreamAggProcessorSupplier<K, Windowed<K>, V, Agg>
        where Agg: V
        where W : Window
    {
        private readonly ILogger log = new LoggerFactory().CreateLogger<KStreamWindowAggregate<K, V, Agg, W>>();

        private readonly string storeName;
        private readonly KafkaStreamsContext context;
        private readonly Windows<W> windows;
        private readonly IInitializer<Agg> initializer;
        private readonly IAggregator<K, V, Agg> aggregator;

        private bool sendOldValues = false;

        public KStreamWindowAggregate(
            KafkaStreamsContext context,
            Windows<W> windows,
            string storeName,
            IInitializer<Agg> initializer,
            IAggregator<K, V, Agg> aggregator)
        {
            this.context = context;
            this.windows = windows;
            this.storeName = storeName;
            this.initializer = initializer;
            this.aggregator = aggregator;
        }

        public IKeyValueProcessor<K, V> Get()
        {
            return new KStreamWindowAggregateProcessor<K, V, Agg, W>(
                this.context,
                this.windows,
                this.storeName,
                this.sendOldValues,
                this.initializer,
                this.aggregator);
        }

        IKeyValueProcessor IProcessorSupplier.Get()
            => this.Get();

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
    }
}
