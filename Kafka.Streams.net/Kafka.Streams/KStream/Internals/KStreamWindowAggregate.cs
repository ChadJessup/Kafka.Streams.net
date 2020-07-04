using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamWindowAggregate<K, V, Agg, W> : IKStreamAggProcessorSupplier<K, IWindowed<K>, V, Agg>
        where Agg: V
        where W : Window
    {
        private readonly ILogger log = new LoggerFactory().CreateLogger<KStreamWindowAggregate<K, V, Agg, W>>();

        private readonly string storeName;
        private readonly KafkaStreamsContext context;
        private readonly Windows<W> windows;
        private readonly Initializer<Agg> initializer;
        private readonly Aggregator<K, V, Agg> aggregator;

        private bool sendOldValues = false;

        public KStreamWindowAggregate(
            KafkaStreamsContext context,
            Windows<W> windows,
            string storeName,
            Initializer<Agg> initializer,
            Aggregator<K, V, Agg> aggregator)
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
            this.sendOldValues = true;
        }

        public IKTableValueGetterSupplier<IWindowed<K>, Agg> View()
        {
            return null;
            //return new KTableValueGetterSupplier<IWindowed<K>, Agg>()
            //{

            //    public KTableValueGetter<IWindowed<K>, Agg> get()
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
