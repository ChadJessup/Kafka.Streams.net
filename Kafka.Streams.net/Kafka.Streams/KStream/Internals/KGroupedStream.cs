using Kafka.Common;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.Nodes;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.TimeStamped;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class KGroupedStream<K, V> : AbstractStream<K, V>, IKGroupedStream<K, V>
    {
        private const string REDUCE_NAME = "KSTREAM-REDUCE-";
        private const string AGGREGATE_NAME = "KSTREAM-AGGREGATE-";
        private readonly GroupedStreamAggregateBuilder<K, V> aggregateBuilder;

        public KGroupedStream(
            KafkaStreamsContext context,
            string Name,
            HashSet<string> sourceNodes,
            GroupedInternal<K, V> groupedInternal,
            bool repartitionRequired,
            StreamsGraphNode streamsGraphNode)
            : base(context,
                  Name,
                  groupedInternal.KeySerde,
                  groupedInternal.ValueSerde,
                  sourceNodes,
                  streamsGraphNode)
        {
            this.aggregateBuilder = new GroupedStreamAggregateBuilder<K, V>(
                this.Context,
                groupedInternal,
                repartitionRequired,
                sourceNodes,
                Name,
                streamsGraphNode);
        }

        public IKTable<K, V> Reduce(Reducer<V> reducer)
        {
            return this.Reduce(
                reducer,
                Materialized.With<K, V, IKeyValueStore<Bytes, byte[]>>(this.KeySerde, this.ValueSerde));
        }

        public IKTable<K, V> Reduce(
            Reducer<V> reducer,
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            reducer = reducer ?? throw new ArgumentNullException(nameof(reducer));
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            var materializedInternal =
               new MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>(materialized, this.Builder, REDUCE_NAME);

            if (materializedInternal.KeySerde == null)
            {
                materializedInternal.WithKeySerde(this.KeySerde);
            }

            if (materializedInternal.ValueSerde == null)
            {
                materializedInternal.WithValueSerde(this.ValueSerde);
            }

            return this.DoAggregate(
            new KStreamReduce<K, V>(
                materializedInternal.StoreName,
                reducer),
            REDUCE_NAME,
            materializedInternal);
        }


        public IKTable<K, VR> Aggregate<VR>(
            Initializer<VR> initializer,
            Aggregator<K, V, VR> aggregator,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            initializer = initializer ?? throw new ArgumentNullException(nameof(initializer));
            aggregator = aggregator ?? throw new ArgumentNullException(nameof(aggregator));
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            var materializedInternal =
               new MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>(materialized, this.Builder, AGGREGATE_NAME);

            if (materializedInternal.KeySerde == null)
            {
                materializedInternal.WithKeySerde(this.KeySerde);
            }

            return this.DoAggregate(
                new KStreamAggregate<K, V, VR>(
                    this.Context,
                    materializedInternal.StoreName,
                    initializer,
                    aggregator),
                AGGREGATE_NAME,
                materializedInternal);
        }

        public IKTable<K, VR> Aggregate<VR>(
            Initializer<VR> initializer,
            Aggregator<K, V, VR> aggregator)
        {
            return this.Aggregate(
                initializer,
                aggregator,
                Materialized.With<K, VR, IKeyValueStore<Bytes, byte[]>>(this.KeySerde, null));
        }

        public IKTable<K, long> Count()
            => this.DoCount(Materialized.With<K, long, IKeyValueStore<Bytes, byte[]>>(this.KeySerde, Serdes.Long()));

        public IKTable<K, long> Count(Materialized<K, long, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            // TODO: Remove this when we do a topology-incompatible release
            // we used to burn a topology Name here, so we have to keep doing it for compatibility
            if (new MaterializedInternal<K, long, IKeyValueStore<Bytes, byte[]>>(materialized).StoreName == null)
            {
                this.Builder.NewStoreName(AGGREGATE_NAME);
            }

            return this.DoCount(materialized);
        }

        private IKTable<K, long> DoCount(Materialized<K, long, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            var materializedInternal = new MaterializedInternal<K, long, IKeyValueStore<Bytes, byte[]>>(
                materialized,
                this.Builder,
                AGGREGATE_NAME);

            if (materializedInternal.KeySerde == null)
            {
                materializedInternal.WithKeySerde(this.KeySerde);
            }

            if (materializedInternal.ValueSerde == null)
            {
                materializedInternal.WithValueSerde(Serdes.Long());
            }

            var kstreamAggregate = new KStreamAggregate<K, V, long>(
                this.Context,
                materializedInternal.StoreName,
                this.aggregateBuilder.countInitializer,
                this.aggregateBuilder.countAggregator);

            return this.DoAggregate(
                kstreamAggregate,
                AGGREGATE_NAME,
                materializedInternal);
        }

        //public ITimeWindowedIIKStream<K, V> windows)
        //    where W : Window
        //{
        //    return new TimeWindowedKStreamImpl<K, V, W>(
        //        windows,
        //        builder,
        //        sourceNodes,
        //        Name,
        //        keySerde,
        //        valSerde,
        //        aggregateBuilder,
        //        streamsGraphNode);
        //}

        //public SessionWindowedKStream<K, V> windowedBy(SessionWindows windows)
        //{
        //    return new SessionWindowedKStreamImpl<>(
        //        windows,
        //        builder,
        //        sourceNodes,
        //        Name,
        //        keySerde,
        //        valSerde,
        //        aggregateBuilder,
        //        streamsGraphNode);
        //}

        private IKTable<K, T> DoAggregate<T>(
            IKStreamAggProcessorSupplier<K, K, V, T> aggregateSupplier,
            string functionName,
            MaterializedInternal<K, T, IKeyValueStore<Bytes, byte[]>> materializedInternal)
        {
            var tkvsm = new TimestampedKeyValueStoreMaterializer<K, T>(this.Context, materializedInternal);
            var materialized = tkvsm.Materialize();

            return this.aggregateBuilder.Build<K, T, ITimestampedKeyValueStore<K,T>>(
                functionName,
                materialized,
                aggregateSupplier,
                materializedInternal.QueryableStoreName(),
                materializedInternal.KeySerde,
                materializedInternal.ValueSerde);
        }

        public ITimeWindowedKStream<K, V> WindowedBy<W>(Windows<W> windows) where W : Window
        {
            throw new NotImplementedException();
        }

        public ISessionWindowedKStream<K, V> WindowedBy(SessionWindows windows)
        {
            throw new NotImplementedException();
        }
    }
}
