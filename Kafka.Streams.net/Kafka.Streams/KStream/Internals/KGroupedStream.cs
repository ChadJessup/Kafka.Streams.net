using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.State.KeyValues;
using NodaTime;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class KGroupedStream<K, V> : AbstractStream<K, V>, IKGroupedStream<K, V>
    {
        const string REDUCE_NAME = "KSTREAM-REDUCE-";
        const string AGGREGATE_NAME = "KSTREAM-AGGREGATE-";
        private readonly IClock clock;
        private readonly GroupedStreamAggregateBuilder<K, V> aggregateBuilder;

        public KGroupedStream(
            IClock clock,
            string name,
            HashSet<string> sourceNodes,
            GroupedInternal<K, V> groupedInternal,
            bool repartitionRequired,
            StreamsGraphNode streamsGraphNode,
            InternalStreamsBuilder builder)
            : base(name, groupedInternal.KeySerde, groupedInternal.ValueSerde, sourceNodes, streamsGraphNode, builder)
        {
            this.clock = clock;

            this.aggregateBuilder = new GroupedStreamAggregateBuilder<K, V>(
                this.clock,
                builder,
                groupedInternal,
                repartitionRequired,
                sourceNodes,
                name,
                streamsGraphNode);
        }

        public IKTable<K, V> Reduce(IReducer<V> reducer)
        {
            return Reduce(reducer, Materialized.With<K, V, IKeyValueStore<Bytes, byte[]>>(keySerde, valSerde));
        }

        public IKTable<K, V> Reduce(
            IReducer<V> reducer,
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            reducer = reducer ?? throw new ArgumentNullException(nameof(reducer));
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            var materializedInternal =
               new MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>(materialized, builder, REDUCE_NAME);

            if (materializedInternal.KeySerde == null)
            {
                materializedInternal.WithKeySerde(keySerde);
            }

            if (materializedInternal.ValueSerde == null)
            {
                materializedInternal.WithValueSerde(valSerde);
            }

            return null;
            //doAggregate(
            //    new KStreamReduce<K, V>(materializedInternal.storeName, reducer),
            //    REDUCE_NAME,
            //    materializedInternal);
        }


        public IKTable<K, VR> Aggregate<VR>(
            IInitializer<VR> initializer,
            IAggregator<K, V, VR> aggregator,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            initializer = initializer ?? throw new ArgumentNullException(nameof(initializer));
            aggregator = aggregator ?? throw new ArgumentNullException(nameof(aggregator));
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            var materializedInternal =
               new MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>(materialized, builder, AGGREGATE_NAME);

            if (materializedInternal.KeySerde == null)
            {
                materializedInternal.WithKeySerde(keySerde);
            }

            return null;
            //doAggregate(
            //    new KStreamAggregate<>(materializedInternal.storeName, initializer, aggregator),
            //    AGGREGATE_NAME,
            //    materializedInternal);
        }

        public IKTable<K, VR> Aggregate<VR>(
            IInitializer<VR> initializer,
            IAggregator<K, V, VR> aggregator)
        {
            return Aggregate(
                initializer,
                aggregator,
                Materialized.With<K, VR, IKeyValueStore<Bytes, byte[]>>(keySerde, null));
        }

        public IKTable<K, long> Count()
            => DoCount(Materialized.With<K, long, IKeyValueStore<Bytes, byte[]>>(keySerde, Serdes.Long()));


        public IKTable<K, long> Count(Materialized<K, long, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            // TODO: Remove this when we do a topology-incompatible release
            // we used to burn a topology name here, so we have to keep doing it for compatibility
            if (new MaterializedInternal<K, long, IKeyValueStore<Bytes, byte[]>>(materialized).StoreName == null)
            {
                builder.NewStoreName(AGGREGATE_NAME);
            }

            return DoCount(materialized);
        }

        private IKTable<K, long> DoCount(Materialized<K, long, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            var materializedInternal =
               new MaterializedInternal<K, long, IKeyValueStore<Bytes, byte[]>>(materialized, builder, AGGREGATE_NAME);

            if (materializedInternal.KeySerde == null)
            {
                materializedInternal.WithKeySerde(keySerde);
            }

            if (materializedInternal.ValueSerde == null)
            {
                materializedInternal.WithValueSerde(Serdes.Long());
            }

            var kstreamAggregate = new KStreamAggregate<K, V, long>(
                    materializedInternal.StoreName,
                    aggregateBuilder.countInitializer,
                    aggregateBuilder.countAggregator);

            return DoAggregate(
                kstreamAggregate,
                AGGREGATE_NAME,
                materializedInternal);
        }

        //public ITimeWindowedKStream<K, V> windowedBy<W>(Windows<W> windows)
        //    where W : Window
        //{
        //    return new TimeWindowedKStreamImpl<K, V, W>(
        //        windows,
        //        builder,
        //        sourceNodes,
        //        name,
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
        //        name,
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
            var tkvsm = new TimestampedKeyValueStoreMaterializer<K, T>(this.clock, materializedInternal);
            var materialized = tkvsm.Materialize();

            return aggregateBuilder.Build(
                functionName,
                materialized,
                aggregateSupplier,
                materializedInternal.QueryableStoreName(),
                materializedInternal.KeySerde,
                materializedInternal.ValueSerde);
        }

        public IKTable<K, V> Reduce(IReducer<V> reducer, Materialized<K, V> materialized)
        {
            throw new NotImplementedException();
        }

        public IKTable<K, VR> Aggregate<VR>(IInitializer<VR> initializer, IAggregator<K, V, VR> aggregator, Materialized<K, VR> materialized)
        {
            throw new NotImplementedException();
        }
    }
}