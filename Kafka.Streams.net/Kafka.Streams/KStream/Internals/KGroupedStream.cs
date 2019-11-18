using Kafka.Common.Utils;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class KGroupedStream<K, V> : AbstractStream<K, V>, IKGroupedStream<K, V>
    {
        static readonly string REDUCE_NAME = "KSTREAM-REDUCE-";
        static readonly string AGGREGATE_NAME = "KSTREAM-AGGREGATE-";

        private readonly GroupedStreamAggregateBuilder<K, V> aggregateBuilder;

        public KGroupedStream(
            string name,
            HashSet<string> sourceNodes,
            GroupedInternal<K, V> groupedInternal,
            bool repartitionRequired,
            StreamsGraphNode streamsGraphNode,
            InternalStreamsBuilder builder)
            : base(name, groupedInternal.keySerde, groupedInternal.valueSerde, sourceNodes, streamsGraphNode, builder)
        {
            this.aggregateBuilder = new GroupedStreamAggregateBuilder<K, V>(
                builder,
                groupedInternal,
                repartitionRequired,
                sourceNodes,
                name,
                streamsGraphNode);
        }

        public IKTable<K, V> reduce(IReducer<V> reducer)
        {
            return reduce(reducer, Materialized<K, V, IKeyValueStore<Bytes, byte[]>>.With(keySerde, valSerde));
        }

        public IKTable<K, V> reduce(
            IReducer<V> reducer,
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            reducer = reducer ?? throw new ArgumentNullException(nameof(reducer));
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal =
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


        public IKTable<K, VR> aggregate<VR>(
            IInitializer<VR> initializer,
            IAggregator<K, V, VR> aggregator,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            initializer = initializer ?? throw new ArgumentNullException(nameof(initializer));
            aggregator = aggregator ?? throw new ArgumentNullException(nameof(aggregator));
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal =
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

        public IKTable<K, VR> aggregate<VR>(
            IInitializer<VR> initializer,
            IAggregator<K, V, VR> aggregator)
        {
            return aggregate(
                initializer,
                aggregator,
                Materialized<K, VR, IKeyValueStore<Bytes, byte[]>>.With(keySerde, null));
        }

        public IKTable<K, long> count()
            => doCount(Materialized<K, long, IKeyValueStore<Bytes, byte[]>>.With(keySerde, Serdes.Long()));


        public IKTable<K, long> count(Materialized<K, long, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            // TODO: Remove this when we do a topology-incompatible release
            // we used to burn a topology name here, so we have to keep doing it for compatibility
            if (new MaterializedInternal<K, long, IKeyValueStore<Bytes, byte[]>>(materialized).StoreName == null)
            {
                builder.NewStoreName(AGGREGATE_NAME);
            }

            return doCount(materialized);
        }

        private IKTable<K, long> doCount(Materialized<K, long, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            MaterializedInternal<K, long, IKeyValueStore<Bytes, byte[]>> materializedInternal =
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

            return doAggregate(
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

        private IKTable<K, T> doAggregate<T>(
            IKStreamAggProcessorSupplier<K, K, V, T> aggregateSupplier,
            string functionName,
            MaterializedInternal<K, T, IKeyValueStore<Bytes, byte[]>> materializedInternal)
        {
            var tkvsm = new TimestampedKeyValueStoreMaterializer<K, T>(materializedInternal);
            var materialized = tkvsm.materialize();

            return aggregateBuilder.build(
                functionName,
                materialized,
                aggregateSupplier,
                materializedInternal.queryableStoreName(),
                materializedInternal.KeySerde,
                materializedInternal.ValueSerde);
        }
    }
}