using Kafka.Common.Utils;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.State.Internals;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class KGroupedStream<K, V> : AbstractStream<K, V>, IKGroupedStream<K, V>
    {
        static string REDUCE_NAME = "KSTREAM-REDUCE-";
        static string AGGREGATE_NAME = "KSTREAM-AGGREGATE-";

        //private GroupedStreamAggregateBuilder<K, V> aggregateBuilder;

        public KGroupedStream(
            string name,
            HashSet<string> sourceNodes,
            GroupedInternal<K, V> groupedInternal,
            bool repartitionRequired,
            StreamsGraphNode streamsGraphNode,
            InternalStreamsBuilder builder)
            : base(name, groupedInternal.keySerde, groupedInternal.valueSerde, sourceNodes, streamsGraphNode, builder)
        {
            //this.aggregateBuilder = new GroupedStreamAggregateBuilder<>(
            //    builder,
            //    groupedInternal,
            //    repartitionRequired,
            //    sourceNodes,
            //    name,
            //    streamsGraphNode);
        }

        public IKTable<K, V> reduce(IReducer<V> reducer)
        {
            return reduce(reducer, Materialized<K, V, IKeyValueStore<Bytes, byte[]>>.with(keySerde, valSerde));
        }

        public IKTable<K, V> reduce(
            IReducer<V> reducer,
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            reducer = reducer ?? throw new ArgumentNullException(nameof(reducer));
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal =
               new MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>(materialized, builder, REDUCE_NAME);

            if (materializedInternal.keySerde == null)
            {
                materializedInternal.withKeySerde(keySerde);
            }

            if (materializedInternal.valueSerde == null)
            {
                materializedInternal.withValueSerde(valSerde);
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

            if (materializedInternal.keySerde == null)
            {
                materializedInternal.withKeySerde(keySerde);
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
                Materialized<K, VR, IKeyValueStore<Bytes, byte[]>>.with(keySerde, null));
        }

        public IKTable<K, long> count()
            => doCount(Materialized<K, long, IKeyValueStore<Bytes, byte[]>>.with(keySerde, Serdes.Long()));


        public IKTable<K, long> count(Materialized<K, long, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            // TODO: Remove this when we do a topology-incompatible release
            // we used to burn a topology name here, so we have to keep doing it for compatibility
            if (new MaterializedInternal<K, long, IKeyValueStore<Bytes, byte[]>>(materialized).storeName == null)
            {
                builder.NewStoreName(AGGREGATE_NAME);
            }

            return doCount(materialized);
        }

        private IKTable<K, long> doCount(Materialized<K, long, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            MaterializedInternal<K, long, IKeyValueStore<Bytes, byte[]>> materializedInternal =
               new MaterializedInternal<K, long, IKeyValueStore<Bytes, byte[]>>(materialized, builder, AGGREGATE_NAME);

            if (materializedInternal.keySerde == null)
            {
                materializedInternal.withKeySerde(keySerde);
            }

            if (materializedInternal.valueSerde == null)
            {
                materializedInternal.withValueSerde(Serdes.Long());
            }

            return null;
            //doAggregate(
            //    new KStreamAggregate<>(
            //        materializedInternal.storeName,
            //        aggregateBuilder.countInitializer,
            //        aggregateBuilder.countAggregator),
            //    AGGREGATE_NAME,
            //    materializedInternal);
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

        //private IKTable<K, T> doAggregate(
        //    KStreamAggIProcessorSupplier<K, K, V, T> aggregateSupplier,
        //    string functionName,
        //    MaterializedInternal<K, T, IKeyValueStore<Bytes, byte[]>> materializedInternal)
        //{
        //    return aggregateBuilder.build(
        //        functionName,
        //        new TimestampedKeyValueStoreMaterializer<>(materializedInternal).materialize(),
        //        aggregateSupplier,
        //        materializedInternal.queryableStoreName(),
        //        materializedInternal.keySerde,
        //        materializedInternal.valueSerde);
        //}
    }
}