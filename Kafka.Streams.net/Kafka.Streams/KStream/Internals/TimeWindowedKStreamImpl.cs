
//using Confluent.Kafka;
//using Kafka.Common.Utils;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.KStream.Internals.Graph;
//using Kafka.Streams.State;
//using Kafka.Streams.State.Interfaces;
//using Kafka.Streams.State.Internals;
//using System.Collections.Generic;

//namespace Kafka.Streams.KStream.Internals
//{





























//    public class TimeWindowedKStreamImpl<K, V, W> : AbstractStream<K, V>, TimeWindowedKStream<K, V>
//            where W : Window
//    {

//        private Windows<W> windows;
//        private GroupedStreamAggregateBuilder<K, V> aggregateBuilder;

//        public TimeWindowedKStreamImpl(
//            Windows<W> windows,
//            InternalStreamsBuilder builder,
//            HashSet<string> sourceNodes,
//            string Name,
//            ISerde<K> keySerde,
//            ISerde<V> valSerde,
//            GroupedStreamAggregateBuilder<K, V> aggregateBuilder,
//            StreamsGraphNode streamsGraphNode)
//            : base(Name, keySerde, valSerde, sourceNodes, streamsGraphNode, builder)
//        {
//            this.windows = windows = windows ?? throw new ArgumentNullException(nameof(windows));
//            this.aggregateBuilder = aggregateBuilder;
//        }


//        public IKTable<IWindowed<K>, long> count()
//        {
//            return doCount(Materialized.with(keySerde, Serializers.Int64));
//        }


//        public IKTable<IWindowed<K>, long> count(Materialized<K, long, IWindowStore<Bytes, byte[]>> materialized)
//        {
//            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

//            // TODO: Remove this when we do a topology-incompatible release
//            // we used to burn a topology Name here, so we have to keep doing it for compatibility
//            if (new MaterializedInternal<K, long, IWindowStore<Bytes, byte[]>>(materialized).storeName() == null)
//            {
//                builder.newStoreName(AGGREGATE_NAME);
//            }

//            return doCount(materialized);
//        }

//        private IKTable<IWindowed<K>, long> doCount(Materialized<K, long, IWindowStore<Bytes, byte[]>> materialized)
//        {
//            MaterializedInternal<K, long, IWindowStore<Bytes, byte[]>> materializedInternal =
//               new MaterializedInternal<K, long, IWindowStore<Bytes, byte[]>>(materialized, builder, AGGREGATE_NAME);

//            if (materializedInternal.keySerde == null)
//            {
//                materializedInternal.withKeySerde(keySerde);
//            }
//            if (materializedInternal.valueSerde == null)
//            {
//                materializedInternal.withValueSerde(Serdes.Long());
//            }

//            return aggregateBuilder.build(
//                AGGREGATE_NAME,
//                materialize(materializedInternal),
//                new KStreamWindowAggregate<>(windows, materializedInternal.storeName(), aggregateBuilder.countInitializer, aggregateBuilder.countAggregator),
//                materializedInternal.queryableStoreName(),
//                materializedInternal.keySerde != null ? new FullTimeWindowedSerde<>(materializedInternal.keySerde, windows.size()) : null,
//                materializedInternal.valueSerde);
//        }


//        public IKTable<IWindowed<K>, VR> aggregate<VR>(IInitializer<VR> initializer,
//                                                       IAggregator<K, V, VR> aggregator)
//        {
//            return aggregate(initializer, aggregator, Materialized.with(keySerde, null));
//        }


//        public IKTable<IWindowed<K>, VR> aggregate<VR>(IInitializer<VR> initializer,
//                                                       IAggregator<K, V, VR> aggregator,
//                                                       Materialized<K, VR, IWindowStore<Bytes, byte[]>> materialized)
//        {
//            initializer = initializer ?? throw new ArgumentNullException(nameof(initializer));
//            aggregator = aggregator ?? throw new ArgumentNullException(nameof(aggregator));
//            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));
//            MaterializedInternal<K, VR, IWindowStore<Bytes, byte[]>> materializedInternal =
//               new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME);
//            if (materializedInternal.keySerde == null)
//            {
//                materializedInternal.withKeySerde(keySerde);
//            }
//            return aggregateBuilder.build(
//                AGGREGATE_NAME,
//                materialize(materializedInternal),
//                new KStreamWindowAggregate<>(windows, materializedInternal.storeName(), initializer, aggregator),
//                materializedInternal.queryableStoreName(),
//                materializedInternal.keySerde != null ? new FullTimeWindowedSerde<>(materializedInternal.keySerde, windows.size()) : null,
//                materializedInternal.valueSerde);
//        }


//        public IKTable<IWindowed<K>, V> reduce(IReducer<V> reducer)
//        {
//            return reduce(reducer, Materialized.with(keySerde, valSerde));
//        }


//        public IKTable<IWindowed<K>, V> reduce(IReducer<V> reducer, Materialized<K, V, IWindowStore<Bytes, byte[]>> materialized)
//        {
//            reducer = reducer ?? throw new ArgumentNullException(nameof(reducer));
//            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

//            MaterializedInternal<K, V, IWindowStore<Bytes, byte[]>> materializedInternal =
//               new MaterializedInternal<>(materialized, builder, REDUCE_NAME);

//            if (materializedInternal.keySerde == null)
//            {
//                materializedInternal.withKeySerde(keySerde);
//            }
//            if (materializedInternal.valueSerde == null)
//            {
//                materializedInternal.withValueSerde(valSerde);
//            }

//            return aggregateBuilder.build(
//                REDUCE_NAME,
//                materialize(materializedInternal),
//                new KStreamWindowAggregate<>(windows, materializedInternal.storeName(), aggregateBuilder.reduceInitializer, aggregatorForReducer(reducer)),
//                materializedInternal.queryableStoreName(),
//                materializedInternal.keySerde != null ? new FullTimeWindowedSerde<>(materializedInternal.keySerde, windows.size()) : null,
//                materializedInternal.valueSerde);
//        }


//        private IStoreBuilder<ITimestampedWindowStore<K, VR>> materialize<VR>(MaterializedInternal<K, VR, IWindowStore<Bytes, byte[]>> materialized)
//        {
//            IWindowBytesStoreSupplier supplier = (IWindowBytesStoreSupplier)materialized.storeSupplier;
//            if (supplier == null)
//            {
//                if (materialized.retention != null)
//                {
//                    // new style retention: use Materialized retention and default segmentInterval
//                    long retentionPeriod = materialized.retention.toMillis();

//                    if ((windows.size() + windows.gracePeriodMs()) > retentionPeriod)
//                    {
//                        throw new System.ArgumentException("The retention period of the window store "
//                                                               + Name + " must be no smaller than its window size plus the grace period."
//                                                               + " Got size=[" + windows.size() + "],"
//                                                               + " grace=[" + windows.gracePeriodMs() + "],"
//                                                               + " retention=[" + retentionPeriod + "]");
//                    }

//                    supplier = Stores.PersistentTimestampedWindowStore(
//                        materialized.storeName(),
//                        TimeSpan.FromMilliseconds(retentionPeriod),
//                        TimeSpan.FromMilliseconds(windows.size()),
//                        false
//                    );

//                }
//                else
//                {

//                    // old style retention: use deprecated Windows retention/segmentInterval.

//                    // NOTE: in the future, when we Remove Windows#maintainMs(), we should set the default retention
//                    // to be (windows.size() + windows.grace()). This will yield the same default behavior.

//                    if ((windows.size() + windows.gracePeriodMs()) > windows.maintainMs())
//                    {
//                        throw new System.ArgumentException("The retention period of the window store "
//                                                               + Name + " must be no smaller than its window size plus the grace period."
//                                                               + " Got size=[" + windows.size() + "],"
//                                                               + " grace=[" + windows.gracePeriodMs() + "],"
//                                                               + " retention=[" + windows.maintainMs() + "]");
//                    }

//                    supplier = new RocksDbWindowBytesStoreSupplier(
//                        materialized.storeName(),
//                        windows.maintainMs(),
//                        Math.Max(windows.maintainMs() / (windows.segments - 1), 60_000L),
//                        windows.size(),
//                        false,
//                        true);
//                }
//            }
//            IStoreBuilder<ITimestampedWindowStore<K, VR>> builder = Stores.timestampedWindowStoreBuilder(
//               supplier,
//               materialized.keySerde,
//               materialized.valueSerde
//           );

//            if (materialized.loggingEnabled)
//            {
//                builder.withLoggingEnabled(materialized.logConfig());
//            }
//            else
//            {

//                builder.withLoggingDisabled();
//            }

//            if (materialized.cachingEnabled)
//            {
//                builder.withCachingEnabled();
//            }
//            return builder;
//        }

//        private IAggregator<K, V, V> aggregatorForReducer(IReducer<V> reducer)
//        {
//            return null; // (aggKey, value, aggregate) => aggregate == null ? value : reducer.apply(aggregate, value);
//        }
//    }
//}