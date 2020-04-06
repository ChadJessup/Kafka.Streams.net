
//using Kafka.Common.Utils;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.KStream.Internals.Graph;
//using Kafka.Streams.State;
//using Kafka.Streams.State.Interfaces;
//using System.Collections.Generic;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class SessionWindowedKStreamImpl<K, V> : AbstractStream<K, V>, SessionWindowedKStream<K, V>
//    {
//        private SessionWindows windows;
//        private GroupedStreamAggregateBuilder<K, V> aggregateBuilder;
//        private IMerger<K, long> countMerger = null; // (aggKey, aggOne, aggTwo)=>aggOne + aggTwo;

//        public SessionWindowedKStreamImpl(
//            SessionWindows windows,
//            InternalStreamsBuilder builder,
//            HashSet<string> sourceNodes,
//            string name,
//            ISerde<K> keySerde,
//            ISerde<V> valSerde,
//            GroupedStreamAggregateBuilder<K, V> aggregateBuilder,
//            StreamsGraphNode streamsGraphNode)
//            : base(name, keySerde, valSerde, sourceNodes, streamsGraphNode, builder)
//        {
//            windows = windows ?? throw new ArgumentNullException(nameof(windows));
//            this.windows = windows;
//            this.aggregateBuilder = aggregateBuilder;
//        }


//        public IKTable<Windowed<K>, long> count()
//        {
//            return doCount(Materialized.with(keySerde, Serdes.Long()));
//        }


//        public IKTable<Windowed<K>, long> count(Materialized<K, long, ISessionStore<Bytes, byte[]>> materialized)
//        {
//            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

//            // TODO: Remove this when we do a topology-incompatible release
//            // we used to burn a topology name here, so we have to keep doing it for compatibility
//            if (new MaterializedInternal<>(materialized).storeName() == null)
//            {
//                builder.newStoreName(AGGREGATE_NAME);
//            }

//            return doCount(materialized);
//        }

//        private IKTable<Windowed<K>, long> doCount(Materialized<K, long, ISessionStore<Bytes, byte[]>> materialized)
//        {
//            MaterializedInternal<K, long, ISessionStore<Bytes, byte[]>> materializedInternal =
//               new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME);
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
//                new KStreamSessionWindowAggregate<>(
//                    windows,
//                    materializedInternal.storeName(),
//                    aggregateBuilder.countInitializer,
//                    aggregateBuilder.countAggregator,
//                    countMerger),
//                materializedInternal.queryableStoreName(),
//                materializedInternal.keySerde != null ? new WindowedSerdes.SessionWindowedSerde<>(materializedInternal.keySerde) : null,
//                materializedInternal.valueSerde);
//        }


//        public IKTable<Windowed<K>, V> reduce(IReducer<V> reducer)
//        {
//            return reduce(reducer, Materialized.with(keySerde, valSerde));
//        }


//        public IKTable<Windowed<K>, V> reduce(IReducer<V> reducer,
//                                              Materialized<K, V, ISessionStore<Bytes, byte[]>> materialized)
//        {
//            reducer = reducer ?? throw new ArgumentNullException(nameof(reducer));
//            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));
//            IAggregator<K, V, V> reduceAggregator = aggregatorForReducer(reducer);
//            MaterializedInternal<K, V, ISessionStore<Bytes, byte[]>> materializedInternal =
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
//                new KStreamSessionWindowAggregate<>(
//                    windows,
//                    materializedInternal.storeName(),
//                    aggregateBuilder.reduceInitializer,
//                    reduceAggregator,
//                    mergerForAggregator(reduceAggregator)
//                ),
//                materializedInternal.queryableStoreName(),
//                materializedInternal.keySerde != null ? new WindowedSerdes.SessionWindowedSerde<>(materializedInternal.keySerde) : null,
//                materializedInternal.valueSerde);
//        }


//        public IKTable<Windowed<K>, T> aggregate<T>(
//            IInitializer<T> initializer,
//            IAggregator<K, V, T> aggregator,
//            IMerger<K, T> sessionMerger)
//        {
//            return aggregate(initializer, aggregator, sessionMerger, Materialized.with(keySerde, null));
//        }


//        public IKTable<Windowed<K>, VR> aggregate<VR>(
//            IInitializer<VR> initializer,
//            IAggregator<K, V, VR> aggregator,
//            IMerger<K, VR> sessionMerger,
//            Materialized<K, VR, ISessionStore<Bytes, byte[]>> materialized)
//        {
//            initializer = initializer ?? throw new ArgumentNullException(nameof(initializer));
//            aggregator = aggregator ?? throw new ArgumentNullException(nameof(aggregator));
//            sessionMerger = sessionMerger ?? throw new ArgumentNullException(nameof(sessionMerger));
//            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));
//            MaterializedInternal<K, VR, ISessionStore<Bytes, byte[]>> materializedInternal =
//               new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME);

//            if (materializedInternal.keySerde == null)
//            {
//                materializedInternal.withKeySerde(keySerde);
//            }

//            return aggregateBuilder.build(
//                AGGREGATE_NAME,
//                materialize(materializedInternal),
//                new KStreamSessionWindowAggregate<>(
//                    windows,
//                    materializedInternal.storeName(),
//                    initializer,
//                    aggregator,
//                    sessionMerger),
//                materializedInternal.queryableStoreName(),
//                materializedInternal.keySerde != null ? new SessionWindowedSerde<>(materializedInternal.keySerde) : null,
//                materializedInternal.valueSerde);
//        }


//        private IStoreBuilder<ISessionStore<K, VR>> materialize<VR>(MaterializedInternal<K, VR, ISessionStore<Bytes, byte[]>> materialized)
//        {
//            SessionBytesStoreSupplier supplier = (SessionBytesStoreSupplier)materialized.storeSupplier;
//            if (supplier == null)
//            {
//                // NOTE: in the future, when we Remove Windows#maintainMs(), we should set the default retention
//                // to be (windows.inactivityGap() + windows.grace()). This will yield the same default behavior.
//                long retentionPeriod = materialized.retention != null ? materialized.retention.toMillis() : windows.maintainMs();

//                if ((windows.inactivityGap() + windows.gracePeriodMs()) > retentionPeriod)
//                {
//                    throw new System.ArgumentException("The retention period of the session store "
//                                                           + materialized.storeName()
//                                                           + " must be no smaller than the session inactivity gap plus the"
//                                                           + " grace period."
//                                                           + " Got gap=[" + windows.inactivityGap() + "],"
//                                                           + " grace=[" + windows.gracePeriodMs() + "],"
//                                                           + " retention=[" + retentionPeriod + "]");
//                }

//                supplier = Stores.PersistentSessionStore(
//                    materialized.storeName(),
//                    TimeSpan.FromMilliseconds(retentionPeriod)
//                );
//            }
//            IStoreBuilder<ISessionStore<K, VR>> builder = Stores.sessionStoreBuilder(
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

//        private IMerger<K, V> mergerForAggregator(IAggregator<K, V, V> aggregator)
//        {
//            return null; // (aggKey, aggOne, aggTwo) => aggregator.apply(aggKey, aggTwo, aggOne);
//        }

//        private IAggregator<K, V, V> aggregatorForReducer(IReducer<V> reducer)
//        {
//            return null; // (aggKey, value, aggregate) => aggregate == null ? value : reducer.apply(aggregate, value);
//        }
//    }
//}