using Kafka.Common.Utils;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.KStream.Internals.Suppress;
using Kafka.Streams.Processor;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    /**
     * The implementation of {@link KTable}.
     *
     * @param the key type
     * @param the source's (parent's) value type
     * @param the value type
     */
    public class KTableImpl
    {
        public static string SourceName = "KTABLE-SOURCE-";
        public static string StateStoreName = "STATE-STORE-";
        public static string FILTER_NAME = "KTABLE-FILTER-";
        public static string JOINTHIS_NAME = "KTABLE-JOINTHIS-";
        public static string JOINOTHER_NAME = "KTABLE-JOINOTHER-";
        public static string MAPVALUES_NAME = "KTABLE-MAPVALUES-";
        public static string MERGE_NAME = "KTABLE-MERGE-";
        public static string SELECT_NAME = "KTABLE-SELECT-";
        public static string SUPPRESS_NAME = "KTABLE-SUPPRESS-";
        public static string TOSTREAM_NAME = "KTABLE-TOSTREAM-";
        public static string TRANSFORMVALUES_NAME = "KTABLE-TRANSFORMVALUES-";
    }

    //    public class KTableImpl<K, S, V> : AbstractStream<K, V>, IKTable<K, V>
    //    {
    //        private static ILogger LOG = new LoggerFactory().CreateLogger<KTableImpl<K, S, V>>();

    //        private IProcessorSupplier<K, object> IProcessorSupplier;

    //        private string queryableStoreName;

    //        private bool sendOldValues = false;

    //        public KTableImpl(
    //            string name,
    //            ISerde<K> keySerde,
    //            ISerde<V> valSerde,
    //            HashSet<string> sourceNodes,
    //            string queryableStoreName,
    //            IProcessorSupplier<K, object> IProcessorSupplier,
    //            StreamsGraphNode streamsGraphNode,
    //            InternalStreamsBuilder builder)
    //            : base(name, keySerde, valSerde, sourceNodes, streamsGraphNode, builder)
    //        {
    //            this.IProcessorSupplier = IProcessorSupplier;
    //            this.queryableStoreName = queryableStoreName;
    //        }

    //        private IKTable<K, V> doFilter(
    //            IPredicate<K, V> predicate,
    //            Named named,
    //            MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal,
    //            bool filterNot)
    //        {
    //            ISerde<K> keySerde;
    //            ISerde<V> valueSerde;
    //            string queryableStoreName;
    //            IStoreBuilder<ITimestampedKeyValueStore<K, V>> storeBuilder;

    //            if (materializedInternal != null)
    //            {
    //                // we actually do not need to generate store names at all since if it is not specified, we will not
    //                // materialize the store; but we still need to burn one index BEFORE generating the processor to keep compatibility.
    //                if (materializedInternal.storeName() == null)
    //                {
    //                    builder.newStoreName(KTableImpl.FILTER_NAME);
    //                }
    //                // we can inherit parent key and value serde if user do not provide specific overrides, more specifically:
    //                // we preserve the key following the order of 1) materialized, 2) parent
    //                keySerde = materializedInternal.keySerde != null ? materializedInternal.keySerde : this.keySerde;
    //                // we preserve the value following the order of 1) materialized, 2) parent
    //                valueSerde = materializedInternal.valueSerde != null ? materializedInternal.valueSerde : this.valSerde;
    //                queryableStoreName = materializedInternal.queryableStoreName();
    //                // only materialize if materialized is specified and it has queryable name
    //                storeBuilder = queryableStoreName != null ? (new TimestampedKeyValueStoreMaterializer<K, V>(materializedInternal)).materialize() : null;
    //            }
    //            else
    //            {
    //                keySerde = this.keySerde;
    //                valueSerde = this.valSerde;
    //                queryableStoreName = null;
    //                storeBuilder = null;
    //            }
    //            string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, FILTER_NAME);

    //            IKTableProcessorSupplier<K, V, V> IProcessorSupplier =
    //               new KTableFilter<K, V>(this, predicate, filterNot, queryableStoreName);

    //            ProcessorParameters<K, V> processorParameters = unsafeCastProcessorParametersToCompletelyDifferentType(
    //               new ProcessorParameters<K, V>(IProcessorSupplier, name)
    //           );

    //            StreamsGraphNode tableNode = new TableProcessorNode<K, V>(
    //               name,
    //               processorParameters,
    //               storeBuilder
    //           );

    //            builder.addGraphNode(this.streamsGraphNode, tableNode);

    //            return new KTableImpl<K, S, V>(
    //                name,
    //                keySerde,
    //                valueSerde,
    //                sourceNodes,
    //                queryableStoreName,
    //                IProcessorSupplier,
    //                tableNode,
    //                builder);
    //        }


    //        public IKTable<K, V> filter(IPredicate<K, V> predicate)
    //        {
    //            predicate = predicate ?? throw new System.ArgumentNullException("predicate can't be null", nameof(predicate));
    //            return doFilter(predicate, NamedInternal.empty(), null, false);
    //        }


    //        public IKTable<K, V> filter(IPredicate<K, V> predicate, Named named)
    //        {
    //            predicate = predicate ?? throw new System.ArgumentNullException("predicate can't be null", nameof(predicate));
    //            return doFilter(predicate, named, null, false);
    //        }


    //        public IKTable<K, V> filter(
    //            IPredicate<K, V> predicate,
    //            Named named,
    //            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
    //        {
    //            predicate = predicate ?? throw new System.ArgumentNullException("predicate can't be null", nameof(predicate));
    //            materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));
    //            MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);

    //            return doFilter(predicate, named, materializedInternal, false);
    //        }


    //        public IKTable<K, V> filter(IPredicate<K, V> predicate,
    //                                    Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
    //        {
    //            return filter(predicate, NamedInternal.empty(), materialized);
    //        }

    //        public IKTable<K, V> filterNot(IPredicate<K, V> predicate)
    //        {
    //            predicate = predicate ?? throw new System.ArgumentNullException("predicate can't be null", nameof(predicate));
    //            return doFilter(predicate, NamedInternal.empty(), null, true);
    //        }


    //        public IKTable<K, V> filterNot(
    //            IPredicate<K, V> predicate,
    //            Named named)
    //        {
    //            predicate = predicate ?? throw new System.ArgumentNullException("predicate can't be null", nameof(predicate));
    //            return doFilter(predicate, named, null, true);
    //        }


    //        public IKTable<K, V> filterNot(
    //            IPredicate<K, V> predicate,
    //            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
    //        {
    //            return filterNot(predicate, NamedInternal.empty(), materialized);
    //        }


    //        public IKTable<K, V> filterNot(
    //            IPredicate<K, V> predicate,
    //            Named named,
    //            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
    //        {
    //            predicate = predicate ?? throw new System.ArgumentNullException("predicate can't be null", nameof(predicate));
    //            materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));
    //            MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);
    //            NamedInternal renamed = new NamedInternal(named);
    //            return doFilter(predicate, renamed, materializedInternal, true);
    //        }

    //        private IKTable<K, VR> doMapValues<VR>(
    //            IValueMapperWithKey<K, V, VR> mapper,
    //            Named named,
    //            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal)
    //        {
    //            ISerde<K> keySerde;
    //            ISerde<VR> valueSerde;
    //            string queryableStoreName;
    //            IStoreBuilder<ITimestampedKeyValueStore<K, VR>> storeBuilder;

    //            if (materializedInternal != null)
    //            {
    //                // we actually do not need to generate store names at all since if it is not specified, we will not
    //                // materialize the store; but we still need to burn one index BEFORE generating the processor to keep compatibility.
    //                if (materializedInternal.storeName() == null)
    //                {
    //                    builder.newStoreName(KTableImpl.MAPVALUES_NAME);
    //                }
    //                keySerde = materializedInternal.keySerde != null ? materializedInternal.keySerde : this.keySerde;
    //                valueSerde = materializedInternal.valueSerde;
    //                queryableStoreName = materializedInternal.queryableStoreName();
    //                // only materialize if materialized is specified and it has queryable name
    //                storeBuilder = queryableStoreName != null ? (new TimestampedKeyValueStoreMaterializer<K, VR>(materializedInternal)).materialize() : null;
    //            }
    //            else
    //            {
    //                keySerde = this.keySerde;
    //                valueSerde = null;
    //                queryableStoreName = null;
    //                storeBuilder = null;
    //            }

    //            string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, MAPVALUES_NAME);

    //            IKTableProcessorSupplier<K, V, VR> IProcessorSupplier = new KTableMapValues<K, V, VR>(this, mapper, queryableStoreName);

    //            // leaving in calls to ITB until building topology with graph

    //            ProcessorParameters<K, VR> processorParameters = unsafeCastProcessorParametersToCompletelyDifferentType(
    //               new ProcessorParameters<K, VR>(IProcessorSupplier, name)
    //           );
    //            StreamsGraphNode tableNode = new TableProcessorNode<K, VR>(
    //               name,
    //               processorParameters,
    //               storeBuilder
    //           );

    //            builder.addGraphNode(this.streamsGraphNode, tableNode);

    //            // don't inherit parent value serde, since this operation may change the value type, more specifically:
    //            // we preserve the key following the order of 1) materialized, 2) parent, 3) null
    //            // we preserve the value following the order of 1) materialized, 2) null
    //            return new KTableImpl<K, VR>(
    //                name,
    //                keySerde,
    //                valueSerde,
    //                sourceNodes,
    //                queryableStoreName,
    //                IProcessorSupplier,
    //                tableNode,
    //                builder
    //            );
    //        }


    //        public IKTable<K, VR> mapValues<VR>(IValueMapper<V, VR> mapper)
    //        {
    //            mapper = mapper ?? throw new System.ArgumentNullException("mapper can't be null", nameof(mapper));
    //            return doMapValues(withKey(mapper), NamedInternal.empty(), null);
    //        }


    //        public IKTable<K, VR> mapValues<VR>(IValueMapper<V, VR> mapper,
    //                                             Named named)
    //        {
    //            mapper = mapper ?? throw new System.ArgumentNullException("mapper can't be null", nameof(mapper));
    //            return doMapValues(withKey(mapper), named, null);
    //        }


    //        public IKTable<K, VR> mapValues<VR>(IValueMapperWithKey<K, V, VR> mapper)
    //        {
    //            mapper = mapper ?? throw new System.ArgumentNullException("mapper can't be null", nameof(mapper));
    //            return doMapValues(mapper, NamedInternal.empty(), null);
    //        }


    //        public IKTable<K, VR> mapValues<VR>(
    //            IValueMapperWithKey<K, V, VR> mapper,
    //            Named named)
    //        {
    //            mapper = mapper ?? throw new System.ArgumentNullException("mapper can't be null", nameof(mapper));
    //            return doMapValues(mapper, named, null);
    //        }


    //        public IKTable<K, VR> mapValues<VR>(
    //            IValueMapper<V, VR> mapper,
    //            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
    //        {
    //            return mapValues(mapper, NamedInternal.empty(), materialized);
    //        }


    //        public IKTable<K, VR> mapValues<VR>(
    //            IValueMapper<V, VR> mapper,
    //            Named named,
    //            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
    //        {
    //            mapper = mapper ?? throw new System.ArgumentNullException("mapper can't be null", nameof(mapper));
    //            materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));

    //            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);

    //            return doMapValues(withKey(mapper), named, materializedInternal);
    //        }


    //        public IKTable<K, VR> mapValues<VR>(
    //            IValueMapperWithKey<K, V, VR> mapper,
    //            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
    //        {
    //            return mapValues(mapper, NamedInternal.empty(), materialized);
    //        }


    //        public IKTable<K, VR> mapValues<VR>(
    //            IValueMapperWithKey<K, V, VR> mapper,
    //            Named named,
    //            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
    //        {
    //            mapper = mapper ?? throw new System.ArgumentNullException("mapper can't be null", nameof(mapper));
    //            materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));

    //            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);

    //            return doMapValues(mapper, named, materializedInternal);
    //        }


    //        public IKTable<K, VR> transformValues<VR>(
    //            IValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
    //            string[] stateStoreNames)
    //        {
    //            return doTransformValues(transformerSupplier, null, NamedInternal.empty(), stateStoreNames);
    //        }


    //        public IKTable<K, VR> transformValues<VR>(IValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
    //                                                   Named named,
    //                                                   string[] stateStoreNames)
    //        {
    //            named = named ?? throw new System.ArgumentNullException("processorName can't be null", nameof(named));
    //            return doTransformValues(transformerSupplier, null, new NamedInternal(named), stateStoreNames);
    //        }


    //        public IKTable<K, VR> transformValues<VR>(
    //            IValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
    //            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized,
    //            string[] stateStoreNames)
    //        {
    //            return transformValues(transformerSupplier, materialized, NamedInternal.empty(), stateStoreNames);
    //        }


    //        public IKTable<K, VR> transformValues<VR>(
    //            IValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
    //            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized,
    //            Named named,
    //            string[] stateStoreNames)
    //        {
    //            materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));
    //            named = named ?? throw new System.ArgumentNullException("named can't be null", nameof(named));
    //            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);

    //            return doTransformValues(transformerSupplier, materializedInternal, new NamedInternal(named), stateStoreNames);
    //        }

    //        private IKTable<K, VR> doTransformValues<VR>(
    //            IValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
    //            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal,
    //            NamedInternal namedInternal,
    //            string[] stateStoreNames)
    //        {
    //            stateStoreNames = stateStoreNames ?? throw new System.ArgumentNullException("stateStoreNames", nameof(stateStoreNames));
    //            ISerde<K> keySerde;
    //            ISerde<VR> valueSerde;
    //            string queryableStoreName;
    //            IStoreBuilder<ITimestampedKeyValueStore<K, VR>> storeBuilder;

    //            if (materializedInternal != null)
    //            {
    //                // don't inherit parent value serde, since this operation may change the value type, more specifically:
    //                // we preserve the key following the order of 1) materialized, 2) parent, 3) null
    //                keySerde = materializedInternal.keySerde != null ? materializedInternal.keySerde : this.keySerde;
    //                // we preserve the value following the order of 1) materialized, 2) null
    //                valueSerde = materializedInternal.valueSerde;
    //                queryableStoreName = materializedInternal.queryableStoreName();
    //                // only materialize if materialized is specified and it has queryable name
    //                storeBuilder = queryableStoreName != null ? (new TimestampedKeyValueStoreMaterializer<K, V>(materializedInternal)).materialize() : null;
    //            }
    //            else
    //            {
    //                keySerde = this.keySerde;
    //                valueSerde = null;
    //                queryableStoreName = null;
    //                storeBuilder = null;
    //            }

    //            string name = namedInternal.orElseGenerateWithPrefix(builder, KTableImpl.TRANSFORMVALUES_NAME);

    //            IKTableProcessorSupplier<K, V, VR> IProcessorSupplier = new KTableTransformValues<K, V, VR>(
    //               this,
    //               transformerSupplier,
    //               queryableStoreName);

    //            ProcessorParameters<K, VR> processorParameters = unsafeCastProcessorParametersToCompletelyDifferentType(
    //               new ProcessorParameters<K, VR>(IProcessorSupplier, name)
    //           );

    //            StreamsGraphNode tableNode = new TableProcessorNode<K, V>(
    //               name,
    //               processorParameters,
    //               storeBuilder,
    //               stateStoreNames
    //           );

    //            builder.addGraphNode(this.streamsGraphNode, tableNode);

    //            return new KTableImpl<K, V, VR>(
    //                name,
    //                keySerde,
    //                valueSerde,
    //                sourceNodes,
    //                queryableStoreName,
    //                IProcessorSupplier,
    //                tableNode,
    //                builder);
    //        }


    //        public IKStream<K, V> toStream()
    //        {
    //            return toStream(NamedInternal.empty());
    //        }


    //        public IKStream<K, V> toStream(Named named)
    //        {
    //            named = named ?? throw new System.ArgumentNullException("named can't be null", nameof(named));

    //            string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, KTableImpl.TOSTREAM_NAME);
    //            IProcessorSupplier<K, Change<V>> kStreamMapValues = new KStreamMapValues<K, V, Change<V>>((key, change) => change.newValue);
    //            ProcessorParameters<K, V> processorParameters = unsafeCastProcessorParametersToCompletelyDifferentType<V>(
    //               new ProcessorParameters<K, Change<V>>(kStreamMapValues, name)
    //           );

    //            ProcessorGraphNode<K, V> toStreamNode = new ProcessorGraphNode<K, V>(
    //               name,
    //               processorParameters
    //           );

    //            builder.addGraphNode(this.streamsGraphNode, toStreamNode);

    //            // we can inherit parent key and value serde
    //            return new KStreamImpl<K, V>(name, keySerde, valSerde, sourceNodes, false, toStreamNode, builder);
    //        }


    //        public IKStream<K1, V> toStream<K1>(IKeyValueMapper<K, V, K1> mapper)
    //        {
    //            return toStream().selectKey(mapper);
    //        }


    //        public IKStream<K1, V> toStream<K1>(
    //            IKeyValueMapper<K, V, K1> mapper,
    //            Named named)
    //        {
    //            return toStream(named).selectKey(mapper);
    //        }


    //        public IKTable<K, V> suppress(ISuppressed<K> suppressed)
    //        {
    //            string name;
    //            if (suppressed is INamedSuppressed<K>)
    //            {
    //                string givenName = ((INamedSuppressed<object>)suppressed).name;
    //                name = givenName != null ? givenName : builder.newProcessorName(SUPPRESS_NAME);
    //            }
    //            else
    //            {
    //                throw new System.ArgumentException("Custom sues of Suppressed are not supported.");
    //            }

    //            SuppressedInternal<K, V> suppressedInternal = buildSuppress(suppressed, name);

    //            string storeName =
    //               suppressedInternal.name != null
    //               ? suppressedInternal.name + "-store"
    //               : builder.newStoreName(KTableImpl.SUPPRESS_NAME);

    //            IProcessorSupplier<K, Change<V>> suppressionSupplier = new KTableSuppressProcessorSupplier<K, V>(
    //               suppressedInternal,
    //               storeName,
    //               this
    //           );

    //            ProcessorGraphNode<K, Change<V>> node = new StatefulProcessorNode<>(
    //               name,
    //               new ProcessorParameters<>(suppressionSupplier, name),
    //               new InMemoryTimeOrderedKeyValueBuffer.Builder<>(storeName, keySerde, valSerde)
    //           );

    //            builder.addGraphNode(streamsGraphNode, node);

    //            return new KTableImpl<K, S, V>(
    //                name,
    //                keySerde,
    //                valSerde,
    //                new HashSet<string> { this.name },
    //                null,
    //                suppressionSupplier,
    //                node,
    //                builder
    //            );
    //        }


    //        private SuppressedInternal<K> buildSuppress(ISuppressed<K> suppress, string name)
    //        {
    //            if (suppress is FinalResultsSuppressionBuilder<K>)
    //            {
    //                long grace = findAndVerifyWindowGrace(streamsGraphNode);
    //                LOG.LogInformation("Using grace period of [{}] as the suppress duration for node [{}].",
    //                         DateTime.ofMillis(grace), name);

    //                FinalResultsSuppressionBuilder<object> builder = (FinalResultsSuppressionBuilder<object>)suppress;

    //                SuppressedInternal<object> finalResultsSuppression =
    //                   builder.buildFinalResultsSuppression(TimeSpan.ofMillis(grace));

    //                return (SuppressedInternal<K>)finalResultsSuppression;
    //            }
    //            else if (suppress is SuppressedInternal<K>)
    //            {
    //                return (SuppressedInternal<K>)suppress;
    //            }
    //            else
    //            {
    //                throw new System.ArgumentException("Custom sues of Suppressed are not allowed.");
    //            }
    //        }

    //        public IKTable<K, R> join<R, V1>(IKTable<K, V1> other,
    //                                          IValueJoiner<V, V1, R> joiner)
    //        {
    //            return doJoin(other, joiner, NamedInternal.empty(), null, false, false);
    //        }


    //        public IKTable<K, R> join<R, V1>(IKTable<K, V1> other,
    //                                          IValueJoiner<V, V1, R> joiner,
    //                                          Named named)
    //        {
    //            return doJoin(other, joiner, named, null, false, false);
    //        }


    //        public IKTable<K, VR> join<VR, VO>(
    //            IKTable<K, VO> other,
    //            IValueJoiner<V, VO, VR> joiner,
    //            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
    //        {
    //            return join(other, joiner, NamedInternal.empty(), materialized);
    //        }


    //        public IKTable<K, VR> join<VR, VO>(IKTable<K, VO> other,
    //                                            IValueJoiner<V, VO, VR> joiner,
    //                                            Named named,
    //                                            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
    //        {
    //            materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));
    //            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal =
    //               new MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>(materialized, builder, KTableImpl.MERGE_NAME);

    //            return doJoin(other, joiner, named, materializedInternal, false, false);
    //        }


    //        public IKTable<K, R> outerJoin<R, V1>(
    //            IKTable<K, V1> other,
    //            IValueJoiner<V, V1, R> joiner)
    //        {
    //            return outerJoin(other, joiner, NamedInternal.empty());
    //        }


    //        public IKTable<K, R> outerJoin<R, V1>(IKTable<K, V1> other,
    //                                               IValueJoiner<V, V1, R> joiner,
    //                                               Named named)
    //        {
    //            return doJoin(other, joiner, named, null, true, true);
    //        }


    //        public IKTable<K, VR> outerJoin<VR, VO>(
    //            IKTable<K, VO> other,
    //            IValueJoiner<V, VO, VR> joiner,
    //            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
    //        {
    //            return outerJoin(other, joiner, NamedInternal.empty(), materialized);
    //        }


    //        public IKTable<K, VR> outerJoin<VR, VO>(
    //            IKTable<K, VO> other,
    //            IValueJoiner<V, VO, VR> joiner,
    //            Named named,
    //            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
    //        {
    //            materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));
    //            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal =
    //               new MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>(materialized, builder, KTableImpl.MERGE_NAME);

    //            return doJoin(other, joiner, named, materializedInternal, true, true);
    //        }


    //        public IKTable<K, R> leftJoin<R, V1>(
    //            IKTable<K, V1> other,
    //            IValueJoiner<V, V1, R> joiner)
    //        {
    //            return leftJoin(other, joiner, NamedInternal.empty());
    //        }


    //        public IKTable<K, R> leftJoin<R, V1>(
    //            IKTable<K, V1> other,
    //            IValueJoiner<V, V1, R> joiner,
    //            Named named)
    //        {
    //            return doJoin(other, joiner, named, null, true, false);
    //        }


    //        public IKTable<K, VR> leftJoin<VR, VO>(
    //            IKTable<K, VO> other,
    //            IValueJoiner<V, VO, VR> joiner,
    //            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
    //        {
    //            return leftJoin(other, joiner, NamedInternal.empty(), materialized);
    //        }


    //        public IKTable<K, VR> leftJoin<VR, VO>(
    //            IKTable<K, VO> other,
    //            IValueJoiner<V, VO, VR> joiner,
    //            Named named,
    //            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
    //        {
    //            materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));
    //            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal =
    //               new MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>(materialized, builder, MERGE_NAME);

    //            return doJoin(other, joiner, named, materializedInternal, true, false);
    //        }


    //        private IKTable<K, VR> doJoin<VR, VO>(
    //            IKTable<K, VO> other,
    //            IValueJoiner<V, VO, VR> joiner,
    //            Named joinName,
    //            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal,
    //            bool leftOuter,
    //            bool rightOuter)
    //        {
    //            other = other ?? throw new System.ArgumentNullException("other can't be null", nameof(other));
    //            joiner = joiner ?? throw new System.ArgumentNullException("joiner can't be null", nameof(joiner));
    //            joinName = joinName ?? throw new System.ArgumentNullException("joinName can't be null", nameof(joinName));

    //            NamedInternal renamed = new NamedInternal(joinName);
    //            string joinMergeName = renamed.orElseGenerateWithPrefix(builder, MERGE_NAME);
    //            HashSet<string> allSourceNodes = ensureJoinableWith((AbstractStream<K, VO>)other);

    //            if (leftOuter)
    //            {
    //                enableSendingOldValues();
    //            }
    //            if (rightOuter)
    //            {
    //                ((KTableImpl)other).enableSendingOldValues();
    //            }

    //            KTableKTableAbstractJoin<K, VR, V, VO> joinThis;
    //            KTableKTableAbstractJoin<K, VR, VO, V> joinOther;

    //            if (!leftOuter)
    //            { // inner
    //                joinThis = new KTableKTableInnerJoin<>(this, (KTableImpl<K, object, VO>)other, joiner);
    //                joinOther = new KTableKTableInnerJoin<>((KTableImpl<K, object, VO>)other, this, reverseJoiner(joiner));
    //            }
    //            else if (!rightOuter)
    //            { // left
    //                joinThis = new KTableKTableLeftJoin<>(this, (KTableImpl<K, object, VO>)other, joiner);
    //                joinOther = new KTableKTableRightJoin<>((KTableImpl<K, object, VO>)other, this, reverseJoiner(joiner));
    //            }
    //            else
    //            { // outer
    //                joinThis = new KTableKTableOuterJoin<>(this, (KTableImpl<K, object, VO>)other, joiner);
    //                joinOther = new KTableKTableOuterJoin<>((KTableImpl<K, object, VO>)other, this, reverseJoiner(joiner));
    //            }

    //            string joinThisName = renamed.suffixWithOrElseGet("-join-this", builder, JOINTHIS_NAME);
    //            string joinOtherName = renamed.suffixWithOrElseGet("-join-other", builder, JOINOTHER_NAME);

    //            ProcessorParameters<K, Change<V>> joinThisProcessorParameters = new ProcessorParameters<>(joinThis, joinThisName);
    //            ProcessorParameters<K, Change<VO>> joinOtherProcessorParameters = new ProcessorParameters<>(joinOther, joinOtherName);

    //            ISerde<K> keySerde;
    //            ISerde<VR> valueSerde;
    //            string queryableStoreName;
    //            StoreBuilder<ITimestampedKeyValueStore<K, VR>> storeBuilder;

    //            if (materializedInternal != null)
    //            {
    //                keySerde = materializedInternal.keySerde != null ? materializedInternal.keySerde : this.keySerde;
    //                valueSerde = materializedInternal.valueSerde;
    //                queryableStoreName = materializedInternal.storeName();
    //                storeBuilder = new TimestampedKeyValueStoreMaterializer<>(materializedInternal).materialize();
    //            }
    //            else
    //            {
    //                keySerde = this.keySerde;
    //                valueSerde = null;
    //                queryableStoreName = null;
    //                storeBuilder = null;
    //            }

    //            KTableKTableJoinNode<K, V, VO, VR> kTableKTableJoinNode =
    //               KTableKTableJoinNode<K, V, VO, VR>.kTableKTableJoinNodeBuilder()
    //                    .withNodeName(joinMergeName)
    //                    .withJoinThisProcessorParameters(joinThisProcessorParameters)
    //                    .withJoinOtherProcessorParameters(joinOtherProcessorParameters)
    //                    .withThisJoinSideNodeName(name)
    //                    .withOtherJoinSideNodeName(((KTableImpl)other).name)
    //                    .withJoinThisStoreNames(valueGetterSupplier().storeNames())
    //                    .withJoinOtherStoreNames(((KTableImpl)other).valueGetterSupplier().storeNames())
    //                    .withKeySerde(keySerde)
    //                    .withValueSerde(valueSerde)
    //                    .withQueryableStoreName(queryableStoreName)
    //                    .withStoreBuilder(storeBuilder)
    //                    .build();

    //            builder.addGraphNode(this.streamsGraphNode, kTableKTableJoinNode);

    //            // we can inherit parent key serde if user do not provide specific overrides
    //            return new KTableImpl<K, Change<VR>, VR>(
    //                kTableKTableJoinNode.nodeName,
    //                kTableKTableJoinNode.keySerde,
    //                kTableKTableJoinNode.valueSerde,
    //                allSourceNodes,
    //                kTableKTableJoinNode.queryableStoreName(),
    //                kTableKTableJoinNode.joinMerger(),
    //                kTableKTableJoinNode,
    //                builder);
    //        }


    //        public IKGroupedTable<K1, V1> groupBy<K1, V1>(IKeyValueMapper<K, V, KeyValue<K1, V1>> selector)
    //        {
    //            return groupBy(selector, Grouped<K, V>.With(null, null));
    //        }


    //        [System.Obsolete]
    //        public IKGroupedTable<K1, V1> groupBy<K1, V1>(
    //            IKeyValueMapper<K, V, KeyValue<K1, V1>> selector,
    //            ISerialized<K1, V1> serialized)
    //        {
    //            selector = selector ?? throw new System.ArgumentNullException("selector can't be null", nameof(selector));
    //            serialized = serialized ?? throw new System.ArgumentNullException("serialized can't be null", nameof(serialized));
    //            SerializedInternal<K1, V1> serializedInternal = new SerializedInternal<K1, V1>(serialized);
    //            return groupBy(selector, Grouped<K1, V1>.With(serializedInternal.keySerde, serializedInternal.valueSerde));
    //        }


    //        public IKGroupedTable<K1, V1> groupBy<K1, V1>(
    //            IKeyValueMapper<K, V, KeyValue<K1, V1>> selector,
    //            Grouped<K1, V1> grouped)
    //        {
    //            selector = selector ?? throw new System.ArgumentNullException("selector can't be null", nameof(selector));
    //            grouped = grouped ?? throw new System.ArgumentNullException("grouped can't be null", nameof(grouped));
    //            GroupedInternal<K1, V1> groupedInternal = new GroupedInternal<K1, V1>(grouped);
    //            string selectName = new NamedInternal(groupedInternal.name).orElseGenerateWithPrefix(builder, SELECT_NAME);

    //            IKTableProcessorSupplier<K, V, KeyValue<K1, V1>> selectSupplier = new KTableRepartitionMap<K, V, K1, V1>(this, selector);
    //            ProcessorParameters<K, Change<V>> processorParameters = new ProcessorParameters<K, Change<V>>(selectSupplier, selectName);

    //            // select the aggregate key and values (old and new), it would require parent to send old values
    //            ProcessorGraphNode<K, Change<V>> groupByMapNode = new ProcessorGraphNode<K, Change<V>>(selectName, processorParameters);

    //            builder.addGraphNode(this.streamsGraphNode, groupByMapNode);

    //            this.enableSendingOldValues();
    //            return new KGroupedTableImpl<K1, V1>(
    //                builder,
    //                selectName,
    //                sourceNodes,
    //                groupedInternal,
    //                groupByMapNode
    //            );
    //        }


    //        public IKTableValueGetterSupplier<K, V> valueGetterSupplier()
    //        {
    //            if (IProcessorSupplier is KTableSource<K, V>)
    //            {
    //                KTableSource<K, V> source = (KTableSource<K, V>)IProcessorSupplier;
    //                // whenever a source ktable is required for getter, it should be materialized
    //                source.materialize();
    //                return new KTableSourceValueGetterSupplier<K, V>(source.queryableName());
    //            }
    //            else if (IProcessorSupplier is IKStreamAggProcessorSupplier<K, V>)
    //            {
    //                return ((KStreamAggIIProcessorSupplier<object, K, S, V>)IProcessorSupplier).view();
    //            }
    //            else
    //            {
    //                return ((IKTableProcessorSupplier<K, S, V>)IProcessorSupplier).view();
    //            }
    //        }


    //        public void enableSendingOldValues()
    //        {
    //            if (!sendOldValues)
    //            {
    //                if (IProcessorSupplier is KTableSource<K, V>)
    //                {
    //                    KTableSource<K, object> source = (KTableSource<K, object>)IProcessorSupplier;
    //                    source.enableSendingOldValues();
    //                }
    //                else if (IProcessorSupplier is KStreamAggIIProcessorSupplier)
    //                {
    //                    ((KStreamAggIIProcessorSupplier<object, K, S, V>)IProcessorSupplier).enableSendingOldValues();
    //                }
    //                else
    //                {
    //                    ((IKTableProcessorSupplier<K, S, V>)IProcessorSupplier).enableSendingOldValues();
    //                }
    //                sendOldValues = true;
    //            }
    //        }

    //        bool sendingOldValueEnabled()
    //        {
    //            return sendOldValues;
    //        }

    //        /**
    //         * We conflate V with Change<V> in many places. It might be nice to fix that eventually.
    //         * For now, I'm just explicitly lying about the parameterized type.
    //         */

    //        private ProcessorParameters<K, VR> unsafeCastProcessorParametersToCompletelyDifferentType<VR>(ProcessorParameters<K, Change<V>> kObjectProcessorParameters)
    //        {
    //            return (ProcessorParameters<K, VR>)kObjectProcessorParameters;
    //        }
    //}
}