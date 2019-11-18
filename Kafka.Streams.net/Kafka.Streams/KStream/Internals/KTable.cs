using Kafka.Common.Utils;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.KStream.Internals.Suppress;
using Kafka.Streams.Processors;
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
    public class KTable
    {
        public readonly static string SourceName = "KTABLE-SOURCE-";
        public readonly static string StateStoreName = "STATE-STORE-";
        public readonly static string FILTER_NAME = "KTABLE-FILTER-";
        public readonly static string JOINTHIS_NAME = "KTABLE-JOINTHIS-";
        public readonly static string JOINOTHER_NAME = "KTABLE-JOINOTHER-";
        public readonly static string MAPVALUES_NAME = "KTABLE-MAPVALUES-";
        public readonly static string MERGE_NAME = "KTABLE-MERGE-";
        public readonly static string SELECT_NAME = "KTABLE-SELECT-";
        public readonly static string SUPPRESS_NAME = "KTABLE-SUPPRESS-";
        public readonly static string TOSTREAM_NAME = "KTABLE-TOSTREAM-";
        public readonly static string TRANSFORMVALUES_NAME = "KTABLE-TRANSFORMVALUES-";
    }

    public class KTable<K, S, V> : AbstractStream<K, V>, IKTable<K, V>
    {
        private static readonly ILogger LOG = new LoggerFactory().CreateLogger<KTable<K, S, V>>();
        private readonly IProcessorSupplier<K, V> IProcessorSupplier;
        private readonly string queryableStoreName;
        private bool sendOldValues = false;
        private readonly StatefulProcessorNode<K, V> statefulProcessorNode;

        public KTable(
            string name,
            ISerde<K> keySerde,
            ISerde<V> valSerde,
            HashSet<string> sourceNodes,
            string queryableStoreName,
            IProcessorSupplier<K, V> IProcessorSupplier,
            StreamsGraphNode streamsGraphNode,
            InternalStreamsBuilder builder)
            : base(name, keySerde, valSerde, sourceNodes, streamsGraphNode, builder)
        {
            this.IProcessorSupplier = IProcessorSupplier;
            this.queryableStoreName = queryableStoreName;
        }

        private IKTable<K, V> doFilter(
            IPredicate<K, V> predicate,
            Named named,
            MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal,
            bool filterNot)
        {
            ISerde<K> keySerde;
            ISerde<V> valueSerde;
            string queryableStoreName;
            IStoreBuilder<ITimestampedKeyValueStore<K, V>> storeBuilder;

            if (materializedInternal != null)
            {
                // we actually do not need to generate store names at all since if it is not specified, we will not
                // materialize the store; but we still need to burn one index BEFORE generating the processor to keep compatibility.
                if (materializedInternal.StoreName == null)
                {
                    builder.NewStoreName(KTable.FILTER_NAME);
                }

                // we can inherit parent key and value serde if user do not provide specific overrides, more specifically:
                // we preserve the key following the order of 1) materialized, 2) parent
                keySerde = materializedInternal.KeySerde != null ? materializedInternal.KeySerde : this.keySerde;

                // we preserve the value following the order of 1) materialized, 2) parent
                valueSerde = materializedInternal.ValueSerde != null ? materializedInternal.ValueSerde : this.valSerde;
                queryableStoreName = materializedInternal.queryableStoreName();

                // only materialize if materialized is specified and it has queryable name
                storeBuilder = queryableStoreName != null
                    ? (new TimestampedKeyValueStoreMaterializer<K, V>(materializedInternal)).materialize()
                    : null;
            }
            else
            {
                keySerde = this.keySerde;
                valueSerde = this.valSerde;
                queryableStoreName = null;
                storeBuilder = null;
            }

            string name = new NamedInternal(named).OrElseGenerateWithPrefix(builder, KTable.FILTER_NAME);

            //IKTableProcessorSupplier<K, V, V> IProcessorSupplier =
            //   new KTableFilter<K, V, VR>(this, predicate, filterNot, queryableStoreName);

            //ProcessorParameters<K, V> processorParameters = unsafeCastProcessorParametersToCompletelyDifferentType(
            //   new ProcessorParameters<K, V>(IProcessorSupplier, name));

            //StreamsGraphNode tableNode = new TableProcessorNode<K, V>(
            //   name,
            //   processorParameters,
            //   storeBuilder);

            // builder.AddGraphNode(this.streamsGraphNode, tableNode);

            return null;
            //new KTable<K, V, VR>(
            //    name,
            //    keySerde,
            //    valueSerde,
            //    sourceNodes,
            //    queryableStoreName,
            //    IProcessorSupplier,
            //    tableNode,
            //    builder);
        }

        public IKTable<K, V> filter(IPredicate<K, V> predicate)
        {
            predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));
            return doFilter(predicate, NamedInternal.empty(), null, false);
        }

        public IKTable<K, V> filter(IPredicate<K, V> predicate, Named named)
        {
            predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));

            return doFilter(predicate, named, null, false);
        }

        public IKTable<K, V> filter(
            IPredicate<K, V> predicate,
            Named named,
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));
            MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>(materialized);

            return doFilter(predicate, named, materializedInternal, false);
        }


        public IKTable<K, V> filter(IPredicate<K, V> predicate,
                                    Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return filter(predicate, NamedInternal.empty(), materialized);
        }

        public IKTable<K, V> filterNot(IPredicate<K, V> predicate)
        {
            predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));

            return doFilter(predicate, NamedInternal.empty(), null, true);
        }

        public IKTable<K, V> filterNot(
            IPredicate<K, V> predicate,
            Named named)
        {
            predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));

            return doFilter(predicate, named, null, true);
        }


        public IKTable<K, V> filterNot(
            IPredicate<K, V> predicate,
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return filterNot(predicate, NamedInternal.empty(), materialized);
        }

        public IKTable<K, V> filterNot(
            IPredicate<K, V> predicate,
            Named named,
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>(materialized);
            NamedInternal renamed = new NamedInternal(named);

            return doFilter(predicate, renamed, materializedInternal, true);
        }

        private IKTable<K, VR> doMapValues<VR>(
            IValueMapperWithKey<K, V, VR> mapper,
            Named named,
            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal)
        {
            ISerde<K> keySerde;
            ISerde<VR> valueSerde;
            string queryableStoreName;
            IStoreBuilder<ITimestampedKeyValueStore<K, VR>> storeBuilder;

            if (materializedInternal != null)
            {
                // we actually do not need to generate store names at all since if it is not specified, we will not
                // materialize the store; but we still need to burn one index BEFORE generating the processor to keep compatibility.
                if (materializedInternal.StoreName == null)
                {
                    builder.NewStoreName(KTable.MAPVALUES_NAME);
                }

                keySerde = materializedInternal.KeySerde != null
                    ? materializedInternal.KeySerde
                    : this.keySerde;

                valueSerde = materializedInternal.ValueSerde;
                queryableStoreName = materializedInternal.queryableStoreName();

                // only materialize if materialized is specified and it has queryable name
                // storeBuilder = queryableStoreName != null ? (new TimestampedKeyValueStoreMaterializer<K, VR>(materializedInternal)).materialize() : null;
            }
            else
            {
                keySerde = this.keySerde;
                valueSerde = null;
                queryableStoreName = null;
                storeBuilder = null;
            }

            string name = new NamedInternal(named).OrElseGenerateWithPrefix(builder, KTable.MAPVALUES_NAME);

            //IKTableProcessorSupplier<K, V, VR> IProcessorSupplier = new KTableMapValues<K, V, VR>(this, mapper, queryableStoreName);

            // leaving in calls to ITB until building topology with graph

            //ProcessorParameters<K, VR> processorParameters = unsafeCastProcessorParametersToCompletelyDifferentType(
            //   new ProcessorParameters<K, VR>(IProcessorSupplier, name));

            //StreamsGraphNode tableNode = new TableProcessorNode<K, VR>(
            //   name,
            //   processorParameters,
            //   storeBuilder);

            //builder.AddGraphNode(this.streamsGraphNode, tableNode);

            // don't inherit parent value serde, since this operation may change the value type, more specifically:
            // we preserve the key following the order of 1) materialized, 2) parent, 3) null
            // we preserve the value following the order of 1) materialized, 2) null

            return null;
            //return new KTable<K, VR>(
            //    name,
            //    keySerde,
            //    valueSerde,
            //    sourceNodes,
            //    queryableStoreName,
            //    IProcessorSupplier,
            //    tableNode,
            //    builder);
        }

        public IKTable<K, VR> mapValues<VR>(IValueMapper<V, VR> mapper)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));

            return doMapValues(withKey(mapper), NamedInternal.empty(), null);
        }

        public IKTable<K, VR> mapValues<VR>(IValueMapper<V, VR> mapper, Named named)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));

            return doMapValues(withKey(mapper), named, null);
        }

        public IKTable<K, VR> mapValues<VR>(IValueMapperWithKey<K, V, VR> mapper)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));

            return doMapValues(mapper, NamedInternal.empty(), null);
        }

        public IKTable<K, VR> mapValues<VR>(
            IValueMapperWithKey<K, V, VR> mapper,
            Named named)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));

            return doMapValues(mapper, named, null);
        }

        public IKTable<K, VR> mapValues<VR>(
            IValueMapper<V, VR> mapper,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return mapValues(mapper, NamedInternal.empty(), materialized);
        }

        public IKTable<K, VR> mapValues<VR>(
            IValueMapper<V, VR> mapper,
            Named named,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>(materialized);

            return doMapValues(withKey(mapper), named, materializedInternal);
        }

        public IKTable<K, VR> mapValues<VR>(
            IValueMapperWithKey<K, V, VR> mapper,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return mapValues(mapper, NamedInternal.empty(), materialized);
        }

        public IKTable<K, VR> mapValues<VR>(
            IValueMapperWithKey<K, V, VR> mapper,
            Named named,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>(materialized);

            return doMapValues(mapper, named, materializedInternal);
        }

        public IKTable<K, VR> transformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
            string[] stateStoreNames)
        {
            return doTransformValues(transformerSupplier, null, NamedInternal.empty(), stateStoreNames);
        }

        public IKTable<K, VR> transformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
            Named named,
            string[] stateStoreNames)
        {
            named = named ?? throw new ArgumentNullException(nameof(named));

            return doTransformValues(transformerSupplier, null, new NamedInternal(named), stateStoreNames);
        }

        public IKTable<K, VR> transformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized,
            string[] stateStoreNames)
        {
            return transformValues(transformerSupplier, materialized, NamedInternal.empty(), stateStoreNames);
        }

        public IKTable<K, VR> transformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized,
            Named named,
            string[] stateStoreNames)
        {
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));
            named = named ?? throw new ArgumentNullException(nameof(named));

            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>(materialized);

            return doTransformValues(transformerSupplier, materializedInternal, new NamedInternal(named), stateStoreNames);
        }

        private IKTable<K, VR> doTransformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal,
            NamedInternal namedInternal,
            string[] stateStoreNames)
        {
            return null;
            //stateStoreNames = stateStoreNames ?? throw new ArgumentNullException(nameof(stateStoreNames));
            //ISerde<K> keySerde;
            //ISerde<VR> valueSerde;
            //string queryableStoreName;
            //IStoreBuilder<ITimestampedKeyValueStore<K, VR>> storeBuilder;

            //if (materializedInternal != null)
            //{
            //    // don't inherit parent value serde, since this operation may change the value type, more specifically:
            //    // we preserve the key following the order of 1) materialized, 2) parent, 3) null
            //    keySerde = materializedInternal.keySerde != null ? materializedInternal.keySerde : this.keySerde;
            //    // we preserve the value following the order of 1) materialized, 2) null
            //    valueSerde = materializedInternal.valueSerde;
            //    queryableStoreName = materializedInternal.queryableStoreName();
            //    // only materialize if materialized is specified and it has queryable name
            //    storeBuilder = queryableStoreName != null
            //        ? (new TimestampedKeyValueStoreMaterializer<K, V>(materializedInternal)).materialize()
            //        : null;
            //}
            //else
            //{
            //    keySerde = this.keySerde;
            //    valueSerde = null;
            //    queryableStoreName = null;
            //    storeBuilder = null;
            //}

            //string name = namedInternal.OrElseGenerateWithPrefix(builder, KTable.TRANSFORMVALUES_NAME);

            // IKTableProcessorSupplier<K, V, VR> IProcessorSupplier = new KTableTransformValues<K, V, VR>(
            //    this,
            //    transformerSupplier,
            //    queryableStoreName);

            // ProcessorParameters<K, VR> processorParameters = unsafeCastProcessorParametersToCompletelyDifferentType(
            //    new ProcessorParameters<K, VR>(IProcessorSupplier, name));

            // StreamsGraphNode tableNode = new TableProcessorNode<K, VR>(
            //    name,
            //    processorParameters,
            //    storeBuilder,
            //    stateStoreNames
            //);

            // builder.AddGraphNode(this.streamsGraphNode, tableNode);

            // return new KTable<K, V, VR>(
            //     name,
            //     keySerde,
            //     valueSerde,
            //     sourceNodes,
            //     queryableStoreName,
            //     IProcessorSupplier,
            //     tableNode,
            //     builder);
        }


        public IKStream<K, V> toStream()
        {
            return toStream(NamedInternal.empty());
        }

        public IKStream<K, V> toStream(Named named)
        {
            named = named ?? throw new ArgumentNullException(nameof(named));

            string name = new NamedInternal(named)
                .OrElseGenerateWithPrefix(builder, KTable.TOSTREAM_NAME);

            IProcessorSupplier<K, Change<V>> kStreamMapValues =
                new KStreamMapValues<K, Change<V>, V>(
                    (key, change) => change.newValue);

            ProcessorParameters<K, V> processorParameters = unsafeCastProcessorParametersToCompletelyDifferentType<V>(
               new ProcessorParameters<K, Change<V>>(kStreamMapValues, name));

            ProcessorGraphNode<K, V> toStreamNode = new ProcessorGraphNode<K, V>(
               name,
               processorParameters);

            builder.AddGraphNode<K, V>(this.streamsGraphNode, toStreamNode);

            // we can inherit parent key and value serde
            return new KStream<K, V>(
                name,
                keySerde,
                valSerde,
                sourceNodes,
                repartitionRequired: false,
                toStreamNode,
                builder);
        }


        public IKStream<K1, V> toStream<K1>(IKeyValueMapper<K, V, K1> mapper)
        {
            return toStream().selectKey(mapper);
        }


        public IKStream<K1, V> toStream<K1>(
            IKeyValueMapper<K, V, K1> mapper,
            Named named)
        {
            return toStream(named).selectKey(mapper);
        }

        public IKTable<K, V> suppress(ISuppressed<K> suppressed)
        {
            string name;
            if (suppressed is INamedSuppressed<K>)
            {
                string givenName = ((INamedSuppressed<object>)suppressed).name;
                name = givenName != null ? givenName : builder.NewProcessorName(KTable.SUPPRESS_NAME);
            }
            else
            {
                throw new System.ArgumentException("Custom sues of Suppressed are not supported.");
            }

            //SuppressedInternal<K, V> suppressedInternal = buildSuppress(suppressed, name);

            // string storeName =
            //    suppressedInternal.name != null
            //    ? suppressedInternal.name + "-store"
            //    : builder.NewStoreName(KTable.SUPPRESS_NAME);

            // IProcessorSupplier<K, Change<V>> suppressionSupplier = new KTableSuppressProcessorSupplier<K, V>(
            //    suppressedInternal,
            //    storeName,
            //    this);

            // ProcessorGraphNode<K, Change<V>> node = new StatefulProcessorNode<>(
            //    name,
            //    new ProcessorParameters<>(suppressionSupplier, name),
            //    new InMemoryTimeOrderedKeyValueBuffer.Builder<>(storeName, keySerde, valSerde)
            //);

            // builder.AddGraphNode(streamsGraphNode, node);

            // return new KTable<K, S, V>(
            //     name,
            //     keySerde,
            //     valSerde,
            //     new HashSet<string> { this.name },
            //     null,
            //     suppressionSupplier,
            //     node,
            //     builder
            // );

            return null;
        }


        //private SuppressedInternal<K> buildSuppress(ISuppressed<K> suppress, string name)
        //{
        //    if (suppress is FinalResultsSuppressionBuilder<K>)
        //    {
        //        long grace = findAndVerifyWindowGrace(streamsGraphNode);
        //        LOG.LogInformation("Using grace period of [{}] as the suppress duration for node [{}].",
        //                 DateTime.ofMillis(grace), name);

        //        FinalResultsSuppressionBuilder<object> builder = (FinalResultsSuppressionBuilder<object>)suppress;

        //        SuppressedInternal<object> finalResultsSuppression =
        //           builder.buildFinalResultsSuppression(TimeSpan.ofMillis(grace));

        //        return (SuppressedInternal<K>)finalResultsSuppression;
        //    }
        //    else if (suppress is SuppressedInternal<K>)
        //    {
        //        return (SuppressedInternal<K>)suppress;
        //    }
        //    else
        //    {
        //        throw new System.ArgumentException("Custom sues of Suppressed are not allowed.");
        //    }
        //}

        public IKTable<K, VR> join<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner)
        {
            return null; // doJoin(other, joiner, NamedInternal.empty(), null, false, false);
        }


        public IKTable<K, VR> join<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Named named)
        {
            return null;// doJoin(other, joiner, named, null, false, false);
        }


        public IKTable<K, VR> join<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return null;// join(other, joiner, NamedInternal.empty(), materialized);
        }


        public IKTable<K, VR> join<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Named named,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal =
               new MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>(materialized, builder, KTable.MERGE_NAME);

            return doJoin(other, joiner, named, materializedInternal, false, false);
        }


        public IKTable<K, VR> outerJoin<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner)
        {
            return outerJoin(other, joiner, NamedInternal.empty());
        }

        public IKTable<K, VR> outerJoin<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Named named)
        {
            return doJoin(other, joiner, named, null, true, true);
        }

        public IKTable<K, VR> outerJoin<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return outerJoin(other, joiner, NamedInternal.empty(), materialized);
        }


        public IKTable<K, VR> outerJoin<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Named named,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal =
               new MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>(materialized, builder, KTable.MERGE_NAME);

            return doJoin(other, joiner, named, materializedInternal, true, true);
        }


        public IKTable<K, VR> leftJoin<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner)
        {
            return leftJoin(other, joiner, NamedInternal.empty());
        }

        public IKTable<K, VR> leftJoin<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Named named)
        {
            return doJoin(other, joiner, named, null, true, false);
        }

        public IKTable<K, VR> leftJoin<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return leftJoin(other, joiner, NamedInternal.empty(), materialized);
        }


        public IKTable<K, VR> leftJoin<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Named named,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal =
               new MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>(materialized, builder, KTable.MERGE_NAME);

            return doJoin(other, joiner, named, materializedInternal, true, false);
        }


        private IKTable<K, VR> doJoin<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Named joinName,
            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal,
            bool leftOuter,
            bool rightOuter)
        {
            other = other ?? throw new ArgumentNullException(nameof(other));
            joiner = joiner ?? throw new ArgumentNullException(nameof(joiner));
            joinName = joinName ?? throw new ArgumentNullException(nameof(joinName));

            NamedInternal renamed = new NamedInternal(joinName);
            string joinMergeName = renamed.OrElseGenerateWithPrefix(builder, KTable.MERGE_NAME);
            HashSet<string> allSourceNodes = ensureJoinableWith((AbstractStream<K, VO>)other);

            if (leftOuter)
            {
                enableSendingOldValues();
            }

            if (rightOuter)
            {
                //((KTable)other).enableSendingOldValues();
            }

            //KTableKTableAbstractJoin<K, VR, V, VO> joinThis;
            //KTableKTableAbstractJoin<K, VR, VO, V> joinOther;

            if (!leftOuter)
            { // inner
                //joinThis = new KTableKTableInnerJoin<>(this, (KTable<K, object, VO>)other, joiner);
                //joinOther = new KTableKTableInnerJoin<>((KTable<K, object, VO>)other, this, reverseJoiner(joiner));
            }
            else if (!rightOuter)
            { // left
                //joinThis = new KTableKTableLeftJoin<>(this, (KTable<K, object, VO>)other, joiner);
                //joinOther = new KTableKTableRightJoin<>((KTable<K, object, VO>)other, this, reverseJoiner(joiner));
            }
            else
            { // outer
                //joinThis = new KTableKTableOuterJoin<>(this, (KTable<K, object, VO>)other, joiner);
                //joinOther = new KTableKTableOuterJoin<>((KTable<K, object, VO>)other, this, reverseJoiner(joiner));
            }

            string joinThisName = renamed.suffixWithOrElseGet("-join-this", builder, KTable.JOINTHIS_NAME);
            string joinOtherName = renamed.suffixWithOrElseGet("-join-other", builder, KTable.JOINOTHER_NAME);

            //ProcessorParameters<K, Change<V>> joinThisProcessorParameters = new ProcessorParameters<K, Change<V>>(joinThis, joinThisName);
            //ProcessorParameters<K, Change<VO>> joinOtherProcessorParameters = new ProcessorParameters<K, Change<VO>>(joinOther, joinOtherName);

            ISerde<K> keySerde;
            ISerde<VR> valueSerde;
            string queryableStoreName;
            IStoreBuilder<ITimestampedKeyValueStore<K, VR>> storeBuilder;

            if (materializedInternal != null)
            {
                keySerde = materializedInternal.KeySerde != null ? materializedInternal.KeySerde : this.keySerde;
                valueSerde = materializedInternal.ValueSerde;
                queryableStoreName = materializedInternal.StoreName;
                //storeBuilder = new TimestampedKeyValueStoreMaterializer<>(materializedInternal).materialize();
            }
            else
            {
                keySerde = this.keySerde;
                valueSerde = null;
                queryableStoreName = null;
                storeBuilder = null;
            }

            //KTableKTableJoinNode<K, V, VO, VR> kTableKTableJoinNode =
            //   KTableKTableJoinNode<K, V, VO, VR>.kTableKTableJoinNodeBuilder()
            //        .withNodeName(joinMergeName)
            //        .withJoinThisProcessorParameters(joinThisProcessorParameters)
            //        .withJoinOtherProcessorParameters(joinOtherProcessorParameters)
            //        .withThisJoinSideNodeName(name)
            //        .withOtherJoinSideNodeName(((KTable)other).name)
            //        .withJoinThisStoreNames(valueGetterSupplier().storeNames())
            //        .withJoinOtherStoreNames(((KTable)other).valueGetterSupplier.storeNames())
            //        .withKeySerde(keySerde)
            //        .withValueSerde(valueSerde)
            //        .withQueryableStoreName(queryableStoreName)
            //        .withStoreBuilder(storeBuilder)
            //        .build();

            //builder.AddGraphNode(this.streamsGraphNode, kTableKTableJoinNode);

            //// we can inherit parent key serde if user do not provide specific overrides
            //return new KTable<K, Change<VR>, VR>(
            //    kTableKTableJoinNode.nodeName,
            //    kTableKTableJoinNode.keySerde,
            //    kTableKTableJoinNode.valueSerde,
            //    allSourceNodes,
            //    kTableKTableJoinNode.queryableStoreName(),
            //    kTableKTableJoinNode.joinMerger(),
            //    kTableKTableJoinNode,
            //    builder);

            return null;
        }

        public IKGroupedTable<KR, VR> groupBy<KR, VR>(
            IKeyValueMapper<K, V, KeyValue<KR, VR>> selector)
        {
            return groupBy(selector, Grouped<KR, VR>.With(null, null));
        }

        [Obsolete]
        public IKGroupedTable<K1, V1> groupBy<K1, V1>(
            IKeyValueMapper<K, V, KeyValue<K1, V1>> selector,
            ISerialized<K1, V1> serialized)
        {
            selector = selector ?? throw new ArgumentNullException(nameof(selector));
            serialized = serialized ?? throw new ArgumentNullException(nameof(serialized));

            SerializedInternal<K1, V1> serializedInternal = new SerializedInternal<K1, V1>(serialized);
            return groupBy(selector, Grouped<K1, V1>.With(null/*serializedInternal.keySerde*/, null/*serializedInternal.valueSerde*/));
        }

        public IKGroupedTable<K1, V1> groupBy<K1, V1>(
            IKeyValueMapper<K, V, KeyValue<K1, V1>> selector,
            Grouped<K1, V1> grouped)
        {
            selector = selector ?? throw new ArgumentNullException(nameof(selector));
            grouped = grouped ?? throw new ArgumentNullException(nameof(grouped));

            GroupedInternal<K1, V1> groupedInternal = new GroupedInternal<K1, V1>(grouped);
            string selectName = new NamedInternal(groupedInternal.name).OrElseGenerateWithPrefix(builder, KTable.SELECT_NAME);

            //IKTableProcessorSupplier<K, V, KeyValue<K1, V1>> selectSupplier = new KTableRepartitionMap<K, V, K1, V1>(this, selector);
            //ProcessorParameters<K, Change<V>> processorParameters = new ProcessorParameters<K, Change<V>>(selectSupplier, selectName);

            // select the aggregate key and values (old and new), it would require parent to send old values
            //ProcessorGraphNode<K, Change<V>> groupByMapNode = new ProcessorGraphNode<K, Change<V>>(selectName, processorParameters);

            //builder.AddGraphNode(this.streamsGraphNode, groupByMapNode);

            this.enableSendingOldValues();
            return null;
            //return new KGroupedTableImpl<K1, V1>(
            //    builder,
            //    selectName,
            //    sourceNodes,
            //    groupedInternal,
            //    groupByMapNode
            //);
        }

        public IKTableValueGetterSupplier<K, V> valueGetterSupplier<VR>()
        {
            //if (IProcessorSupplier is KTableSource<K, V>)
            //{
            //    KTableSource<K, V> source = (KTableSource<K, V>)IProcessorSupplier;
            //    // whenever a source ktable is required for getter, it should be materialized
            //    source.materialize();
            //    return new KTableSourceValueGetterSupplier<K, V>(source.queryableName());
            //}
            //else if (IProcessorSupplier is IKStreamAggProcessorSupplier<K, V>)
            //{
            //    return ((KStreamAggIIProcessorSupplier<object, K, S, V>)IProcessorSupplier).view();
            //}
            //else
            {
                return ((IKTableProcessorSupplier<K, VR, V>)IProcessorSupplier).view();
            }
        }


        public void enableSendingOldValues()
        {
            if (!sendOldValues)
            {
                //if (IProcessorSupplier is KTableSource<K, V>)
                //{
                //    KTableSource<K, object> source = (KTableSource<K, object>)IProcessorSupplier;
                //    source.enableSendingOldValues();
                //}
                //else if (IProcessorSupplier is KStreamAggIIProcessorSupplier)
                //{
                //    ((KStreamAggIIProcessorSupplier<object, K, S, V>)IProcessorSupplier).enableSendingOldValues();
                //}
                //else
                //{
                //    ((IKTableProcessorSupplier<K, VR, V>)IProcessorSupplier).enableSendingOldValues();
                //}
                sendOldValues = true;
            }
        }

        bool sendingOldValueEnabled()
        {
            return sendOldValues;
        }

        /**
         * We conflate V with Change<V> in many places. It might be nice to fix that eventually.
         * For now, I'm just explicitly lying about the parameterized type.
         */
        private ProcessorParameters<K, VR> unsafeCastProcessorParametersToCompletelyDifferentType<VR>(
            ProcessorParameters<K, Change<V>> kObjectProcessorParameters)
        {
            return null;// return ProcessorParameters<K, VR>.ConvertFrom(kObjectProcessorParameters);

            //return pp;
        }

        string IKTable<K, V>.queryableStoreName()
        {
            throw new NotImplementedException();
        }
    }
}