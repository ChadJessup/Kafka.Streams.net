using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.KStream.Internals.Suppress;
using Kafka.Streams.Processors;
using Kafka.Streams.State;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.TimeStamped;
using Microsoft.Extensions.Logging;
using NodaTime;
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
    public static class KTable
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
        where S : IStateStore
    {
        private static readonly ILogger LOG = new LoggerFactory().CreateLogger<KTable<K, S, V>>();
        private readonly IProcessorSupplier<K, V> processorSupplier;
        private readonly IClock clock;
        public string? QueryableStoreName { get; private set; }
        private bool sendOldValues = false;

        public KTable(
            IClock clock,
            string name,
            ISerde<K>? keySerde,
            ISerde<V>? valSerde,
            HashSet<string> sourceNodes,
            string? queryableStoreName,
            IProcessorSupplier<K, V> IProcessorSupplier,
            StreamsGraphNode streamsGraphNode,
            InternalStreamsBuilder builder)
            : base(name, keySerde, valSerde, sourceNodes, streamsGraphNode, builder)
        {
            this.clock = clock;
            this.processorSupplier = IProcessorSupplier;
            this.QueryableStoreName = queryableStoreName;
        }

        private IKTable<K, V> DoFilter(
            Func<K, V, bool> predicate,
            Named named,
            MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>? materializedInternal,
            bool filterNot)
        {
            ISerde<K>? keySerde;
            ISerde<V>? valueSerde;
            string? queryableStoreName;
            IStoreBuilder<ITimestampedKeyValueStore<K, V>>? storeBuilder;

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
                keySerde = materializedInternal?.KeySerde ?? this.keySerde ?? null;

                // we preserve the value following the order of 1) materialized, 2) parent
                valueSerde = materializedInternal?.ValueSerde ?? this.valSerde ?? null;

                queryableStoreName = materializedInternal?.QueryableStoreName();

                // only materialize if materialized is specified and it has queryable name
                storeBuilder = queryableStoreName != null
                    ? (new TimestampedKeyValueStoreMaterializer<K, V>(this.clock, materializedInternal)).Materialize()
                    : null;
            }
            else
            {
                keySerde = this.keySerde;
                valueSerde = this.valSerde;
                queryableStoreName = null;
                storeBuilder = null;
            }

            var name = new NamedInternal(named).OrElseGenerateWithPrefix(builder, KTable.FILTER_NAME);

            static ProcessorParameters<K, VR> UnsafeCastProcessorParametersToCompletelyDifferentType<VR>(
                ProcessorParameters<K, Change<V>> processorParameters)
            {
                var convert = (IProcessorSupplier<K, VR>)processorParameters.ProcessorSupplier;
                var converted = new ProcessorParameters<K, VR>(
                    convert,
                    processorParameters.ProcessorName);

                return converted;
            }

            IKTableProcessorSupplier<K, V, V> processorSupplier = new KTableFilter<K, V>(
                this,
                predicate,
                filterNot,
                queryableStoreName);

            var pp = new ProcessorParameters<K, Change<V>>(processorSupplier, name);
            var processorParameters = UnsafeCastProcessorParametersToCompletelyDifferentType<V>(pp);

            StreamsGraphNode tableNode = new TableProcessorNode<K, V>(
               name,
               processorParameters,
               storeBuilder);

            builder.AddGraphNode<K, V>(this.streamsGraphNode, tableNode);

            return new KTable<K, S, V>(
                this.clock,
                name,
                keySerde,
                valueSerde,
                sourceNodes,
                queryableStoreName,
                (IProcessorSupplier<K, V>)processorSupplier,
                tableNode,
                builder);
        }

        public IKTable<K, V> Filter(Func<K, V, bool> predicate)
        {
            predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));

            return DoFilter(predicate, NamedInternal.Empty(), null, false);
        }

        public IKTable<K, V> Filter(Func<K, V, bool> predicate, Named named)
        {
            predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));

            return DoFilter(predicate, named, null, false);
        }

        public IKTable<K, V> Filter(
            Func<K, V, bool> predicate,
            Named named,
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));
            var materializedInternal = new MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>(materialized);

            return DoFilter(predicate, named, materializedInternal, false);
        }


        public IKTable<K, V> Filter(
            Func<K, V, bool> predicate,
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return Filter(predicate, NamedInternal.Empty(), materialized);
        }

        public IKTable<K, V> FilterNot(Func<K, V, bool> predicate)
        {
            predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));

            return DoFilter(predicate, NamedInternal.Empty(), null, true);
        }

        public IKTable<K, V> FilterNot(
            Func<K, V, bool> predicate,
            Named named)
        {
            predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));

            return DoFilter(predicate, named, null, true);
        }


        public IKTable<K, V> FilterNot(
            Func<K, V, bool> predicate,
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return FilterNot(predicate, NamedInternal.Empty(), materialized);
        }

        public IKTable<K, V> FilterNot(
            Func<K, V, bool> predicate,
            Named named,
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            var materializedInternal = new MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>(materialized);
            var renamed = new NamedInternal(named);

            return DoFilter(predicate, renamed, materializedInternal, true);
        }

        private IKTable<K, VR> DoMapValues<VR>(
            IValueMapperWithKey<K, V, VR> mapper,
            Named named,
            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>? materializedInternal)
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

                keySerde = materializedInternal.KeySerde ?? this.keySerde;

                valueSerde = materializedInternal.ValueSerde;
                queryableStoreName = materializedInternal.QueryableStoreName();

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

            var name = new NamedInternal(named).OrElseGenerateWithPrefix(builder, KTable.MAPVALUES_NAME);

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

        public IKTable<K, VR> MapValues<VR>(IValueMapper<V, VR> mapper)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));

            return DoMapValues(WithKey(mapper), NamedInternal.Empty(), null);
        }

        public IKTable<K, VR> MapValues<VR>(IValueMapper<V, VR> mapper, Named named)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));

            return DoMapValues(WithKey(mapper), named, null);
        }

        public IKTable<K, VR> MapValues<VR>(IValueMapperWithKey<K, V, VR> mapper)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));

            return DoMapValues(mapper, NamedInternal.Empty(), null);
        }

        public IKTable<K, VR> MapValues<VR>(
            IValueMapperWithKey<K, V, VR> mapper,
            Named named)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));

            return DoMapValues(mapper, named, null);
        }

        public IKTable<K, VR> MapValues<VR>(
            IValueMapper<V, VR> mapper,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return MapValues(mapper, NamedInternal.Empty(), materialized);
        }

        public IKTable<K, VR> MapValues<VR>(
            IValueMapper<V, VR> mapper,
            Named named,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            var materializedInternal = new MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>(materialized);

            return DoMapValues(WithKey(mapper), named, materializedInternal);
        }

        public IKTable<K, VR> MapValues<VR>(
            IValueMapperWithKey<K, V, VR> mapper,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return MapValues(mapper, NamedInternal.Empty(), materialized);
        }

        public IKTable<K, VR> MapValues<VR>(
            IValueMapperWithKey<K, V, VR> mapper,
            Named named,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            var materializedInternal = new MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>(materialized);

            return DoMapValues(mapper, named, materializedInternal);
        }

        public IKTable<K, VR> TransformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
            string[] stateStoreNames)
        {
            return DoTransformValues(transformerSupplier, null, NamedInternal.Empty(), stateStoreNames);
        }

        public IKTable<K, VR> TransformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
            Named named,
            string[] stateStoreNames)
        {
            named = named ?? throw new ArgumentNullException(nameof(named));

            return DoTransformValues(transformerSupplier, null, new NamedInternal(named), stateStoreNames);
        }

        public IKTable<K, VR> TransformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized,
            string[] stateStoreNames)
        {
            return TransformValues(transformerSupplier, materialized, NamedInternal.Empty(), stateStoreNames);
        }

        public IKTable<K, VR> TransformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized,
            Named named,
            string[] stateStoreNames)
        {
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));
            named = named ?? throw new ArgumentNullException(nameof(named));

            var materializedInternal = new MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>(materialized);

            return DoTransformValues(transformerSupplier, materializedInternal, new NamedInternal(named), stateStoreNames);
        }

        private IKTable<K, VR> DoTransformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>? materializedInternal,
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


        public IKStream<K, V> ToStream()
        {
            return ToStream(NamedInternal.Empty());
        }

        public IKStream<K, V> ToStream(Named named)
        {
            named = named ?? throw new ArgumentNullException(nameof(named));

            var name = new NamedInternal(named)
                .OrElseGenerateWithPrefix(builder, KTable.TOSTREAM_NAME);

            IProcessorSupplier<K, Change<V>> kStreamMapValues =
                new KStreamMapValues<K, Change<V>, V>(
                    (key, change) => change.NewValue);

            //ProcessorParameters<K, V> processorParameters = UnsafeCastProcessorParametersToCompletelyDifferentType<V>(
            //new ProcessorParameters<K, Change<V>>(kStreamMapValues, name));

            var toStreamNode = new ProcessorGraphNode<K, Change<V>>(
               name,
               new ProcessorParameters<K, Change<V>>(kStreamMapValues, name));

            builder.AddGraphNode<K, V>(this.streamsGraphNode, toStreamNode);

            // we can inherit parent key and value serde
            return new KStream<K, V>(
                this.clock,
                name,
                keySerde,
                valSerde,
                sourceNodes,
                repartitionRequired: false,
                toStreamNode,
                builder);
        }


        public IKStream<K1, V> ToStream<K1>(IKeyValueMapper<K, V, K1> mapper)
        {
            return ToStream().SelectKey(mapper);
        }


        public IKStream<K1, V> ToStream<K1>(
            IKeyValueMapper<K, V, K1> mapper,
            Named named)
        {
            return ToStream(named).SelectKey(mapper);
        }

        public IKTable<K, V> Suppress(ISuppressed<K> suppressed)
        {
            string name;
            if (suppressed is INamedSuppressed<K>)
            {
                var givenName = ((INamedSuppressed<object>)suppressed).name;
                name = givenName ?? builder.NewProcessorName(KTable.SUPPRESS_NAME);
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
        //                 DateTime.FromMilliseconds(grace), name);

        //        FinalResultsSuppressionBuilder<object> builder = (FinalResultsSuppressionBuilder<object>)suppress;

        //        SuppressedInternal<object> finalResultsSuppression =
        //           builder.buildFinalResultsSuppression(TimeSpan.FromMilliseconds(grace));

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

        public IKTable<K, VR> Join<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner)
        {
            return null; // doJoin(other, joiner, NamedInternal.empty(), null, false, false);
        }


        public IKTable<K, VR> Join<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Named named)
        {
            return null;// doJoin(other, joiner, named, null, false, false);
        }


        public IKTable<K, VR> Join<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return null;// join(other, joiner, NamedInternal.empty(), materialized);
        }


        public IKTable<K, VR> Join<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Named named,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            var materializedInternal =
               new MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>(materialized, builder, KTable.MERGE_NAME);

            return DoJoin(other, joiner, named, materializedInternal, false, false);
        }


        public IKTable<K, VR> OuterJoin<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner)
        {
            return OuterJoin(other, joiner, NamedInternal.Empty());
        }

        public IKTable<K, VR> OuterJoin<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Named named)
        {
            return DoJoin(other, joiner, named, null, true, true);
        }

        public IKTable<K, VR> OuterJoin<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return OuterJoin(other, joiner, NamedInternal.Empty(), materialized);
        }


        public IKTable<K, VR> OuterJoin<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Named named,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            var materializedInternal =
               new MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>(materialized, builder, KTable.MERGE_NAME);

            return DoJoin(other, joiner, named, materializedInternal, true, true);
        }

        public IKTable<K, VR> LeftJoin<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner)
        {
            return LeftJoin(other, joiner, NamedInternal.Empty());
        }

        public IKTable<K, VR> LeftJoin<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Named named)
        {
            return DoJoin(other, joiner, named, null, true, false);
        }

        public IKTable<K, VR> LeftJoin<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return LeftJoin(other, joiner, NamedInternal.Empty(), materialized);
        }


        public IKTable<K, VR> LeftJoin<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Named named,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            var materializedInternal =
               new MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>(materialized, builder, KTable.MERGE_NAME);

            return DoJoin(other, joiner, named, materializedInternal, true, false);
        }

        private IKTable<K, VR> DoJoin<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Named joinName,
            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>? materializedInternal,
            bool leftOuter,
            bool rightOuter)
        {
            other = other ?? throw new ArgumentNullException(nameof(other));
            joiner = joiner ?? throw new ArgumentNullException(nameof(joiner));
            joinName = joinName ?? throw new ArgumentNullException(nameof(joinName));

            var renamed = new NamedInternal(joinName);
            var joinMergeName = renamed.OrElseGenerateWithPrefix(builder, KTable.MERGE_NAME);
            HashSet<string> allSourceNodes = EnsureJoinableWith((AbstractStream<K, VO>)other);

            if (leftOuter)
            {
                EnableSendingOldValues();
            }

            if (rightOuter)
            {
                other.EnableSendingOldValues();
            }

            KTableKTableAbstractJoin<K, S, VR, V, VO>? joinThis  = null;
            KTableKTableAbstractJoin<K, S, VR, VO, V>? joinOther = null;

            if (!leftOuter)
            { // inner
                joinThis = new KTableKTableInnerJoin<K, S, VR, V, VO>(this, (KTable<K, S, VO>)other, joiner, this.QueryableStoreName);
                joinOther = new KTableKTableInnerJoin<K, S, VR, VO, V>((KTable<K, S, VO>)other, this, ReverseJoiner(joiner), this.QueryableStoreName);
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

            var joinThisName = renamed.SuffixWithOrElseGet("-join-this", builder, KTable.JOINTHIS_NAME);
            var joinOtherName = renamed.SuffixWithOrElseGet("-join-other", builder, KTable.JOINOTHER_NAME);

            var joinThisProcessorParameters = new ProcessorParameters<K, Change<V>>(joinThis, joinThisName);
            var joinOtherProcessorParameters = new ProcessorParameters<K, Change<VO>>(joinOther, joinOtherName);

            ISerde<K>? keySerde;
            ISerde<VR>? valueSerde;
            string? queryableStoreName;
            IStoreBuilder<ITimestampedKeyValueStore<K, VR>>? storeBuilder;

            if (materializedInternal != null)
            {
                keySerde = materializedInternal.KeySerde ?? this.keySerde;

                valueSerde = materializedInternal.ValueSerde;
                queryableStoreName = materializedInternal.StoreName;
                storeBuilder = new TimestampedKeyValueStoreMaterializer<K, VR>(
                        this.clock, 
                        materializedInternal)
                    .Materialize();
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

        public IKGroupedTable<KR, VR> GroupBy<KR, VR>(
            IKeyValueMapper<K, V, KeyValuePair<KR, VR>> selector)
        {
            return GroupBy(selector, Grouped.With<KR, VR>(null, null));
        }

        [Obsolete]
        public IKGroupedTable<K1, V1> GroupBy<K1, V1>(
            IKeyValueMapper<K, V, KeyValuePair<K1, V1>> selector,
            ISerialized<K1, V1> serialized)
        {
            selector = selector ?? throw new ArgumentNullException(nameof(selector));
            serialized = serialized ?? throw new ArgumentNullException(nameof(serialized));

            var serializedInternal = new SerializedInternal<K1, V1>(serialized);
            return GroupBy(selector, Grouped.With(serializedInternal.keySerde, serializedInternal.valueSerde));
        }

        public IKGroupedTable<K1, V1> GroupBy<K1, V1>(
            IKeyValueMapper<K, V, KeyValuePair<K1, V1>>? selector,
            Grouped<K1, V1>? grouped)
        {
            selector = selector ?? throw new ArgumentNullException(nameof(selector));
            grouped = grouped ?? throw new ArgumentNullException(nameof(grouped));

            var groupedInternal = new GroupedInternal<K1, V1>(grouped);
            var selectName = new NamedInternal(groupedInternal.Name)
                .OrElseGenerateWithPrefix(builder, KTable.SELECT_NAME);

            //IKTableProcessorSupplier<K, V, KeyValuePair<K1, V1>> selectSupplier = new KTableRepartitionMap<K, V, K1, V1>(this, selector);
            //ProcessorParameters<K, Change<V>> processorParameters = new ProcessorParameters<K, Change<V>>(selectSupplier, selectName);

            // select the aggregate key and values (old and new), it would require parent to send old values
            //ProcessorGraphNode<K, Change<V>> groupByMapNode = new ProcessorGraphNode<K, Change<V>>(selectName, processorParameters);

            //builder.AddGraphNode(this.streamsGraphNode, groupByMapNode);

            this.EnableSendingOldValues();
            return null;
            //return new KGroupedTableImpl<K1, V1>(
            //    builder,
            //    selectName,
            //    sourceNodes,
            //    groupedInternal,
            //    groupByMapNode
            //);
        }

        public IKTableValueGetterSupplier<K, V> ValueGetterSupplier<VR>()
        {
            if (processorSupplier is KTableSource<K, V> source)
            {
                // whenever a source ktable is required for getter, it should be materialized
                source.Materialize();

                return new KTableSourceValueGetterSupplier<K, V>(source.queryableName);
            }
            else if (processorSupplier is IKStreamAggProcessorSupplier)
            {
                return ((IKStreamAggProcessorSupplier<object, K, S, V>)processorSupplier).View();
            }
            else
            {
                return ((IKTableProcessorSupplier<K, VR, V>)processorSupplier).View();
            }
        }

        public void EnableSendingOldValues()
        {
            if (!sendOldValues)
            {
                if (processorSupplier is KTableSource<K, V>)
                {
                    var source = (KTableSource<K, object>)processorSupplier;
                    source.EnableSendingOldValues();
                }
                else if (processorSupplier is IKStreamAggProcessorSupplier)
                {
                    ((IKStreamAggProcessorSupplier<object, K, S, V>)processorSupplier).EnableSendingOldValues();
                }
                else
                {
                    ((IKTableProcessorSupplier<K, S, V>)processorSupplier).EnableSendingOldValues();
                }

                sendOldValues = true;
            }
        }
    }
}
