using Kafka.Common;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.KStream.Internals.Suppress;
using Kafka.Streams.Processors;
using Kafka.Streams.State;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.TimeStamped;
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
        private readonly IProcessorSupplier processorSupplier;
        private readonly KafkaStreamsContext context;
        public string? QueryableStoreName { get; private set; }
        private bool sendOldValues = false;

        public KTable(
            KafkaStreamsContext context,
            string Name,
            ISerde<K>? keySerde,
            ISerde<V>? valSerde,
            HashSet<string> sourceNodes,
            string? queryableStoreName,
            IProcessorSupplier processorSupplier,
            StreamsGraphNode streamsGraphNode,
            InternalStreamsBuilder builder)
            : base(Name, keySerde, valSerde, sourceNodes, streamsGraphNode, builder)
        {
            this.context = context;
            this.processorSupplier = processorSupplier;
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
                // we actually do not need to generate store names at All since if it is not specified, we will not
                // materialize the store; but we still need to burn one index BEFORE generating the processor to keep compatibility.
                if (materializedInternal.StoreName == null)
                {
                    this.Builder.NewStoreName(KTable.FILTER_NAME);
                }

                // we can inherit parent key and value serde if user do not provide specific overrides, more specifically:
                // we preserve the key following the order of 1) materialized, 2) parent
                keySerde = materializedInternal?.KeySerde ?? this.KeySerde ?? null;

                // we preserve the value following the order of 1) materialized, 2) parent
                valueSerde = materializedInternal?.ValueSerde ?? this.ValueSerde ?? null;

                queryableStoreName = materializedInternal?.QueryableStoreName();

                // only materialize if materialized is specified and it has queryable Name
                storeBuilder = queryableStoreName != null
                    ? new TimestampedKeyValueStoreMaterializer<K, V>(this.context, materializedInternal).Materialize()
                    : null;
            }
            else
            {
                keySerde = this.KeySerde;
                valueSerde = this.ValueSerde;
                queryableStoreName = null;
                storeBuilder = null;
            }

            var Name = new NamedInternal(named).OrElseGenerateWithPrefix(this.Builder, KTable.FILTER_NAME);

            IKTableProcessorSupplier<K, V, V> processorSupplier = new KTableFilter<K, V>(
                this.context,
                this,
                predicate,
                filterNot,
                queryableStoreName);

            var processorParameters = new ProcessorParameters<K, IChange<V>>(processorSupplier, Name);

            var tableNode = new TableProcessorNode<K, V, ITimestampedKeyValueStore<K, V>>(
               Name,
               processorParameters,
               storeBuilder);

            this.Builder.AddGraphNode<K, V>(this.StreamsGraphNode, tableNode);

            return new KTable<K, S, V>(
                this.context,
                Name,
                keySerde,
                valueSerde,
                this.SourceNodes,
                queryableStoreName,
                processorSupplier,
                tableNode,
                this.Builder);
        }

        public IKTable<K, V> Filter(Func<K, V, bool> predicate)
        {
            predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));

            return this.DoFilter(predicate, NamedInternal.Empty(), null, false);
        }

        public IKTable<K, V> Filter(Func<K, V, bool> predicate, Named named)
        {
            predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));

            return this.DoFilter(predicate, named, null, false);
        }

        public IKTable<K, V> Filter(
            Func<K, V, bool> predicate,
            Named named,
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));
            var materializedInternal = new MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>(materialized);

            return this.DoFilter(predicate, named, materializedInternal, false);
        }


        public IKTable<K, V> Filter(
            Func<K, V, bool> predicate,
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return this.Filter(predicate, NamedInternal.Empty(), materialized);
        }

        public IKTable<K, V> FilterNot(Func<K, V, bool> predicate)
        {
            predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));

            return this.DoFilter(predicate, NamedInternal.Empty(), null, true);
        }

        public IKTable<K, V> FilterNot(
            Func<K, V, bool> predicate,
            Named named)
        {
            predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));

            return this.DoFilter(predicate, named, null, true);
        }


        public IKTable<K, V> FilterNot(
            Func<K, V, bool> predicate,
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return this.FilterNot(predicate, NamedInternal.Empty(), materialized);
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

            return this.DoFilter(predicate, renamed, materializedInternal, true);
        }

        private IKTable<K, VR> DoMapValues<VR>(
            ValueMapperWithKey<K, V, VR> mapper,
            Named named,
            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>? materializedInternal)
        {
            ISerde<K> keySerde;
            ISerde<VR> valueSerde;
            string queryableStoreName;
            IStoreBuilder<ITimestampedKeyValueStore<K, VR>> storeBuilder;

            if (materializedInternal != null)
            {
                // we actually do not need to generate store names at All since if it is not specified, we will not
                // materialize the store; but we still need to burn one index BEFORE generating the processor to keep compatibility.
                if (materializedInternal.StoreName == null)
                {
                    this.Builder.NewStoreName(KTable.MAPVALUES_NAME);
                }

                keySerde = materializedInternal.KeySerde ?? this.KeySerde;

                valueSerde = materializedInternal.ValueSerde;
                queryableStoreName = materializedInternal.QueryableStoreName();

                // only materialize if materialized is specified and it has queryable Name
                // storeBuilder = queryableStoreName != null ? (new TimestampedKeyValueStoreMaterializer<K, VR>(materializedInternal)).materialize() : null;
            }
            else
            {
                keySerde = this.KeySerde;
                valueSerde = null;
                queryableStoreName = null;
                storeBuilder = null;
            }

            var Name = new NamedInternal(named).OrElseGenerateWithPrefix(this.Builder, KTable.MAPVALUES_NAME);

            //IKTableProcessorSupplier<K, V, VR> IProcessorSupplier = new KTableMapValues<K, V, VR>(this, mapper, queryableStoreName);

            // leaving in calls to ITB until building topology with graph

            //ProcessorParameters<K, VR> processorParameters = unsafeCastProcessorParametersToCompletelyDifferentType(
            //   new ProcessorParameters<K, VR>(IProcessorSupplier, Name));

            //StreamsGraphNode tableNode = new TableProcessorNode<K, VR>(
            //   Name,
            //   processorParameters,
            //   storeBuilder);

            //builder.AddGraphNode(this.streamsGraphNode, tableNode);

            // don't inherit parent value serde, since this operation may change the value type, more specifically:
            // we preserve the key following the order of 1) materialized, 2) parent, 3) null
            // we preserve the value following the order of 1) materialized, 2) null

            return null;
            //return new KTable<K, VR>(
            //    Name,
            //    keySerde,
            //    valueSerde,
            //    sourceNodes,
            //    queryableStoreName,
            //    IProcessorSupplier,
            //    tableNode,
            //    builder);
        }

        public IKTable<K, VR> MapValues<VR>(ValueMapper<V, VR> mapper)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));

            return this.DoMapValues(WithKey(mapper), NamedInternal.Empty(), null);
        }

        public IKTable<K, VR> MapValues<VR>(ValueMapper<V, VR> mapper, Named named)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));

            return this.DoMapValues(WithKey(mapper), named, null);
        }

        public IKTable<K, VR> MapValues<VR>(ValueMapperWithKey<K, V, VR> mapper)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));

            return this.DoMapValues(mapper, NamedInternal.Empty(), null);
        }

        public IKTable<K, VR> MapValues<VR>(
            ValueMapperWithKey<K, V, VR> mapper,
            Named named)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));

            return this.DoMapValues(mapper, named, null);
        }

        public IKTable<K, VR> MapValues<VR>(
            ValueMapper<V, VR> mapper,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return this.MapValues(mapper, NamedInternal.Empty(), materialized);
        }

        public IKTable<K, VR> MapValues<VR>(
            ValueMapper<V, VR> mapper,
            Named named,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            var materializedInternal = new MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>(materialized);

            return this.DoMapValues(WithKey(mapper), named, materializedInternal);
        }

        public IKTable<K, VR> MapValues<VR>(
            ValueMapperWithKey<K, V, VR> mapper,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return this.MapValues(mapper, NamedInternal.Empty(), materialized);
        }

        public IKTable<K, VR> MapValues<VR>(
            ValueMapperWithKey<K, V, VR> mapper,
            Named named,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            var materializedInternal = new MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>(materialized);

            return this.DoMapValues(mapper, named, materializedInternal);
        }

        public IKTable<K, VR> TransformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
            string[] stateStoreNames)
        {
            return this.DoTransformValues(transformerSupplier, null, NamedInternal.Empty(), stateStoreNames);
        }

        public IKTable<K, VR> TransformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
            Named named,
            string[] stateStoreNames)
        {
            named = named ?? throw new ArgumentNullException(nameof(named));

            return this.DoTransformValues(transformerSupplier, null, new NamedInternal(named), stateStoreNames);
        }

        public IKTable<K, VR> TransformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized,
            string[] stateStoreNames)
        {
            return this.TransformValues(transformerSupplier, materialized, NamedInternal.Empty(), stateStoreNames);
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

            return this.DoTransformValues(transformerSupplier, materializedInternal, new NamedInternal(named), stateStoreNames);
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
            //    // only materialize if materialized is specified and it has queryable Name
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

            //string Name = namedInternal.OrElseGenerateWithPrefix(builder, KTable.TRANSFORMVALUES_NAME);

            // IKTableProcessorSupplier<K, V, VR> IProcessorSupplier = new KTableTransformValues<K, V, VR>(
            //    this,
            //    transformerSupplier,
            //    queryableStoreName);

            // ProcessorParameters<K, VR> processorParameters = unsafeCastProcessorParametersToCompletelyDifferentType(
            //    new ProcessorParameters<K, VR>(IProcessorSupplier, Name));

            // StreamsGraphNode tableNode = new TableProcessorNode<K, VR>(
            //    Name,
            //    processorParameters,
            //    storeBuilder,
            //    stateStoreNames
            //);

            // builder.AddGraphNode(this.streamsGraphNode, tableNode);

            // return new KTable<K, V, VR>(
            //     Name,
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
            return this.ToStream(NamedInternal.Empty());
        }

        public IKStream<K, V> ToStream(Named named)
        {
            named = named ?? throw new ArgumentNullException(nameof(named));

            var Name = new NamedInternal(named)
                .OrElseGenerateWithPrefix(this.Builder, KTable.TOSTREAM_NAME);

            IProcessorSupplier<K, IChange<V>> kStreamMapValues =
                new KStreamMapValues<K, IChange<V>, V>((key, change) => change.NewValue);

            //ProcessorParameters<K, V> processorParameters = UnsafeCastProcessorParametersToCompletelyDifferentType<V>(
            //new ProcessorParameters<K, Change<V>>(kStreamMapValues, Name));

            var toStreamNode = new ProcessorGraphNode<K, IChange<V>>(
               Name,
               new ProcessorParameters<K, IChange<V>>(kStreamMapValues, Name));

            this.Builder.AddGraphNode<K, V>(this.StreamsGraphNode, toStreamNode);

            // we can inherit parent key and value serde
            return new KStream<K, V>(
                this.context,
                Name,
                this.KeySerde,
                this.ValueSerde,
                this.SourceNodes,
                repartitionRequired: false,
                toStreamNode,
                this.Builder);
        }


        public IKStream<K1, V> ToStream<K1>(KeyValueMapper<K, V, K1> mapper)
        {
            return this.ToStream().SelectKey(mapper);
        }


        public IKStream<K1, V> ToStream<K1>(
            KeyValueMapper<K, V, K1> mapper,
            Named named)
        {
            return this.ToStream(named).SelectKey(mapper);
        }

        public IKTable<K, V> Suppress(ISuppressed<K> suppressed)
        {
            string Name;
            if (suppressed is INamedSuppressed<K>)
            {
                var givenName = ((INamedSuppressed<object>)suppressed).Name;
                Name = givenName ?? this.Builder.NewProcessorName(KTable.SUPPRESS_NAME);
            }
            else
            {
                throw new System.ArgumentException("Custom sues of Suppressed are not supported.");
            }

            //SuppressedInternal<K, V> suppressedInternal = buildSuppress(suppressed, Name);

            // string storeName =
            //    suppressedInternal.Name != null
            //    ? suppressedInternal.Name + "-store"
            //    : builder.NewStoreName(KTable.SUPPRESS_NAME);

            // IProcessorSupplier<K, Change<V>> suppressionSupplier = new KTableSuppressProcessorSupplier<K, V>(
            //    suppressedInternal,
            //    storeName,
            //    this);

            // ProcessorGraphNode<K, Change<V>> node = new StatefulProcessorNode<>(
            //    Name,
            //    new ProcessorParameters<>(suppressionSupplier, Name),
            //    new InMemoryTimeOrderedKeyValueBuffer.Builder<>(storeName, keySerde, valSerde)
            //);

            // builder.AddGraphNode(streamsGraphNode, node);

            // return new KTable<K, S, V>(
            //     Name,
            //     keySerde,
            //     valSerde,
            //     new HashSet<string> { this.Name },
            //     null,
            //     suppressionSupplier,
            //     node,
            //     builder
            // );

            return null;
        }


        //private SuppressedInternal<K> buildSuppress(ISuppressed<K> suppress, string Name)
        //{
        //    if (suppress is FinalResultsSuppressionBuilder<K>)
        //    {
        //        long grace = findAndVerifyWindowGrace(streamsGraphNode);
        //        LOG.LogInformation("Using grace period of [{}] as the suppress duration for node [{}].",
        //                 DateTime.FromMilliseconds(grace), Name);

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

        public IKTable<K, R> Join<V1, R>(
            IKTable<K, V1> other,
            ValueJoiner<V, V1, R> joiner)
        {
            return this.DoJoin(
                other,
                joiner,
                NamedInternal.Empty(),
                null,
                false,
                false);
        }


        public IKTable<K, VR> Join<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            Named named)
        {
            return this.DoJoin(
                other,
                joiner,
                named,
                null,
                leftOuter: false,
                rightOuter: false);
        }


        public IKTable<K, VR> Join<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return null;// join(other, joiner, NamedInternal.empty(), materialized);
        }


        public IKTable<K, VR> Join<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            Named named,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            var materializedInternal =
               new MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>(materialized, this.Builder, KTable.MERGE_NAME);

            return this.DoJoin(other, joiner, named, materializedInternal, false, false);
        }


        public IKTable<K, VR> OuterJoin<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner)
        {
            return this.OuterJoin(other, joiner, NamedInternal.Empty());
        }

        public IKTable<K, VR> OuterJoin<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            Named named)
        {
            return this.DoJoin(other, joiner, named, null, true, true);
        }

        public IKTable<K, VR> OuterJoin<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return this.OuterJoin(other, joiner, NamedInternal.Empty(), materialized);
        }


        public IKTable<K, VR> OuterJoin<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            Named named,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            var materializedInternal =
               new MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>(materialized, this.Builder, KTable.MERGE_NAME);

            return this.DoJoin(other, joiner, named, materializedInternal, true, true);
        }

        public IKTable<K, VR> LeftJoin<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner)
        {
            return this.LeftJoin(other, joiner, NamedInternal.Empty());
        }

        public IKTable<K, VR> LeftJoin<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            Named named)
        {
            return this.DoJoin(other, joiner, named, null, true, false);
        }

        public IKTable<K, VR> LeftJoin<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return this.LeftJoin(other, joiner, NamedInternal.Empty(), materialized);
        }


        public IKTable<K, VR> LeftJoin<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            Named named,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            var materializedInternal =
               new MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>(materialized, this.Builder, KTable.MERGE_NAME);

            return this.DoJoin(other, joiner, named, materializedInternal, true, false);
        }

        private IKTable<K, VR> DoJoin<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            Named joinName,
            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>? materializedInternal,
            bool leftOuter,
            bool rightOuter)
        {
            other = other ?? throw new ArgumentNullException(nameof(other));
            joiner = joiner ?? throw new ArgumentNullException(nameof(joiner));
            joinName = joinName ?? throw new ArgumentNullException(nameof(joinName));

            var renamed = new NamedInternal(joinName);
            var joinMergeName = renamed.OrElseGenerateWithPrefix(this.Builder, KTable.MERGE_NAME);
            HashSet<string> allSourceNodes = this.EnsureJoinableWith((AbstractStream<K, VO>)other);

            if (leftOuter)
            {
                this.EnableSendingOldValues();
            }

            if (rightOuter)
            {
                other.EnableSendingOldValues();
            }

            KTableKTableAbstractJoin<K, VR, V, VO>? joinThis = null;
            KTableKTableAbstractJoin<K, VR, VO, V>? joinOther = null;

            if (!leftOuter)
            { // inner
                joinThis = new KTableKTableInnerJoin<K, VR, V, VO>(this, (KTable<K, S, VO>)other, joiner, this.QueryableStoreName);
                joinOther = new KTableKTableInnerJoin<K, VR, VO, V>((KTable<K, S, VO>)other, this, ReverseJoiner(joiner), this.QueryableStoreName);
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

            var joinThisName = renamed.SuffixWithOrElseGet("-join-this", this.Builder, KTable.JOINTHIS_NAME);
            var joinOtherName = renamed.SuffixWithOrElseGet("-join-other", this.Builder, KTable.JOINOTHER_NAME);

            var joinThisProcessorParameters = new ProcessorParameters<K, IChange<V>>(joinThis, joinThisName);
            var joinOtherProcessorParameters = new ProcessorParameters<K, IChange<VO>>(joinOther, joinOtherName);

            ISerde<K>? keySerde;
            ISerde<VR>? valueSerde;
            string? queryableStoreName;
            IStoreBuilder<ITimestampedKeyValueStore<K, VR>>? storeBuilder;

            if (materializedInternal != null)
            {
                keySerde = materializedInternal.KeySerde ?? this.KeySerde;

                valueSerde = materializedInternal.ValueSerde;
                queryableStoreName = materializedInternal.StoreName;
                //storeBuilder = new TimestampedKeyValueStoreMaterializer<K, VR>(
                //        this.clock, 
                //        materializedInternal)
                //    .Materialize();
            }
            else
            {
                keySerde = this.KeySerde;
                valueSerde = null;
                queryableStoreName = null;
                storeBuilder = null;
            }

            //KTableKTableJoinNode<K, V, VO, VR> kTableKTableJoinNode =
            //   KTableKTableJoinNode<K, V, VO, VR>.kTableKTableJoinNodeBuilder()
            //        .withNodeName(joinMergeName)
            //        .withJoinThisProcessorParameters(joinThisProcessorParameters)
            //        .withJoinOtherProcessorParameters(joinOtherProcessorParameters)
            //        .withThisJoinSideNodeName(Name)
            //        .withOtherJoinSideNodeName(((KTable)other).Name)
            //        .withJoinThisStoreNames(valueGetterSupplier().storeNames())
            //        .withJoinOtherStoreNames(((KTable)other).valueGetterSupplier.storeNames())
            //        .withKeySerde(keySerde)
            //        .WithValueSerde(valueSerde)
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
            KeyValueMapper<K, V, KeyValuePair<KR, VR>> selector)
        {
            return this.GroupBy(selector, Grouped.With<KR, VR>(null, null));
        }

        [Obsolete]
        public IKGroupedTable<K1, V1> GroupBy<K1, V1>(
            KeyValueMapper<K, V, KeyValuePair<K1, V1>> selector,
            ISerialized<K1, V1> serialized)
        {
            selector = selector ?? throw new ArgumentNullException(nameof(selector));
            serialized = serialized ?? throw new ArgumentNullException(nameof(serialized));

            var serializedInternal = new SerializedInternal<K1, V1>(serialized);
            return this.GroupBy(selector, Grouped.With(serializedInternal.keySerde, serializedInternal.valueSerde));
        }

        public IKGroupedTable<K1, V1> GroupBy<K1, V1>(
            KeyValueMapper<K, V, KeyValuePair<K1, V1>>? selector,
            Grouped<K1, V1>? grouped)
        {
            selector = selector ?? throw new ArgumentNullException(nameof(selector));
            grouped = grouped ?? throw new ArgumentNullException(nameof(grouped));

            var groupedInternal = new GroupedInternal<K1, V1>(grouped);
            var selectName = new NamedInternal(groupedInternal.Name)
                .OrElseGenerateWithPrefix(this.Builder, KTable.SELECT_NAME);

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
            if (this.processorSupplier is KTableSource<K, V> source)
            {
                // whenever a source ktable is required for getter, it should be materialized
                source.Materialize();

                return new KTableSourceValueGetterSupplier<K, V>(this.context, source.queryableName);
            }
            else if (this.processorSupplier is IKStreamAggProcessorSupplier)
            {
                return ((IKStreamAggProcessorSupplier<object, K, S, V>)this.processorSupplier).View();
            }
            else
            {
                return ((IKTableProcessorSupplier<K, VR, V>)this.processorSupplier).View();
            }
        }

        public void EnableSendingOldValues()
        {
            if (!this.sendOldValues)
            {
                if (this.processorSupplier is KTableSource<K, V>)
                {
                    var source = (KTableSource<K, object>)this.processorSupplier;
                    source.EnableSendingOldValues();
                }
                else if (this.processorSupplier is IKStreamAggProcessorSupplier)
                {
                    ((IKStreamAggProcessorSupplier<object, K, S, V>)this.processorSupplier).EnableSendingOldValues();
                }
                else
                {
                    ((IKTableProcessorSupplier<K, S, V>)this.processorSupplier).EnableSendingOldValues();
                }

                this.sendOldValues = true;
            }
        }
    }
}
