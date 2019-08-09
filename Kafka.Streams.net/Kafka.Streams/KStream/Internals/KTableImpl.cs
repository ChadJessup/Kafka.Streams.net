using Kafka.Common.Utils;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.Processor;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using Microsoft.Extensions.Logging;
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
    public class KTableImpl<K, S, V> : AbstractStream<K, V>, IKTable<K, V>
    {
        private static ILogger LOG = new LoggerFactory().CreateLogger<KTableImpl<K, S, V>>();

        public static string SOURCE_NAME = "KTABLE-SOURCE-";

        static string STATE_STORE_NAME = "STATE-STORE-";

        private static string FILTER_NAME = "KTABLE-FILTER-";

        private static string JOINTHIS_NAME = "KTABLE-JOINTHIS-";

        private static string JOINOTHER_NAME = "KTABLE-JOINOTHER-";

        private static string MAPVALUES_NAME = "KTABLE-MAPVALUES-";

        private static string MERGE_NAME = "KTABLE-MERGE-";

        private static string SELECT_NAME = "KTABLE-SELECT-";

        private static string SUPPRESS_NAME = "KTABLE-SUPPRESS-";

        private static string TOSTREAM_NAME = "KTABLE-TOSTREAM-";

        private static string TRANSFORMVALUES_NAME = "KTABLE-TRANSFORMVALUES-";

        private IProcessorSupplier<K, object> processorSupplier;

        private string queryableStoreName;

        private bool sendOldValues = false;

        public KTableImpl(string name,
                           ISerde<K> keySerde,
                           ISerde<V> valSerde,
                           HashSet<string> sourceNodes,
                           string queryableStoreName,
                           IProcessorSupplier<K, object> processorSupplier,
                           StreamsGraphNode streamsGraphNode,
                           InternalStreamsBuilder builder)
            : base(name, keySerde, valSerde, sourceNodes, streamsGraphNode, builder)
        {
            this.processorSupplier = processorSupplier;
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
                if (materializedInternal.storeName() == null)
                {
                    builder.newStoreName(FILTER_NAME);
                }
                // we can inherit parent key and value serde if user do not provide specific overrides, more specifically:
                // we preserve the key following the order of 1) materialized, 2) parent
                keySerde = materializedInternal.keySerde != null ? materializedInternal.keySerde : this.keySerde;
                // we preserve the value following the order of 1) materialized, 2) parent
                valueSerde = materializedInternal.valueSerde != null ? materializedInternal.valueSerde : this.valSerde;
                queryableStoreName = materializedInternal.queryableStoreName();
                // only materialize if materialized is specified and it has queryable name
                storeBuilder = queryableStoreName != null ? (new TimestampedKeyValueStoreMaterializer<K, V>(materializedInternal)).materialize() : null;
            }
            else
            {
                keySerde = this.keySerde;
                valueSerde = this.valSerde;
                queryableStoreName = null;
                storeBuilder = null;
            }
            string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, FILTER_NAME);

            IKTableProcessorSupplier<K, V, V> processorSupplier =
               new KTableFilter<K, V>(this, predicate, filterNot, queryableStoreName);

            ProcessorParameters<K, V> processorParameters = unsafeCastProcessorParametersToCompletelyDifferentType(
               new ProcessorParameters<K, V>(processorSupplier, name)
           );

            StreamsGraphNode tableNode = new TableProcessorNode<>(
               name,
               processorParameters,
               storeBuilder
           );

            builder.addGraphNode(this.streamsGraphNode, tableNode);

            return new KTableImpl<>(name,
                                    keySerde,
                                    valueSerde,
                                    sourceNodes,
                                    queryableStoreName,
                                    processorSupplier,
                                    tableNode,
                                    builder);
        }


        public IKTable<K, V> filter(IPredicate<K, V> predicate)
        {
            predicate = predicate ?? throw new System.ArgumentNullException("predicate can't be null", nameof(predicate));
            return doFilter(predicate, NamedInternal.empty(), null, false);
        }


        public IKTable<K, V> filter(IPredicate<K, V> predicate, Named named)
        {
            predicate = predicate ?? throw new System.ArgumentNullException("predicate can't be null", nameof(predicate));
            return doFilter(predicate, named, null, false);
        }


        public IKTable<K, V> filter(IPredicate<K, V> predicate,
                                    Named named,
                                    Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            predicate = predicate ?? throw new System.ArgumentNullException("predicate can't be null", nameof(predicate));
            materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));
            MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);

            return doFilter(predicate, named, materializedInternal, false);
        }


        public IKTable<K, V> filter(IPredicate<K, V> predicate,
                                    Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return filter(predicate, NamedInternal.empty(), materialized);
        }


        public IKTable<K, V> filterNot(IPredicate<K, V> predicate)
        {
            predicate = predicate ?? throw new System.ArgumentNullException("predicate can't be null", nameof(predicate));
            return doFilter(predicate, NamedInternal.empty(), null, true);
        }


        public IKTable<K, V> filterNot(IPredicate<K, V> predicate,
                                       Named named)
        {
            predicate = predicate ?? throw new System.ArgumentNullException("predicate can't be null", nameof(predicate));
            return doFilter(predicate, named, null, true);
        }


        public IKTable<K, V> filterNot(IPredicate<K, V> predicate,
                                       Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return filterNot(predicate, NamedInternal.empty(), materialized);
        }


        public IKTable<K, V> filterNot(IPredicate<K, V> predicate,
                                       Named named,
                                       Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            predicate = predicate ?? throw new System.ArgumentNullException("predicate can't be null", nameof(predicate));
            materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));
            MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);
            NamedInternal renamed = new NamedInternal(named);
            return doFilter(predicate, renamed, materializedInternal, true);
        }

        private IKTable<K, VR> doMapValues(IValueMapperWithKey<K, V, VR> mapper,
                                                Named named,
                                                MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal)
        {
            ISerde<K> keySerde;
            ISerde<VR> valueSerde;
            string queryableStoreName;
            StoreBuilder<ITimestampedKeyValueStore<K, VR>> storeBuilder;

            if (materializedInternal != null)
            {
                // we actually do not need to generate store names at all since if it is not specified, we will not
                // materialize the store; but we still need to burn one index BEFORE generating the processor to keep compatibility.
                if (materializedInternal.storeName() == null)
                {
                    builder.newStoreName(MAPVALUES_NAME);
                }
                keySerde = materializedInternal.keySerde != null ? materializedInternal.keySerde : this.keySerde;
                valueSerde = materializedInternal.valueSerde;
                queryableStoreName = materializedInternal.queryableStoreName();
                // only materialize if materialized is specified and it has queryable name
                storeBuilder = queryableStoreName != null ? (new TimestampedKeyValueStoreMaterializer<>(materializedInternal)).materialize() : null;
            }
            else
            {
                keySerde = this.keySerde;
                valueSerde = null;
                queryableStoreName = null;
                storeBuilder = null;
            }

            string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, MAPVALUES_NAME);

            IKTableProcessorSupplier<K, V, VR> processorSupplier = new KTableMapValues<>(this, mapper, queryableStoreName);

            // leaving in calls to ITB until building topology with graph

            ProcessorParameters<K, VR> processorParameters = unsafeCastProcessorParametersToCompletelyDifferentType(
               new ProcessorParameters<>(processorSupplier, name)
           );
            StreamsGraphNode tableNode = new TableProcessorNode<>(
               name,
               processorParameters,
               storeBuilder
           );

            builder.addGraphNode(this.streamsGraphNode, tableNode);

            // don't inherit parent value serde, since this operation may change the value type, more specifically:
            // we preserve the key following the order of 1) materialized, 2) parent, 3) null
            // we preserve the value following the order of 1) materialized, 2) null
            return new KTableImpl<>(
                name,
                keySerde,
                valueSerde,
                sourceNodes,
                queryableStoreName,
                processorSupplier,
                tableNode,
                builder
            );
        }


        public IKTable<K, VR> mapValues(IValueMapper<V, VR> mapper)
        {
            mapper = mapper ?? throw new System.ArgumentNullException("mapper can't be null", nameof(mapper));
            return doMapValues(withKey(mapper), NamedInternal.empty(), null);
        }


        public IKTable<K, VR> mapValues(IValueMapper<V, VR> mapper,
                                             Named named)
        {
            mapper = mapper ?? throw new System.ArgumentNullException("mapper can't be null", nameof(mapper));
            return doMapValues(withKey(mapper), named, null);
        }


        public IKTable<K, VR> mapValues(IValueMapperWithKey<K, V, VR> mapper)
        {
            mapper = mapper ?? throw new System.ArgumentNullException("mapper can't be null", nameof(mapper));
            return doMapValues(mapper, NamedInternal.empty(), null);
        }


        public IKTable<K, VR> mapValues(IValueMapperWithKey<K, V, VR> mapper,
                                             Named named)
        {
            mapper = mapper ?? throw new System.ArgumentNullException("mapper can't be null", nameof(mapper));
            return doMapValues(mapper, named, null);
        }


        public IKTable<K, VR> mapValues(IValueMapper<V, VR> mapper,
                                             Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return mapValues(mapper, NamedInternal.empty(), materialized);
        }


        public IKTable<K, VR> mapValues(IValueMapper<V, VR> mapper,
                                             Named named,
                                             Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            mapper = mapper ?? throw new System.ArgumentNullException("mapper can't be null", nameof(mapper));
            materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));

            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);

            return doMapValues(withKey(mapper), named, materializedInternal);
        }


        public IKTable<K, VR> mapValues(IValueMapperWithKey<K, V, VR> mapper,
                                             Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return mapValues(mapper, NamedInternal.empty(), materialized);
        }


        public IKTable<K, VR> mapValues(IValueMapperWithKey<K, V, VR> mapper,
                                             Named named,
                                             Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            mapper = mapper ?? throw new System.ArgumentNullException("mapper can't be null", nameof(mapper));
            materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));

            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);

            return doMapValues(mapper, named, materializedInternal);
        }


        public IKTable<K, VR> transformValues(IValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
                                                   string[] stateStoreNames)
        {
            return doTransformValues(transformerSupplier, null, NamedInternal.empty(), stateStoreNames);
        }


        public IKTable<K, VR> transformValues(IValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
                                                   Named named,
                                                   string[] stateStoreNames)
        {
            named = named ?? throw new System.ArgumentNullException("processorName can't be null", nameof(named));
            return doTransformValues(transformerSupplier, null, new NamedInternal(named), stateStoreNames);
        }


        public IKTable<K, VR> transformValues(IValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
                                                   Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized,
                                                   string[] stateStoreNames)
        {
            return transformValues(transformerSupplier, materialized, NamedInternal.empty(), stateStoreNames);
        }


        public IKTable<K, VR> transformValues(IValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
                                                   Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized,
                                                   Named named,
                                                   string[] stateStoreNames)
        {
            materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));
            named = named ?? throw new System.ArgumentNullException("named can't be null", nameof(named));
            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);

            return doTransformValues(transformerSupplier, materializedInternal, new NamedInternal(named), stateStoreNames);
        }

        private IKTable<K, VR> doTransformValues(IValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
                                                      MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal,
                                                      NamedInternal namedInternal,
                                                      string[] stateStoreNames)
        {
            stateStoreNames = stateStoreNames ?? throw new System.ArgumentNullException("stateStoreNames", nameof(stateStoreNames));
            ISerde<K> keySerde;
            ISerde<VR> valueSerde;
            string queryableStoreName;
            StoreBuilder<ITimestampedKeyValueStore<K, VR>> storeBuilder;

            if (materializedInternal != null)
            {
                // don't inherit parent value serde, since this operation may change the value type, more specifically:
                // we preserve the key following the order of 1) materialized, 2) parent, 3) null
                keySerde = materializedInternal.keySerde != null ? materializedInternal.keySerde : this.keySerde;
                // we preserve the value following the order of 1) materialized, 2) null
                valueSerde = materializedInternal.valueSerde;
                queryableStoreName = materializedInternal.queryableStoreName();
                // only materialize if materialized is specified and it has queryable name
                storeBuilder = queryableStoreName != null ? (new TimestampedKeyValueStoreMaterializer<>(materializedInternal)).materialize() : null;
            }
            else
            {
                keySerde = this.keySerde;
                valueSerde = null;
                queryableStoreName = null;
                storeBuilder = null;
            }

            string name = namedInternal.orElseGenerateWithPrefix(builder, TRANSFORMVALUES_NAME);

            IKTableProcessorSupplier<K, V, VR> processorSupplier = new KTableTransformValues<>(
               this,
               transformerSupplier,
               queryableStoreName);

            ProcessorParameters<K, VR> processorParameters = unsafeCastProcessorParametersToCompletelyDifferentType(
               new ProcessorParameters<>(processorSupplier, name)
           );

            StreamsGraphNode tableNode = new TableProcessorNode<>(
               name,
               processorParameters,
               storeBuilder,
               stateStoreNames
           );

            builder.addGraphNode(this.streamsGraphNode, tableNode);

            return new KTableImpl<>(
                name,
                keySerde,
                valueSerde,
                sourceNodes,
                queryableStoreName,
                processorSupplier,
                tableNode,
                builder);
        }


        public IKStream<K, V> toStream()
        {
            return toStream(NamedInternal.empty());
        }


        public IKStream<K, V> toStream(Named named)
        {
            named = named ?? throw new System.ArgumentNullException("named can't be null", nameof(named));

            string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, TOSTREAM_NAME);
            ProcessorSupplier<K, Change<V>> kStreamMapValues = new KStreamMapValues<>((key, change)=>change.newValue);
            ProcessorParameters<K, V> processorParameters = unsafeCastProcessorParametersToCompletelyDifferentType(
               new ProcessorParameters<>(kStreamMapValues, name)
           );

            ProcessorGraphNode<K, V> toStreamNode = new ProcessorGraphNode<>(
               name,
               processorParameters
           );

            builder.addGraphNode(this.streamsGraphNode, toStreamNode);

            // we can inherit parent key and value serde
            return new KStreamImpl<>(name, keySerde, valSerde, sourceNodes, false, toStreamNode, builder);
        }


        public IKStream<K1, V> toStream(IKeyValueMapper<K, V, K1> mapper)
        {
            return toStream().selectKey(mapper);
        }


        public IKStream<K1, V> toStream(IKeyValueMapper<K, V, K1> mapper,
                                             Named named)
        {
            return toStream(named).selectKey(mapper);
        }


        public IKTable<K, V> suppress(ISuppressed<K> suppressed)
        {
            string name;
            if (suppressed is NamedSuppressed)
            {
                string givenName = ((NamedSuppressed<object>)suppressed).name();
                name = givenName != null ? givenName : builder.newProcessorName(SUPPRESS_NAME);
            }
            else
            {
                throw new System.ArgumentException("Custom sues of Suppressed are not supported.");
            }

            SuppressedInternal<K> suppressedInternal = buildSuppress(suppressed, name);

            string storeName =
               suppressedInternal.name() != null ? suppressedInternal.name() + "-store" : builder.newStoreName(SUPPRESS_NAME);

            ProcessorSupplier<K, Change<V>> suppressionSupplier = new KTableSuppressProcessorSupplier<>(
               suppressedInternal,
               storeName,
               this
           );

            ProcessorGraphNode<K, Change<V>> node = new StatefulProcessorNode<>(
               name,
               new ProcessorParameters<>(suppressionSupplier, name),
               new InMemoryTimeOrderedKeyValueBuffer.Builder<>(storeName, keySerde, valSerde)
           );

            builder.addGraphNode(streamsGraphNode, node);

            return new KTableImpl<K, S, V>(
                name,
                keySerde,
                valSerde,
                Collections.singleton(this.name),
                null,
                suppressionSupplier,
                node,
                builder
            );
        }


        private SuppressedInternal<K> buildSuppress(ISuppressed<K> suppress, string name)
        {
            if (suppress is FinalResultsSuppressionBuilder)
            {
                long grace = findAndVerifyWindowGrace(streamsGraphNode);
                LOG.LogInformation("Using grace period of [{}] as the suppress duration for node [{}].",
                         Duration.ofMillis(grace), name);

                FinalResultsSuppressionBuilder<object> builder = (FinalResultsSuppressionBuilder<object>)suppress;

                SuppressedInternal<object> finalResultsSuppression =
                   builder.buildFinalResultsSuppression(Duration.ofMillis(grace));

                return (SuppressedInternal<K>)finalResultsSuppression;
            }
            else if (suppress is SuppressedInternal)
            {
                return (SuppressedInternal<K>)suppress;
            }
            else
            {
                throw new System.ArgumentException("Custom sues of Suppressed are not allowed.");
            }
        }

        public IKTable<K, R> join(IKTable<K, V1> other,
                                          IValueJoiner<V, V1, R> joiner)
        {
            return doJoin(other, joiner, NamedInternal.empty(), null, false, false);
        }


        public IKTable<K, R> join(IKTable<K, V1> other,
                                          IValueJoiner<V, V1, R> joiner,
                                          Named named)
        {
            return doJoin(other, joiner, named, null, false, false);
        }


        public IKTable<K, VR> join(IKTable<K, VO> other,
                                            IValueJoiner<V, VO, VR> joiner,
                                            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return join(other, joiner, NamedInternal.empty(), materialized);
        }


        public IKTable<K, VR> join(IKTable<K, VO> other,
                                            IValueJoiner<V, VO, VR> joiner,
                                            Named named,
                                            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));
            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal =
               new MaterializedInternal<>(materialized, builder, MERGE_NAME);

            return doJoin(other, joiner, named, materializedInternal, false, false);
        }


        public IKTable<K, R> outerJoin(IKTable<K, V1> other,
                                               IValueJoiner<V, V1, R> joiner)
        {
            return outerJoin(other, joiner, NamedInternal.empty());
        }


        public IKTable<K, R> outerJoin(IKTable<K, V1> other,
                                               IValueJoiner<V, V1, R> joiner,
                                               Named named)
        {
            return doJoin(other, joiner, named, null, true, true);
        }


        public IKTable<K, VR> outerJoin(IKTable<K, VO> other,
                                                 IValueJoiner<V, VO, VR> joiner,
                                                 Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return outerJoin(other, joiner, NamedInternal.empty(), materialized);
        }


        public IKTable<K, VR> outerJoin(IKTable<K, VO> other,
                                                 IValueJoiner<V, VO, VR> joiner,
                                                 Named named,
                                                 Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));
            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal =
               new MaterializedInternal<>(materialized, builder, MERGE_NAME);

            return doJoin(other, joiner, named, materializedInternal, true, true);
        }


        public IKTable<K, R> leftJoin(IKTable<K, V1> other,
                                              IValueJoiner<V, V1, R> joiner)
        {
            return leftJoin(other, joiner, NamedInternal.empty());
        }


        public IKTable<K, R> leftJoin(IKTable<K, V1> other,
                                              IValueJoiner<V, V1, R> joiner,
                                              Named named)
        {
            return doJoin(other, joiner, named, null, true, false);
        }


        public IKTable<K, VR> leftJoin(IKTable<K, VO> other,
                                                IValueJoiner<V, VO, VR> joiner,
                                                Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            return leftJoin(other, joiner, NamedInternal.empty(), materialized);
        }


        public IKTable<K, VR> leftJoin<VR, VO>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Named named,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));
            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal =
               new MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>(materialized, builder, MERGE_NAME);

            return doJoin(other, joiner, named, materializedInternal, true, false);
        }


        private IKTable<K, VR> doJoin<VR, VO>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Named joinName,
            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal,
            bool leftOuter,
            bool rightOuter)
        {
            other = other ?? throw new System.ArgumentNullException("other can't be null", nameof(other));
            joiner = joiner ?? throw new System.ArgumentNullException("joiner can't be null", nameof(joiner));
            joinName = joinName ?? throw new System.ArgumentNullException("joinName can't be null", nameof(joinName));

            NamedInternal renamed = new NamedInternal(joinName);
            string joinMergeName = renamed.orElseGenerateWithPrefix(builder, MERGE_NAME);
            HashSet<string> allSourceNodes = ensureJoinableWith((AbstractStream<K, VO>)other);

            if (leftOuter)
            {
                enableSendingOldValues();
            }
            if (rightOuter)
            {
                ((KTableImpl)other).enableSendingOldValues();
            }

            KTableKTableAbstractJoin<K, VR, V, VO> joinThis;
            KTableKTableAbstractJoin<K, VR, VO, V> joinOther;

            if (!leftOuter)
            { // inner
                joinThis = new KTableKTableInnerJoin<>(this, (KTableImpl<K, object, VO>) other, joiner);
                joinOther = new KTableKTableInnerJoin<>((KTableImpl<K, object, VO>) other, this, reverseJoiner(joiner));
            }
            else if (!rightOuter)
            { // left
                joinThis = new KTableKTableLeftJoin<>(this, (KTableImpl<K, object, VO>) other, joiner);
                joinOther = new KTableKTableRightJoin<>((KTableImpl<K, object, VO>) other, this, reverseJoiner(joiner));
            }
            else
            { // outer
                joinThis = new KTableKTableOuterJoin<>(this, (KTableImpl< K, object, VO>) other, joiner);
                joinOther = new KTableKTableOuterJoin<>((KTableImpl<K, object, VO>) other, this, reverseJoiner(joiner));
            }

            string joinThisName = renamed.suffixWithOrElseGet("-join-this", builder, JOINTHIS_NAME);
            string joinOtherName = renamed.suffixWithOrElseGet("-join-other", builder, JOINOTHER_NAME);

            ProcessorParameters<K, Change<V>> joinThisProcessorParameters = new ProcessorParameters<>(joinThis, joinThisName);
            ProcessorParameters<K, Change<VO>> joinOtherProcessorParameters = new ProcessorParameters<>(joinOther, joinOtherName);

            ISerde<K> keySerde;
            ISerde<VR> valueSerde;
            string queryableStoreName;
            StoreBuilder<ITimestampedKeyValueStore<K, VR>> storeBuilder;

            if (materializedInternal != null)
            {
                keySerde = materializedInternal.keySerde != null ? materializedInternal.keySerde : this.keySerde;
                valueSerde = materializedInternal.valueSerde;
                queryableStoreName = materializedInternal.storeName();
                storeBuilder = new TimestampedKeyValueStoreMaterializer<>(materializedInternal).materialize();
            }
            else
            {
                keySerde = this.keySerde;
                valueSerde = null;
                queryableStoreName = null;
                storeBuilder = null;
            }

            KTableKTableJoinNode<K, V, VO, VR> kTableKTableJoinNode =
               KTableKTableJoinNode.< K, V, VO, VR> kTableKTableJoinNodeBuilder()
                    .withNodeName(joinMergeName)
                    .withJoinThisProcessorParameters(joinThisProcessorParameters)
                    .withJoinOtherProcessorParameters(joinOtherProcessorParameters)
                    .withThisJoinSideNodeName(name)
                    .withOtherJoinSideNodeName(((KTableImpl)other).name)
                    .withJoinThisStoreNames(valueGetterSupplier().storeNames())
                    .withJoinOtherStoreNames(((KTableImpl)other).valueGetterSupplier().storeNames())
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde)
                    .withQueryableStoreName(queryableStoreName)
                    .withStoreBuilder(storeBuilder)
                    .build();
            builder.addGraphNode(this.streamsGraphNode, kTableKTableJoinNode);

            // we can inherit parent key serde if user do not provide specific overrides
            return new KTableImpl<K, Change<VR>, VR>(
                kTableKTableJoinNode.nodeName(),
                kTableKTableJoinNode.keySerde,
                kTableKTableJoinNode.valueSerde,
                allSourceNodes,
                kTableKTableJoinNode.queryableStoreName(),
                kTableKTableJoinNode.joinMerger(),
                kTableKTableJoinNode,
                builder
            );
        }


        public IKGroupedTable<K1, V1> groupBy(IKeyValueMapper<K, V, KeyValue<K1, V1>> selector)
        {
            return groupBy(selector, Grouped.with(null, null));
        }


        [System.Obsolete]
        public IKGroupedTable<K1, V1> groupBy(IKeyValueMapper<K, V, KeyValue<K1, V1>> selector,
                                                       ISerialized<K1, V1> serialized)
        {
            selector = selector ?? throw new System.ArgumentNullException("selector can't be null", nameof(selector));
            serialized = serialized ?? throw new System.ArgumentNullException("serialized can't be null", nameof(serialized));
            SerializedInternal<K1, V1> serializedInternal = new SerializedInternal<>(serialized);
            return groupBy(selector, Grouped.with(serializedInternal.keySerde, serializedInternal.valueSerde));
        }


        public IKGroupedTable<K1, V1> groupBy(IKeyValueMapper<K, V, KeyValue<K1, V1>> selector,
                                                       Grouped<K1, V1> grouped)
        {
            selector = selector ?? throw new System.ArgumentNullException("selector can't be null", nameof(selector));
            grouped = grouped ?? throw new System.ArgumentNullException("grouped can't be null", nameof(grouped));
            GroupedInternal<K1, V1> groupedInternal = new GroupedInternal<>(grouped);
            string selectName = new NamedInternal(groupedInternal.name).orElseGenerateWithPrefix(builder, SELECT_NAME);

            IKTableProcessorSupplier<K, V, KeyValue<K1, V1>> selectSupplier = new KTableRepartitionMap<>(this, selector);
            ProcessorParameters<K, Change<V>> processorParameters = new ProcessorParameters<>(selectSupplier, selectName);

            // select the aggregate key and values (old and new), it would require parent to send old values
            ProcessorGraphNode<K, Change<V>> groupByMapNode = new ProcessorGraphNode<>(selectName, processorParameters);

            builder.addGraphNode(this.streamsGraphNode, groupByMapNode);

            this.enableSendingOldValues();
            return new KGroupedTableImpl<>(
                builder,
                selectName,
                sourceNodes,
                groupedInternal,
                groupByMapNode
            );
        }


        public IKTableValueGetterSupplier<K, V> valueGetterSupplier()
        {
            if (processorSupplier is KTableSource)
            {
                KTableSource<K, V> source = (KTableSource<K, V>)processorSupplier;
                // whenever a source ktable is required for getter, it should be materialized
                source.materialize();
                return new KTableSourceValueGetterSupplier<>(source.queryableName());
            }
            else if (processorSupplier is KStreamAggProcessorSupplier)
            {
                return ((KStreamAggProcessorSupplier<object, K, S, V>)processorSupplier).view();
            }
            else
            {
                return ((IKTableProcessorSupplier<K, S, V>)processorSupplier).view();
            }
        }


        public void enableSendingOldValues()
        {
            if (!sendOldValues)
            {
                if (processorSupplier is KTableSource)
                {
                    KTableSource<K, object> source = (KTableSource<K, V>)processorSupplier;
                    source.enableSendingOldValues();
                }
                else if (processorSupplier is KStreamAggProcessorSupplier)
                {
                    ((KStreamAggProcessorSupplier<object, K, S, V>)processorSupplier).enableSendingOldValues();
                }
                else
                {
                    ((IKTableProcessorSupplier<K, S, V>)processorSupplier).enableSendingOldValues();
                }
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

        private ProcessorParameters<K, VR> unsafeCastProcessorParametersToCompletelyDifferentType<VR>(ProcessorParameters<K, Change<V>> kObjectProcessorParameters)
        {
            return (ProcessorParameters<K, VR>)kObjectProcessorParameters;
        }
    }
}
