/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for.Additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
namespace Kafka.streams.kstream.internals;










































/**
 * The implementation class of {@link KTable}.
 *
 * @param <K> the key type
 * @param <S> the source's (parent's) value type
 * @param <V> the value type
 */
public class KTableImpl<K, S, V> : AbstractStream<K, V> : KTable<K, V> {
    private static  Logger LOG = LoggerFactory.getLogger(KTableImpl.class);

    static  string SOURCE_NAME = "KTABLE-SOURCE-";

    static  string STATE_STORE_NAME = "STATE-STORE-";

    private static  string FILTER_NAME = "KTABLE-FILTER-";

    private static  string JOINTHIS_NAME = "KTABLE-JOINTHIS-";

    private static  string JOINOTHER_NAME = "KTABLE-JOINOTHER-";

    private static  string MAPVALUES_NAME = "KTABLE-MAPVALUES-";

    private static  string MERGE_NAME = "KTABLE-MERGE-";

    private static  string SELECT_NAME = "KTABLE-SELECT-";

    private static  string SUPPRESS_NAME = "KTABLE-SUPPRESS-";

    private static  string TOSTREAM_NAME = "KTABLE-TOSTREAM-";

    private static  string TRANSFORMVALUES_NAME = "KTABLE-TRANSFORMVALUES-";

    private  ProcessorSupplier<?, ?> processorSupplier;

    private  string queryableStoreName;

    private bool sendOldValues = false;

    public KTableImpl( string name,
                       ISerde<K> keySerde,
                       ISerde<V> valSerde,
                       HashSet<string> sourceNodes,
                       string queryableStoreName,
                       ProcessorSupplier<?, ?> processorSupplier,
                       StreamsGraphNode streamsGraphNode,
                       InternalStreamsBuilder builder)
{
        super(name, keySerde, valSerde, sourceNodes, streamsGraphNode, builder);
        this.processorSupplier = processorSupplier;
        this.queryableStoreName = queryableStoreName;
    }

    
    public string queryableStoreName()
{
        return queryableStoreName;
    }

    private KTable<K, V> doFilter( Predicate<K, V> predicate,
                                   Named named,
                                   MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal,
                                   bool filterNot)
{
         ISerde<K> keySerde;
         ISerde<V> valueSerde;
         string queryableStoreName;
         StoreBuilder<TimestampedKeyValueStore<K, V>> storeBuilder;

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
            keySerde = materializedInternal.keySerde() != null ? materializedInternal.keySerde() : this.keySerde;
            // we preserve the value following the order of 1) materialized, 2) parent
            valueSerde = materializedInternal.valueSerde() != null ? materializedInternal.valueSerde() : this.valSerde;
            queryableStoreName = materializedInternal.queryableStoreName();
            // only materialize if materialized is specified and it has queryable name
            storeBuilder = queryableStoreName != null ? (new TimestampedKeyValueStoreMaterializer<>(materializedInternal)).materialize() : null;
        } else {
            keySerde = this.keySerde;
            valueSerde = this.valSerde;
            queryableStoreName = null;
            storeBuilder = null;
        }
         string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, FILTER_NAME);

         KTableProcessorSupplier<K, V, V> processorSupplier =
            new KTableFilter<>(this, predicate, filterNot, queryableStoreName);

         ProcessorParameters<K, V> processorParameters = unsafeCastProcessorParametersToCompletelyDifferentType(
            new ProcessorParameters<>(processorSupplier, name)
        );

         StreamsGraphNode tableNode = new TableProcessorNode<>(
            name,
            processorParameters,
            storeBuilder
        );

        builder.AddGraphNode(this.streamsGraphNode, tableNode);

        return new KTableImpl<>(name,
                                keySerde,
                                valueSerde,
                                sourceNodes,
                                queryableStoreName,
                                processorSupplier,
                                tableNode,
                                builder);
    }

    
    public KTable<K, V> filter( Predicate<K, V> predicate)
{
        Objects.requireNonNull(predicate, "predicate can't be null");
        return doFilter(predicate, NamedInternal.empty(), null, false);
    }

    
    public KTable<K, V> filter( Predicate<K, V> predicate,  Named named)
{
        Objects.requireNonNull(predicate, "predicate can't be null");
        return doFilter(predicate, named, null, false);
    }

    
    public KTable<K, V> filter( Predicate<K, V> predicate,
                                Named named,
                                Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
{
        Objects.requireNonNull(predicate, "predicate can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
         MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);

        return doFilter(predicate, named, materializedInternal, false);
    }

    
    public KTable<K, V> filter( Predicate<K, V> predicate,
                                Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
{
        return filter(predicate, NamedInternal.empty(), materialized);
    }

    
    public KTable<K, V> filterNot( Predicate<K, V> predicate)
{
        Objects.requireNonNull(predicate, "predicate can't be null");
        return doFilter(predicate, NamedInternal.empty(), null, true);
    }

    
    public KTable<K, V> filterNot( Predicate<K, V> predicate,
                                   Named named)
{
        Objects.requireNonNull(predicate, "predicate can't be null");
        return doFilter(predicate, named, null, true);
    }

    
    public KTable<K, V> filterNot( Predicate<K, V> predicate,
                                   Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
{
        return filterNot(predicate, NamedInternal.empty(), materialized);
    }

    
    public KTable<K, V> filterNot( Predicate<K, V> predicate,
                                   Named named,
                                   Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
{
        Objects.requireNonNull(predicate, "predicate can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
         MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);
         NamedInternal renamed = new NamedInternal(named);
        return doFilter(predicate, renamed, materializedInternal, true);
    }

    private KTable<K, VR> doMapValues( ValueMapperWithKey<K, V, VR> mapper,
                                            Named named,
                                            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal)
{
         ISerde<K> keySerde;
         ISerde<VR> valueSerde;
         string queryableStoreName;
         StoreBuilder<TimestampedKeyValueStore<K, VR>> storeBuilder;

        if (materializedInternal != null)
{
            // we actually do not need to generate store names at all since if it is not specified, we will not
            // materialize the store; but we still need to burn one index BEFORE generating the processor to keep compatibility.
            if (materializedInternal.storeName() == null)
{
                builder.newStoreName(MAPVALUES_NAME);
            }
            keySerde = materializedInternal.keySerde() != null ? materializedInternal.keySerde() : this.keySerde;
            valueSerde = materializedInternal.valueSerde();
            queryableStoreName = materializedInternal.queryableStoreName();
            // only materialize if materialized is specified and it has queryable name
            storeBuilder = queryableStoreName != null ? (new TimestampedKeyValueStoreMaterializer<>(materializedInternal)).materialize() : null;
        } else {
            keySerde = this.keySerde;
            valueSerde = null;
            queryableStoreName = null;
            storeBuilder = null;
        }

         string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, MAPVALUES_NAME);

         KTableProcessorSupplier<K, V, VR> processorSupplier = new KTableMapValues<>(this, mapper, queryableStoreName);

        // leaving in calls to ITB until building topology with graph

         ProcessorParameters<K, VR> processorParameters = unsafeCastProcessorParametersToCompletelyDifferentType(
            new ProcessorParameters<>(processorSupplier, name)
        );
         StreamsGraphNode tableNode = new TableProcessorNode<>(
            name,
            processorParameters,
            storeBuilder
        );

        builder.AddGraphNode(this.streamsGraphNode, tableNode);

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

    
    public KTable<K, VR> mapValues( ValueMapper<V, VR> mapper)
{
        Objects.requireNonNull(mapper, "mapper can't be null");
        return doMapValues(withKey(mapper), NamedInternal.empty(), null);
    }

    
    public KTable<K, VR> mapValues( ValueMapper<V, VR> mapper,
                                         Named named)
{
        Objects.requireNonNull(mapper, "mapper can't be null");
        return doMapValues(withKey(mapper), named, null);
    }

    
    public KTable<K, VR> mapValues( ValueMapperWithKey<K, V, VR> mapper)
{
        Objects.requireNonNull(mapper, "mapper can't be null");
        return doMapValues(mapper, NamedInternal.empty(), null);
    }

    
    public KTable<K, VR> mapValues( ValueMapperWithKey<K, V, VR> mapper,
                                         Named named)
{
        Objects.requireNonNull(mapper, "mapper can't be null");
        return doMapValues(mapper, named, null);
    }

    
    public KTable<K, VR> mapValues( ValueMapper<V, VR> mapper,
                                         Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
{
        return mapValues(mapper, NamedInternal.empty(), materialized);
    }

    
    public KTable<K, VR> mapValues( ValueMapper<V, VR> mapper,
                                         Named named,
                                         Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
{
        Objects.requireNonNull(mapper, "mapper can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");

         MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);

        return doMapValues(withKey(mapper), named, materializedInternal);
    }

    
    public KTable<K, VR> mapValues( ValueMapperWithKey<K, V, VR> mapper,
                                         Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
{
        return mapValues(mapper, NamedInternal.empty(), materialized);
    }

    
    public KTable<K, VR> mapValues( ValueMapperWithKey<K, V, VR> mapper,
                                         Named named,
                                         Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
{
        Objects.requireNonNull(mapper, "mapper can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");

         MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);

        return doMapValues(mapper, named, materializedInternal);
    }

    
    public KTable<K, VR> transformValues( ValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
                                               string[] stateStoreNames)
{
        return doTransformValues(transformerSupplier, null, NamedInternal.empty(), stateStoreNames);
    }

    
    public KTable<K, VR> transformValues( ValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
                                               Named named,
                                               string[] stateStoreNames)
{
        Objects.requireNonNull(named, "processorName can't be null");
        return doTransformValues(transformerSupplier, null, new NamedInternal(named), stateStoreNames);
    }

    
    public KTable<K, VR> transformValues( ValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
                                               Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized,
                                               string[] stateStoreNames)
{
        return transformValues(transformerSupplier, materialized, NamedInternal.empty(), stateStoreNames);
    }

    
    public KTable<K, VR> transformValues( ValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
                                               Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized,
                                               Named named,
                                               string[] stateStoreNames)
{
        Objects.requireNonNull(materialized, "materialized can't be null");
        Objects.requireNonNull(named, "named can't be null");
         MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);

        return doTransformValues(transformerSupplier, materializedInternal, new NamedInternal(named), stateStoreNames);
    }

    private KTable<K, VR> doTransformValues( ValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
                                                  MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal,
                                                  NamedInternal namedInternal,
                                                  string[] stateStoreNames)
{
        Objects.requireNonNull(stateStoreNames, "stateStoreNames");
         ISerde<K> keySerde;
         ISerde<VR> valueSerde;
         string queryableStoreName;
         StoreBuilder<TimestampedKeyValueStore<K, VR>> storeBuilder;

        if (materializedInternal != null)
{
            // don't inherit parent value serde, since this operation may change the value type, more specifically:
            // we preserve the key following the order of 1) materialized, 2) parent, 3) null
            keySerde = materializedInternal.keySerde() != null ? materializedInternal.keySerde() : this.keySerde;
            // we preserve the value following the order of 1) materialized, 2) null
            valueSerde = materializedInternal.valueSerde();
            queryableStoreName = materializedInternal.queryableStoreName();
            // only materialize if materialized is specified and it has queryable name
            storeBuilder = queryableStoreName != null ? (new TimestampedKeyValueStoreMaterializer<>(materializedInternal)).materialize() : null;
        } else {
            keySerde = this.keySerde;
            valueSerde = null;
            queryableStoreName = null;
            storeBuilder = null;
        }

         string name = namedInternal.orElseGenerateWithPrefix(builder, TRANSFORMVALUES_NAME);

         KTableProcessorSupplier<K, V, VR> processorSupplier = new KTableTransformValues<>(
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

        builder.AddGraphNode(this.streamsGraphNode, tableNode);

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

    
    public KStream<K, V> toStream()
{
        return toStream(NamedInternal.empty());
    }

    
    public KStream<K, V> toStream( Named named)
{
        Objects.requireNonNull(named, "named can't be null");

         string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, TOSTREAM_NAME);
         ProcessorSupplier<K, Change<V>> kStreamMapValues = new KStreamMapValues<>((key, change) -> change.newValue);
         ProcessorParameters<K, V> processorParameters = unsafeCastProcessorParametersToCompletelyDifferentType(
            new ProcessorParameters<>(kStreamMapValues, name)
        );

         ProcessorGraphNode<K, V> toStreamNode = new ProcessorGraphNode<>(
            name,
            processorParameters
        );

        builder.AddGraphNode(this.streamsGraphNode, toStreamNode);

        // we can inherit parent key and value serde
        return new KStreamImpl<>(name, keySerde, valSerde, sourceNodes, false, toStreamNode, builder);
    }

    
    public <K1> KStream<K1, V> toStream( IKeyValueMapper<K, V, K1> mapper)
{
        return toStream().selectKey(mapper);
    }

    
    public <K1> KStream<K1, V> toStream( IKeyValueMapper<K, V, K1> mapper,
                                         Named named)
{
        return toStream(named).selectKey(mapper);
    }

    
    public KTable<K, V> suppress( Suppressed<K> suppressed)
{
         string name;
        if (suppressed is NamedSuppressed)
{
             string givenName = ((NamedSuppressed<?>) suppressed).name();
            name = givenName != null ? givenName : builder.newProcessorName(SUPPRESS_NAME);
        } else {
            throw new ArgumentException("Custom subclasses of Suppressed are not supported.");
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

        builder.AddGraphNode(streamsGraphNode, node);

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

    
    private SuppressedInternal<K> buildSuppress( Suppressed<K> suppress,  string name)
{
        if (suppress is FinalResultsSuppressionBuilder)
{
             long grace = findAndVerifyWindowGrace(streamsGraphNode);
            LOG.info("Using grace period of [{}] as the suppress duration for node [{}].",
                     Duration.ofMillis(grace), name);

             FinalResultsSuppressionBuilder<?> builder = (FinalResultsSuppressionBuilder<?>) suppress;

             SuppressedInternal<?> finalResultsSuppression =
                builder.buildFinalResultsSuppression(Duration.ofMillis(grace));

            return (SuppressedInternal<K>) finalResultsSuppression;
        } else if (suppress is SuppressedInternal)
{
            return (SuppressedInternal<K>) suppress;
        } else {
            throw new ArgumentException("Custom subclasses of Suppressed are not allowed.");
        }
    }

    
    public <V1, R> KTable<K, R> join( KTable<K, V1> other,
                                      ValueJoiner<V, V1, R> joiner)
{
        return doJoin(other, joiner, NamedInternal.empty(), null, false, false);
    }

    
    public <V1, R> KTable<K, R> join( KTable<K, V1> other,
                                      ValueJoiner<V, V1, R> joiner,
                                      Named named)
{
        return doJoin(other, joiner, named, null, false, false);
    }

    
    public KTable<K, VR> join( KTable<K, VO> other,
                                        ValueJoiner<V, VO, VR> joiner,
                                        Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
{
        return join(other, joiner, NamedInternal.empty(), materialized);
    }

    
    public KTable<K, VR> join( KTable<K, VO> other,
                                        ValueJoiner<V, VO, VR> joiner,
                                        Named named,
                                        Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
{
        Objects.requireNonNull(materialized, "materialized can't be null");
         MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, MERGE_NAME);

        return doJoin(other, joiner, named, materializedInternal, false, false);
    }

    
    public <V1, R> KTable<K, R> outerJoin( KTable<K, V1> other,
                                           ValueJoiner<V, V1, R> joiner)
{
        return outerJoin(other, joiner, NamedInternal.empty());
    }

    
    public <V1, R> KTable<K, R> outerJoin( KTable<K, V1> other,
                                           ValueJoiner<V, V1, R> joiner,
                                           Named named)
{
        return doJoin(other, joiner, named, null, true, true);
    }

    
    public KTable<K, VR> outerJoin( KTable<K, VO> other,
                                             ValueJoiner<V, VO, VR> joiner,
                                             Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
{
        return outerJoin(other, joiner, NamedInternal.empty(), materialized);
    }

    
    public KTable<K, VR> outerJoin( KTable<K, VO> other,
                                             ValueJoiner<V, VO, VR> joiner,
                                             Named named,
                                             Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
{
        Objects.requireNonNull(materialized, "materialized can't be null");
         MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, MERGE_NAME);

        return doJoin(other, joiner, named, materializedInternal, true, true);
    }

    
    public <V1, R> KTable<K, R> leftJoin( KTable<K, V1> other,
                                          ValueJoiner<V, V1, R> joiner)
{
        return leftJoin(other, joiner, NamedInternal.empty());
    }

    
    public <V1, R> KTable<K, R> leftJoin( KTable<K, V1> other,
                                          ValueJoiner<V, V1, R> joiner,
                                          Named named)
{
        return doJoin(other, joiner, named, null, true, false);
    }

    
    public KTable<K, VR> leftJoin( KTable<K, VO> other,
                                            ValueJoiner<V, VO, VR> joiner,
                                            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
{
        return leftJoin(other, joiner, NamedInternal.empty(), materialized);
    }

    
    public KTable<K, VR> leftJoin( KTable<K, VO> other,
                                            ValueJoiner<V, VO, VR> joiner,
                                            Named named,
                                            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
{
        Objects.requireNonNull(materialized, "materialized can't be null");
         MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, MERGE_NAME);

        return doJoin(other, joiner, named, materializedInternal, true, false);
    }

    
    private <VO, VR> KTable<K, VR> doJoin( KTable<K, VO> other,
                                           ValueJoiner<V, VO, VR> joiner,
                                           Named joinName,
                                           MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal,
                                           bool leftOuter,
                                           bool rightOuter)
{
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(joinName, "joinName can't be null");

         NamedInternal renamed = new NamedInternal(joinName);
         string joinMergeName = renamed.orElseGenerateWithPrefix(builder, MERGE_NAME);
         HashSet<string> allSourceNodes = ensureJoinableWith((AbstractStream<K, VO>) other);

        if (leftOuter)
{
            enableSendingOldValues();
        }
        if (rightOuter)
{
            ((KTableImpl) other).enableSendingOldValues();
        }

         KTableKTableAbstractJoin<K, VR, V, VO> joinThis;
         KTableKTableAbstractJoin<K, VR, VO, V> joinOther;

        if (!leftOuter) { // inner
            joinThis = new KTableKTableInnerJoin<>(this, (KTableImpl<K, ?, VO>) other, joiner);
            joinOther = new KTableKTableInnerJoin<>((KTableImpl<K, ?, VO>) other, this, reverseJoiner(joiner));
        } else if (!rightOuter) { // left
            joinThis = new KTableKTableLeftJoin<>(this, (KTableImpl<K, ?, VO>) other, joiner);
            joinOther = new KTableKTableRightJoin<>((KTableImpl<K, ?, VO>) other, this, reverseJoiner(joiner));
        } else { // outer
            joinThis = new KTableKTableOuterJoin<>(this, (KTableImpl<K, ?, VO>) other, joiner);
            joinOther = new KTableKTableOuterJoin<>((KTableImpl<K, ?, VO>) other, this, reverseJoiner(joiner));
        }

         string joinThisName = renamed.suffixWithOrElseGet("-join-this", builder, JOINTHIS_NAME);
         string joinOtherName = renamed.suffixWithOrElseGet("-join-other", builder, JOINOTHER_NAME);

         ProcessorParameters<K, Change<V>> joinThisProcessorParameters = new ProcessorParameters<>(joinThis, joinThisName);
         ProcessorParameters<K, Change<VO>> joinOtherProcessorParameters = new ProcessorParameters<>(joinOther, joinOtherName);

         ISerde<K> keySerde;
         ISerde<VR> valueSerde;
         string queryableStoreName;
         StoreBuilder<TimestampedKeyValueStore<K, VR>> storeBuilder;

        if (materializedInternal != null)
{
            keySerde = materializedInternal.keySerde() != null ? materializedInternal.keySerde() : this.keySerde;
            valueSerde = materializedInternal.valueSerde();
            queryableStoreName = materializedInternal.storeName();
            storeBuilder = new TimestampedKeyValueStoreMaterializer<>(materializedInternal).materialize();
        } else {
            keySerde = this.keySerde;
            valueSerde = null;
            queryableStoreName = null;
            storeBuilder = null;
        }

         KTableKTableJoinNode<K, V, VO, VR> kTableKTableJoinNode =
            KTableKTableJoinNode.<K, V, VO, VR>kTableKTableJoinNodeBuilder()
                .withNodeName(joinMergeName)
                .withJoinThisProcessorParameters(joinThisProcessorParameters)
                .withJoinOtherProcessorParameters(joinOtherProcessorParameters)
                .withThisJoinSideNodeName(name)
                .withOtherJoinSideNodeName(((KTableImpl) other).name)
                .withJoinThisStoreNames(valueGetterSupplier().storeNames())
                .withJoinOtherStoreNames(((KTableImpl) other).valueGetterSupplier().storeNames())
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde)
                .withQueryableStoreName(queryableStoreName)
                .withStoreBuilder(storeBuilder)
                .build();
        builder.AddGraphNode(this.streamsGraphNode, kTableKTableJoinNode);

        // we can inherit parent key serde if user do not provide specific overrides
        return new KTableImpl<K, Change<VR>, VR>(
            kTableKTableJoinNode.nodeName(),
            kTableKTableJoinNode.keySerde(),
            kTableKTableJoinNode.valueSerde(),
            allSourceNodes,
            kTableKTableJoinNode.queryableStoreName(),
            kTableKTableJoinNode.joinMerger(),
            kTableKTableJoinNode,
            builder
        );
    }

    
    public KGroupedTable<K1, V1> groupBy( IKeyValueMapper<K, V, KeyValue<K1, V1>> selector)
{
        return groupBy(selector, Grouped.with(null, null));
    }

    
    @Deprecated
    public KGroupedTable<K1, V1> groupBy( IKeyValueMapper<K, V, KeyValue<K1, V1>> selector,
                                                   org.apache.kafka.streams.kstream.Serialized<K1, V1> serialized)
{
        Objects.requireNonNull(selector, "selector can't be null");
        Objects.requireNonNull(serialized, "serialized can't be null");
         SerializedInternal<K1, V1> serializedInternal = new SerializedInternal<>(serialized);
        return groupBy(selector, Grouped.with(serializedInternal.keySerde(), serializedInternal.valueSerde()));
    }

    
    public KGroupedTable<K1, V1> groupBy( IKeyValueMapper<K, V, KeyValue<K1, V1>> selector,
                                                   Grouped<K1, V1> grouped)
{
        Objects.requireNonNull(selector, "selector can't be null");
        Objects.requireNonNull(grouped, "grouped can't be null");
         GroupedInternal<K1, V1> groupedInternal = new GroupedInternal<>(grouped);
         string selectName = new NamedInternal(groupedInternal.name()).orElseGenerateWithPrefix(builder, SELECT_NAME);

         KTableProcessorSupplier<K, V, KeyValue<K1, V1>> selectSupplier = new KTableRepartitionMap<>(this, selector);
         ProcessorParameters<K, Change<V>> processorParameters = new ProcessorParameters<>(selectSupplier, selectName);

        // select the aggregate key and values (old and new), it would require parent to send old values
         ProcessorGraphNode<K, Change<V>> groupByMapNode = new ProcessorGraphNode<>(selectName, processorParameters);

        builder.AddGraphNode(this.streamsGraphNode, groupByMapNode);

        this.enableSendingOldValues();
        return new KGroupedTableImpl<>(
            builder,
            selectName,
            sourceNodes,
            groupedInternal,
            groupByMapNode
        );
    }

    
    public KTableValueGetterSupplier<K, V> valueGetterSupplier()
{
        if (processorSupplier is KTableSource)
{
             KTableSource<K, V> source = (KTableSource<K, V>) processorSupplier;
            // whenever a source ktable is required for getter, it should be materialized
            source.materialize();
            return new KTableSourceValueGetterSupplier<>(source.queryableName());
        } else if (processorSupplier is KStreamAggProcessorSupplier)
{
            return ((KStreamAggProcessorSupplier<?, K, S, V>) processorSupplier).view();
        } else {
            return ((KTableProcessorSupplier<K, S, V>) processorSupplier).view();
        }
    }

    
    public void enableSendingOldValues()
{
        if (!sendOldValues)
{
            if (processorSupplier is KTableSource)
{
                 KTableSource<K, ?> source = (KTableSource<K, V>) processorSupplier;
                source.enableSendingOldValues();
            } else if (processorSupplier is KStreamAggProcessorSupplier)
{
                ((KStreamAggProcessorSupplier<?, K, S, V>) processorSupplier).enableSendingOldValues();
            } else {
                ((KTableProcessorSupplier<K, S, V>) processorSupplier).enableSendingOldValues();
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
    
    private ProcessorParameters<K, VR> unsafeCastProcessorParametersToCompletelyDifferentType( ProcessorParameters<K, Change<V>> kObjectProcessorParameters)
{
        return (ProcessorParameters<K, VR>) kObjectProcessorParameters;
    }

}
