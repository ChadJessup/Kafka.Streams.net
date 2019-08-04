/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.kstream.internals.graph.OptimizableRepartitionNode;
import org.apache.kafka.streams.kstream.internals.graph.OptimizableRepartitionNode.OptimizableRepartitionNodeBuilder;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorGraphNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.kstream.internals.graph.StatefulProcessorNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamSinkNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamStreamJoinNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamTableJoinNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.internals.StaticTopicNameExtractor;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class KStreamImpl<K, V> : AbstractStream<K, V> : KStream<K, V> {

    static  string SOURCE_NAME = "KSTREAM-SOURCE-";

    static  string SINK_NAME = "KSTREAM-SINK-";

    static  string REPARTITION_TOPIC_SUFFIX = "-repartition";

    private static  string BRANCH_NAME = "KSTREAM-BRANCH-";

    private static  string BRANCHCHILD_NAME = "KSTREAM-BRANCHCHILD-";

    private static  string FILTER_NAME = "KSTREAM-FILTER-";

    private static  string PEEK_NAME = "KSTREAM-PEEK-";

    private static  string FLATMAP_NAME = "KSTREAM-FLATMAP-";

    private static  string FLATMAPVALUES_NAME = "KSTREAM-FLATMAPVALUES-";

    private static  string JOINTHIS_NAME = "KSTREAM-JOINTHIS-";

    private static  string JOINOTHER_NAME = "KSTREAM-JOINOTHER-";

    private static  string JOIN_NAME = "KSTREAM-JOIN-";

    private static  string LEFTJOIN_NAME = "KSTREAM-LEFTJOIN-";

    private static  string MAP_NAME = "KSTREAM-MAP-";

    private static  string MAPVALUES_NAME = "KSTREAM-MAPVALUES-";

    private static  string MERGE_NAME = "KSTREAM-MERGE-";

    private static  string OUTERTHIS_NAME = "KSTREAM-OUTERTHIS-";

    private static  string OUTEROTHER_NAME = "KSTREAM-OUTEROTHER-";

    private static  string PROCESSOR_NAME = "KSTREAM-PROCESSOR-";

    private static  string PRINTING_NAME = "KSTREAM-PRINTER-";

    private static  string KEY_SELECT_NAME = "KSTREAM-KEY-SELECT-";

    private static  string TRANSFORM_NAME = "KSTREAM-TRANSFORM-";

    private static  string TRANSFORMVALUES_NAME = "KSTREAM-TRANSFORMVALUES-";

    private static  string WINDOWED_NAME = "KSTREAM-WINDOWED-";

    private static  string FOREACH_NAME = "KSTREAM-FOREACH-";

    private  bool repartitionRequired;

    KStreamImpl( string name,
                 ISerde<K> keySerde,
                 ISerde<V> valueSerde,
                 Set<string> sourceNodes,
                 bool repartitionRequired,
                 StreamsGraphNode streamsGraphNode,
                 InternalStreamsBuilder builder)
{
        super(name, keySerde, valueSerde, sourceNodes, streamsGraphNode, builder);
        this.repartitionRequired = repartitionRequired;
    }

    
    public KStream<K, V> filter( Predicate<? super K, ? super V> predicate)
{
        return filter(predicate, NamedInternal.empty());
    }

    
    public KStream<K, V> filter( Predicate<? super K, ? super V> predicate,  Named named)
{
        Objects.requireNonNull(predicate, "predicate can't be null");
        Objects.requireNonNull(named, "named can't be null");
         string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, FILTER_NAME);
         ProcessorParameters<? super K, ? super V> processorParameters = new ProcessorParameters<>(new KStreamFilter<>(predicate, false), name);
         ProcessorGraphNode<? super K, ? super V> filterProcessorNode = new ProcessorGraphNode<>(name, processorParameters);
        builder.addGraphNode(this.streamsGraphNode, filterProcessorNode);

        return new KStreamImpl<>(
                name,
                keySerde,
                valSerde,
                sourceNodes,
                repartitionRequired,
                filterProcessorNode,
                builder);
    }

    
    public KStream<K, V> filterNot( Predicate<? super K, ? super V> predicate)
{
        return filterNot(predicate, NamedInternal.empty());
    }

    
    public KStream<K, V> filterNot( Predicate<? super K, ? super V> predicate,  Named named)
{
        Objects.requireNonNull(predicate, "predicate can't be null");
        Objects.requireNonNull(named, "named can't be null");
         string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, FILTER_NAME);
         ProcessorParameters<? super K, ? super V> processorParameters = new ProcessorParameters<>(new KStreamFilter<>(predicate, true), name);
         ProcessorGraphNode<? super K, ? super V> filterNotProcessorNode = new ProcessorGraphNode<>(name, processorParameters);

        builder.addGraphNode(this.streamsGraphNode, filterNotProcessorNode);

        return new KStreamImpl<>(
                name,
                keySerde,
                valSerde,
                sourceNodes,
                repartitionRequired,
                filterNotProcessorNode,
                builder);
    }

    
    public <KR> KStream<KR, V> selectKey( IKeyValueMapper<? super K, ? super V, ? : KR> mapper)
{
        return selectKey(mapper, NamedInternal.empty());
    }

    
    public <KR> KStream<KR, V> selectKey( IKeyValueMapper<? super K, ? super V, ? : KR> mapper,  Named named)
{
        Objects.requireNonNull(mapper, "mapper can't be null");
        Objects.requireNonNull(named, "named can't be null");

         ProcessorGraphNode<K, V> selectKeyProcessorNode = internalSelectKey(mapper, new NamedInternal(named));

        selectKeyProcessorNode.keyChangingOperation(true);
        builder.addGraphNode(this.streamsGraphNode, selectKeyProcessorNode);

        // key serde cannot be preserved
        return new KStreamImpl<>(selectKeyProcessorNode.nodeName(), null, valSerde, sourceNodes, true, selectKeyProcessorNode, builder);
    }

    private <KR> ProcessorGraphNode<K, V> internalSelectKey( IKeyValueMapper<? super K, ? super V, ? : KR> mapper,
                                                             NamedInternal named)
{
         string name = named.orElseGenerateWithPrefix(builder, KEY_SELECT_NAME);
         KStreamMap<K, V, KR, V> kStreamMap = new KStreamMap<>((key, value) -> new KeyValue<>(mapper.apply(key, value), value));

         ProcessorParameters<K, V> processorParameters = new ProcessorParameters<>(kStreamMap, name);

        return new ProcessorGraphNode<>(name, processorParameters);
    }

    
    public <KR, VR> KStream<KR, VR> map( IKeyValueMapper<? super K, ? super V, ? : KeyValue<? : KR, ? : VR>> mapper)
{
        return map(mapper, NamedInternal.empty());
    }

    
    public <KR, VR> KStream<KR, VR> map( IKeyValueMapper<? super K, ? super V, ? : KeyValue<? : KR, ? : VR>> mapper,  Named named)
{
        Objects.requireNonNull(mapper, "mapper can't be null");
        Objects.requireNonNull(named, "named can't be null");
         string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, MAP_NAME);
         ProcessorParameters<? super K, ? super V> processorParameters = new ProcessorParameters<>(new KStreamMap<>(mapper), name);

         ProcessorGraphNode<? super K, ? super V> mapProcessorNode = new ProcessorGraphNode<>(name, processorParameters);

        mapProcessorNode.keyChangingOperation(true);
        builder.addGraphNode(this.streamsGraphNode, mapProcessorNode);

        // key and value serde cannot be preserved
        return new KStreamImpl<>(
                name,
                null,
                null,
                sourceNodes,
                true,
                mapProcessorNode,
                builder);
    }

    
    public <VR> KStream<K, VR> mapValues( ValueMapper<? super V, ? : VR> mapper)
{
        return mapValues(withKey(mapper));
    }

    
    public <VR> KStream<K, VR> mapValues( ValueMapper<? super V, ? : VR> mapper,  Named named)
{
        return mapValues(withKey(mapper), named);
    }

    
    public <VR> KStream<K, VR> mapValues( ValueMapperWithKey<? super K, ? super V, ? : VR> mapper)
{
        return mapValues(mapper, NamedInternal.empty());
    }

    
    public <VR> KStream<K, VR> mapValues( ValueMapperWithKey<? super K, ? super V, ? : VR> mapper,  Named named)
{
        Objects.requireNonNull(mapper, "mapper can't be null");
        Objects.requireNonNull(mapper, "named can't be null");
         string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, MAPVALUES_NAME);
         ProcessorParameters<? super K, ? super V> processorParameters = new ProcessorParameters<>(new KStreamMapValues<>(mapper), name);
         ProcessorGraphNode<? super K, ? super V> mapValuesProcessorNode = new ProcessorGraphNode<>(name, processorParameters);

        mapValuesProcessorNode.setValueChangingOperation(true);
        builder.addGraphNode(this.streamsGraphNode, mapValuesProcessorNode);

        // value serde cannot be preserved
        return new KStreamImpl<>(
                name,
                keySerde,
                null,
                sourceNodes,
                repartitionRequired,
                mapValuesProcessorNode,
                builder);
    }

    
    public void print( Printed<K, V> printed)
{
        Objects.requireNonNull(printed, "printed can't be null");
         PrintedInternal<K, V> printedInternal = new PrintedInternal<>(printed);
         string name = new NamedInternal(printedInternal.name()).orElseGenerateWithPrefix(builder, PRINTING_NAME);
         ProcessorParameters<? super K, ? super V> processorParameters = new ProcessorParameters<>(printedInternal.build(this.name), name);
         ProcessorGraphNode<? super K, ? super V> printNode = new ProcessorGraphNode<>(name, processorParameters);

        builder.addGraphNode(this.streamsGraphNode, printNode);
    }

    
    public <KR, VR> KStream<KR, VR> flatMap( IKeyValueMapper<? super K, ? super V, ? : Iterable<? : KeyValue<? : KR, ? : VR>>> mapper)
{
        return flatMap(mapper, NamedInternal.empty());
    }

    
    public <KR, VR> KStream<KR, VR> flatMap( IKeyValueMapper<? super K, ? super V, ? : Iterable<? : KeyValue<? : KR, ? : VR>>> mapper,
                                             Named named)
{
        Objects.requireNonNull(mapper, "mapper can't be null");
        Objects.requireNonNull(named, "named can't be null");
         string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, FLATMAP_NAME);
         ProcessorParameters<? super K, ? super V> processorParameters = new ProcessorParameters<>(new KStreamFlatMap<>(mapper), name);
         ProcessorGraphNode<? super K, ? super V> flatMapNode = new ProcessorGraphNode<>(name, processorParameters);
        flatMapNode.keyChangingOperation(true);

        builder.addGraphNode(this.streamsGraphNode, flatMapNode);

        // key and value serde cannot be preserved
        return new KStreamImpl<>(name, null, null, sourceNodes, true, flatMapNode, builder);
    }

    
    public <VR> KStream<K, VR> flatMapValues( ValueMapper<? super V, ? : Iterable<? : VR>> mapper)
{
        return flatMapValues(withKey(mapper));
    }

    
    public <VR> KStream<K, VR> flatMapValues( ValueMapper<? super V, ? : Iterable<? : VR>> mapper,
                                              Named named)
{
        return flatMapValues(withKey(mapper), named);
    }

    
    public <VR> KStream<K, VR> flatMapValues( ValueMapperWithKey<? super K, ? super V, ? : Iterable<? : VR>> mapper)
{
        return flatMapValues(mapper, NamedInternal.empty());
    }

    
    public <VR> KStream<K, VR> flatMapValues( ValueMapperWithKey<? super K, ? super V, ? : Iterable<? : VR>> mapper,
                                              Named named)
{
        Objects.requireNonNull(mapper, "mapper can't be null");
        Objects.requireNonNull(named, "named can't be null");
         string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, FLATMAPVALUES_NAME);
         ProcessorParameters<? super K, ? super V> processorParameters = new ProcessorParameters<>(new KStreamFlatMapValues<>(mapper), name);
         ProcessorGraphNode<? super K, ? super V> flatMapValuesNode = new ProcessorGraphNode<>(name, processorParameters);

        flatMapValuesNode.setValueChangingOperation(true);
        builder.addGraphNode(this.streamsGraphNode, flatMapValuesNode);

        // value serde cannot be preserved
        return new KStreamImpl<>(name, keySerde, null, sourceNodes, this.repartitionRequired, flatMapValuesNode, builder);
    }

    
    @SuppressWarnings("unchecked")
    public KStream<K, V>[] branch( Predicate<? super K, ? super V>[] predicates)
{
        return doBranch(NamedInternal.empty(), predicates);
    }

    
    @SuppressWarnings("unchecked")
    public KStream<K, V>[] branch( Named name,  Predicate<? super K, ? super V>[] predicates)
{
        Objects.requireNonNull(name, "name can't be null");
        return doBranch(new NamedInternal(name), predicates);
    }

    @SuppressWarnings("unchecked")
    private KStream<K, V>[] doBranch( NamedInternal named,
                                      Predicate<? super K, ? super V>[] predicates)
{
        if (predicates.Length == 0)
{
            throw new ArgumentException("you must provide at least one predicate");
        }
        foreach ( Predicate<? super K, ? super V> predicate in predicates)
{
            Objects.requireNonNull(predicate, "predicates can't have null values");
        }

         string branchName = named.orElseGenerateWithPrefix(builder, BRANCH_NAME);

         string[] childNames = new string[predicates.Length];
        for (int i = 0; i < predicates.Length; i++)
{
            childNames[i] = named.suffixWithOrElseGet("-predicate-" + i, builder, BRANCHCHILD_NAME];
        }

         ProcessorParameters processorParameters = new ProcessorParameters<>(new KStreamBranch(predicates.clone(), childNames), branchName);
         ProcessorGraphNode<K, V> branchNode = new ProcessorGraphNode<>(branchName, processorParameters);
        builder.addGraphNode(this.streamsGraphNode, branchNode);

         KStream<K, V>[] branchChildren = (KStream<K, V>[]) Array.newInstance(KStream.class, predicates.Length];

        for (int i = 0; i < predicates.Length; i++)
{
             ProcessorParameters innerProcessorParameters = new ProcessorParameters<>(new KStreamPassThrough<K, V>(), childNames[i]];
             ProcessorGraphNode<K, V> branchChildNode = new ProcessorGraphNode<>(childNames[i], innerProcessorParameters];

            builder.addGraphNode(branchNode, branchChildNode);
            branchChildren[i] = new KStreamImpl<>(childNames[i], keySerde, valSerde, sourceNodes, repartitionRequired, branchChildNode, builder];
        }

        return branchChildren;
    }

    
    public KStream<K, V> merge( KStream<K, V> stream)
{
        Objects.requireNonNull(stream);
        return merge(builder, stream, NamedInternal.empty());
    }

    
    public KStream<K, V> merge( KStream<K, V> stream,  Named processorName)
{
        Objects.requireNonNull(stream);
        return merge(builder, stream, new NamedInternal(processorName));
    }

    private KStream<K, V> merge( InternalStreamsBuilder builder,
                                 KStream<K, V> stream,
                                 NamedInternal processorName)
{
         KStreamImpl<K, V> streamImpl = (KStreamImpl<K, V>) stream;
         string name = processorName.orElseGenerateWithPrefix(builder, MERGE_NAME);
         Set<string> allSourceNodes = new HashSet<>();

         bool requireRepartitioning = streamImpl.repartitionRequired || repartitionRequired;
        allSourceNodes.addAll(sourceNodes);
        allSourceNodes.addAll(streamImpl.sourceNodes);

         ProcessorParameters<? super K, ? super V> processorParameters = new ProcessorParameters<>(new KStreamPassThrough<>(), name);

         ProcessorGraphNode<? super K, ? super V> mergeNode = new ProcessorGraphNode<>(name, processorParameters);
        mergeNode.setMergeNode(true);
        builder.addGraphNode(Arrays.asList(this.streamsGraphNode, streamImpl.streamsGraphNode), mergeNode);

        // drop the serde as we cannot safely use either one to represent both streams
        return new KStreamImpl<>(name, null, null, allSourceNodes, requireRepartitioning, mergeNode, builder);
    }

    
    public void foreach( ForeachAction<? super K, ? super V> action)
{
        foreach(action, NamedInternal.empty());
    }

    
    public void foreach( ForeachAction<? super K, ? super V> action,  Named named)
{
        Objects.requireNonNull(action, "action can't be null");
         string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, FOREACH_NAME);
         ProcessorParameters<? super K, ? super V> processorParameters = new ProcessorParameters<>(
                new KStreamPeek<>(action, false),
                name
        );

         ProcessorGraphNode<? super K, ? super V> foreachNode = new ProcessorGraphNode<>(name, processorParameters);
        builder.addGraphNode(this.streamsGraphNode, foreachNode);
    }

    
    public KStream<K, V> peek( ForeachAction<? super K, ? super V> action)
{
        return peek(action, NamedInternal.empty());
    }

    
    public KStream<K, V> peek( ForeachAction<? super K, ? super V> action,  Named named)
{
        Objects.requireNonNull(action, "action can't be null");
        Objects.requireNonNull(named, "named can't be null");
         string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, PEEK_NAME);

         ProcessorParameters<? super K, ? super V> processorParameters = new ProcessorParameters<>(
                new KStreamPeek<>(action, true),
                name
        );

         ProcessorGraphNode<? super K, ? super V> peekNode = new ProcessorGraphNode<>(name, processorParameters);

        builder.addGraphNode(this.streamsGraphNode, peekNode);

        return new KStreamImpl<>(name, keySerde, valSerde, sourceNodes, repartitionRequired, peekNode, builder);
    }

    
    public KStream<K, V> through( string topic)
{
        return through(topic, Produced.with(keySerde, valSerde, null));
    }

    
    public KStream<K, V> through( string topic,  Produced<K, V> produced)
{
        Objects.requireNonNull(topic, "topic can't be null");
        Objects.requireNonNull(produced, "Produced can't be null");
         ProducedInternal<K, V> producedInternal = new ProducedInternal<>(produced);
        if (producedInternal.keySerde() == null)
{
            producedInternal.withKeySerde(keySerde);
        }
        if (producedInternal.valueSerde() == null)
{
            producedInternal.withValueSerde(valSerde);
        }
        to(topic, producedInternal);
        return builder.stream(
            Collections.singleton(topic),
            new ConsumedInternal<>(
                producedInternal.keySerde(),
                producedInternal.valueSerde(),
                new FailOnInvalidTimestamp(),
                null
            )
        );
    }

    
    public void to( string topic)
{
        to(topic, Produced.with(keySerde, valSerde, null));
    }

    
    public void to( string topic,  Produced<K, V> produced)
{
        Objects.requireNonNull(topic, "topic can't be null");
        Objects.requireNonNull(produced, "Produced can't be null");
         ProducedInternal<K, V> producedInternal = new ProducedInternal<>(produced);
        if (producedInternal.keySerde() == null)
{
            producedInternal.withKeySerde(keySerde);
        }
        if (producedInternal.valueSerde() == null)
{
            producedInternal.withValueSerde(valSerde);
        }
        to(new StaticTopicNameExtractor<>(topic), producedInternal);
    }

    
    public void to( TopicNameExtractor<K, V> topicExtractor)
{
        to(topicExtractor, Produced.with(keySerde, valSerde, null));
    }

    
    public void to( TopicNameExtractor<K, V> topicExtractor,  Produced<K, V> produced)
{
        Objects.requireNonNull(topicExtractor, "topic extractor can't be null");
        Objects.requireNonNull(produced, "Produced can't be null");
         ProducedInternal<K, V> producedInternal = new ProducedInternal<>(produced);
        if (producedInternal.keySerde() == null)
{
            producedInternal.withKeySerde(keySerde);
        }
        if (producedInternal.valueSerde() == null)
{
            producedInternal.withValueSerde(valSerde);
        }
        to(topicExtractor, producedInternal);
    }

    private void to( TopicNameExtractor<K, V> topicExtractor,  ProducedInternal<K, V> produced)
{
         string name = new NamedInternal(produced.name()).orElseGenerateWithPrefix(builder, SINK_NAME);
         StreamSinkNode<K, V> sinkNode = new StreamSinkNode<>(
            name,
            topicExtractor,
            produced
        );

        builder.addGraphNode(this.streamsGraphNode, sinkNode);
    }

    
    public <KR, VR> KStream<KR, VR> transform( TransformerSupplier<? super K, ? super V, KeyValue<KR, VR>> transformerSupplier,
                                               string[] stateStoreNames)
{
        Objects.requireNonNull(transformerSupplier, "transformerSupplier can't be null");
         string name = builder.newProcessorName(TRANSFORM_NAME);
        return flatTransform(new TransformerSupplierAdapter<>(transformerSupplier), Named.as(name), stateStoreNames);
    }

    
    public <KR, VR> KStream<KR, VR> transform( TransformerSupplier<? super K, ? super V, KeyValue<KR, VR>> transformerSupplier,
                                               Named named,
                                               string[] stateStoreNames)
{
        Objects.requireNonNull(transformerSupplier, "transformerSupplier can't be null");
        return flatTransform(new TransformerSupplierAdapter<>(transformerSupplier), named, stateStoreNames);
    }

    
    public <K1, V1> KStream<K1, V1> flatTransform( TransformerSupplier<? super K, ? super V, Iterable<KeyValue<K1, V1>>> transformerSupplier,
                                                   string[] stateStoreNames)
{
        Objects.requireNonNull(transformerSupplier, "transformerSupplier can't be null");
         string name = builder.newProcessorName(TRANSFORM_NAME);
        return flatTransform(transformerSupplier, Named.as(name), stateStoreNames);
    }

    
    public <K1, V1> KStream<K1, V1> flatTransform( TransformerSupplier<? super K, ? super V, Iterable<KeyValue<K1, V1>>> transformerSupplier,
                                                   Named named,
                                                   string[] stateStoreNames)
{
        Objects.requireNonNull(transformerSupplier, "transformerSupplier can't be null");
        Objects.requireNonNull(named, "named can't be null");

         string name = new NamedInternal(named).name();
         StatefulProcessorNode<? super K, ? super V> transformNode = new StatefulProcessorNode<>(
                name,
                new ProcessorParameters<>(new KStreamFlatTransform<>(transformerSupplier), name),
                stateStoreNames
        );

        transformNode.keyChangingOperation(true);
        builder.addGraphNode(streamsGraphNode, transformNode);

        // cannot inherit key and value serde
        return new KStreamImpl<>(name, null, null, sourceNodes, true, transformNode, builder);
    }

    
    public <VR> KStream<K, VR> transformValues( ValueTransformerSupplier<? super V, ? : VR> valueTransformerSupplier,
                                                string[] stateStoreNames)
{
        Objects.requireNonNull(valueTransformerSupplier, "valueTransformerSupplier can't be null");
        return doTransformValues(toValueTransformerWithKeySupplier(valueTransformerSupplier), NamedInternal.empty(), stateStoreNames);
    }

    
    public <VR> KStream<K, VR> transformValues( ValueTransformerSupplier<? super V, ? : VR> valueTransformerSupplier,
                                                Named named,
                                                string[] stateStoreNames)
{
        Objects.requireNonNull(valueTransformerSupplier, "valueTransformSupplier can't be null");
        Objects.requireNonNull(named, "named can't be null");
        return doTransformValues(toValueTransformerWithKeySupplier(valueTransformerSupplier),
                new NamedInternal(named), stateStoreNames);
    }

    
    public <VR> KStream<K, VR> transformValues( ValueTransformerWithKeySupplier<? super K, ? super V, ? : VR> valueTransformerSupplier,
                                                string[] stateStoreNames)
{
        Objects.requireNonNull(valueTransformerSupplier, "valueTransformerSupplier can't be null");
        return doTransformValues(valueTransformerSupplier, NamedInternal.empty(), stateStoreNames);
    }

    
    public <VR> KStream<K, VR> transformValues( ValueTransformerWithKeySupplier<? super K, ? super V, ? : VR> valueTransformerSupplier,
                                                Named named,
                                                string[] stateStoreNames)
{
        Objects.requireNonNull(valueTransformerSupplier, "valueTransformSupplier can't be null");
        Objects.requireNonNull(named, "named can't be null");
        return doTransformValues(valueTransformerSupplier, new NamedInternal(named), stateStoreNames);
    }

    private <VR> KStream<K, VR> doTransformValues( ValueTransformerWithKeySupplier<? super K, ? super V, ? : VR> valueTransformerWithKeySupplier,
                                                   NamedInternal named,
                                                   string[] stateStoreNames)
{

         string name = named.orElseGenerateWithPrefix(builder, TRANSFORMVALUES_NAME);
         StatefulProcessorNode<? super K, ? super V> transformNode = new StatefulProcessorNode<>(
            name,
            new ProcessorParameters<>(new KStreamTransformValues<>(valueTransformerWithKeySupplier), name),
            stateStoreNames
        );

        transformNode.setValueChangingOperation(true);
        builder.addGraphNode(this.streamsGraphNode, transformNode);

        // cannot inherit value serde
        return new KStreamImpl<>(name, keySerde, null, sourceNodes, repartitionRequired, transformNode, builder);
    }

    
    public <VR> KStream<K, VR> flatTransformValues( ValueTransformerSupplier<? super V, Iterable<VR>> valueTransformerSupplier,
                                                    string[] stateStoreNames)
{
        Objects.requireNonNull(valueTransformerSupplier, "valueTransformerSupplier can't be null");

        return doFlatTransformValues(toValueTransformerWithKeySupplier(valueTransformerSupplier), NamedInternal.empty(), stateStoreNames);
    }

    
    public <VR> KStream<K, VR> flatTransformValues( ValueTransformerSupplier<? super V, Iterable<VR>> valueTransformerSupplier,
                                                    Named named,
                                                    string[] stateStoreNames)
{
        Objects.requireNonNull(valueTransformerSupplier, "valueTransformerSupplier can't be null");

        return doFlatTransformValues(toValueTransformerWithKeySupplier(valueTransformerSupplier), named, stateStoreNames);
    }

    
    public <VR> KStream<K, VR> flatTransformValues( ValueTransformerWithKeySupplier<? super K, ? super V, Iterable<VR>> valueTransformerSupplier,
                                                    string[] stateStoreNames)
{
        Objects.requireNonNull(valueTransformerSupplier, "valueTransformerSupplier can't be null");

        return doFlatTransformValues(valueTransformerSupplier, NamedInternal.empty(), stateStoreNames);
    }

    
    public <VR> KStream<K, VR> flatTransformValues( ValueTransformerWithKeySupplier<? super K, ? super V, Iterable<VR>> valueTransformerSupplier,
                                                    Named named,
                                                    string[] stateStoreNames)
{
        Objects.requireNonNull(valueTransformerSupplier, "valueTransformerSupplier can't be null");

        return doFlatTransformValues(valueTransformerSupplier, named, stateStoreNames);
    }

    private <VR> KStream<K, VR> doFlatTransformValues( ValueTransformerWithKeySupplier<? super K, ? super V, Iterable<VR>> valueTransformerWithKeySupplier,
                                                       Named named,
                                                       string[] stateStoreNames)
{
         string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, TRANSFORMVALUES_NAME);

         StatefulProcessorNode<? super K, ? super V> transformNode = new StatefulProcessorNode<>(
            name,
            new ProcessorParameters<>(new KStreamFlatTransformValues<>(valueTransformerWithKeySupplier), name),
            stateStoreNames
        );

        transformNode.setValueChangingOperation(true);
        builder.addGraphNode(this.streamsGraphNode, transformNode);

        // cannot inherit value serde
        return new KStreamImpl<>(name, keySerde, null, sourceNodes, repartitionRequired, transformNode, builder);
    }

    
    public void process( ProcessorSupplier<? super K, ? super V> processorSupplier,
                         string[] stateStoreNames)
{

        Objects.requireNonNull(processorSupplier, "ProcessSupplier cant' be null");
         string name = builder.newProcessorName(PROCESSOR_NAME);
        process(processorSupplier, Named.as(name), stateStoreNames);
    }

    
    public void process( ProcessorSupplier<? super K, ? super V> processorSupplier,
                         Named named,
                         string[] stateStoreNames)
{
        Objects.requireNonNull(processorSupplier, "ProcessSupplier cant' be null");
        Objects.requireNonNull(named, "named cant' be null");

         string name = new NamedInternal(named).name();
         StatefulProcessorNode<? super K, ? super V> processNode = new StatefulProcessorNode<>(
                name,
                new ProcessorParameters<>(processorSupplier, name),
                stateStoreNames
        );

        builder.addGraphNode(this.streamsGraphNode, processNode);
    }

    
    public <VO, VR> KStream<K, VR> join( KStream<K, VO> other,
                                         ValueJoiner<? super V, ? super VO, ? : VR> joiner,
                                         JoinWindows windows)
{
        return join(other, joiner, windows, Joined.with(null, null, null));
    }

    
    public <VO, VR> KStream<K, VR> join( KStream<K, VO> otherStream,
                                         ValueJoiner<? super V, ? super VO, ? : VR> joiner,
                                         JoinWindows windows,
                                         Joined<K, V, VO> joined)
{

        return doJoin(otherStream,
                      joiner,
                      windows,
                      joined,
                      new KStreamImplJoin(false, false));

    }

    
    public <VO, VR> KStream<K, VR> outerJoin( KStream<K, VO> other,
                                              ValueJoiner<? super V, ? super VO, ? : VR> joiner,
                                              JoinWindows windows)
{
        return outerJoin(other, joiner, windows, Joined.with(null, null, null));
    }

    
    public <VO, VR> KStream<K, VR> outerJoin( KStream<K, VO> other,
                                              ValueJoiner<? super V, ? super VO, ? : VR> joiner,
                                              JoinWindows windows,
                                              Joined<K, V, VO> joined)
{
        return doJoin(other, joiner, windows, joined, new KStreamImplJoin(true, true));
    }

    private <VO, VR> KStream<K, VR> doJoin( KStream<K, VO> other,
                                            ValueJoiner<? super V, ? super VO, ? : VR> joiner,
                                            JoinWindows windows,
                                            Joined<K, V, VO> joined,
                                            KStreamImplJoin join)
{
        Objects.requireNonNull(other, "other KStream can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(windows, "windows can't be null");
        Objects.requireNonNull(joined, "joined can't be null");

        KStreamImpl<K, V> joinThis = this;
        KStreamImpl<K, VO> joinOther = (KStreamImpl<K, VO>) other;

         JoinedInternal<K, V, VO> joinedInternal = new JoinedInternal<>(joined);
         NamedInternal name = new NamedInternal(joinedInternal.name());
        if (joinThis.repartitionRequired)
{
             string joinThisName = joinThis.name;
             string leftJoinRepartitionTopicName = name.suffixWithOrElseGet("-left", joinThisName);
            joinThis = joinThis.repartitionForJoin(leftJoinRepartitionTopicName, joined.keySerde(), joined.valueSerde());
        }

        if (joinOther.repartitionRequired)
{
             string joinOtherName = joinOther.name;
             string rightJoinRepartitionTopicName = name.suffixWithOrElseGet("-right", joinOtherName);
            joinOther = joinOther.repartitionForJoin(rightJoinRepartitionTopicName, joined.keySerde(), joined.otherValueSerde());
        }

        joinThis.ensureJoinableWith(joinOther);

        return join.join(
            joinThis,
            joinOther,
            joiner,
            windows,
            joined
        );
    }

    /**
     * Repartition a stream. This is required on join operations occurring after
     * an operation that changes the key, i.e, selectKey, map(..), flatMap(..).
     */
    private KStreamImpl<K, V> repartitionForJoin( string repartitionName,
                                                  ISerde<K> keySerdeOverride,
                                                  ISerde<V> valueSerdeOverride)
{
         ISerde<K> repartitionKeySerde = keySerdeOverride != null ? keySerdeOverride : keySerde;
         ISerde<V> repartitionValueSerde = valueSerdeOverride != null ? valueSerdeOverride : valSerde;
         OptimizableRepartitionNodeBuilder<K, V> optimizableRepartitionNodeBuilder =
            OptimizableRepartitionNode.optimizableRepartitionNodeBuilder();
         string repartitionedSourceName = createRepartitionedSource(builder,
                                                                         repartitionKeySerde,
                                                                         repartitionValueSerde,
                                                                         repartitionName,
                                                                         optimizableRepartitionNodeBuilder);

         OptimizableRepartitionNode<K, V> optimizableRepartitionNode = optimizableRepartitionNodeBuilder.build();
        builder.addGraphNode(this.streamsGraphNode, optimizableRepartitionNode);

        return new KStreamImpl<>(repartitionedSourceName, repartitionKeySerde, repartitionValueSerde, Collections.singleton(repartitionedSourceName), false, optimizableRepartitionNode, builder);
    }

    static <K1, V1> string createRepartitionedSource( InternalStreamsBuilder builder,
                                                      ISerde<K1> keySerde,
                                                      ISerde<V1> valSerde,
                                                      string repartitionTopicNamePrefix,
                                                      OptimizableRepartitionNodeBuilder<K1, V1> optimizableRepartitionNodeBuilder)
{


         string repartitionTopic = repartitionTopicNamePrefix + REPARTITION_TOPIC_SUFFIX;
         string sinkName = builder.newProcessorName(SINK_NAME);
         string nullKeyFilterProcessorName = builder.newProcessorName(FILTER_NAME);
         string sourceName = builder.newProcessorName(SOURCE_NAME);

         Predicate<K1, V1> notNullKeyPredicate = (k, v) -> k != null;

         ProcessorParameters processorParameters = new ProcessorParameters<>(
            new KStreamFilter<>(notNullKeyPredicate, false),
            nullKeyFilterProcessorName
        );

        optimizableRepartitionNodeBuilder.withKeySerde(keySerde)
                                         .withValueSerde(valSerde)
                                         .withSourceName(sourceName)
                                         .withRepartitionTopic(repartitionTopic)
                                         .withSinkName(sinkName)
                                         .withProcessorParameters(processorParameters)
                                         // reusing the source name for the graph node name
                                         // adding explicit variable as it simplifies logic
                                         .withNodeName(sourceName);

        return sourceName;
    }

    
    public <VO, VR> KStream<K, VR> leftJoin( KStream<K, VO> other,
                                             ValueJoiner<? super V, ? super VO, ? : VR> joiner,
                                             JoinWindows windows)
{
        return leftJoin(other, joiner, windows, Joined.with(null, null, null));
    }

    
    public <VO, VR> KStream<K, VR> leftJoin( KStream<K, VO> other,
                                             ValueJoiner<? super V, ? super VO, ? : VR> joiner,
                                             JoinWindows windows,
                                             Joined<K, V, VO> joined)
{
        Objects.requireNonNull(joined, "joined can't be null");
        return doJoin(
            other,
            joiner,
            windows,
            joined,
            new KStreamImplJoin(true, false)
        );

    }

    
    public <VO, VR> KStream<K, VR> join( KTable<K, VO> other,
                                         ValueJoiner<? super V, ? super VO, ? : VR> joiner)
{
        return join(other, joiner, Joined.with(null, null, null));
    }

    
    public <VO, VR> KStream<K, VR> join( KTable<K, VO> other,
                                         ValueJoiner<? super V, ? super VO, ? : VR> joiner,
                                         Joined<K, V, VO> joined)
{
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(joined, "joined can't be null");

         JoinedInternal<K, V, VO> joinedInternal = new JoinedInternal<>(joined);
         string name = joinedInternal.name();
        if (repartitionRequired)
{
             KStreamImpl<K, V> thisStreamRepartitioned = repartitionForJoin(
                name != null ? name : this.name,
                joined.keySerde(),
                joined.valueSerde()
            );
            return thisStreamRepartitioned.doStreamTableJoin(other, joiner, joined, false);
        } else {
            return doStreamTableJoin(other, joiner, joined, false);
        }
    }

    
    public <VO, VR> KStream<K, VR> leftJoin( KTable<K, VO> other,  ValueJoiner<? super V, ? super VO, ? : VR> joiner)
{
        return leftJoin(other, joiner, Joined.with(null, null, null));
    }

    
    public <VO, VR> KStream<K, VR> leftJoin( KTable<K, VO> other,
                                             ValueJoiner<? super V, ? super VO, ? : VR> joiner,
                                             Joined<K, V, VO> joined)
{
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(joined, "joined can't be null");
         JoinedInternal<K, V, VO> joinedInternal = new JoinedInternal<>(joined);
         string internalName = joinedInternal.name();
        if (repartitionRequired)
{
             KStreamImpl<K, V> thisStreamRepartitioned = repartitionForJoin(
                internalName != null ? internalName : name,
                joined.keySerde(),
                joined.valueSerde()
            );
            return thisStreamRepartitioned.doStreamTableJoin(other, joiner, joined, true);
        } else {
            return doStreamTableJoin(other, joiner, joined, true);
        }
    }

    
    public <KG, VG, VR> KStream<K, VR> join( GlobalKTable<KG, VG> globalTable,
                                             IKeyValueMapper<? super K, ? super V, ? : KG> keyMapper,
                                             ValueJoiner<? super V, ? super VG, ? : VR> joiner)
{
        return globalTableJoin(globalTable, keyMapper, joiner, false, NamedInternal.empty());
    }

    
    public <KG, VG, VR> KStream<K, VR> join( GlobalKTable<KG, VG> globalTable,
                                             IKeyValueMapper<? super K, ? super V, ? : KG> keyMapper,
                                             ValueJoiner<? super V, ? super VG, ? : VR> joiner,
                                             Named named)
{
        return globalTableJoin(globalTable, keyMapper, joiner, false, named);
    }

    
    public <KG, VG, VR> KStream<K, VR> leftJoin( GlobalKTable<KG, VG> globalTable,
                                                 IKeyValueMapper<? super K, ? super V, ? : KG> keyMapper,
                                                 ValueJoiner<? super V, ? super VG, ? : VR> joiner)
{
        return globalTableJoin(globalTable, keyMapper, joiner, true, NamedInternal.empty());
    }

    
    public <KG, VG, VR> KStream<K, VR> leftJoin( GlobalKTable<KG, VG> globalTable,
                                                 IKeyValueMapper<? super K, ? super V, ? : KG> keyMapper,
                                                 ValueJoiner<? super V, ? super VG, ? : VR> joiner,
                                                 Named named)
{
        return globalTableJoin(globalTable, keyMapper, joiner, true, named);
    }


    private <KG, VG, VR> KStream<K, VR> globalTableJoin( GlobalKTable<KG, VG> globalTable,
                                                         IKeyValueMapper<? super K, ? super V, ? : KG> keyMapper,
                                                         ValueJoiner<? super V, ? super VG, ? : VR> joiner,
                                                         bool leftJoin,
                                                         Named named)
{
        Objects.requireNonNull(globalTable, "globalTable can't be null");
        Objects.requireNonNull(keyMapper, "keyMapper can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(named, "named can't be null");

         KTableValueGetterSupplier<KG, VG> valueGetterSupplier = ((GlobalKTableImpl<KG, VG>) globalTable).valueGetterSupplier();
         string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, LEFTJOIN_NAME);

         ProcessorSupplier<K, V> processorSupplier = new KStreamGlobalKTableJoin<>(
            valueGetterSupplier,
            joiner,
            keyMapper,
            leftJoin
        );
         ProcessorParameters<K, V> processorParameters = new ProcessorParameters<>(processorSupplier, name);

         StreamTableJoinNode<K, V> streamTableJoinNode = new StreamTableJoinNode<>(name,
                                                                                        processorParameters,
                                                                                        new string[] {},
                                                                                        null);
        builder.addGraphNode(this.streamsGraphNode, streamTableJoinNode);

        // do not have serde for joined result
        return new KStreamImpl<>(name, keySerde, null, sourceNodes, repartitionRequired, streamTableJoinNode, builder);
    }

    @SuppressWarnings("unchecked")
    private <VO, VR> KStream<K, VR> doStreamTableJoin( KTable<K, VO> other,
                                                       ValueJoiner<? super V, ? super VO, ? : VR> joiner,
                                                       Joined<K, V, VO> joined,
                                                       bool leftJoin)
{
        Objects.requireNonNull(other, "other KTable can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");

         Set<string> allSourceNodes = ensureJoinableWith((AbstractStream<K, VO>) other);

         JoinedInternal<K, V, VO> joinedInternal = new JoinedInternal<>(joined);
         NamedInternal renamed = new NamedInternal(joinedInternal.name());

         string name = renamed.orElseGenerateWithPrefix(builder, leftJoin ? LEFTJOIN_NAME : JOIN_NAME);
         ProcessorSupplier<K, V> processorSupplier = new KStreamKTableJoin<>(
            ((KTableImpl<K, ?, VO>) other).valueGetterSupplier(),
            joiner,
            leftJoin
        );

         ProcessorParameters<K, V> processorParameters = new ProcessorParameters<>(processorSupplier, name);
         StreamTableJoinNode<K, V> streamTableJoinNode = new StreamTableJoinNode<>(
            name,
            processorParameters,
            ((KTableImpl) other).valueGetterSupplier().storeNames(),
            this.name
        );

        builder.addGraphNode(this.streamsGraphNode, streamTableJoinNode);

        // do not have serde for joined result
        return new KStreamImpl<>(name, joined.keySerde() != null ? joined.keySerde() : keySerde, null, allSourceNodes, false, streamTableJoinNode, builder);

    }

    
    public <KR> KGroupedStream<KR, V> groupBy( IKeyValueMapper<? super K, ? super V, KR> selector)
{
        return groupBy(selector, Grouped.with(null, valSerde));
    }

    
    @Deprecated
    public <KR> KGroupedStream<KR, V> groupBy( IKeyValueMapper<? super K, ? super V, KR> selector,
                                               org.apache.kafka.streams.kstream.Serialized<KR, V> serialized)
{
        Objects.requireNonNull(selector, "selector can't be null");
        Objects.requireNonNull(serialized, "serialized can't be null");
         SerializedInternal<KR, V> serializedInternal = new SerializedInternal<>(serialized);

        return groupBy(selector, Grouped.with(serializedInternal.keySerde(), serializedInternal.valueSerde()));
    }

    
    public <KR> KGroupedStream<KR, V> groupBy( IKeyValueMapper<? super K, ? super V, KR> selector,
                                               Grouped<KR, V> grouped)
{
        Objects.requireNonNull(selector, "selector can't be null");
        Objects.requireNonNull(grouped, "grouped can't be null");
         GroupedInternal<KR, V> groupedInternal = new GroupedInternal<>(grouped);
         ProcessorGraphNode<K, V> selectKeyMapNode = internalSelectKey(selector, new NamedInternal(groupedInternal.name()));
        selectKeyMapNode.keyChangingOperation(true);

        builder.addGraphNode(this.streamsGraphNode, selectKeyMapNode);

        return new KGroupedStreamImpl<>(
            selectKeyMapNode.nodeName(),
            sourceNodes,
            groupedInternal,
            true,
            selectKeyMapNode,
            builder);
    }

    
    public KGroupedStream<K, V> groupByKey()
{
        return groupByKey(Grouped.with(keySerde, valSerde));
    }

    
    @Deprecated
    public KGroupedStream<K, V> groupByKey( org.apache.kafka.streams.kstream.Serialized<K, V> serialized)
{
         SerializedInternal<K, V> serializedInternal = new SerializedInternal<>(serialized);
        return groupByKey(Grouped.with(serializedInternal.keySerde(), serializedInternal.valueSerde()));
    }

    
    public KGroupedStream<K, V> groupByKey( Grouped<K, V> grouped)
{
         GroupedInternal<K, V> groupedInternal = new GroupedInternal<>(grouped);

        return new KGroupedStreamImpl<>(
            name,
            sourceNodes,
            groupedInternal,
            repartitionRequired,
            streamsGraphNode,
            builder);
    }

    @SuppressWarnings("deprecation") // continuing to support Windows#maintainMs/segmentInterval in fallback mode
    private static <K, V> StoreBuilder<WindowStore<K, V>> joinWindowStoreBuilder( string joinName,
                                                                                  JoinWindows windows,
                                                                                  ISerde<K> keySerde,
                                                                                  ISerde<V> valueSerde)
{
        return Stores.windowStoreBuilder(
            Stores.persistentWindowStore(
                joinName + "-store",
                Duration.ofMillis(windows.size() + windows.gracePeriodMs()),
                Duration.ofMillis(windows.size()),
                true
            ),
            keySerde,
            valueSerde
        );
    }

    private class KStreamImplJoin {

        private  bool leftOuter;
        private  bool rightOuter;


        KStreamImplJoin( bool leftOuter,
                         bool rightOuter)
{
            this.leftOuter = leftOuter;
            this.rightOuter = rightOuter;
        }

        public <K1, R, V1, V2> KStream<K1, R> join( KStream<K1, V1> lhs,
                                                    KStream<K1, V2> other,
                                                    ValueJoiner<? super V1, ? super V2, ? : R> joiner,
                                                    JoinWindows windows,
                                                    Joined<K1, V1, V2> joined)
{

             JoinedInternal<K1, V1, V2>  joinedInternal = new JoinedInternal<>(joined);
             NamedInternal renamed = new NamedInternal(joinedInternal.name());

             string thisWindowStreamName =  renamed.suffixWithOrElseGet(
                    "-this-windowed", builder, WINDOWED_NAME);
             string otherWindowStreamName = renamed.suffixWithOrElseGet(
                    "-other-windowed", builder, WINDOWED_NAME);

             string joinThisName = rightOuter ?
                    renamed.suffixWithOrElseGet("-outer-this-join", builder, OUTERTHIS_NAME)
                    : renamed.suffixWithOrElseGet("-this-join", builder, JOINTHIS_NAME);
             string joinOtherName = leftOuter ?
                    renamed.suffixWithOrElseGet("-outer-other-join", builder, OUTEROTHER_NAME)
                    : renamed.suffixWithOrElseGet("-other-join", builder, JOINOTHER_NAME);
             string joinMergeName = renamed.suffixWithOrElseGet(
                    "-merge", builder, MERGE_NAME);
             StreamsGraphNode thisStreamsGraphNode = ((AbstractStream) lhs).streamsGraphNode;
             StreamsGraphNode otherStreamsGraphNode = ((AbstractStream) other).streamsGraphNode;


             StoreBuilder<WindowStore<K1, V1>> thisWindowStore =
                joinWindowStoreBuilder(joinThisName, windows, joined.keySerde(), joined.valueSerde());
             StoreBuilder<WindowStore<K1, V2>> otherWindowStore =
                joinWindowStoreBuilder(joinOtherName, windows, joined.keySerde(), joined.otherValueSerde());

             KStreamJoinWindow<K1, V1> thisWindowedStream = new KStreamJoinWindow<>(thisWindowStore.name());

             ProcessorParameters<K1, V1> thisWindowStreamProcessorParams = new ProcessorParameters<>(thisWindowedStream, thisWindowStreamName);
             ProcessorGraphNode<K1, V1> thisWindowedStreamsNode = new ProcessorGraphNode<>(thisWindowStreamName, thisWindowStreamProcessorParams);
            builder.addGraphNode(thisStreamsGraphNode, thisWindowedStreamsNode);

             KStreamJoinWindow<K1, V2> otherWindowedStream = new KStreamJoinWindow<>(otherWindowStore.name());

             ProcessorParameters<K1, V2> otherWindowStreamProcessorParams = new ProcessorParameters<>(otherWindowedStream, otherWindowStreamName);
             ProcessorGraphNode<K1, V2> otherWindowedStreamsNode = new ProcessorGraphNode<>(otherWindowStreamName, otherWindowStreamProcessorParams);
            builder.addGraphNode(otherStreamsGraphNode, otherWindowedStreamsNode);

             KStreamKStreamJoin<K1, R, V1, V2> joinThis = new KStreamKStreamJoin<>(
                otherWindowStore.name(),
                windows.beforeMs,
                windows.afterMs,
                joiner,
                leftOuter
            );

             KStreamKStreamJoin<K1, R, V2, V1> joinOther = new KStreamKStreamJoin<>(
                thisWindowStore.name(),
                windows.afterMs,
                windows.beforeMs,
                reverseJoiner(joiner),
                rightOuter
            );

             KStreamPassThrough<K1, R> joinMerge = new KStreamPassThrough<>();

             StreamStreamJoinNode.StreamStreamJoinNodeBuilder<K1, V1, V2, R> joinBuilder = StreamStreamJoinNode.streamStreamJoinNodeBuilder();

             ProcessorParameters<K1, V1> joinThisProcessorParams = new ProcessorParameters<>(joinThis, joinThisName);
             ProcessorParameters<K1, V2> joinOtherProcessorParams = new ProcessorParameters<>(joinOther, joinOtherName);
             ProcessorParameters<K1, R> joinMergeProcessorParams = new ProcessorParameters<>(joinMerge, joinMergeName);

            joinBuilder.withJoinMergeProcessorParameters(joinMergeProcessorParams)
                       .withJoinThisProcessorParameters(joinThisProcessorParams)
                       .withJoinOtherProcessorParameters(joinOtherProcessorParams)
                       .withThisWindowStoreBuilder(thisWindowStore)
                       .withOtherWindowStoreBuilder(otherWindowStore)
                       .withThisWindowedStreamProcessorParameters(thisWindowStreamProcessorParams)
                       .withOtherWindowedStreamProcessorParameters(otherWindowStreamProcessorParams)
                       .withValueJoiner(joiner)
                       .withNodeName(joinMergeName);

             StreamsGraphNode joinGraphNode = joinBuilder.build();

            builder.addGraphNode(Arrays.asList(thisStreamsGraphNode, otherStreamsGraphNode), joinGraphNode);

             Set<string> allSourceNodes = new HashSet<>(((KStreamImpl<K1, V1>) lhs).sourceNodes);
            allSourceNodes.addAll(((KStreamImpl<K1, V2>) other).sourceNodes);

            // do not have serde for joined result;
            // also for key serde we do not inherit from either since we cannot tell if these two serdes are different
            return new KStreamImpl<>(joinMergeName, joined.keySerde(), null, allSourceNodes, false, joinGraphNode, builder);
        }
    }

}
