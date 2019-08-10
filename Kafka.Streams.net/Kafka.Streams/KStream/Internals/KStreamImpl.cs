using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.Processor;
using Kafka.Streams.State;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamImpl<K, V> : AbstractStream<K, V>, IKStream<K, V>
    {
        public static string SOURCE_NAME = "KSTREAM-SOURCE-";
        public static string SINK_NAME = "KSTREAM-SINK-";
        public static string REPARTITION_TOPIC_SUFFIX = "-repartition";

        private static string BRANCH_NAME = "KSTREAM-BRANCH-";

        private static string BRANCHCHILD_NAME = "KSTREAM-BRANCHCHILD-";

        private static string FILTER_NAME = "KSTREAM-FILTER-";

        private static string PEEK_NAME = "KSTREAM-PEEK-";

        private static string FLATMAP_NAME = "KSTREAM-FLATMAP-";

        private static string FLATMAPVALUES_NAME = "KSTREAM-FLATMAPVALUES-";

        private static string JOINTHIS_NAME = "KSTREAM-JOINTHIS-";

        private static string JOINOTHER_NAME = "KSTREAM-JOINOTHER-";

        private static string JOIN_NAME = "KSTREAM-JOIN-";

        private static string LEFTJOIN_NAME = "KSTREAM-LEFTJOIN-";

        private static string MAP_NAME = "KSTREAM-MAP-";

        private static string MAPVALUES_NAME = "KSTREAM-MAPVALUES-";

        private static string MERGE_NAME = "KSTREAM-MERGE-";

        private static string OUTERTHIS_NAME = "KSTREAM-OUTERTHIS-";

        private static string OUTEROTHER_NAME = "KSTREAM-OUTEROTHER-";

        private static string PROCESSOR_NAME = "KSTREAM-PROCESSOR-";

        private static string PRINTING_NAME = "KSTREAM-PRINTER-";

        private static string KEY_SELECT_NAME = "KSTREAM-KEY-SELECT-";

        private static string TRANSFORM_NAME = "KSTREAM-TRANSFORM-";

        private static string TRANSFORMVALUES_NAME = "KSTREAM-TRANSFORMVALUES-";

        private static string WINDOWED_NAME = "KSTREAM-WINDOWED-";

        private static string FOREACH_NAME = "KSTREAM-FOREACH-";

        private bool repartitionRequired;

        public KStreamImpl(
            string name,
            ISerde<K> keySerde,
            ISerde<V> valueSerde,
            HashSet<string> sourceNodes,
            bool repartitionRequired,
            StreamsGraphNode streamsGraphNode,
            InternalStreamsBuilder builder)
            : base(name, keySerde, valueSerde, sourceNodes, streamsGraphNode, builder)
        {
            this.repartitionRequired = repartitionRequired;
        }


        public IKStream<K, V> filter(IPredicate<K, V> predicate)
        {
            return filter(predicate, NamedInternal.empty());
        }


        public IKStream<K, V> filter(IPredicate<K, V> predicate, Named named)
        {
            predicate = predicate ?? throw new System.ArgumentNullException("predicate can't be null", nameof(predicate));
            named = named ?? throw new System.ArgumentNullException("named can't be null", nameof(named));
            string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, FILTER_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<>(new KStreamFilter<>(predicate, false), name);
            ProcessorGraphNode<K, V> filterProcessorNode = new ProcessorGraphNode<>(name, processorParameters);
            builder.addGraphNode(this.streamsGraphNode, filterProcessorNode);

            return new KStreamImpl<K, V>(
                    name,
                    keySerde,
                    valSerde,
                    sourceNodes,
                    repartitionRequired,
                    filterProcessorNode,
                    builder);
        }


        public IKStream<K, V> filterNot(IPredicate<K, V> predicate)
        {
            return filterNot(predicate, NamedInternal.empty());
        }


        public IKStream<K, V> filterNot(IPredicate<K, V> predicate, Named named)
        {
            predicate = predicate ?? throw new System.ArgumentNullException("predicate can't be null", nameof(predicate));
            named = named ?? throw new System.ArgumentNullException("named can't be null", nameof(named));
            string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, FILTER_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<>(new KStreamFilter<>(predicate, true), name);
            ProcessorGraphNode<K, V> filterNotProcessorNode = new ProcessorGraphNode<>(name, processorParameters);

            builder.addGraphNode(this.streamsGraphNode, filterNotProcessorNode);

            return new KStreamImpl<K, V>(
                    name,
                    keySerde,
                    valSerde,
                    sourceNodes,
                    repartitionRequired,
                    filterNotProcessorNode,
                    builder);
        }


        public IKStream<KR, V> selectKey<KR>(IKeyValueMapper<K, V, KR> mapper)
        {
            return selectKey(mapper, NamedInternal.empty());
        }


        public IKStream<KR, V> selectKey<KR>(IKeyValueMapper<K, V, KR> mapper, Named named)
        {
            mapper = mapper ?? throw new System.ArgumentNullException("mapper can't be null", nameof(mapper));
            named = named ?? throw new System.ArgumentNullException("named can't be null", nameof(named));

            ProcessorGraphNode<K, V> selectKeyProcessorNode = internalSelectKey(mapper, new NamedInternal(named));

            selectKeyProcessorNode.keyChangingOperation(true);
            builder.addGraphNode(this.streamsGraphNode, selectKeyProcessorNode);

            // key serde cannot be preserved
            return new KStreamImpl<K, V>(selectKeyProcessorNode.nodeName, null, valSerde, sourceNodes, true, selectKeyProcessorNode, builder);
        }

        private ProcessorGraphNode<K, V> internalSelectKey<KR>(IKeyValueMapper<K, V, KR> mapper,
                                                                 NamedInternal named)
        {
            string name = named.orElseGenerateWithPrefix(builder, KEY_SELECT_NAME);
            //KStreamMap<K, V, KR, V> kStreamMap = new KStreamMap<>((key, value)=> new KeyValue<>(mapper.apply(key, value), value));

            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(kStreamMap, name);

            return new ProcessorGraphNode<K, V>(name, processorParameters);
        }


        public IKStream<KR, VR> map<KR, VR>(IKeyValueMapper<K, V, KeyValue<KR, VR>> mapper)
        {
            return map(mapper, NamedInternal.empty());
        }


        public IKStream<KR, VR> map<KR, VR>(IKeyValueMapper<K, V, KeyValue<KR, VR>> mapper, Named named)
        {
            mapper = mapper ?? throw new System.ArgumentNullException("mapper can't be null", nameof(mapper));
            named = named ?? throw new System.ArgumentNullException("named can't be null", nameof(named));
            string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, MAP_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamMap<K, V, KR, VR>(mapper), name);

            ProcessorGraphNode<K, V> mapProcessorNode = new ProcessorGraphNode<K, V>(name, processorParameters);

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


        public IKStream<K, VR> mapValues<VR>(IValueMapper<V, VR> mapper)
        {
            return mapValues(withKey(mapper));
        }


        public IKStream<K, VR> mapValues<VR>(IValueMapper<V, VR> mapper, Named named)
        {
            return mapValues(withKey(mapper), named);
        }


        public IKStream<K, VR> mapValues<VR>(IValueMapperWithKey<K, V, VR> mapper)
        {
            return mapValues(mapper, NamedInternal.empty());
        }


        public IKStream<K, VR> mapValues<VR>(IValueMapperWithKey<K, V, VR> mapper, Named named)
        {
            mapper = mapper ?? throw new System.ArgumentNullException("mapper can't be null", nameof(mapper));
            mapper = mapper ?? throw new System.ArgumentNullException("named can't be null", nameof(mapper));
            string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, MAPVALUES_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<>(new KStreamMapValues<>(mapper), name);
            ProcessorGraphNode<K, V> mapValuesProcessorNode = new ProcessorGraphNode<>(name, processorParameters);

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


        public void print(Printed<K, V> printed)
        {
            printed = printed ?? throw new System.ArgumentNullException("printed can't be null", nameof(printed));
            PrintedInternal<K, V> printedInternal = new PrintedInternal<>(printed);
            string name = new NamedInternal(printedInternal.name).orElseGenerateWithPrefix(builder, PRINTING_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<>(printedInternal.build(this.name), name);
            ProcessorGraphNode<K, V> printNode = new ProcessorGraphNode<>(name, processorParameters);

            builder.addGraphNode(this.streamsGraphNode, printNode);
        }


        public IKStream<KR, VR> flatMap<KR, VR>(IKeyValueMapper<K, V, IEnumerable<KeyValue<KR, VR>>> mapper)
        {
            return flatMap(mapper, NamedInternal.empty());
        }


        public IKStream<KR, VR> flatMap<KR, VR>(
            IKeyValueMapper<K, V, IEnumerable<KeyValue<KR, VR>>> mapper,
            Named named)
        {
            mapper = mapper ?? throw new System.ArgumentNullException("mapper can't be null", nameof(mapper));
            named = named ?? throw new System.ArgumentNullException("named can't be null", nameof(named));
            string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, FLATMAP_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<>(new KStreamFlatMap<>(mapper), name);
            ProcessorGraphNode<K, V> flatMapNode = new ProcessorGraphNode<>(name, processorParameters);
            flatMapNode.keyChangingOperation(true);

            builder.addGraphNode(this.streamsGraphNode, flatMapNode);

            // key and value serde cannot be preserved
            return new KStreamImpl<>(name, null, null, sourceNodes, true, flatMapNode, builder);
        }


        public IKStream<K, VR> flatMapValues<VR>(IValueMapper<V, IEnumerable<VR>> mapper)
        {
            return flatMapValues(withKey(mapper));
        }


        public IKStream<K, VR> flatMapValues<VR>(IValueMapper<V, IEnumerable<VR>> mapper,
                                                  Named named)
        {
            return flatMapValues(withKey(mapper), named);
        }


        public IKStream<K, VR> flatMapValues<VR>(IValueMapperWithKey<K, V, IEnumerable<VR>> mapper)
        {
            return flatMapValues(mapper, NamedInternal.empty());
        }


        public IKStream<K, VR> flatMapValues<VR>(
            IValueMapperWithKey<K, V, IEnumerable<VR>> mapper,
            Named named)
        {
            mapper = mapper ?? throw new System.ArgumentNullException("mapper can't be null", nameof(mapper));
            named = named ?? throw new System.ArgumentNullException("named can't be null", nameof(named));
            string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, FLATMAPVALUES_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamFlatMapValues<K, V, VR>(mapper), name);
            ProcessorGraphNode<K, V> flatMapValuesNode = new ProcessorGraphNode<K, V>(name, processorParameters);

            flatMapValuesNode.setValueChangingOperation(true);
            builder.addGraphNode(this.streamsGraphNode, flatMapValuesNode);

            // value serde cannot be preserved
            return new KStreamImpl<>(name, keySerde, null, sourceNodes, this.repartitionRequired, flatMapValuesNode, builder);
        }



        public IKStream<K, V>[] branch(IPredicate<K, V>[] predicates)
        {
            return doBranch(NamedInternal.empty(), predicates);
        }



        public IKStream<K, V>[] branch(Named name, IPredicate<K, V>[] predicates)
        {
            name = name ?? throw new System.ArgumentNullException("name can't be null", nameof(name));
            return doBranch(new NamedInternal(name), predicates);
        }


        private IKStream<K, V>[] doBranch(NamedInternal named,
                                          IPredicate<K, V>[] predicates)
        {
            if (predicates.Length == 0)
            {
                throw new System.ArgumentException("you must provide at least one predicate");
            }
            foreach (IPredicate<K, V> predicate in predicates)
            {
                predicate = predicate ?? throw new System.ArgumentNullException("predicates can't have null values", nameof(predicate));
            }

            string branchName = named.orElseGenerateWithPrefix(builder, BRANCH_NAME);

            string[] childNames = new string[predicates.Length];
            for (int i = 0; i < predicates.Length; i++)
            {
                childNames[i] = named.suffixWithOrElseGet("-predicate-" + i, builder, BRANCHCHILD_NAME);
            }

            ProcessorParameters processorParameters = new ProcessorParameters<>(new KStreamBranch(predicates.clone(), childNames), branchName);
            ProcessorGraphNode<K, V> branchNode = new ProcessorGraphNode<>(branchName, processorParameters);
            builder.addGraphNode(this.streamsGraphNode, branchNode);

            IKStream<K, V>[] branchChildren = (IKStream<K, V>[])Array.newInstance(KStream, predicates.Length);

            for (int i = 0; i < predicates.Length; i++)
            {
                ProcessorParameters innerProcessorParameters = new ProcessorParameters<>(new KStreamPassThrough<K, V>(), childNames[i]);
                ProcessorGraphNode<K, V> branchChildNode = new ProcessorGraphNode<>(childNames[i], innerProcessorParameters);

                builder.addGraphNode(branchNode, branchChildNode);
                branchChildren[i] = new KStreamImpl<>(childNames[i], keySerde, valSerde, sourceNodes, repartitionRequired, branchChildNode, builder);
            }

            return branchChildren;
        }


        public IKStream<K, V> merge(IKStream<K, V> stream)
        {
            Objects.requireNonNull(stream);
            return merge(builder, stream, NamedInternal.empty());
        }


        public IKStream<K, V> merge(IKStream<K, V> stream, Named processorName)
        {
            Objects.requireNonNull(stream);
            return merge(builder, stream, new NamedInternal(processorName));
        }

        private IKStream<K, V> merge(InternalStreamsBuilder builder,
                                     IKStream<K, V> stream,
                                     NamedInternal processorName)
        {
            KStreamImpl<K, V> streamImpl = (KStreamImpl<K, V>)stream;
            string name = processorName.orElseGenerateWithPrefix(builder, MERGE_NAME);
            HashSet<string> allSourceNodes = new HashSet<>();

            bool requireRepartitioning = streamImpl.repartitionRequired || repartitionRequired;
            allSourceNodes.AddAll(sourceNodes);
            allSourceNodes.AddAll(streamImpl.sourceNodes);

            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<>(new KStreamPassThrough<>(), name);

            ProcessorGraphNode<K, V> mergeNode = new ProcessorGraphNode<>(name, processorParameters);
            mergeNode.setMergeNode(true);
            builder.addGraphNode(Arrays.asList(this.streamsGraphNode, streamImpl.streamsGraphNode), mergeNode);

            // drop the serde as we cannot safely use either one to represent both streams
            return new KStreamImpl<>(name, null, null, allSourceNodes, requireRepartitioning, mergeNode, builder);
        }


        public void ForEach(ForeachAction<K, V> action)
        {
            ForEach(action, NamedInternal.empty());
        }


        public void ForEach(ForeachAction<K, V> action, Named named)
        {
            action = action ?? throw new System.ArgumentNullException("action can't be null", nameof(action));
            string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, FOREACH_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<>(
                   new KStreamPeek<>(action, false),
                   name
           );

            ProcessorGraphNode<K, V> foreachNode = new ProcessorGraphNode<>(name, processorParameters);
            builder.addGraphNode(this.streamsGraphNode, foreachNode);
        }


        public IKStream<K, V> peek(ForeachAction<K, V> action)
        {
            return peek(action, NamedInternal.empty());
        }


        public IKStream<K, V> peek(ForeachAction<K, V> action, Named named)
        {
            action = action ?? throw new System.ArgumentNullException("action can't be null", nameof(action));
            named = named ?? throw new System.ArgumentNullException("named can't be null", nameof(named));
            string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, PEEK_NAME);

            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<>(
                   new KStreamPeek<>(action, true),
                   name
           );

            ProcessorGraphNode<K, V> peekNode = new ProcessorGraphNode<>(name, processorParameters);

            builder.addGraphNode(this.streamsGraphNode, peekNode);

            return new KStreamImpl<>(name, keySerde, valSerde, sourceNodes, repartitionRequired, peekNode, builder);
        }


        public IKStream<K, V> through(string topic)
        {
            return through(topic, Produced.with(keySerde, valSerde, null));
        }


        public IKStream<K, V> through(string topic, Produced<K, V> produced)
        {
            topic = topic ?? throw new System.ArgumentNullException("topic can't be null", nameof(topic));
            produced = produced ?? throw new System.ArgumentNullException("Produced can't be null", nameof(produced));
            ProducedInternal<K, V> producedInternal = new ProducedInternal<>(produced);
            if (producedInternal.keySerde == null)
            {
                producedInternal.withKeySerde(keySerde);
            }
            if (producedInternal.valueSerde == null)
            {
                producedInternal.withValueSerde(valSerde);
            }
            to(topic, producedInternal);
            return builder.stream(
                Collections.singleton(topic),
                new ConsumedInternal<>(
                    producedInternal.keySerde,
                    producedInternal.valueSerde,
                    new FailOnInvalidTimestamp(),
                    null
                )
            );
        }


        public void to(string topic)
        {
            to(topic, Produced.with(keySerde, valSerde, null));
        }


        public void to(string topic, Produced<K, V> produced)
        {
            topic = topic ?? throw new System.ArgumentNullException("topic can't be null", nameof(topic));
            produced = produced ?? throw new System.ArgumentNullException("Produced can't be null", nameof(produced));
            ProducedInternal<K, V> producedInternal = new ProducedInternal<>(produced);
            if (producedInternal.keySerde == null)
            {
                producedInternal.withKeySerde(keySerde);
            }
            if (producedInternal.valueSerde == null)
            {
                producedInternal.withValueSerde(valSerde);
            }
            to(new StaticTopicNameExtractor<>(topic), producedInternal);
        }


        public void to(ITopicNameExtractor<K, V> topicExtractor)
        {
            to(topicExtractor, Produced.with(keySerde, valSerde, null));
        }


        public void to(ITopicNameExtractor<K, V> topicExtractor, Produced<K, V> produced)
        {
            topicExtractor = topicExtractor ?? throw new System.ArgumentNullException("topic extractor can't be null", nameof(topicExtractor));
            produced = produced ?? throw new System.ArgumentNullException("Produced can't be null", nameof(produced));
            ProducedInternal<K, V> producedInternal = new ProducedInternal<>(produced);
            if (producedInternal.keySerde == null)
            {
                producedInternal.withKeySerde(keySerde);
            }
            if (producedInternal.valueSerde == null)
            {
                producedInternal.withValueSerde(valSerde);
            }
            to(topicExtractor, producedInternal);
        }

        private void to(ITopicNameExtractor<K, V> topicExtractor, ProducedInternal<K, V> produced)
        {
            string name = new NamedInternal(produced.name).orElseGenerateWithPrefix(builder, SINK_NAME);
            StreamSinkNode<K, V> sinkNode = new StreamSinkNode<>(
               name,
               topicExtractor,
               produced
           );

            builder.addGraphNode(this.streamsGraphNode, sinkNode);
        }


        public IKStream<KR, VR> transform<KR, VR>(ITransformerSupplier<K, V, KeyValue<KR, VR>> transformerSupplier,
                                                   string[] stateStoreNames)
        {
            transformerSupplier = transformerSupplier ?? throw new System.ArgumentNullException("transformerSupplier can't be null", nameof(transformerSupplier));
            string name = builder.newProcessorName(TRANSFORM_NAME);
            return flatTransform(new TransformerSupplierAdapter<>(transformerSupplier), Named.As(name), stateStoreNames);
        }


        public IKStream<KR, VR> transform<KR, VR>(ITransformerSupplier<K, V, KeyValue<KR, VR>> transformerSupplier,
                                                   Named named,
                                                   string[] stateStoreNames)
        {
            transformerSupplier = transformerSupplier ?? throw new System.ArgumentNullException("transformerSupplier can't be null", nameof(transformerSupplier));
            return flatTransform(new TransformerSupplierAdapter<>(transformerSupplier), named, stateStoreNames);
        }


        public IKStream<K1, V1> flatTransform<K1, V1>(ITransformerSupplier<K, V, IEnumerable<KeyValue<K1, V1>>> transformerSupplier,
                                                       string[] stateStoreNames)
        {
            transformerSupplier = transformerSupplier ?? throw new System.ArgumentNullException("transformerSupplier can't be null", nameof(transformerSupplier));
            string name = builder.newProcessorName(TRANSFORM_NAME);
            return flatTransform(transformerSupplier, Named.As(name), stateStoreNames);
        }


        public IKStream<K1, V1> flatTransform<K1, V1>(ITransformerSupplier<K, V, IEnumerable<KeyValue<K1, V1>>> transformerSupplier,
                                                       Named named,
                                                       string[] stateStoreNames)
        {
            transformerSupplier = transformerSupplier ?? throw new System.ArgumentNullException("transformerSupplier can't be null", nameof(transformerSupplier));
            named = named ?? throw new System.ArgumentNullException("named can't be null", nameof(named));

            string name = new NamedInternal(named).name;
            StatefulProcessorNode<K, V> transformNode = new StatefulProcessorNode<>(
                   name,
                   new ProcessorParameters<>(new KStreamFlatTransform<>(transformerSupplier), name),
                   stateStoreNames
           );

            transformNode.keyChangingOperation(true);
            builder.addGraphNode(streamsGraphNode, transformNode);

            // cannot inherit key and value serde
            return new KStreamImpl<>(name, null, null, sourceNodes, true, transformNode, builder);
        }


        public IKStream<K, VR> transformValues<VR>(IValueTransformerSupplier<V, VR> valueTransformerSupplier,
                                                    string[] stateStoreNames)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new System.ArgumentNullException("valueTransformerSupplier can't be null", nameof(valueTransformerSupplier));
            return doTransformValues(toValueTransformerWithKeySupplier(valueTransformerSupplier), NamedInternal.empty(), stateStoreNames);
        }


        public IKStream<K, VR> transformValues<VR>(IValueTransformerSupplier<V, VR> valueTransformerSupplier,
                                                    Named named,
                                                    string[] stateStoreNames)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new System.ArgumentNullException("valueTransformSupplier can't be null", nameof(valueTransformerSupplier));
            named = named ?? throw new System.ArgumentNullException("named can't be null", nameof(named));
            return doTransformValues(toValueTransformerWithKeySupplier(valueTransformerSupplier),
                    new NamedInternal(named), stateStoreNames);
        }


        public IKStream<K, VR> transformValues<VR>(IValueTransformerWithKeySupplier<K, V, VR> valueTransformerSupplier,
                                                    string[] stateStoreNames)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new System.ArgumentNullException("valueTransformerSupplier can't be null", nameof(valueTransformerSupplier));
            return doTransformValues(valueTransformerSupplier, NamedInternal.empty(), stateStoreNames);
        }


        public IKStream<K, VR> transformValues<VR>(IValueTransformerWithKeySupplier<K, V, VR> valueTransformerSupplier,
                                                    Named named,
                                                    string[] stateStoreNames)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new System.ArgumentNullException("valueTransformSupplier can't be null", nameof(valueTransformerSupplier));
            named = named ?? throw new System.ArgumentNullException("named can't be null", nameof(named));
            return doTransformValues(valueTransformerSupplier, new NamedInternal(named), stateStoreNames);
        }

        private IKStream<K, VR> doTransformValues<VR>(IValueTransformerWithKeySupplier<K, V, VR> valueTransformerWithKeySupplier,
                                                       NamedInternal named,
                                                       string[] stateStoreNames)
        {

            string name = named.orElseGenerateWithPrefix(builder, TRANSFORMVALUES_NAME);
            StatefulProcessorNode<K, V> transformNode = new StatefulProcessorNode<>(
               name,
               new ProcessorParameters<>(new KStreamTransformValues<>(valueTransformerWithKeySupplier), name),
               stateStoreNames
           );

            transformNode.setValueChangingOperation(true);
            builder.addGraphNode(this.streamsGraphNode, transformNode);

            // cannot inherit value serde
            return new KStreamImpl<>(name, keySerde, null, sourceNodes, repartitionRequired, transformNode, builder);
        }


        public IKStream<K, VR> flatTransformValues<VR>(IValueTransformerSupplier<V, IEnumerable<VR>> valueTransformerSupplier,
                                                        string[] stateStoreNames)
        {
            //valueTransformerSupplier = valueTransformerSupplier ?? throw new System.ArgumentNullException("valueTransformerSupplier can't be null", nameof(valueTransformerSupplier));

            return doFlatTransformValues(toValueTransformerWithKeySupplier(valueTransformerSupplier), NamedInternal.empty(), stateStoreNames);
        }


        public IKStream<K, VR> flatTransformValues<VR>(IValueTransformerSupplier<V, IEnumerable<VR>> valueTransformerSupplier,
                                                        Named named,
                                                        string[] stateStoreNames)
        {
            //valueTransformerSupplier = valueTransformerSupplier ?? throw new System.ArgumentNullException("valueTransformerSupplier can't be null", nameof(valueTransformerSupplier));

            return doFlatTransformValues(toValueTransformerWithKeySupplier(valueTransformerSupplier), named, stateStoreNames);
        }


        public IKStream<K, VR> flatTransformValues<VR>(IValueTransformerWithKeySupplier<K, V, IEnumerable<VR>> valueTransformerSupplier,
                                                        string[] stateStoreNames)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new System.ArgumentNullException("valueTransformerSupplier can't be null", nameof(valueTransformerSupplier));

            return doFlatTransformValues(valueTransformerSupplier, NamedInternal.empty(), stateStoreNames);
        }


        public IKStream<K, VR> flatTransformValues<VR>(IValueTransformerWithKeySupplier<K, V, IEnumerable<VR>> valueTransformerSupplier,
                                                        Named named,
                                                        string[] stateStoreNames)
        {
            //valueTransformerSupplier = valueTransformerSupplier ?? throw new System.ArgumentNullException("valueTransformerSupplier can't be null", nameof(valueTransformerSupplier));

            return doFlatTransformValues(valueTransformerSupplier, named, stateStoreNames);
        }

        private IKStream<K, VR> doFlatTransformValues<VR>(IValueTransformerWithKeySupplier<K, V, IEnumerable<VR>> valueTransformerWithKeySupplier,
                                                           Named named,
                                                           string[] stateStoreNames)
        {
            string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, TRANSFORMVALUES_NAME);

            StatefulProcessorNode<K, V> transformNode = new StatefulProcessorNode<>(
               name,
               new ProcessorParameters<>(new KStreamFlatTransformValues<>(valueTransformerWithKeySupplier), name),
               stateStoreNames
           );

            transformNode.setValueChangingOperation(true);
            builder.addGraphNode(this.streamsGraphNode, transformNode);

            // cannot inherit value serde
            return new KStreamImpl<>(name, keySerde, null, sourceNodes, repartitionRequired, transformNode, builder);
        }


        public void process(IProcessorSupplier<K, V> processorSupplier,
                             string[] stateStoreNames)
        {
            //processorSupplier = processorSupplier ?? throw new System.ArgumentNullException("ProcessSupplier cant' be null", nameof(processorSupplier));
            string name = builder.newProcessorName(PROCESSOR_NAME);
            process(processorSupplier, Named.As(name), stateStoreNames);
        }


        public void process(IProcessorSupplier<K, V> processorSupplier,
                             Named named,
                             string[] stateStoreNames)
        {
            //processorSupplier = processorSupplier ?? throw new System.ArgumentNullException("ProcessSupplier cant' be null", nameof(processorSupplier));
            //named = named ?? throw new System.ArgumentNullException("named cant' be null", nameof(named));

            string name = new NamedInternal(named).name;
            StatefulProcessorNode<K, V> processNode = new StatefulProcessorNode<>(
                   name,
                   new ProcessorParameters<>(processorSupplier, name),
                   stateStoreNames
           );

            builder.addGraphNode(this.streamsGraphNode, processNode);
        }


        public IKStream<K, VR> join<VO, VR>(IKStream<K, VO> other,
                                             IValueJoiner<V, VO, VR> joiner,
                                             JoinWindows windows)
        {
            return join(other, joiner, windows, Joined.with(null, null, null));
        }


        public IKStream<K, VR> join<VO, VR>(IKStream<K, VO> otherStream,
                                             IValueJoiner<V, VO, VR> joiner,
                                             JoinWindows windows,
                                             Joined<K, V, VO> joined)
        {

            return doJoin(otherStream,
                          joiner,
                          windows,
                          joined,
                          new KStreamImplJoin(false, false));

        }


        public IKStream<K, VR> outerJoin<VO, VR>(IKStream<K, VO> other,
                                                  IValueJoiner<V, VO, VR> joiner,
                                                  JoinWindows windows)
        {
            return outerJoin(other, joiner, windows, Joined.with(null, null, null));
        }


        public IKStream<K, VR> outerJoin<VO, VR>(IKStream<K, VO> other,
                                                  IValueJoiner<V, VO, VR> joiner,
                                                  JoinWindows windows,
                                                  Joined<K, V, VO> joined)
        {
            return doJoin(other, joiner, windows, joined, new KStreamImplJoin(true, true));
        }

        private IKStream<K, VR> doJoin<VO, VR>(IKStream<K, VO> other,
                                                IValueJoiner<V, VO, VR> joiner,
                                                JoinWindows windows,
                                                Joined<K, V, VO> joined,
                                                KStreamImplJoin join)
        {
            //other = other ?? throw new System.ArgumentNullException("other KStream can't be null", nameof(other));
            //joiner = joiner ?? throw new System.ArgumentNullException("joiner can't be null", nameof(joiner));
            //windows = windows ?? throw new System.ArgumentNullException("windows can't be null", nameof(windows));
            //joined = joined ?? throw new System.ArgumentNullException("joined can't be null", nameof(joined));

            KStreamImpl<K, V> joinThis = this;
            KStreamImpl<K, VO> joinOther = (KStreamImpl<K, VO>)other;

            JoinedInternal<K, V, VO> joinedInternal = new JoinedInternal<>(joined);
            NamedInternal name = new NamedInternal(joinedInternal.name);
            if (joinThis.repartitionRequired)
            {
                string joinThisName = joinThis.name;
                string leftJoinRepartitionTopicName = name.suffixWithOrElseGet("-left", joinThisName);
                joinThis = joinThis.repartitionForJoin(leftJoinRepartitionTopicName, joined.keySerde, joined.valueSerde);
            }

            if (joinOther.repartitionRequired)
            {
                string joinOtherName = joinOther.name;
                string rightJoinRepartitionTopicName = name.suffixWithOrElseGet("-right", joinOtherName);
                joinOther = joinOther.repartitionForJoin(rightJoinRepartitionTopicName, joined.keySerde, joined.otherValueSerde());
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
        private KStreamImpl<K, V> repartitionForJoin(string repartitionName,
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

        public static string createRepartitionedSource<K1, V1>(
            InternalStreamsBuilder builder,
            ISerde<K1> keySerde,
            ISerde<V1> valSerde,
            string repartitionTopicNamePrefix,
            OptimizableRepartitionNodeBuilder<K1, V1> optimizableRepartitionNodeBuilder)
        {


            string repartitionTopic = repartitionTopicNamePrefix + REPARTITION_TOPIC_SUFFIX;
            string sinkName = builder.newProcessorName(SINK_NAME);
            string nullKeyFilterProcessorName = builder.newProcessorName(FILTER_NAME);
            string sourceName = builder.newProcessorName(SOURCE_NAME);

            IPredicate<K1, V1> notNullKeyPredicate = (k, v)=>k != null;

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
                                             //.Adding explicit variable as it simplifies logic
                                             .withNodeName(sourceName);

            return sourceName;
        }


        public IKStream<K, VR> leftJoin<VO, VR>(IKStream<K, VO> other,
                                                 IValueJoiner<V, VO, VR> joiner,
                                                 JoinWindows windows)
        {
            return leftJoin(other, joiner, windows, Joined.with(null, null, null));
        }


        public IKStream<K, VR> leftJoin<VO, VR>(
            IKStream<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            JoinWindows windows,
            Joined<K, V, VO> joined)
        {
            //joined = joined ?? throw new System.ArgumentNullException("joined can't be null", nameof(joined));
            return doJoin(
                other,
                joiner,
                windows,
                joined,
                new KStreamImplJoin(true, false)
            );

        }


        public IKStream<K, VR> join<VO, VR>(IKTable<K, VO> other,
                                             IValueJoiner<V, VO, VR> joiner)
        {
            return join(other, joiner, Joined.with(null, null, null));
        }


        public IKStream<K, VR> join<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Joined<K, V, VO> joined)
        {
            //other = other ?? throw new System.ArgumentNullException("other can't be null", nameof(other));
            //joiner = joiner ?? throw new System.ArgumentNullException("joiner can't be null", nameof(joiner));
            //joined = joined ?? throw new System.ArgumentNullException("joined can't be null", nameof(joined));

            JoinedInternal<K, V, VO> joinedInternal = new JoinedInternal<K, V, VO>(joined);
            string name = joinedInternal.name;
            if (repartitionRequired)
            {
                KStreamImpl<K, V> thisStreamRepartitioned = repartitionForJoin(
                   name != null ? name : this.name,
                   joined.keySerde,
                   joined.valueSerde
               );
                return thisStreamRepartitioned.doStreamTableJoin(other, joiner, joined, false);
            }
            else
            {

                return doStreamTableJoin(other, joiner, joined, false);
            }
        }


        public IKStream<K, VR> leftJoin<VO, VR>(IKTable<K, VO> other, IValueJoiner<V, VO, VR> joiner)
        {
            return leftJoin(other, joiner, Joined.with(null, null, null));
        }


        public IKStream<K, VR> leftJoin<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Joined<K, V, VO> joined)
        {
            //other = other ?? throw new System.ArgumentNullException("other can't be null", nameof(other));
            //joiner = joiner ?? throw new System.ArgumentNullException("joiner can't be null", nameof(joiner));
            //joined = joined ?? throw new System.ArgumentNullException("joined can't be null", nameof(joined));
            JoinedInternal<K, V, VO> joinedInternal = new JoinedInternal<>(joined);
            string internalName = joinedInternal.name;
            if (repartitionRequired)
            {
                KStreamImpl<K, V> thisStreamRepartitioned = repartitionForJoin(
                   internalName != null ? internalName : name,
                   joined.keySerde,
                   joined.valueSerde
               );
                return thisStreamRepartitioned.doStreamTableJoin(other, joiner, joined, true);
            }
            else
            {

                return doStreamTableJoin(other, joiner, joined, true);
            }
        }


        public IKStream<K, VR> join<KG, VG, VR>(IGlobalKTable<KG, VG> globalTable,
                                                 IKeyValueMapper<K, V, KG> keyMapper,
                                                 IValueJoiner<V, VG, VR> joiner)
        {
            return globalTableJoin(globalTable, keyMapper, joiner, false, NamedInternal.empty());
        }


        public IKStream<K, VR> join<KG, VG, VR>(IGlobalKTable<KG, VG> globalTable,
                                                 IKeyValueMapper<K, V, KG> keyMapper,
                                                 IValueJoiner<V, VG, VR> joiner,
                                                 Named named)
        {
            return globalTableJoin(globalTable, keyMapper, joiner, false, named);
        }


        public IKStream<K, VR> leftJoin<KG, VG, VR>(
            IGlobalKTable<KG, VG> globalTable,
            IKeyValueMapper<K, V, KG> keyMapper,
            IValueJoiner<V, VG, VR> joiner)
        {
            return globalTableJoin(globalTable, keyMapper, joiner, true, NamedInternal.empty());
        }


        public IKStream<K, VR> leftJoin<KG, VG, VR>(
            IGlobalKTable<KG, VG> globalTable,
            IKeyValueMapper<K, V, KG> keyMapper,
            IValueJoiner<V, VG, VR> joiner,
            Named named)
        {
            return globalTableJoin(globalTable, keyMapper, joiner, true, named);
        }


        private IKStream<K, VR> globalTableJoin<KG, VG, VR>(
            IGlobalKTable<KG, VG> globalTable,
            IKeyValueMapper<K, V, KG> keyMapper,
            IValueJoiner<V, VG, VR> joiner,
            bool leftJoin,
            Named named)
        {
            //globalTable = globalTable ?? throw new System.ArgumentNullException("globalTable can't be null", nameof(globalTable));
            //keyMapper = keyMapper ?? throw new System.ArgumentNullException("keyMapper can't be null", nameof(keyMapper));
            //joiner = joiner ?? throw new System.ArgumentNullException("joiner can't be null", nameof(joiner));
            //named = named ?? throw new System.ArgumentNullException("named can't be null", nameof(named));

            IKTableValueGetterSupplier<KG, VG> valueGetterSupplier = ((GlobalKTableImpl<KG, VG>)globalTable).valueGetterSupplier();
            string name = new NamedInternal(named).orElseGenerateWithPrefix(builder, LEFTJOIN_NAME);

            IProcessorSupplier<K, V> processorSupplier = new KStreamGlobalKTableJoin<K, V>(
               valueGetterSupplier,
               joiner,
               keyMapper,
               leftJoin);

            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(processorSupplier, name);

            StreamTableJoinNode<K, V> streamTableJoinNode = new StreamTableJoinNode<K, V>(
                name,
                processorParameters,
                new string[] { },
                null);

            builder.addGraphNode(this.streamsGraphNode, streamTableJoinNode);

            // do not have serde for joined result
            return new KStreamImpl<>(name, keySerde, null, sourceNodes, repartitionRequired, streamTableJoinNode, builder);
        }


        private IKStream<K, VR> doStreamTableJoin<VR, VO>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Joined<K, V, VO> joined,
            bool leftJoin)
        {
            other = other ?? throw new System.ArgumentNullException("other KTable can't be null", nameof(other));
            joiner = joiner ?? throw new System.ArgumentNullException("joiner can't be null", nameof(joiner));

            HashSet<string> allSourceNodes = ensureJoinableWith((AbstractStream<K, VO>)other);

            JoinedInternal<K, V, VO> joinedInternal = new JoinedInternal<>(joined);
            NamedInternal renamed = new NamedInternal(joinedInternal.name);

            string name = renamed.orElseGenerateWithPrefix(builder, leftJoin ? LEFTJOIN_NAME : JOIN_NAME);
            IProcessorSupplier<K, V> processorSupplier = new KStreamKTableJoin<>(
               ((KTableImpl<K, object, VO>) other).valueGetterSupplier(),
               joiner,
               leftJoin
            );

            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(processorSupplier, name);
            StreamTableJoinNode<K, V> streamTableJoinNode = new StreamTableJoinNode<K, V>(
               name,
               processorParameters,
               ((KTableImpl)other).valueGetterSupplier().storeNames(),
               this.name
           );

            builder.addGraphNode(this.streamsGraphNode, streamTableJoinNode);

            // do not have serde for joined result
            return new KStreamImpl<>(name, joined.keySerde != null ? joined.keySerde : keySerde, null, allSourceNodes, false, streamTableJoinNode, builder);

        }


        public IKGroupedStream<KR, V> groupBy<KR>(IKeyValueMapper<K, V, KR> selector)
        {
            return groupBy(selector, Grouped.with(null, valSerde));
        }

        public IKGroupedStream<KR, V> groupBy<KR>(
            IKeyValueMapper<K, V, KR> selector,
            Grouped<KR, V> grouped)
        {
            selector = selector ?? throw new System.ArgumentNullException("selector can't be null", nameof(selector));
            grouped = grouped ?? throw new System.ArgumentNullException("grouped can't be null", nameof(grouped));
            GroupedInternal<KR, V> groupedInternal = new GroupedInternal<KR, V>(grouped);
            ProcessorGraphNode<K, V> selectKeyMapNode = internalSelectKey(selector, new NamedInternal(groupedInternal.name));
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


        public IKGroupedStream<K, V> groupByKey()
        {
            return groupByKey(Grouped.with(keySerde, valSerde));
        }

        public IKGroupedStream<K, V> groupByKey(Grouped<K, V> grouped)
        {
            GroupedInternal<K, V> groupedInternal = new GroupedInternal<K, V>(grouped);

            return new KGroupedStreamImpl<>(
                name,
                sourceNodes,
                groupedInternal,
                repartitionRequired,
                streamsGraphNode,
                builder);
        }

        //
        private static IStoreBuilder<IWindowStore<K, V>> joinWindowStoreBuilder(
            string joinName,
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

        private class KStreamImplJoin
        {


            private bool leftOuter;
            private bool rightOuter;


            KStreamImplJoin(bool leftOuter,
                             bool rightOuter)
            {
                this.leftOuter = leftOuter;
                this.rightOuter = rightOuter;
            }

            public IKStream<K1, R> join<K1, V1, V2, R>(
                IKStream<K1, V1> lhs,
                IKStream<K1, V2> other,
                IValueJoiner<V1, V2, R> joiner,
                JoinWindows windows,
                Joined<K1, V1, V2> joined)
            {

                JoinedInternal<K1, V1, V2> joinedInternal = new JoinedInternal<K1, V1, V2>(joined);
                NamedInternal renamed = new NamedInternal(joinedInternal.name);

                string thisWindowStreamName = renamed.suffixWithOrElseGet(
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
                StreamsGraphNode thisStreamsGraphNode = ((AbstractStream)lhs).streamsGraphNode;
                StreamsGraphNode otherStreamsGraphNode = ((AbstractStream)other).streamsGraphNode;


                IStoreBuilder<IWindowStore<K1, V1>> thisWindowStore =
                   joinWindowStoreBuilder(joinThisName, windows, joined.keySerde, joined.valueSerde);
                IStoreBuilder<IWindowStore<K1, V2>> otherWindowStore =
                   joinWindowStoreBuilder(joinOtherName, windows, joined.keySerde, joined.otherValueSerde());

                KStreamJoinWindow<K1, V1> thisWindowedStream = new KStreamJoinWindow<>(thisWindowStore.name);

                ProcessorParameters<K1, V1> thisWindowStreamProcessorParams = new ProcessorParameters<>(thisWindowedStream, thisWindowStreamName);
                ProcessorGraphNode<K1, V1> thisWindowedStreamsNode = new ProcessorGraphNode<>(thisWindowStreamName, thisWindowStreamProcessorParams);
                builder.addGraphNode(thisStreamsGraphNode, thisWindowedStreamsNode);

                KStreamJoinWindow<K1, V2> otherWindowedStream = new KStreamJoinWindow<>(otherWindowStore.name);

                ProcessorParameters<K1, V2> otherWindowStreamProcessorParams = new ProcessorParameters<>(otherWindowedStream, otherWindowStreamName);
                ProcessorGraphNode<K1, V2> otherWindowedStreamsNode = new ProcessorGraphNode<>(otherWindowStreamName, otherWindowStreamProcessorParams);
                builder.addGraphNode(otherStreamsGraphNode, otherWindowedStreamsNode);

                KStreamKStreamJoin<K1, R, V1, V2> joinThis = new KStreamKStreamJoin<>(
                   otherWindowStore.name,
                   windows.beforeMs,
                   windows.afterMs,
                   joiner,
                   leftOuter
               );

                KStreamKStreamJoin<K1, R, V2, V1> joinOther = new KStreamKStreamJoin<>(
                   thisWindowStore.name,
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

                HashSet<string> allSourceNodes = new HashSet<>(((KStreamImpl<K1, V1>)lhs).sourceNodes);
                allSourceNodes.AddAll(((KStreamImpl<K1, V2>)other).sourceNodes);

                // do not have serde for joined result;
                // also for key serde we do not inherit from either since we cannot tell if these two serdes are different
                return new KStreamImpl<>(joinMergeName, joined.keySerde, null, allSourceNodes, false, joinGraphNode, builder);
            }
        }

    }
}