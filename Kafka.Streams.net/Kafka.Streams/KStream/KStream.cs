using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.KStream.Mappers;
using Kafka.Streams.Processor;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.Processor.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.Interfaces;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class KStream
    {
        public static string SourceName = "KSTREAM-SOURCE-";
        public static string SinkName = "KSTREAM-SINK-";
        public static string RepartitionTopicSuffix = "-repartition";
        public static string BranchName = "KSTREAM-BRANCH-";
        public static string BranchChildName = "KSTREAM-BRANCHCHILD-";
        public static string FilterName = "KSTREAM-FILTER-";
        public static string PeekName = "KSTREAM-PEEK-";
        public static string FlatmapName = "KSTREAM-FLATMAP-";
        public static string FlatmapValuesName = "KSTREAM-FLATMAPVALUES-";
        public static string JOINTHIS_NAME = "KSTREAM-JOINTHIS-";
        public static string JOINOTHER_NAME = "KSTREAM-JOINOTHER-";
        public static string JOIN_NAME = "KSTREAM-JOIN-";
        public static string LEFTJOIN_NAME = "KSTREAM-LEFTJOIN-";
        public static string MapName = "KSTREAM-MAP-";
        public static string MAPVALUES_NAME = "KSTREAM-MAPVALUES-";
        public static string MERGE_NAME = "KSTREAM-MERGE-";
        public static string OUTERTHIS_NAME = "KSTREAM-OUTERTHIS-";
        public static string OUTEROTHER_NAME = "KSTREAM-OUTEROTHER-";
        public static string ProcessorName = "KSTREAM-PROCESSOR-";
        public static string PRINTING_NAME = "KSTREAM-PRINTER-";
        public static string KEY_SELECT_NAME = "KSTREAM-KEY-SELECT-";
        public static string TransformName = "KSTREAM-TRANSFORM-";
        public static string TransformValuesName = "KSTREAM-TRANSFORMVALUES-";
        public static string WINDOWED_NAME = "KSTREAM-WINDOWED-";
        public static string ForEachName = "KSTREAM-FOREACH-";

        public static IStoreBuilder<IWindowStore<K, V>> joinWindowStoreBuilder<K, V>(
            string joinName,
            JoinWindows windows,
            ISerde<K> keySerde,
            ISerde<V> valueSerde)
        {
            return Stores.windowStoreBuilder(
                Stores.persistentWindowStore(
                    joinName + "-store",
                    windows.size() + windows.gracePeriodMs(),
                    windows.size(),
                    true),
                keySerde,
                valueSerde);
        }
    }

    public class KStream<K, V> : AbstractStream<K, V>, IKStream<K, V>
    {
        private readonly bool repartitionRequired;

        public KStream(
            string name,
            ISerde<K> keySerde,
            ISerde<V> valueSerde,
            HashSet<string> sourceNodes,
            bool repartitionRequired,
            StreamsGraphNode streamsGraphNode,
            InternalStreamsBuilder builder)
            : base(
                  name,
                  keySerde,
                  valueSerde,
                  sourceNodes,
                  streamsGraphNode,
                  builder)
        {
            this.repartitionRequired = repartitionRequired;
        }

        public IKStream<K, V> filter(Func<K, V, bool> predicate)
        {
            return filter(predicate, NamedInternal.empty());
        }

        public IKStream<K, V> filter(Func<K, V, bool> predicate, Named named)
        {
            predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));
            named = named ?? throw new ArgumentNullException(nameof(named));

            string name = new NamedInternal(named).OrElseGenerateWithPrefix(builder, KStream.FilterName);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamFilter<K, V>(predicate, false), name);
            ProcessorGraphNode<K, V> filterProcessorNode = new ProcessorGraphNode<K, V>(name, processorParameters);

            builder.AddGraphNode<K, V>(this.streamsGraphNode, filterProcessorNode);

            return new KStream<K, V>(
                    name,
                    keySerde,
                    valSerde,
                    sourceNodes,
                    repartitionRequired,
                    filterProcessorNode,
                    builder);
        }


        public IKStream<K, V> filterNot(Func<K, V, bool> predicate)
        {
            return filterNot(predicate, NamedInternal.empty());
        }


        public IKStream<K, V> filterNot(Func<K, V, bool> predicate, Named named)
        {
            predicate = predicate ?? throw new ArgumentNullException("predicate can't be null", nameof(predicate));
            named = named ?? throw new ArgumentNullException("named can't be null", nameof(named));

            string name = new NamedInternal(named).OrElseGenerateWithPrefix(builder, KStream.FilterName);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamFilter<K, V>(predicate, true), name);
            ProcessorGraphNode<K, V> filterNotProcessorNode = new ProcessorGraphNode<K, V>(name, processorParameters);

            builder.AddGraphNode<K, V>(this.streamsGraphNode, filterNotProcessorNode);

            return new KStream<K, V>(
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
            mapper = mapper ?? throw new ArgumentNullException("mapper can't be null", nameof(mapper));
            named = named ?? throw new ArgumentNullException("named can't be null", nameof(named));

            ProcessorGraphNode<K, V> selectKeyProcessorNode = internalSelectKey(mapper, new NamedInternal(named));

            selectKeyProcessorNode.IsKeyChangingOperation = true;
            builder.AddGraphNode<K, V>(this.streamsGraphNode, selectKeyProcessorNode);

            // key serde cannot be preserved
            return new KStream<KR, V>(
                selectKeyProcessorNode.NodeName,
                null,
                valSerde,
                sourceNodes,
                true,
                selectKeyProcessorNode,
                builder);
        }

        private ProcessorGraphNode<K, V> internalSelectKey<KR>(
            IKeyValueMapper<K, V, KR> mapper,
            NamedInternal named)
        {
            string name = named.OrElseGenerateWithPrefix(builder, KStream.KEY_SELECT_NAME);
            KStreamMap<K, V, KR, V> kStreamMap = new KStreamMap<K, V, KR, V>(null);// (key, value)=> new KeyValue<K, V>(mapper.apply(key, value), value));

            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(kStreamMap, name);

            return new ProcessorGraphNode<K, V>(name, processorParameters);
        }

        public IKStream<KR, VR> map<KR, VR>(IKeyValueMapper<K, V, KeyValue<KR, VR>> mapper)
            => map(mapper, NamedInternal.empty());

        public IKStream<KR, VR> map<KR, VR>(IKeyValueMapper<K, V, KeyValue<KR, VR>> mapper, Named named)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
            named = named ?? throw new ArgumentNullException(nameof(named));

            string name = new NamedInternal(named).OrElseGenerateWithPrefix(builder, KStream.MapName);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamMap<K, V, KR, VR>(mapper), name);

            ProcessorGraphNode<K, V> mapProcessorNode = new ProcessorGraphNode<K, V>(name, processorParameters);

            mapProcessorNode.IsKeyChangingOperation = true;
            builder.AddGraphNode<K, V>(this.streamsGraphNode, mapProcessorNode);

            // key and value serde cannot be preserved
            return new KStream<KR, VR>(
                    name,
                    null,
                    null,
                    sourceNodes,
                    true,
                    mapProcessorNode,
                    builder);
        }

        public IKStream<K, VR> mapValues<VR>(IValueMapper<V, VR> mapper)
            => mapValues(withKey<VR>(mapper));

        public IKStream<K, VR> mapValues<VR>(IValueMapper<V, VR> mapper, Named named)
            => mapValues(withKey(mapper), named);

        public IKStream<K, VR> mapValues<VR>(IValueMapperWithKey<K, V, VR> mapper)
            => mapValues(mapper, NamedInternal.empty());

        public IKStream<K, VR> mapValues<VR>(IValueMapperWithKey<K, V, VR> mapper, Named named)
        {
            mapper = mapper ?? throw new ArgumentNullException("mapper can't be null", nameof(mapper));
            mapper = mapper ?? throw new ArgumentNullException("named can't be null", nameof(mapper));

            string name = new NamedInternal(named).OrElseGenerateWithPrefix(builder, KStream.MAPVALUES_NAME);

            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamMapValues<K, V, VR>(mapper), name);
            ProcessorGraphNode<K, V> mapValuesProcessorNode = new ProcessorGraphNode<K, V>(name, processorParameters)
            {
                IsValueChangingOperation = true
            };

            builder.AddGraphNode<K, V>(this.streamsGraphNode, mapValuesProcessorNode);

            // value serde cannot be preserved
            return new KStream<K, VR>(
                    name,
                    keySerde,
                    null,
                    sourceNodes,
                    repartitionRequired,
                    mapValuesProcessorNode,
                    builder);
        }

        //public void print(Printed<K, V> printed)
        //{
        //    printed = printed ?? throw new System.ArgumentNullException("printed can't be null", nameof(printed));
        //    PrintedInternal<K, V> printedInternal = new PrintedInternal<K, V>(printed);
        //    //            string name = new NamedInternal(printedInternal.name).OrElseGenerateWithPrefix(builder, KStream.PRINTING_NAME);
        //    ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(printedInternal.build(this.name), name);
        //    ProcessorGraphNode<K, V> printNode = new ProcessorGraphNode<K, V>(name, processorParameters);

        //    builder.AddGraphNode(this.streamsGraphNode, printNode);
        //}

        public IKStream<KR, VR> flatMap<KR, VR>(IKeyValueMapper<K, V, IEnumerable<KeyValue<KR, VR>>> mapper)
            => flatMap(mapper, NamedInternal.empty());

        public IKStream<KR, VR> flatMap<KR, VR>(
            IKeyValueMapper<K, V, IEnumerable<KeyValue<KR, VR>>> mapper,
            Named named)
        {
            mapper = mapper ?? throw new ArgumentNullException("mapper can't be null", nameof(mapper));
            named = named ?? throw new ArgumentNullException("named can't be null", nameof(named));

            string name = new NamedInternal(named).OrElseGenerateWithPrefix(builder, KStream.FlatmapName);

            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamFlatMap<K, V, KR, VR>(mapper), name);
            ProcessorGraphNode<K, V> flatMapNode = new ProcessorGraphNode<K, V>(name, processorParameters);
            flatMapNode.IsKeyChangingOperation = true;

            builder.AddGraphNode<K, V>(this.streamsGraphNode, flatMapNode);

            // key and value serde cannot be preserved
            return new KStream<KR, VR>(
                name,
                null,
                null,
                sourceNodes,
                true,
                flatMapNode,
                builder);
        }

        public IKStream<K, VR> flatMapValues<VR>(IValueMapper<V, IEnumerable<VR>> mapper)
            => flatMapValues<VR>(withKey<IEnumerable<VR>>(mapper));

        public IKStream<K, VR> flatMapValues<VR>(
            IValueMapper<V, IEnumerable<VR>> mapper, Named named)
            => flatMapValues(withKey(mapper), named);

        //public IKStream<K, VR> flatMapValues<VR>(Func<V, IEnumerable<VR>> mapper)
        //    => flatMapValues(withKey(mapper));

        public IKStream<K, VR> flatMapValues<VR>(IValueMapperWithKey<K, V, IEnumerable<VR>> mapper)
            => flatMapValues(mapper, NamedInternal.empty());

        //public IKStream<K, VR> flatMapValues<VR>(Func<V, IEnumerable<VR>> mapper)
        //    where VR : IEnumerable<VR>
        //{
        //    var vm = new ValueMapper<V, IEnumerable<VR>>(mapper);

        //    return flatMapValues(vm);
        //}

        public IKStream<K, VR> flatMapValues<VR>(
            IValueMapperWithKey<K, V, IEnumerable<VR>> mapper,
            Named named)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
            named = named ?? throw new ArgumentNullException(nameof(named));

            string name = new NamedInternal(named).OrElseGenerateWithPrefix(builder, KStream.FlatmapValuesName);

            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamFlatMapValues<K, V, VR>(mapper), name);
            ProcessorGraphNode<K, V> flatMapValuesNode = new ProcessorGraphNode<K, V>(name, processorParameters);

            flatMapValuesNode.IsValueChangingOperation = true;

            builder.AddGraphNode<K, V>(this.streamsGraphNode, flatMapValuesNode);

            // value serde cannot be preserved
            return new KStream<K, VR>(
                name,
                keySerde,
                null,
                sourceNodes,
                this.repartitionRequired,
                flatMapValuesNode,
                builder);
        }

        public IKStream<K, V>[] branch(IPredicate<K, V>[] predicates)
            => doBranch(NamedInternal.empty(), predicates);

        public IKStream<K, V>[] branch(Named name, IPredicate<K, V>[] predicates)
        {
            name = name ?? throw new ArgumentNullException(nameof(name));

            return doBranch(new NamedInternal(name), predicates);
        }

        private IKStream<K, V>[] doBranch(
            NamedInternal named,
            IPredicate<K, V>[] predicates)
        {
            if (predicates.Length == 0)
            {
                throw new ArgumentException("you must provide at least one predicate");
            }

            foreach (IPredicate<K, V> predicate in predicates)
            {
                if (predicate == null)
                {
                    throw new ArgumentNullException(nameof(predicate));
                }
            }

            string branchName = named.OrElseGenerateWithPrefix(builder, KStream.BranchName);

            string[] childNames = new string[predicates.Length];
            for (int i = 0; i < predicates.Length; i++)
            {
                childNames[i] = named.suffixWithOrElseGet("-predicate-" + i, builder, KStream.BranchChildName);
            }

            var processorParameters = new ProcessorParameters<K, V>(new KStreamBranch<K, V>((IPredicate<K, V>[])predicates.Clone(), childNames), branchName);
            var branchNode = new ProcessorGraphNode<K, V>(branchName, processorParameters);
            builder.AddGraphNode<K, V>(this.streamsGraphNode, branchNode);

            IKStream<K, V>[] branchChildren = new IKStream<K, V>[predicates.Length];

            for (int i = 0; i < predicates.Length; i++)
            {
                var innerProcessorParameters = new ProcessorParameters<K, V>(new KStreamPassThrough<K, V>(), childNames[i]);
                var branchChildNode = new ProcessorGraphNode<K, V>(childNames[i], innerProcessorParameters);

                builder.AddGraphNode(branchNode, branchChildNode);

                branchChildren[i] = new KStream<K, V>(
                    childNames[i],
                    keySerde,
                    valSerde,
                    sourceNodes,
                    repartitionRequired,
                    branchChildNode,
                    builder);
            }

            return branchChildren;
        }

        public IKStream<K, V> merge(IKStream<K, V> stream)
        {
            stream = stream ?? throw new ArgumentNullException(nameof(stream));

            return merge(builder, stream, NamedInternal.empty());
        }


        public IKStream<K, V> merge(IKStream<K, V> stream, Named processorName)
        {
            stream = stream ?? throw new ArgumentNullException(nameof(stream));

            return merge(builder, stream, new NamedInternal(processorName));
        }

        private IKStream<K, V> merge(
            InternalStreamsBuilder builder,
            IKStream<K, V> stream,
            NamedInternal processorName)
        {
            KStream<K, V> streamImpl = (KStream<K, V>)stream;
            string name = processorName.OrElseGenerateWithPrefix(builder, KStream.MERGE_NAME);
            HashSet<string> allSourceNodes = new HashSet<string>();

            bool requireRepartitioning = streamImpl.repartitionRequired || repartitionRequired;
            //allSourceNodes.AddAll(sourceNodes);
            //allSourceNodes.AddAll(streamImpl.sourceNodes);

            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamPassThrough<K, V>(), name);

            ProcessorGraphNode<K, V> mergeNode = new ProcessorGraphNode<K, V>(name, processorParameters);
            mergeNode.SetMergeNode(true);

            var parents = new HashSet<StreamsGraphNode> { this.streamsGraphNode, streamImpl.streamsGraphNode };
            builder.AddGraphNode(parents, mergeNode);

            // drop the serde as we cannot safely use either one to represent both streams
            return new KStream<K, V>(name, null, null, allSourceNodes, requireRepartitioning, mergeNode, builder);
        }

        public void ForEach(IForeachAction<K, V> action)
            => ForEach(action, NamedInternal.empty());

        public void ForEach(IForeachAction<K, V> action, Named named)
        {
            action = action ?? throw new ArgumentNullException("action can't be null", nameof(action));

            string name = new NamedInternal(named).OrElseGenerateWithPrefix(builder, KStream.ForEachName);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(
                   new KStreamPeek<K, V>(action, false),
                   name);

            ProcessorGraphNode<K, V> foreachNode = new ProcessorGraphNode<K, V>(name, processorParameters);
            builder.AddGraphNode(this.streamsGraphNode, foreachNode);
        }

        public IKStream<K, V> peek(IForeachAction<K, V> action)
            => peek(action, NamedInternal.empty());

        public IKStream<K, V> peek(IForeachAction<K, V> action, Named named)
        {
            action = action ?? throw new ArgumentNullException(nameof(action));
            named = named ?? throw new ArgumentNullException(nameof(named));

            // string name = new NamedInternal(named).OrElseGenerateWithPrefix(builder, KStream.PEEK_NAME);

            // ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(
            //        new KStreamPeek<K, V>(action, true),
            //        name
            //);

            // ProcessorGraphNode<K, V> peekNode = new ProcessorGraphNode<>(name, processorParameters);

            // builder.AddGraphNode(this.streamsGraphNode, peekNode);

            // return new KStream<>(name, keySerde, valSerde, sourceNodes, repartitionRequired, peekNode, builder);
            return null;
        }

        public IKStream<K, V> through(string topic)
        {
            return null;
            //return through(topic, Produced.with(keySerde, valSerde, null));
        }


        public IKStream<K, V> through(string topic, Produced<K, V> produced)
        {
            topic = topic ?? throw new ArgumentNullException(nameof(topic));
            produced = produced ?? throw new ArgumentNullException(nameof(produced));

            ProducedInternal<K, V> producedInternal = new ProducedInternal<K, V>(produced);

            if (producedInternal.keySerde == null)
            {
                producedInternal.withKeySerde(keySerde);
            }

            if (producedInternal.valueSerde == null)
            {
                producedInternal.withValueSerde(valSerde);
            }

            to(topic, producedInternal);

            return builder.Stream(
                new List<string> { topic },
                new ConsumedInternal<K, V>(
                    producedInternal.keySerde,
                    producedInternal.valueSerde,
                    new FailOnInvalidTimestamp(null),
                    offsetReset: null));
        }

        public void to(string topic)
            => to(topic, Produced<K, V>.with(keySerde, valSerde, null));


        public void to(string topic, Produced<K, V> produced)
        {
            topic = topic ?? throw new ArgumentNullException(nameof(topic));
            produced = produced ?? throw new ArgumentNullException(nameof(produced));

            ProducedInternal<K, V> producedInternal = new ProducedInternal<K, V>(produced);

            if (producedInternal.keySerde == null)
            {
                producedInternal.withKeySerde(keySerde);
            }

            if (producedInternal.valueSerde == null)
            {
                producedInternal.withValueSerde(valSerde);
            }

            to(new StaticTopicNameExtractor(topic), producedInternal);
        }

        public void to(ITopicNameExtractor topicExtractor)
            => to(topicExtractor, Produced<K, V>.with(keySerde, valSerde, null));

        public void to(ITopicNameExtractor topicExtractor, Produced<K, V> produced)
        {
            topicExtractor = topicExtractor ?? throw new ArgumentNullException(nameof(topicExtractor));
            produced = produced ?? throw new ArgumentNullException(nameof(produced));

            ProducedInternal<K, V> producedInternal = new ProducedInternal<K, V>(produced);

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

        private void to(ITopicNameExtractor topicExtractor, ProducedInternal<K, V> produced)
        {
            string name = new NamedInternal(produced.name).OrElseGenerateWithPrefix(builder, KStream.SinkName);
            var sinkNode = new StreamSinkNode<K, V>(
               name,
               topicExtractor,
               produced);

            builder.AddGraphNode(this.streamsGraphNode, sinkNode);
        }

        public IKStream<KR, VR> transform<KR, VR>(
            ITransformerSupplier<K, V, KeyValue<KR, VR>> transformerSupplier,
            string[] stateStoreNames)
        {
            transformerSupplier = transformerSupplier ?? throw new ArgumentNullException(nameof(transformerSupplier));
            string name = builder.NewProcessorName(KStream.TransformName);

            return flatTransform(new TransformerSupplierAdapter<K, V, KR, VR>(transformerSupplier), Named.As(name), stateStoreNames);
        }

        public IKStream<KR, VR> transform<KR, VR>(
            ITransformerSupplier<K, V, KeyValue<KR, VR>> transformerSupplier,
            Named named,
            string[] stateStoreNames)
        {
            transformerSupplier = transformerSupplier ?? throw new ArgumentNullException(nameof(transformerSupplier));

            return flatTransform(new TransformerSupplierAdapter<K, V, KR, VR>(transformerSupplier), named, stateStoreNames);
        }

        public IKStream<K1, V1> flatTransform<K1, V1>(
            ITransformerSupplier<K, V, IEnumerable<KeyValue<K1, V1>>> transformerSupplier,
            string[] stateStoreNames)
        {
            transformerSupplier = transformerSupplier ?? throw new ArgumentNullException(nameof(transformerSupplier));

            string name = builder.NewProcessorName(KStream.TransformName);

            return flatTransform(transformerSupplier, Named.As(name), stateStoreNames);
        }

        public IKStream<K1, V1> flatTransform<K1, V1>(
            ITransformerSupplier<K, V, IEnumerable<KeyValue<K1, V1>>> transformerSupplier,
            Named named,
            string[] stateStoreNames)
        {
            transformerSupplier = transformerSupplier ?? throw new ArgumentNullException(nameof(transformerSupplier));
            named = named ?? throw new ArgumentNullException(nameof(named));

            string name = new NamedInternal(named).name;

            var transformNode = new StatefulProcessorNode<K, V>(
                   name,
                   new ProcessorParameters<K, V>(new KStreamFlatTransform<K, V, K1, V1>(transformerSupplier), name),
                   stateStoreNames)
            {
                IsKeyChangingOperation = true
            };

            builder.AddGraphNode(streamsGraphNode, transformNode);

            // cannot inherit key and value serde
            return new KStream<K1, V1>(
                name,
                null,
                null,
                sourceNodes,
                true,
                transformNode,
                builder);
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

        public IKStream<K, VR> transformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, VR> valueTransformerSupplier,
            Named named,
            string[] stateStoreNames)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new System.ArgumentNullException("valueTransformSupplier can't be null", nameof(valueTransformerSupplier));
            named = named ?? throw new System.ArgumentNullException("named can't be null", nameof(named));
            return doTransformValues(valueTransformerSupplier, new NamedInternal(named), stateStoreNames);
        }

        private IKStream<K, VR> doTransformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, VR> valueTransformerWithKeySupplier,
            NamedInternal named,
            string[] stateStoreNames)
        {
            string name = named.OrElseGenerateWithPrefix(builder, KStream.TransformValuesName);

            var transformNode = new StatefulProcessorNode<K, V>(
               name,
               new ProcessorParameters<K, V>(new KStreamTransformValues<K, V, VR>(valueTransformerWithKeySupplier), name),
               stateStoreNames)
            {
                IsValueChangingOperation = true
            };

            builder.AddGraphNode(this.streamsGraphNode, transformNode);

            // cannot inherit value serde
            return new KStream<K, VR>(
                name,
                keySerde,
                null,
                sourceNodes,
                repartitionRequired,
                transformNode,
                builder);
        }

        public IKStream<K, VR> flatTransformValues<VR>(
            IValueTransformerSupplier<V, IEnumerable<VR>> valueTransformerSupplier,
            string[] stateStoreNames)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new System.ArgumentNullException(nameof(valueTransformerSupplier));

            return doFlatTransformValues(toValueTransformerWithKeySupplier(valueTransformerSupplier), NamedInternal.empty(), stateStoreNames);
        }

        public IKStream<K, VR> flatTransformValues<VR>(
            IValueTransformerSupplier<V, IEnumerable<VR>> valueTransformerSupplier,
            Named named,
            string[] stateStoreNames)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new System.ArgumentNullException("valueTransformerSupplier can't be null", nameof(valueTransformerSupplier));

            return doFlatTransformValues(toValueTransformerWithKeySupplier(valueTransformerSupplier), named, stateStoreNames);
        }

        public IKStream<K, VR> flatTransformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, IEnumerable<VR>> valueTransformerSupplier,
            string[] stateStoreNames)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new System.ArgumentNullException("valueTransformerSupplier can't be null", nameof(valueTransformerSupplier));

            return doFlatTransformValues(valueTransformerSupplier, NamedInternal.empty(), stateStoreNames);
        }

        public IKStream<K, VR> flatTransformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, IEnumerable<VR>> valueTransformerSupplier,
            Named named,
            string[] stateStoreNames)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new System.ArgumentNullException("valueTransformerSupplier can't be null", nameof(valueTransformerSupplier));

            return doFlatTransformValues(valueTransformerSupplier, named, stateStoreNames);
        }

        private IKStream<K, VR> doFlatTransformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, IEnumerable<VR>> valueTransformerWithKeySupplier,
            Named named,
            string[] stateStoreNames)
        {
            string name = new NamedInternal(named).OrElseGenerateWithPrefix(builder, KStream.TransformValuesName);

            var transformNode = new StatefulProcessorNode<K, V>(
               name,
               new ProcessorParameters<K, V>(new KStreamFlatTransformValues<K, V, VR>(valueTransformerWithKeySupplier), name),
               stateStoreNames)
            {
                IsValueChangingOperation = true
            };

            builder.AddGraphNode(this.streamsGraphNode, transformNode);

            // cannot inherit value serde
            return new KStream<K, VR>(
                name,
                keySerde,
                null,
                sourceNodes,
                repartitionRequired,
                transformNode,
                builder);
        }

        public void process(
            IProcessorSupplier<K, V> IProcessorSupplier,
            string[] stateStoreNames)
        {
            IProcessorSupplier = IProcessorSupplier ?? throw new ArgumentNullException(nameof(IProcessorSupplier));

            string name = builder.NewProcessorName(KStream.ProcessorName);

            process(IProcessorSupplier, Named.As(name), stateStoreNames);
        }

        public void process(
            IProcessorSupplier<K, V> IProcessorSupplier,
            Named named,
            string[] stateStoreNames)
        {
            IProcessorSupplier = IProcessorSupplier ?? throw new ArgumentNullException(nameof(IProcessorSupplier));
            named = named ?? throw new ArgumentNullException(nameof(named));

            string name = new NamedInternal(named).name;

            var processNode = new StatefulProcessorNode<K, V>(
                   name,
                   new ProcessorParameters<K, V>(IProcessorSupplier, name),
                   stateStoreNames);

            builder.AddGraphNode(this.streamsGraphNode, processNode);
        }

        public IKStream<K, VR> join<VO, VR>(
            IKStream<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            JoinWindows windows)
        {
            return join(other, joiner, windows, Joined<K, V, VO>.with(null, null, null));
        }

        public IKStream<K, VR> join<VO, VR>(
            IKStream<K, VO> otherStream,
            IValueJoiner<V, VO, VR> joiner,
            JoinWindows windows,
            Joined<K, V, VO> joined)
        {
            return doJoin(otherStream,
                          joiner,
                          windows,
                          joined,
                          new KStreamJoin(this.builder, false, false));
        }

        public IKStream<K, VR> outerJoin<VO, VR>(
            IKStream<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            JoinWindows windows)
        {
            return outerJoin(other, joiner, windows, Joined<K, V, VO>.with(null, null, null));
        }

        public IKStream<K, VR> outerJoin<VO, VR>(
            IKStream<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            JoinWindows windows,
            Joined<K, V, VO> joined)
        {
            return doJoin(
                other,
                joiner,
                windows,
                joined,
                new KStreamJoin(this.builder, true, true));
        }

        private IKStream<K, VR> doJoin<VO, VR>(
            IKStream<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            JoinWindows windows,
            Joined<K, V, VO> joined,
            KStreamJoin join)
        {
            other = other ?? throw new ArgumentNullException(nameof(other));
            joiner = joiner ?? throw new ArgumentNullException(nameof(joiner));
            windows = windows ?? throw new ArgumentNullException(nameof(windows));
            joined = joined ?? throw new ArgumentNullException(nameof(joined));

            KStream<K, V> joinThis = this;
            KStream<K, VO> joinOther = (KStream<K, VO>)other;

            JoinedInternal<K, V, VO> joinedInternal = new JoinedInternal<K, V, VO>(joined);
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
                joinOther = joinOther.repartitionForJoin(rightJoinRepartitionTopicName, joined.keySerde, joined.otherValueSerde);
            }

            joinThis.ensureJoinableWith(joinOther);

            return join.join(
                joinThis,
                joinOther,
                joiner,
                windows,
                joined);
        }

        /**
         * Repartition a stream. This is required on join operations occurring after
         * an operation that changes the key, i.e, selectKey, map(..), flatMap(..).
         */
        private KStream<K, V> repartitionForJoin(
            string repartitionName,
            ISerde<K> keySerdeOverride,
            ISerde<V> valueSerdeOverride)
        {
            var repartitionKeySerde = keySerdeOverride ?? keySerde;
            var repartitionValueSerde = valueSerdeOverride ?? valSerde;

            var optimizableRepartitionNodeBuilder =
               OptimizableRepartitionNode.GetOptimizableRepartitionNodeBuilder<K, V>();

            string repartitionedSourceName = CreateRepartitionedSource(
                builder,
                repartitionKeySerde,
                repartitionValueSerde,
                repartitionName,
                optimizableRepartitionNodeBuilder);

            var optimizableRepartitionNode = optimizableRepartitionNodeBuilder.Build();

            builder.AddGraphNode<K, V>(this.streamsGraphNode, optimizableRepartitionNode);

            return new KStream<K, V>(
                repartitionedSourceName,
                repartitionKeySerde,
                repartitionValueSerde,
                new HashSet<string> { repartitionedSourceName },
                false,
                optimizableRepartitionNode,
                builder);
        }

        public static string CreateRepartitionedSource<K1, V1>(
            InternalStreamsBuilder builder,
            ISerde<K1> keySerde,
            ISerde<V1> valSerde,
            string repartitionTopicNamePrefix,
            OptimizableRepartitionNodeBuilder<K1, V1> optimizableRepartitionNodeBuilder)
        {
            string repartitionTopic = repartitionTopicNamePrefix + KStream.RepartitionTopicSuffix;
            string sinkName = builder.NewProcessorName(KStream.SinkName);
            string nullKeyFilterProcessorName = builder.NewProcessorName(KStream.FilterName);
            string sourceName = builder.NewProcessorName(KStream.SourceName);

            Func<K1, V1, bool> notNullKeyPredicate = (k, v) => k != null;

            var processorParameters = new ProcessorParameters<K1, V1>(
               new KStreamFilter<K1, V1>(notNullKeyPredicate, false),
               nullKeyFilterProcessorName);

            optimizableRepartitionNodeBuilder
                .WithKeySerde(keySerde)
                .WithValueSerde(valSerde)
                .WithSourceName(sourceName)
                .WithRepartitionTopic(repartitionTopic)
                .WithSinkName(sinkName)
                .WithProcessorParameters(processorParameters)
                // reusing the source name for the graph node name
                // adding explicit variable as it simplifies logic
                .WithNodeName(sourceName);

            return sourceName;
        }

        public IKStream<K, VR> leftJoin<VO, VR>(
            IKStream<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            JoinWindows windows)
            => leftJoin(other, joiner, windows, Joined<K, V, VO>.with(null, null, null));

        public IKStream<K, VR> leftJoin<VO, VR>(
            IKStream<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            JoinWindows windows,
            Joined<K, V, VO> joined)
        {
            joined = joined ?? throw new ArgumentNullException(nameof(joined));

            return doJoin(
                other,
                joiner,
                windows,
                joined,
                new KStreamJoin(this.builder, true, false));
        }

        public IKStream<K, VR> join<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner)
            => join(
                other,
                joiner,
                Joined<K, V, VO>.with(null, null, null));

        public IKStream<K, VR> join<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Joined<K, V, VO> joined)
        {
            other = other ?? throw new ArgumentNullException(nameof(other));
            joiner = joiner ?? throw new ArgumentNullException(nameof(joiner));
            joined = joined ?? throw new ArgumentNullException(nameof(joined));

            JoinedInternal<K, V, VO> joinedInternal = new JoinedInternal<K, V, VO>(joined);
            string name = joinedInternal.name;
            if (repartitionRequired)
            {
                KStream<K, V> thisStreamRepartitioned = repartitionForJoin(
                   name != null ? name : this.name,
                   joined.keySerde,
                   joined.valueSerde);

                return thisStreamRepartitioned.doStreamTableJoin(other, joiner, joined, false);
            }
            else
            {
                return doStreamTableJoin(other, joiner, joined, false);
            }
        }

        public IKStream<K, VR> leftJoin<VO, VR>(IKTable<K, VO> other, IValueJoiner<V, VO, VR> joiner)
            => leftJoin(other, joiner, Joined<K, V, VO>.with(null, null, null));

        public IKStream<K, VR> leftJoin<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Joined<K, V, VO> joined)
        {
            other = other ?? throw new ArgumentNullException(nameof(other));
            joiner = joiner ?? throw new ArgumentNullException(nameof(joiner));
            joined = joined ?? throw new ArgumentNullException(nameof(joined));

            JoinedInternal<K, V, VO> joinedInternal = new JoinedInternal<K, V, VO>(joined);
            string internalName = joinedInternal.name;

            if (repartitionRequired)
            {
                KStream<K, V> thisStreamRepartitioned = repartitionForJoin(
                   internalName != null ? internalName : name,
                   joined.keySerde,
                   joined.valueSerde);

                return thisStreamRepartitioned.doStreamTableJoin(other, joiner, joined, true);
            }
            else
            {
                return doStreamTableJoin(other, joiner, joined, true);
            }
        }

        public IKStream<K, VR> join<KG, VG, VR>(
            IGlobalKTable<KG, VG> globalTable,
            IKeyValueMapper<K, V, KG> keyMapper,
            IValueJoiner<V, VG, VR> joiner)
            => globalTableJoin(
                globalTable,
                keyMapper,
                joiner,
                false,
                NamedInternal.empty());

        public IKStream<K, VR> join<KG, VG, VR>(
            IGlobalKTable<KG, VG> globalTable,
            IKeyValueMapper<K, V, KG> keyMapper,
            IValueJoiner<V, VG, VR> joiner,
            Named named)
            => globalTableJoin(
                globalTable,
                keyMapper,
                joiner,
                false,
                named);

        public IKStream<K, VR> leftJoin<KG, VG, VR>(
            IGlobalKTable<KG, VG> globalTable,
            IKeyValueMapper<K, V, KG> keyMapper,
            IValueJoiner<V, VG, VR> joiner)
            => globalTableJoin(
                globalTable,
                keyMapper,
                joiner,
                leftJoin: true,
                NamedInternal.empty());

        public IKStream<K, VR> leftJoin<KG, VG, VR>(
            IGlobalKTable<KG, VG> globalTable,
            IKeyValueMapper<K, V, KG> keyMapper,
            IValueJoiner<V, VG, VR> joiner,
            Named named)
            => globalTableJoin(
                globalTable,
                keyMapper,
                joiner,
                true,
                named);

        private IKStream<K, VR> globalTableJoin<KG, VG, VR>(
            IGlobalKTable<KG, VG> globalTable,
            IKeyValueMapper<K, V, KG> keyMapper,
            IValueJoiner<V, VG, VR> joiner,
            bool leftJoin,
            Named named)
        {
            globalTable = globalTable ?? throw new ArgumentNullException(nameof(globalTable));
            keyMapper = keyMapper ?? throw new ArgumentNullException(nameof(keyMapper));
            joiner = joiner ?? throw new ArgumentNullException(nameof(joiner));
            named = named ?? throw new ArgumentNullException(nameof(named));

            IKTableValueGetterSupplier<KG, VG> valueGetterSupplier = ((GlobalKTableImpl<KG, VG>)globalTable).valueGetterSupplier;
            string name = new NamedInternal(named).OrElseGenerateWithPrefix(builder, KStream.LEFTJOIN_NAME);

            IProcessorSupplier<K, V> IProcessorSupplier = new KStreamGlobalKTableJoin<K, KG, VR, V, VG>(
               valueGetterSupplier,
               joiner,
               keyMapper,
               leftJoin);

            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(IProcessorSupplier, name);

            StreamTableJoinNode<K, V> streamTableJoinNode = new StreamTableJoinNode<K, V>(
                name,
                processorParameters,
                new string[] { },
                null);

            builder.AddGraphNode(this.streamsGraphNode, streamTableJoinNode);

            // do not have serde for joined result
            return new KStream<K, VR>(
                name,
                keySerde,
                null,
                sourceNodes,
                repartitionRequired,
                streamTableJoinNode,
                builder);
        }

        private IKStream<K, VR> doStreamTableJoin<VR, VO>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Joined<K, V, VO> joined,
            bool leftJoin)
        {
            other = other ?? throw new ArgumentNullException(nameof(other));
            joiner = joiner ?? throw new ArgumentNullException(nameof(joiner));

            HashSet<string> allSourceNodes = ensureJoinableWith((AbstractStream<K, VO>)other);

            JoinedInternal<K, V, VO> joinedInternal = new JoinedInternal<K, V, VO>(joined);
            NamedInternal renamed = new NamedInternal(joinedInternal.name);

            string name = renamed.OrElseGenerateWithPrefix(builder, leftJoin ? KStream.LEFTJOIN_NAME : KStream.JOIN_NAME);
            // IProcessorSupplier<K, V> IProcessorSupplier = new KStreamKTableJoin<K, VR, V, VO>(
            //    ((KTable<K, V, VO>)other).valueGetterSupplier(),
            //    joiner,
            //    leftJoin);

            // ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(IProcessorSupplier, name);
            // StreamTableJoinNode<K, V> streamTableJoinNode = new StreamTableJoinNode<K, V>(
            //    name,
            //    processorParameters,
            //    ((KTable)other).valueGetterSupplier.storeNames(),
            //    this.name
            //);

            // builder.AddGraphNode(this.streamsGraphNode, streamTableJoinNode);

            // // do not have serde for joined result
            // return new KStream<K, VR>(
            //     name,
            //     joined.keySerde != null ? joined.keySerde : keySerde,
            //     null,
            //     allSourceNodes,
            //     false,
            //     streamTableJoinNode,
            //     builder);

            return null;
        }

        public IKStream<K, RV> join<RV, GK, GV>(IGlobalKTable<GK, GV> globalKTable, IKeyValueMapper<K, V, GK> keyValueMapper, IValueJoiner<V, GV, RV> joiner)
            => throw new NotImplementedException();

        public IKStream<K, RV> join<RV, GK, GV>(IGlobalKTable<GK, GV> globalKTable, IKeyValueMapper<K, V, GK> keyValueMapper, IValueJoiner<V, GV, RV> joiner, Named named)
            => throw new NotImplementedException();

        public IKStream<K, RV> leftJoin<RV, GK, GV>(IGlobalKTable<GK, GV> globalKTable, IKeyValueMapper<K, V, GK> keyValueMapper, IValueJoiner<V, GV, RV> valueJoiner)
            => throw new NotImplementedException();

        public IKStream<K, RV> leftJoin<RV, GK, GV>(IGlobalKTable<GK, GV> globalKTable, IKeyValueMapper<K, V, GK> keyValueMapper, IValueJoiner<V, GV, RV> valueJoiner, Named named)
            => throw new NotImplementedException();

        public IKGroupedStream<KR, V> groupBy<KR>(IKeyValueMapper<K, V, KR> selector)
            => groupBy(selector, Grouped<KR, V>.With(null, valSerde));

        public IKGroupedStream<KR, V> groupBy<KR>(Func<K, V, KR> selector)
        {
            var kvm = new KeyValueMapper<K, V, KR>(selector);

            return groupBy(kvm);
        }

        public IKGroupedStream<KR, V> groupBy<KR>(IKeyValueMapper<K, V, KR> selector, ISerialized<KR, V> serialized)
        {
            throw new NotImplementedException();
        }

        public IKGroupedStream<KR, V> groupBy<KR>(
            IKeyValueMapper<K, V, KR> selector,
            Grouped<KR, V> grouped)
        {
            selector = selector ?? throw new ArgumentNullException(nameof(selector));
            grouped = grouped ?? throw new ArgumentNullException(nameof(grouped));

            GroupedInternal<KR, V> groupedInternal = new GroupedInternal<KR, V>(grouped);
            ProcessorGraphNode<K, V> selectKeyMapNode = internalSelectKey(selector, new NamedInternal(groupedInternal.name));
            selectKeyMapNode.IsKeyChangingOperation = true;

            builder.AddGraphNode(this.streamsGraphNode, selectKeyMapNode);

            return new KGroupedStream<KR, V>(
                selectKeyMapNode.NodeName,
                sourceNodes,
                groupedInternal,
                repartitionRequired: true,
                selectKeyMapNode,
                builder);
        }

        public IKGroupedStream<K, V> groupByKey(ISerialized<K, V> serialized)
        {
            var serializedInternal = new SerializedInternal<K, V>(serialized);

            return groupByKey(Grouped<K, V>.With(
                serializedInternal.keySerde,
                serializedInternal.valueSerde));
        }

        public IKGroupedStream<K, V> groupByKey()
        {
            return groupByKey(Grouped<K, V>.With(keySerde, valSerde));
        }

        public IKGroupedStream<K, V> groupByKey(Grouped<K, V> grouped)
        {
            GroupedInternal<K, V> groupedInternal = new GroupedInternal<K, V>(grouped);
            return new KGroupedStream<K, V>(
                name,
                sourceNodes,
                groupedInternal,
                repartitionRequired,
                streamsGraphNode,
                builder);
        }
    }
}