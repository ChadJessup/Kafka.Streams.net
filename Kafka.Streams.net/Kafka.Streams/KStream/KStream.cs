using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.KStream.Mappers;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.Windowed;
using NodaTime;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public static class KStream
    {
        public const string SourceName = "KSTREAM-SOURCE-";
        public const string SinkName = "KSTREAM-SINK-";
        public const string RepartitionTopicSuffix = "-repartition";
        public const string BranchName = "KSTREAM-BRANCH-";
        public const string BranchChildName = "KSTREAM-BRANCHCHILD-";
        public const string FilterName = "KSTREAM-FILTER-";
        public const string PeekName = "KSTREAM-PEEK-";
        public const string FlatmapName = "KSTREAM-FLATMAP-";
        public const string FlatmapValuesName = "KSTREAM-FLATMAPVALUES-";
        public const string JOINTHIS_NAME = "KSTREAM-JOINTHIS-";
        public const string JOINOTHER_NAME = "KSTREAM-JOINOTHER-";
        public const string JOIN_NAME = "KSTREAM-JOIN-";
        public const string LEFTJOIN_NAME = "KSTREAM-LEFTJOIN-";
        public const string MapName = "KSTREAM-MAP-";
        public const string MAPVALUES_NAME = "KSTREAM-MAPVALUES-";
        public const string MERGE_NAME = "KSTREAM-MERGE-";
        public const string OUTERTHIS_NAME = "KSTREAM-OUTERTHIS-";
        public const string OUTEROTHER_NAME = "KSTREAM-OUTEROTHER-";
        public const string ProcessorName = "KSTREAM-PROCESSOR-";
        public const string PRINTING_NAME = "KSTREAM-PRINTER-";
        public const string KEY_SELECT_NAME = "KSTREAM-KEY-SELECT-";
        public const string TransformName = "KSTREAM-TRANSFORM-";
        public const string TransformValuesName = "KSTREAM-TRANSFORMVALUES-";
        public const string WINDOWED_NAME = "KSTREAM-WINDOWED-";
        public const string ForEachName = "KSTREAM-FOREACH-";

        public static IStoreBuilder<IWindowStore<K, V>> JoinWindowStoreBuilder<K, V>(
            IClock clock,
            string joinName,
            JoinWindows windows,
            ISerde<K> keySerde,
            ISerde<V> valueSerde)
        {
            if (windows is null)
            {
                throw new ArgumentNullException(nameof(windows));
            }

            return Stores.WindowStoreBuilder(
                clock,
                Stores.PersistentWindowStore(
                    joinName + "-store",
                    windows.Size() + windows.GracePeriod(),
                    windows.Size(),
                    true),
                keySerde,
                valueSerde);
        }
    }

    public class KStream<K, V> : AbstractStream<K, V>, IKStream<K, V>
    {
        private readonly IClock clock;
        private readonly bool repartitionRequired;

        public KStream(
            IClock clock,
            string name,
            ISerde<K>? keySerde,
            ISerde<V>? valueSerde,
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
            this.clock = clock;
            this.repartitionRequired = repartitionRequired;
        }

        public IKStream<K, V> Filter(Func<K, V, bool> predicate)
        {
            return Filter(predicate, NamedInternal.Empty());
        }

        public IKStream<K, V> Filter(Func<K, V, bool> predicate, Named named)
        {
            predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));
            named = named ?? throw new ArgumentNullException(nameof(named));

            var name = new NamedInternal(named).OrElseGenerateWithPrefix(builder, KStream.FilterName);
            var processorParameters = new ProcessorParameters<K, V>(new KStreamFilter<K, V>(predicate, false), name);
            var filterProcessorNode = new ProcessorGraphNode<K, V>(name, processorParameters);

            builder.AddGraphNode<K, V>(this.streamsGraphNode, filterProcessorNode);

            return new KStream<K, V>(
                this.clock,
                name,
                keySerde,
                valSerde,
                sourceNodes,
                repartitionRequired,
                filterProcessorNode,
                builder);
        }

        public IKStream<K, V> FilterNot(Func<K, V, bool> predicate)
        {
            return FilterNot(predicate, NamedInternal.Empty());
        }

        public IKStream<K, V> FilterNot(Func<K, V, bool> predicate, Named named)
        {
            predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));
            named = named ?? throw new ArgumentNullException(nameof(named));

            var name = new NamedInternal(named).OrElseGenerateWithPrefix(builder, KStream.FilterName);
            var processorParameters = new ProcessorParameters<K, V>(new KStreamFilter<K, V>(predicate, true), name);
            var filterNotProcessorNode = new ProcessorGraphNode<K, V>(name, processorParameters);

            builder.AddGraphNode<K, V>(this.streamsGraphNode, filterNotProcessorNode);

            return new KStream<K, V>(
                this.clock,
                    name,
                    keySerde,
                    valSerde,
                    sourceNodes,
                    repartitionRequired,
                    filterNotProcessorNode,
                    builder);
        }

        public IKStream<KR, V> SelectKey<KR>(IKeyValueMapper<K, V, KR> mapper)
        {
            return SelectKey(mapper, NamedInternal.Empty());
        }

        public IKStream<KR, V> SelectKey<KR>(IKeyValueMapper<K, V, KR> mapper, Named named)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
            named = named ?? throw new ArgumentNullException(nameof(named));

            ProcessorGraphNode<K, V> selectKeyProcessorNode = InternalSelectKey(mapper, new NamedInternal(named));

            selectKeyProcessorNode.IsKeyChangingOperation = true;
            builder.AddGraphNode<K, V>(this.streamsGraphNode, selectKeyProcessorNode);

            // key serde cannot be preserved
            return new KStream<KR, V>(
                this.clock,
                selectKeyProcessorNode.NodeName,
                null,
                valSerde,
                sourceNodes,
                true,
                selectKeyProcessorNode,
                builder);
        }

        private ProcessorGraphNode<K, V> InternalSelectKey<KR>(
            IKeyValueMapper<K, V, KR> mapper,
            NamedInternal named)
        {
            var name = named.OrElseGenerateWithPrefix(builder, KStream.KEY_SELECT_NAME);
            var kStreamMap = new KStreamMap<K, V, KR, V>((key, value) =>
                new KeyValuePair<KR, V>(mapper.Apply(key, value), value));

            var processorParameters = new ProcessorParameters<K, V>(kStreamMap, name);

            return new ProcessorGraphNode<K, V>(name, processorParameters);
        }

        public IKStream<KR, VR> Map<KR, VR>(IKeyValueMapper<K, V, KeyValuePair<KR, VR>> mapper)
            => Map(mapper, NamedInternal.Empty());

        public IKStream<KR, VR> Map<KR, VR>(IKeyValueMapper<K, V, KeyValuePair<KR, VR>> mapper, Named named)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
            named = named ?? throw new ArgumentNullException(nameof(named));

            var name = new NamedInternal(named).OrElseGenerateWithPrefix(builder, KStream.MapName);
            var processorParameters = new ProcessorParameters<K, V>(new KStreamMap<K, V, KR, VR>(mapper), name);

            var mapProcessorNode = new ProcessorGraphNode<K, V>(name, processorParameters)
            {
                IsKeyChangingOperation = true
            };

            builder.AddGraphNode<K, V>(this.streamsGraphNode, mapProcessorNode);

            // key and value serde cannot be preserved
            return new KStream<KR, VR>(
                this.clock,
                    name,
                    null,
                    null,
                    sourceNodes,
                    true,
                    mapProcessorNode,
                    builder);
        }

        public IKStream<K, VR> MapValues<VR>(IValueMapper<V, VR> mapper)
            => MapValues(WithKey<VR>(mapper));

        public IKStream<K, VR> MapValues<VR>(IValueMapper<V, VR> mapper, Named named)
            => MapValues(WithKey(mapper), named);

        public IKStream<K, VR> MapValues<VR>(IValueMapperWithKey<K, V, VR> mapper)
            => MapValues(mapper, NamedInternal.Empty());

        public IKStream<K, VR> MapValues<VR>(IValueMapperWithKey<K, V, VR> mapper, Named named)
        {
            named = named ?? throw new ArgumentNullException(nameof(named));
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));

            var name = new NamedInternal(named)
                .OrElseGenerateWithPrefix(builder, KStream.MAPVALUES_NAME);

            var processorParameters = new ProcessorParameters<K, V>(new KStreamMapValues<K, V, VR>(mapper), name);
            var mapValuesProcessorNode = new ProcessorGraphNode<K, V>(name, processorParameters)
            {
                IsValueChangingOperation = true
            };

            builder.AddGraphNode<K, V>(this.streamsGraphNode, mapValuesProcessorNode);

            // value serde cannot be preserved
            return new KStream<K, VR>(
                this.clock,
                name,
                keySerde,
                valueSerde: null,
                sourceNodes,
                repartitionRequired,
                mapValuesProcessorNode,
                builder);
        }

        //public void print(Printed<K, V> printed)
        //{
        //    printed = printed ?? throw new ArgumentNullException(nameof(printed));
        //    PrintedInternal<K, V> printedInternal = new PrintedInternal<K, V>(printed);
        //    //            string name = new NamedInternal(printedInternal.name).OrElseGenerateWithPrefix(builder, KStream.PRINTING_NAME);
        //    ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(printedInternal.build(this.name), name);
        //    ProcessorGraphNode<K, V> printNode = new ProcessorGraphNode<K, V>(name, processorParameters);

        //    builder.AddGraphNode(this.streamsGraphNode, printNode);
        //}

        public IKStream<KR, VR> FlatMap<KR, VR>(IKeyValueMapper<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper)
            => FlatMap(mapper, NamedInternal.Empty());

        public IKStream<KR, VR> FlatMap<KR, VR>(
            IKeyValueMapper<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper,
            Named named)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
            named = named ?? throw new ArgumentNullException(nameof(named));

            var name = new NamedInternal(named).OrElseGenerateWithPrefix(builder, KStream.FlatmapName);

            var processorParameters = new ProcessorParameters<K, V>(new KStreamFlatMap<K, V, KR, VR>(mapper), name);
            var flatMapNode = new ProcessorGraphNode<K, V>(name, processorParameters)
            {
                IsKeyChangingOperation = true
            };

            builder.AddGraphNode<K, V>(this.streamsGraphNode, flatMapNode);

            // key and value serde cannot be preserved
            return new KStream<KR, VR>(
                this.clock,
                name,
                null,
                null,
                sourceNodes,
                true,
                flatMapNode,
                builder);
        }

        public IKStream<K, VR> FlatMapValues<VR>(IValueMapper<V, IEnumerable<VR>> mapper)
            => FlatMapValues(WithKey(mapper));

        public IKStream<K, VR> FlatMapValues<VR>(
            IValueMapper<V, IEnumerable<VR>> mapper, Named named)
            => FlatMapValues(WithKey(mapper), named);

        //public IKStream<K, VR> flatMapValues<VR>(Func<V, IEnumerable<VR>> mapper)
        //    => flatMapValues(withKey(mapper));

        public IKStream<K, VR> FlatMapValues<VR>(IValueMapperWithKey<K, V, IEnumerable<VR>> mapper)
            => FlatMapValues(mapper, NamedInternal.Empty());

        //public IKStream<K, VR> flatMapValues<VR>(Func<V, IEnumerable<VR>> mapper)
        //    where VR : IEnumerable<VR>
        //{
        //    var vm = new ValueMapper<V, IEnumerable<VR>>(mapper);

        //    return flatMapValues(vm);
        //}

        public IKStream<K, VR> FlatMapValues<VR>(
            IValueMapperWithKey<K, V, IEnumerable<VR>> mapper,
            Named named)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
            named = named ?? throw new ArgumentNullException(nameof(named));

            var name = new NamedInternal(named).OrElseGenerateWithPrefix(builder, KStream.FlatmapValuesName);

            var processorParameters = new ProcessorParameters<K, V>(new KStreamFlatMapValues<K, V, VR>(mapper), name);
            var flatMapValuesNode = new ProcessorGraphNode<K, V>(name, processorParameters)
            {
                IsValueChangingOperation = true
            };

            builder.AddGraphNode<K, V>(this.streamsGraphNode, flatMapValuesNode);

            // value serde cannot be preserved
            return new KStream<K, VR>(
                this.clock,
                name,
                keySerde,
                null,
                sourceNodes,
                this.repartitionRequired,
                flatMapValuesNode,
                builder);
        }

        public IKStream<K, V>[] Branch(Func<K, V, bool>[] predicates)
        {
            if (predicates is null)
            {
                throw new ArgumentNullException(nameof(predicates));
            }

            return DoBranch(NamedInternal.Empty(), predicates);
        }

        public IKStream<K, V>[] Branch(Named name, Func<K, V, bool>[] predicates)
        {
            if (predicates is null)
            {
                throw new ArgumentNullException(nameof(predicates));
            }

            name = name ?? throw new ArgumentNullException(nameof(name));

            return DoBranch(new NamedInternal(name), predicates);
        }

        private IKStream<K, V>[] DoBranch(
            NamedInternal named,
            Func<K, V, bool>[] predicates)
        {
            if (predicates.Length == 0)
            {
                throw new ArgumentException("you must provide at least one predicate");
            }

            foreach (Func<K, V, bool> predicate in predicates)
            {
                if (predicate == null)
                {
                    throw new ArgumentNullException(nameof(predicate));
                }
            }

            var branchName = named.OrElseGenerateWithPrefix(builder, KStream.BranchName);

            var childNames = new string[predicates.Length];
            for (var i = 0; i < predicates.Length; i++)
            {
                childNames[i] = named.SuffixWithOrElseGet("-predicate-" + i, builder, KStream.BranchChildName);
            }

            var processorParameters = new ProcessorParameters<K, V>(new KStreamBranch<K, V>((Func<K, V, bool>[])predicates.Clone(), childNames), branchName);
            var branchNode = new ProcessorGraphNode<K, V>(branchName, processorParameters);
            builder.AddGraphNode<K, V>(this.streamsGraphNode, branchNode);

            var branchChildren = new IKStream<K, V>[predicates.Length];

            for (var i = 0; i < predicates.Length; i++)
            {
                var innerProcessorParameters = new ProcessorParameters<K, V>(new KStreamPassThrough<K, V>(), childNames[i]);
                var branchChildNode = new ProcessorGraphNode<K, V>(childNames[i], innerProcessorParameters);

                builder.AddGraphNode<K, V>(branchNode, branchChildNode);

                branchChildren[i] = new KStream<K, V>(
                    this.clock,
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

        public IKStream<K, V> Merge(IKStream<K, V> stream)
        {
            stream = stream ?? throw new ArgumentNullException(nameof(stream));

            return Merge(builder, stream, NamedInternal.Empty());
        }

        public IKStream<K, V> Merge(IKStream<K, V> stream, Named processorName)
        {
            stream = stream ?? throw new ArgumentNullException(nameof(stream));

            return Merge(builder, stream, new NamedInternal(processorName));
        }

        private IKStream<K, V> Merge(
            InternalStreamsBuilder builder,
            IKStream<K, V> stream,
            NamedInternal processorName)
        {
            var streamImpl = (KStream<K, V>)stream;
            var name = processorName.OrElseGenerateWithPrefix(builder, KStream.MERGE_NAME);
            var allSourceNodes = new HashSet<string>();

            var requireRepartitioning = streamImpl.repartitionRequired || repartitionRequired;
            allSourceNodes.AddRange(sourceNodes);
            allSourceNodes.AddRange(streamImpl.sourceNodes);

            var processorParameters = new ProcessorParameters<K, V>(new KStreamPassThrough<K, V>(), name);

            var mergeNode = new ProcessorGraphNode<K, V>(name, processorParameters);
            mergeNode.SetMergeNode(true);

            var parents = new HashSet<StreamsGraphNode> { this.streamsGraphNode, streamImpl.streamsGraphNode };
            builder.AddGraphNode<K, V>(parents, mergeNode);

            // drop the serde as we cannot safely use either one to represent both streams
            return new KStream<K, V>(
                this.clock,
                name,
                null,
                null,
                allSourceNodes,
                requireRepartitioning,
                mergeNode,
                builder);
        }

        public void ForEach(Action<K, V> action)
            => ForEach(action, NamedInternal.Empty());

        public void ForEach(Action<K, V> action, Named named)
        {
            action = action ?? throw new ArgumentNullException(nameof(action));

            var name = new NamedInternal(named).OrElseGenerateWithPrefix(builder, KStream.ForEachName);
            var processorParameters = new ProcessorParameters<K, V>(
                   new KStreamPeek<K, V>(action, false),
                   name);

            var foreachNode = new ProcessorGraphNode<K, V>(name, processorParameters);
            builder.AddGraphNode<K, V>(this.streamsGraphNode, foreachNode);
        }

        public IKStream<K, V> Peek(Action<K, V> action)
            => Peek(action, NamedInternal.Empty());

        public IKStream<K, V> Peek(Action<K, V> action, Named named)
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

        public IKStream<K, V> Through(string topic)
        {
            return Through(topic, Produced.With(keySerde, valSerde, null));
        }


        public IKStream<K, V> Through(string topic, Produced<K, V> produced)
        {
            topic = topic ?? throw new ArgumentNullException(nameof(topic));
            produced = produced ?? throw new ArgumentNullException(nameof(produced));

            var producedInternal = new ProducedInternal<K, V>(produced);

            if (producedInternal.KeySerde == null)
            {
                producedInternal.WithKeySerde(keySerde);
            }

            if (producedInternal.ValueSerde == null)
            {
                producedInternal.WithValueSerde(valSerde);
            }

            To(topic, producedInternal);

            return builder.Stream(
                new List<string> { topic },
                new ConsumedInternal<K, V>(
                    producedInternal.KeySerde,
                    producedInternal.ValueSerde,
                    new FailOnInvalidTimestamp(null),
                    offsetReset: null));
        }

        public void To(string topic)
            => To(topic, Produced.With(keySerde, valSerde, null));


        public void To(string topic, Produced<K, V> produced)
        {
            topic = topic ?? throw new ArgumentNullException(nameof(topic));
            produced = produced ?? throw new ArgumentNullException(nameof(produced));

            var producedInternal = new ProducedInternal<K, V>(produced);

            if (producedInternal.KeySerde == null)
            {
                producedInternal.WithKeySerde(keySerde);
            }

            if (producedInternal.ValueSerde == null)
            {
                producedInternal.WithValueSerde(valSerde);
            }

            To(new StaticTopicNameExtractor(topic), producedInternal);
        }

        public void To(ITopicNameExtractor topicExtractor)
            => To(topicExtractor, Produced.With(keySerde, valSerde, null));

        public void To(ITopicNameExtractor topicExtractor, Produced<K, V> produced)
        {
            topicExtractor = topicExtractor ?? throw new ArgumentNullException(nameof(topicExtractor));
            produced = produced ?? throw new ArgumentNullException(nameof(produced));

            var producedInternal = new ProducedInternal<K, V>(produced);

            if (producedInternal.KeySerde == null)
            {
                producedInternal.WithKeySerde(keySerde);
            }

            if (producedInternal.ValueSerde == null)
            {
                producedInternal.WithValueSerde(valSerde);
            }

            To(topicExtractor, producedInternal);
        }

        private void To(ITopicNameExtractor topicExtractor, ProducedInternal<K, V> produced)
        {
            var name = new NamedInternal(produced.name).OrElseGenerateWithPrefix(builder, KStream.SinkName);
            var sinkNode = new StreamSinkNode<K, V>(
               name,
               topicExtractor,
               produced);

            builder.AddGraphNode<K, V>(this.streamsGraphNode, sinkNode);
        }

        public IKStream<KR, VR> Transform<KR, VR>(
            ITransformerSupplier<K, V, KeyValuePair<KR, VR>> transformerSupplier,
            string[] stateStoreNames)
        {
            transformerSupplier = transformerSupplier ?? throw new ArgumentNullException(nameof(transformerSupplier));
            var name = builder.NewProcessorName(KStream.TransformName);

            return FlatTransform(new TransformerSupplierAdapter<K, V, KR, VR>(transformerSupplier), Named.As(name), stateStoreNames);
        }

        public IKStream<KR, VR> Transform<KR, VR>(
            ITransformerSupplier<K, V, KeyValuePair<KR, VR>> transformerSupplier,
            Named named,
            string[] stateStoreNames)
        {
            transformerSupplier = transformerSupplier ?? throw new ArgumentNullException(nameof(transformerSupplier));

            return FlatTransform(new TransformerSupplierAdapter<K, V, KR, VR>(transformerSupplier), named, stateStoreNames);
        }

        public IKStream<K1, V1> FlatTransform<K1, V1>(
            ITransformerSupplier<K, V, IEnumerable<KeyValuePair<K1, V1>>> transformerSupplier,
            string[] stateStoreNames)
        {
            transformerSupplier = transformerSupplier ?? throw new ArgumentNullException(nameof(transformerSupplier));

            var name = builder.NewProcessorName(KStream.TransformName);

            return FlatTransform(transformerSupplier, Named.As(name), stateStoreNames);
        }

        public IKStream<K1, V1> FlatTransform<K1, V1>(
            ITransformerSupplier<K, V, IEnumerable<KeyValuePair<K1, V1>>> transformerSupplier,
            Named named,
            string[] stateStoreNames)
        {
            transformerSupplier = transformerSupplier ?? throw new ArgumentNullException(nameof(transformerSupplier));
            named = named ?? throw new ArgumentNullException(nameof(named));

            var name = new NamedInternal(named).Name;

            var transformNode = new StatefulProcessorNode<K, V>(
                   name,
                   new ProcessorParameters<K, V>(new KStreamFlatTransform<K, V, K1, V1>(transformerSupplier), name),
                   stateStoreNames)
            {
                IsKeyChangingOperation = true
            };

            builder.AddGraphNode<K, V>(streamsGraphNode, transformNode);

            // cannot inherit key and value serde
            return new KStream<K1, V1>(
                this.clock,
                name,
                null,
                null,
                sourceNodes,
                true,
                transformNode,
                builder);
        }

        public IKStream<K, VR> TransformValues<VR>(IValueTransformerSupplier<V, VR> valueTransformerSupplier,
                                                    string[] stateStoreNames)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new ArgumentNullException(nameof(valueTransformerSupplier));
            return DoTransformValues(ToValueTransformerWithKeySupplier(valueTransformerSupplier), NamedInternal.Empty(), stateStoreNames);
        }

        public IKStream<K, VR> TransformValues<VR>(IValueTransformerSupplier<V, VR> valueTransformerSupplier,
                                                    Named named,
                                                    string[] stateStoreNames)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new ArgumentNullException(nameof(valueTransformerSupplier));
            named = named ?? throw new ArgumentNullException(nameof(named));
            return DoTransformValues(ToValueTransformerWithKeySupplier(valueTransformerSupplier),
                    new NamedInternal(named), stateStoreNames);
        }

        public IKStream<K, VR> TransformValues<VR>(IValueTransformerWithKeySupplier<K, V, VR> valueTransformerSupplier,
                                                    string[] stateStoreNames)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new ArgumentNullException(nameof(valueTransformerSupplier));
            return DoTransformValues(valueTransformerSupplier, NamedInternal.Empty(), stateStoreNames);
        }

        public IKStream<K, VR> TransformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, VR> valueTransformerSupplier,
            Named named,
            string[] stateStoreNames)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new ArgumentNullException(nameof(valueTransformerSupplier));
            named = named ?? throw new ArgumentNullException(nameof(named));
            return DoTransformValues(valueTransformerSupplier, new NamedInternal(named), stateStoreNames);
        }

        private IKStream<K, VR> DoTransformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, VR> valueTransformerWithKeySupplier,
            NamedInternal named,
            string[] stateStoreNames)
        {
            var name = named.OrElseGenerateWithPrefix(builder, KStream.TransformValuesName);

            var transformNode = new StatefulProcessorNode<K, V>(
               name,
               new ProcessorParameters<K, V>(new KStreamTransformValues<K, V, VR>(valueTransformerWithKeySupplier), name),
               stateStoreNames)
            {
                IsValueChangingOperation = true
            };

            builder.AddGraphNode<K, V>(this.streamsGraphNode, transformNode);

            // cannot inherit value serde
            return new KStream<K, VR>(
                this.clock,
                name,
                keySerde,
                null,
                sourceNodes,
                repartitionRequired,
                transformNode,
                builder);
        }

        public IKStream<K, VR> FlatTransformValues<VR>(
            IValueTransformerSupplier<V, IEnumerable<VR>> valueTransformerSupplier,
            string[] stateStoreNames)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new ArgumentNullException(nameof(valueTransformerSupplier));

            return DoFlatTransformValues(ToValueTransformerWithKeySupplier(valueTransformerSupplier), NamedInternal.Empty(), stateStoreNames);
        }

        public IKStream<K, VR> FlatTransformValues<VR>(
            IValueTransformerSupplier<V, IEnumerable<VR>> valueTransformerSupplier,
            Named named,
            string[] stateStoreNames)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new ArgumentNullException(nameof(valueTransformerSupplier));

            return DoFlatTransformValues(ToValueTransformerWithKeySupplier(valueTransformerSupplier), named, stateStoreNames);
        }

        public IKStream<K, VR> FlatTransformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, IEnumerable<VR>> valueTransformerSupplier,
            string[] stateStoreNames)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new ArgumentNullException(nameof(valueTransformerSupplier));

            return DoFlatTransformValues(valueTransformerSupplier, NamedInternal.Empty(), stateStoreNames);
        }

        public IKStream<K, VR> FlatTransformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, IEnumerable<VR>> valueTransformerSupplier,
            Named named,
            string[] stateStoreNames)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new ArgumentNullException(nameof(valueTransformerSupplier));

            return DoFlatTransformValues(valueTransformerSupplier, named, stateStoreNames);
        }

        private IKStream<K, VR> DoFlatTransformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, IEnumerable<VR>> valueTransformerWithKeySupplier,
            Named named,
            string[] stateStoreNames)
        {
            var name = new NamedInternal(named).OrElseGenerateWithPrefix(builder, KStream.TransformValuesName);

            var transformNode = new StatefulProcessorNode<K, V>(
               name,
               new ProcessorParameters<K, V>(new KStreamFlatTransformValues<K, V, VR>(valueTransformerWithKeySupplier), name),
               stateStoreNames)
            {
                IsValueChangingOperation = true
            };

            builder.AddGraphNode<K, V>(this.streamsGraphNode, transformNode);

            // cannot inherit value serde
            return new KStream<K, VR>(
                this.clock,
                name,
                keySerde,
                null,
                sourceNodes,
                repartitionRequired,
                transformNode,
                builder);
        }

        public void Process(
            IProcessorSupplier<K, V> IProcessorSupplier,
            string[] stateStoreNames)
        {
            IProcessorSupplier = IProcessorSupplier ?? throw new ArgumentNullException(nameof(IProcessorSupplier));

            var name = builder.NewProcessorName(KStream.ProcessorName);

            Process(IProcessorSupplier, Named.As(name), stateStoreNames);
        }

        public void Process(Func<IKeyValueProcessor<K, V>> processor)
        {
            throw new NotImplementedException();
        }

        public void Process(
            IProcessorSupplier<K, V> IProcessorSupplier,
            Named named,
            string[] stateStoreNames)
        {
            IProcessorSupplier = IProcessorSupplier ?? throw new ArgumentNullException(nameof(IProcessorSupplier));
            named = named ?? throw new ArgumentNullException(nameof(named));

            var name = new NamedInternal(named).Name;

            var processNode = new StatefulProcessorNode<K, V>(
                   name,
                   new ProcessorParameters<K, V>(IProcessorSupplier, name),
                   stateStoreNames);

            builder.AddGraphNode<K, V>(this.streamsGraphNode, processNode);
        }

        public IKStream<K, VR> Join<VO, VR>(
            IKStream<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            JoinWindows windows)
        {
            return Join(
                other,
                joiner,
                windows,
                Joined.With<K, V, VO>(null, null, null));
        }

        public IKStream<K, VR> Join<VO, VR>(
            IKStream<K, VO> otherStream,
            IValueJoiner<V, VO, VR> joiner,
            JoinWindows windows,
            Joined<K, V, VO> joined)
        {
            var kStreamJoin = new KStreamJoin(
                this.clock,
                this.builder,
                leftOuter: false,
                rightOuter: false);

            return DoJoin(otherStream,
                          joiner,
                          windows,
                          joined,
                          kStreamJoin);
        }

        public IKStream<K, VR> OuterJoin<VO, VR>(
            IKStream<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            JoinWindows windows)
        {
            return OuterJoin(
                other,
                joiner,
                windows,
                Joined.With<K, V, VO>(null, null, null));
        }

        public IKStream<K, VR> OuterJoin<VO, VR>(
            IKStream<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            JoinWindows windows,
            Joined<K, V, VO> joined)
        {
            return DoJoin(
                other,
                joiner,
                windows,
                joined,
                new KStreamJoin(this.clock, this.builder, true, true));
        }

        private IKStream<K, VR> DoJoin<VO, VR>(
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
            var joinOther = (KStream<K, VO>)other;

            var joinedInternal = new JoinedInternal<K, V, VO>(joined);
            var name = new NamedInternal(joinedInternal.Name);

            if (joinThis.repartitionRequired)
            {
                var joinThisName = joinThis.name;
                var leftJoinRepartitionTopicName = name.SuffixWithOrElseGet("-left", joinThisName);
                joinThis = joinThis.RepartitionForJoin(leftJoinRepartitionTopicName, joined.KeySerde, joined.ValueSerde);
            }

            if (joinOther.repartitionRequired)
            {
                var joinOtherName = joinOther.name;
                var rightJoinRepartitionTopicName = name.SuffixWithOrElseGet("-right", joinOtherName);
                joinOther = joinOther.RepartitionForJoin(rightJoinRepartitionTopicName, joined.KeySerde, joined.OtherValueSerde);
            }

            joinThis.EnsureJoinableWith(joinOther);

            return join.Join(
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
        private KStream<K, V> RepartitionForJoin(
            string repartitionName,
            ISerde<K>? keySerdeOverride,
            ISerde<V>? valueSerdeOverride)
        {
            var repartitionKeySerde = keySerdeOverride ?? keySerde;
            var repartitionValueSerde = valueSerdeOverride ?? valSerde;

            var optimizableRepartitionNodeBuilder =
               OptimizableRepartitionNode.GetOptimizableRepartitionNodeBuilder<K, V>();

            var repartitionedSourceName = CreateRepartitionedSource(
                builder,
                repartitionKeySerde,
                repartitionValueSerde,
                repartitionName,
                optimizableRepartitionNodeBuilder);

            var optimizableRepartitionNode = optimizableRepartitionNodeBuilder.Build();

            builder.AddGraphNode<K, V>(this.streamsGraphNode, optimizableRepartitionNode);

            return new KStream<K, V>(
                this.clock,
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
            ISerde<K1>? keySerde,
            ISerde<V1>? valSerde,
            string repartitionTopicNamePrefix,
            OptimizableRepartitionNodeBuilder<K1, V1> optimizableRepartitionNodeBuilder)
        {
            if (builder is null)
            {
                throw new ArgumentNullException(nameof(builder));
            }

            if (optimizableRepartitionNodeBuilder is null)
            {
                throw new ArgumentNullException(nameof(optimizableRepartitionNodeBuilder));
            }

            var repartitionTopic = repartitionTopicNamePrefix + KStream.RepartitionTopicSuffix;
            var sinkName = builder.NewProcessorName(KStream.SinkName);
            var nullKeyFilterProcessorName = builder.NewProcessorName(KStream.FilterName);
            var sourceName = builder.NewProcessorName(KStream.SourceName);

            var processorParameters = new ProcessorParameters<K1, V1>(
               new KStreamFilter<K1, V1>((k, v) => k != null, false),
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

        public IKStream<K, VR> LeftJoin<VO, VR>(
            IKStream<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            JoinWindows windows)
            => LeftJoin(
                other,
                joiner,
                windows,
                Joined.With<K, V, VO>(null, null, null));

        public IKStream<K, VR> LeftJoin<VO, VR>(
            IKStream<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            JoinWindows windows,
            Joined<K, V, VO> joined)
        {
            joined = joined ?? throw new ArgumentNullException(nameof(joined));

            return DoJoin(
                other,
                joiner,
                windows,
                joined,
                new KStreamJoin(this.clock, this.builder, true, false));
        }

        public IKStream<K, VR> Join<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner)
            => Join(
                other,
                joiner,
                Joined.With<K, V, VO>(null, null, null));

        public IKStream<K, VR> Join<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Joined<K, V, VO> joined)
        {
            other = other ?? throw new ArgumentNullException(nameof(other));
            joiner = joiner ?? throw new ArgumentNullException(nameof(joiner));
            joined = joined ?? throw new ArgumentNullException(nameof(joined));

            var joinedInternal = new JoinedInternal<K, V, VO>(joined);
            var name = joinedInternal.Name;
            if (repartitionRequired)
            {
                IKStream<K, V> thisStreamRepartitioned = RepartitionForJoin(
                   name ?? this.name,
                   joined.KeySerde,
                   joined.ValueSerde);

                return thisStreamRepartitioned.DoStreamTableJoin(other, joiner, joined, false);
            }
            else
            {
                return DoStreamTableJoin(other, joiner, joined, false);
            }
        }

        public IKStream<K, VR> LeftJoin<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner)
            => LeftJoin(
                other,
                joiner,
                Joined.With<K, V, VO>(null, null, null));

        public IKStream<K, VR> LeftJoin<VO, VR>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Joined<K, V, VO> joined)
        {
            other = other ?? throw new ArgumentNullException(nameof(other));
            joiner = joiner ?? throw new ArgumentNullException(nameof(joiner));
            joined = joined ?? throw new ArgumentNullException(nameof(joined));

            var joinedInternal = new JoinedInternal<K, V, VO>(joined);
            var internalName = joinedInternal.Name;

            if (repartitionRequired)
            {
                IKStream<K, V> thisStreamRepartitioned = RepartitionForJoin(
                   internalName ?? name,
                   joined.KeySerde,
                   joined.ValueSerde);

                return thisStreamRepartitioned.DoStreamTableJoin(other, joiner, joined, true);
            }
            else
            {
                return DoStreamTableJoin(other, joiner, joined, true);
            }
        }

        public IKStream<K, VR> Join<KG, VG, VR>(
            IGlobalKTable<KG, VG> globalTable,
            IKeyValueMapper<K, V, KG> keyMapper,
            IValueJoiner<V, VG, VR> joiner)
            => GlobalTableJoin(
                globalTable,
                keyMapper,
                joiner,
                false,
                NamedInternal.Empty());

        public IKStream<K, VR> Join<KG, VG, VR>(
            IGlobalKTable<KG, VG> globalTable,
            IKeyValueMapper<K, V, KG> keyMapper,
            IValueJoiner<V, VG, VR> joiner,
            Named named)
            => GlobalTableJoin(
                globalTable,
                keyMapper,
                joiner,
                false,
                named);

        public IKStream<K, VR> LeftJoin<KG, VG, VR>(
            IGlobalKTable<KG, VG> globalTable,
            IKeyValueMapper<K, V, KG> keyMapper,
            IValueJoiner<V, VG, VR> joiner)
            => GlobalTableJoin(
                globalTable,
                keyMapper,
                joiner,
                leftJoin: true,
                NamedInternal.Empty());

        public IKStream<K, VR> LeftJoin<KG, VG, VR>(
            IGlobalKTable<KG, VG> globalTable,
            IKeyValueMapper<K, V, KG> keyMapper,
            IValueJoiner<V, VG, VR> joiner,
            Named named)
            => GlobalTableJoin(
                globalTable,
                keyMapper,
                joiner,
                true,
                named);

        private IKStream<K, VR> GlobalTableJoin<KG, VG, VR>(
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
            var name = new NamedInternal(named).OrElseGenerateWithPrefix(builder, KStream.LEFTJOIN_NAME);

            IProcessorSupplier<K, V> IProcessorSupplier = new KStreamGlobalKTableJoin<K, KG, VR, V, VG>(
               valueGetterSupplier,
               joiner,
               keyMapper,
               leftJoin);

            var processorParameters = new ProcessorParameters<K, V>(IProcessorSupplier, name);

            var streamTableJoinNode = new StreamTableJoinNode<K, V>(
                name,
                processorParameters,
                Array.Empty<string>(),
                null);

            builder.AddGraphNode<K, V>(this.streamsGraphNode, streamTableJoinNode);

            // do not have serde for joined result
            return new KStream<K, VR>(
                this.clock,
                name,
                keySerde,
                null,
                sourceNodes,
                repartitionRequired,
                streamTableJoinNode,
                builder);
        }

        public IKStream<K, VR> DoStreamTableJoin<VR, VO>(
            IKTable<K, VO> other,
            IValueJoiner<V, VO, VR> joiner,
            Joined<K, V, VO> joined,
            bool leftJoin)
        {
            if (joined is null)
            {
                throw new ArgumentNullException(nameof(joined));
            }

            other = other ?? throw new ArgumentNullException(nameof(other));
            joiner = joiner ?? throw new ArgumentNullException(nameof(joiner));

            HashSet<string> allSourceNodes = EnsureJoinableWith((AbstractStream<K, VO>)other);

            var joinedInternal = new JoinedInternal<K, V, VO>(joined);
            var renamed = new NamedInternal(joinedInternal.Name);

            var name = renamed.OrElseGenerateWithPrefix(builder, leftJoin ? KStream.LEFTJOIN_NAME : KStream.JOIN_NAME);
            var processorSupplier = new KStreamKTableJoin<K, VR, V, VO>(
               other.ValueGetterSupplier<VO>(),
               joiner,
               leftJoin);

            var processorParameters = new ProcessorParameters<K, V>(processorSupplier, name);
            var streamTableJoinNode = new StreamTableJoinNode<K, V>(
               name,
               processorParameters,
               other.ValueGetterSupplier<VR>().StoreNames(),
               this.name);

            builder.AddGraphNode<K, V>(this.streamsGraphNode, streamTableJoinNode);

            // do not have serde for joined result
            return new KStream<K, VR>(
                this.clock,
                name,
                joined.KeySerde ?? keySerde,
                null,
                allSourceNodes,
                repartitionRequired: false,
                streamTableJoinNode,
                builder);
        }

        public IKGroupedStream<KR, V> GroupBy<KR>(IKeyValueMapper<K, V, KR> selector)
            => GroupBy(selector, Grouped.With<KR, V>(null, valSerde));

        public IKGroupedStream<KR, V> GroupBy<KR>(Func<K, V, KR> selector)
        {
            var kvm = new KeyValueMapper<K, V, KR>(selector);

            return GroupBy(kvm);
        }

        public IKGroupedStream<KR, V> GroupBy<KR>(
            IKeyValueMapper<K, V, KR> selector,
            Grouped<KR, V> grouped)
        {
            selector = selector ?? throw new ArgumentNullException(nameof(selector));
            grouped = grouped ?? throw new ArgumentNullException(nameof(grouped));

            var groupedInternal = new GroupedInternal<KR, V>(grouped);
            ProcessorGraphNode<K, V> selectKeyMapNode = InternalSelectKey(selector, new NamedInternal(groupedInternal.Name));
            selectKeyMapNode.IsKeyChangingOperation = true;

            builder.AddGraphNode<K, V>(this.streamsGraphNode, selectKeyMapNode);

            return new KGroupedStream<KR, V>(
                this.clock,
                selectKeyMapNode.NodeName,
                sourceNodes,
                groupedInternal,
                repartitionRequired: true,
                selectKeyMapNode,
                builder);
        }

        public IKGroupedStream<K, V> GroupByKey(ISerialized<K, V> serialized)
        {
            var serializedInternal = new SerializedInternal<K, V>(serialized);

            return GroupByKey(Grouped.With(
                serializedInternal.keySerde,
                serializedInternal.valueSerde));
        }

        public IKGroupedStream<K, V> GroupByKey()
        {
            return GroupByKey(Grouped.With(keySerde, valSerde));
        }

        public IKGroupedStream<K, V> GroupByKey(Grouped<K, V> grouped)
        {
            var groupedInternal = new GroupedInternal<K, V>(grouped);
            return new KGroupedStream<K, V>(
                this.clock,
                name,
                sourceNodes,
                groupedInternal,
                repartitionRequired,
                streamsGraphNode,
                builder);
        }

        public IKStream<K, VR> MapValues<VR>(Func<V, VR> mapper)
        {
            throw new NotImplementedException();
        }
    }
}
