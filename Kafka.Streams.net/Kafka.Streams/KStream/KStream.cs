using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.Windowed;
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
                // reusing the source Name for the graph node Name
                // adding explicit variable as it simplifies logic
                .WithNodeName(sourceName);

            return sourceName;
        }

        public static IStoreBuilder<IWindowStore<K, V>> JoinWindowStoreBuilder<K, V>(
            KafkaStreamsContext context,
            string joinName,
            JoinWindows windows,
            ISerde<K> keySerde,
            ISerde<V> valueSerde)
        {
            if (windows is null)
            {
                throw new ArgumentNullException(nameof(windows));
            }

            return context.StoresFactory.WindowStoreBuilder(
                context,
                context.StoresFactory.PersistentWindowStore(
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
        private readonly KafkaStreamsContext context;
        private readonly bool repartitionRequired;

        public KStream(
            KafkaStreamsContext context,
            string Name,
            ISerde<K>? keySerde,
            ISerde<V>? valueSerde,
            HashSet<string> sourceNodes,
            bool repartitionRequired,
            StreamsGraphNode streamsGraphNode,
            InternalStreamsBuilder builder)
            : base(
                  Name,
                  keySerde,
                  valueSerde,
                  sourceNodes,
                  streamsGraphNode,
                  builder)
        {
            this.context = context;
            this.repartitionRequired = repartitionRequired;
        }

        public IKStream<K, V> Filter(FilterPredicate<K, V> predicate)
        {
            return this.Filter(predicate, NamedInternal.Empty());
        }

        public IKStream<K, V> Filter(FilterPredicate<K, V> predicate, Named named)
        {
            predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));
            named = named ?? throw new ArgumentNullException(nameof(named));

            var Name = new NamedInternal(named).OrElseGenerateWithPrefix(this.Builder, KStream.FilterName);
            var processorParameters = new ProcessorParameters<K, V>(new KStreamFilter<K, V>(predicate, false), Name);
            var filterProcessorNode = new ProcessorGraphNode<K, V>(Name, processorParameters);

            this.Builder.AddGraphNode<K, V>(this.StreamsGraphNode, filterProcessorNode);

            return new KStream<K, V>(
                this.context,
                Name,
                this.KeySerde,
                this.ValueSerde,
                this.SourceNodes,
                this.repartitionRequired,
                filterProcessorNode,
                this.Builder);
        }

        public IKStream<K, V> FilterNot(FilterPredicate<K, V> predicate)
        {
            return this.FilterNot(predicate, NamedInternal.Empty());
        }

        public IKStream<K, V> FilterNot(FilterPredicate<K, V> predicate, Named named)
        {
            predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));
            named = named ?? throw new ArgumentNullException(nameof(named));

            var Name = new NamedInternal(named).OrElseGenerateWithPrefix(this.Builder, KStream.FilterName);
            var processorParameters = new ProcessorParameters<K, V>(new KStreamFilter<K, V>(predicate, true), Name);
            var filterNotProcessorNode = new ProcessorGraphNode<K, V>(Name, processorParameters);

            this.Builder.AddGraphNode<K, V>(this.StreamsGraphNode, filterNotProcessorNode);

            return new KStream<K, V>(
                this.context,
                    Name,
                    this.KeySerde,
                    this.ValueSerde,
                    this.SourceNodes,
                    this.repartitionRequired,
                    filterNotProcessorNode,
                    this.Builder);
        }

        public IKStream<KR, V> SelectKey<KR>(KeyValueMapper<K, V, KR> mapper)
        {
            return this.SelectKey(mapper, NamedInternal.Empty());
        }

        public IKStream<KR, V> SelectKey<KR>(KeyValueMapper<K, V, KR> mapper, Named named)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
            named = named ?? throw new ArgumentNullException(nameof(named));

            ProcessorGraphNode<K, V> selectKeyProcessorNode = this.InternalSelectKey(mapper, new NamedInternal(named));

            selectKeyProcessorNode.IsKeyChangingOperation = true;
            this.Builder.AddGraphNode<K, V>(this.StreamsGraphNode, selectKeyProcessorNode);

            // key serde cannot be preserved
            return new KStream<KR, V>(
                this.context,
                selectKeyProcessorNode.NodeName,
                null,
                this.ValueSerde,
                this.SourceNodes,
                true,
                selectKeyProcessorNode,
                this.Builder);
        }

        private ProcessorGraphNode<K, V> InternalSelectKey<KR>(
            KeyValueMapper<K, V, KR> mapper,
            NamedInternal named)
        {
            var Name = named.OrElseGenerateWithPrefix(this.Builder, KStream.KEY_SELECT_NAME);
            var kStreamMap = new KStreamMap<K, V, KR, V>((key, value) =>
                new KeyValuePair<KR, V>(mapper(key, value), value));

            var processorParameters = new ProcessorParameters<K, V>(kStreamMap, Name);

            return new ProcessorGraphNode<K, V>(Name, processorParameters);
        }

        public IKStream<KR, VR> Map<KR, VR>(KeyValueMapper<K, V, KeyValuePair<KR, VR>> mapper)
            => this.Map(mapper, NamedInternal.Empty());

        public IKStream<KR, VR> Map<KR, VR>(KeyValueMapper<K, V, KeyValuePair<KR, VR>> mapper, Named named)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
            named = named ?? throw new ArgumentNullException(nameof(named));

            var Name = new NamedInternal(named).OrElseGenerateWithPrefix(this.Builder, KStream.MapName);
            var processorParameters = new ProcessorParameters<K, V>(new KStreamMap<K, V, KR, VR>(mapper), Name);

            var mapProcessorNode = new ProcessorGraphNode<K, V>(Name, processorParameters)
            {
                IsKeyChangingOperation = true
            };

            this.Builder.AddGraphNode<K, V>(this.StreamsGraphNode, mapProcessorNode);

            // key and value serde cannot be preserved
            return new KStream<KR, VR>(
                this.context,
                    Name,
                    null,
                    null,
                    this.SourceNodes,
                    true,
                    mapProcessorNode,
                    this.Builder);
        }

        public IKStream<K, VR> MapValues<VR>(ValueMapper<V, VR> mapper)
            => this.MapValues(WithKey(mapper));

        public IKStream<K, VR> MapValues<VR>(ValueMapper<V, VR> mapper, Named named)
            => this.MapValues(WithKey(mapper), named);

        public IKStream<K, VR> MapValues<VR>(ValueMapperWithKey<K, V, VR> mapper)
            => this.MapValues(mapper, NamedInternal.Empty());

        public IKStream<K, VR> MapValues<VR>(ValueMapperWithKey<K, V, VR> mapper, Named named)
        {
            named = named ?? throw new ArgumentNullException(nameof(named));
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));

            var Name = new NamedInternal(named)
                .OrElseGenerateWithPrefix(this.Builder, KStream.MAPVALUES_NAME);

            var processorParameters = new ProcessorParameters<K, V>(new KStreamMapValues<K, V, VR>(mapper), Name);
            var mapValuesProcessorNode = new ProcessorGraphNode<K, V>(Name, processorParameters)
            {
                IsValueChangingOperation = true
            };

            this.Builder.AddGraphNode<K, V>(this.StreamsGraphNode, mapValuesProcessorNode);

            // value serde cannot be preserved
            return new KStream<K, VR>(
                this.context,
                Name,
                this.KeySerde,
                valueSerde: null,
                this.SourceNodes,
                this.repartitionRequired,
                mapValuesProcessorNode,
                this.Builder);
        }

        //public void print(Printed<K, V> printed)
        //{
        //    printed = printed ?? throw new ArgumentNullException(nameof(printed));
        //    PrintedInternal<K, V> printedInternal = new PrintedInternal<K, V>(printed);
        //    //            string Name = new NamedInternal(printedInternal.Name).OrElseGenerateWithPrefix(builder, KStream.PRINTING_NAME);
        //    ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(printedInternal.build(this.Name), Name);
        //    ProcessorGraphNode<K, V> printNode = new ProcessorGraphNode<K, V>(Name, processorParameters);

        //    builder.AddGraphNode(this.streamsGraphNode, printNode);
        //}

        public IKStream<KR, VR> FlatMap<KR, VR>(KeyValueMapper<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper)
            => this.FlatMap(mapper, NamedInternal.Empty());

        public IKStream<KR, VR> FlatMap<KR, VR>(
            KeyValueMapper<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper,
            Named named)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
            named = named ?? throw new ArgumentNullException(nameof(named));

            var Name = new NamedInternal(named).OrElseGenerateWithPrefix(this.Builder, KStream.FlatmapName);

            var processorParameters = new ProcessorParameters<K, V>(new KStreamFlatMap<K, V, KR, VR>(mapper), Name);
            var flatMapNode = new ProcessorGraphNode<K, V>(Name, processorParameters)
            {
                IsKeyChangingOperation = true
            };

            this.Builder.AddGraphNode<K, V>(this.StreamsGraphNode, flatMapNode);

            // key and value serde cannot be preserved
            return new KStream<KR, VR>(
                this.context,
                Name,
                null,
                null,
                this.SourceNodes,
                true,
                flatMapNode,
                this.Builder);
        }

        public IKStream<K, VR> FlatMapValues<VR>(ValueMapper<V, IEnumerable<VR>> mapper)
            => this.FlatMapValues(WithKey(mapper));

        public IKStream<K, VR> FlatMapValues<VR>(
            ValueMapper<V, IEnumerable<VR>> mapper, Named named)
            => this.FlatMapValues(WithKey(mapper), named);

        //public IKStream<K, VR> flatMapValues<VR>(Func<V, IEnumerable<VR>> mapper)
        //    => flatMapValues(withKey(mapper));

        public IKStream<K, VR> FlatMapValues<VR>(ValueMapperWithKey<K, V, IEnumerable<VR>> mapper)
            => this.FlatMapValues(mapper, NamedInternal.Empty());

        public IKStream<K, VR> FlatMapValues<VR>(Func<V, IEnumerable<VR>> mapper)
            => this.FlatMapValues(mapper);


        public IKStream<K, VR> FlatMapValues<VR>(
            ValueMapperWithKey<K, V, IEnumerable<VR>> mapper,
            Named named)
        {
            mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
            named = named ?? throw new ArgumentNullException(nameof(named));

            var Name = new NamedInternal(named).OrElseGenerateWithPrefix(this.Builder, KStream.FlatmapValuesName);

            var processorParameters = new ProcessorParameters<K, V>(new KStreamFlatMapValues<K, V, VR>(mapper), Name);
            var flatMapValuesNode = new ProcessorGraphNode<K, V>(Name, processorParameters)
            {
                IsValueChangingOperation = true
            };

            this.Builder.AddGraphNode<K, V>(this.StreamsGraphNode, flatMapValuesNode);

            // value serde cannot be preserved
            return new KStream<K, VR>(
                this.context,
                Name,
                this.KeySerde,
                null,
                this.SourceNodes,
                this.repartitionRequired,
                flatMapValuesNode,
                this.Builder);
        }

        public IKStream<K, V>[] Branch(Func<K, V, bool>[] predicates)
        {
            if (predicates is null)
            {
                throw new ArgumentNullException(nameof(predicates));
            }

            return this.DoBranch(NamedInternal.Empty(), predicates);
        }

        public IKStream<K, V>[] Branch(Named Name, Func<K, V, bool>[] predicates)
        {
            if (predicates is null)
            {
                throw new ArgumentNullException(nameof(predicates));
            }

            Name = Name ?? throw new ArgumentNullException(nameof(Name));

            return this.DoBranch(new NamedInternal(Name), predicates);
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

            var branchName = named.OrElseGenerateWithPrefix(this.Builder, KStream.BranchName);

            var childNames = new string[predicates.Length];
            for (var i = 0; i < predicates.Length; i++)
            {
                childNames[i] = named.SuffixWithOrElseGet("-predicate-" + i, this.Builder, KStream.BranchChildName);
            }

            var processorParameters = new ProcessorParameters<K, V>(new KStreamBranch<K, V>((Func<K, V, bool>[])predicates.Clone(), childNames), branchName);
            var branchNode = new ProcessorGraphNode<K, V>(branchName, processorParameters);
            this.Builder.AddGraphNode<K, V>(this.StreamsGraphNode, branchNode);

            var branchChildren = new IKStream<K, V>[predicates.Length];

            for (var i = 0; i < predicates.Length; i++)
            {
                var innerProcessorParameters = new ProcessorParameters<K, V>(new KStreamPassThrough<K, V>(), childNames[i]);
                var branchChildNode = new ProcessorGraphNode<K, V>(childNames[i], innerProcessorParameters);

                this.Builder.AddGraphNode<K, V>(branchNode, branchChildNode);

                branchChildren[i] = new KStream<K, V>(
                    this.context,
                    childNames[i],
                    this.KeySerde,
                    this.ValueSerde,
                    this.SourceNodes,
                    this.repartitionRequired,
                    branchChildNode,
                    this.Builder);
            }

            return branchChildren;
        }

        public IKStream<K, V> Merge(IKStream<K, V> stream)
        {
            stream = stream ?? throw new ArgumentNullException(nameof(stream));

            return this.Merge(this.Builder, stream, NamedInternal.Empty());
        }

        public IKStream<K, V> Merge(IKStream<K, V> stream, Named processorName)
        {
            stream = stream ?? throw new ArgumentNullException(nameof(stream));

            return this.Merge(this.Builder, stream, new NamedInternal(processorName));
        }

        private IKStream<K, V> Merge(
            InternalStreamsBuilder builder,
            IKStream<K, V> stream,
            NamedInternal processorName)
        {
            var streamImpl = (KStream<K, V>)stream;
            var Name = processorName.OrElseGenerateWithPrefix(builder, KStream.MERGE_NAME);
            var allSourceNodes = new HashSet<string>();

            var requireRepartitioning = streamImpl.repartitionRequired || this.repartitionRequired;
            allSourceNodes.AddRange(this.SourceNodes);
            allSourceNodes.AddRange(streamImpl.SourceNodes);

            var processorParameters = new ProcessorParameters<K, V>(new KStreamPassThrough<K, V>(), Name);

            var mergeNode = new ProcessorGraphNode<K, V>(Name, processorParameters);
            mergeNode.SetMergeNode(true);

            var parents = new HashSet<StreamsGraphNode> { this.StreamsGraphNode, streamImpl.StreamsGraphNode };
            builder.AddGraphNode<K, V>(parents, mergeNode);

            // drop the serde as we cannot safely use either one to represent both streams
            return new KStream<K, V>(
                this.context,
                Name,
                null,
                null,
                allSourceNodes,
                requireRepartitioning,
                mergeNode,
                builder);
        }

        public void ForEach(Action<K, V> action)
            => this.ForEach(action, NamedInternal.Empty());

        public void ForEach(Action<K, V> action, Named named)
        {
            action = action ?? throw new ArgumentNullException(nameof(action));

            var Name = new NamedInternal(named).OrElseGenerateWithPrefix(this.Builder, KStream.ForEachName);
            var processorParameters = new ProcessorParameters<K, V>(
                   new KStreamPeek<K, V>(action, false),
                   Name);

            var foreachNode = new ProcessorGraphNode<K, V>(Name, processorParameters);
            this.Builder.AddGraphNode<K, V>(this.StreamsGraphNode, foreachNode);
        }

        public IKStream<K, V> Peek(Action<K, V> action)
            => this.Peek(action, NamedInternal.Empty());

        public IKStream<K, V> Peek(Action<K, V> action, Named named)
        {
            action = action ?? throw new ArgumentNullException(nameof(action));
            named = named ?? throw new ArgumentNullException(nameof(named));

            // string Name = new NamedInternal(named).OrElseGenerateWithPrefix(builder, KStream.PEEK_NAME);

            // ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(
            //        new KStreamPeek<K, V>(action, true),
            //        Name
            //);

            // ProcessorGraphNode<K, V> peekNode = new ProcessorGraphNode<>(Name, processorParameters);

            // builder.AddGraphNode(this.streamsGraphNode, peekNode);

            // return new KStream<>(Name, keySerde, valSerde, sourceNodes, repartitionRequired, peekNode, builder);
            return null;
        }

        public IKStream<K, V> Through(string topic)
        {
            return this.Through(topic, Produced.With(this.KeySerde, this.ValueSerde, null));
        }


        public IKStream<K, V> Through(string topic, Produced<K, V> produced)
        {
            topic = topic ?? throw new ArgumentNullException(nameof(topic));
            produced = produced ?? throw new ArgumentNullException(nameof(produced));

            var producedInternal = new ProducedInternal<K, V>(produced);

            if (producedInternal.KeySerde == null)
            {
                producedInternal.WithKeySerde(this.KeySerde);
            }

            if (producedInternal.ValueSerde == null)
            {
                producedInternal.WithValueSerde(this.ValueSerde);
            }

            this.To(topic, producedInternal);

            return this.Builder.Stream(
                new List<string> { topic },
                new ConsumedInternal<K, V>(
                    producedInternal.KeySerde,
                    producedInternal.ValueSerde,
                    new FailOnInvalidTimestamp(null),
                    offsetReset: null));
        }

        public void To(string topic)
            => this.To(topic, Produced.With(this.KeySerde, this.ValueSerde, null));


        public void To(string topic, Produced<K, V> produced)
        {
            topic = topic ?? throw new ArgumentNullException(nameof(topic));
            produced = produced ?? throw new ArgumentNullException(nameof(produced));

            var producedInternal = new ProducedInternal<K, V>(produced);

            if (producedInternal.KeySerde == null)
            {
                producedInternal.WithKeySerde(this.KeySerde);
            }

            if (producedInternal.ValueSerde == null)
            {
                producedInternal.WithValueSerde(this.ValueSerde);
            }

            this.To(new StaticTopicNameExtractor(topic).Extract, producedInternal);
        }

        public void To(TopicNameExtractor<K, V> topicExtractor)
            => this.To(topicExtractor, Produced.With(this.KeySerde, this.ValueSerde, null));

        public void To(TopicNameExtractor<K, V> topicExtractor, Produced<K, V> produced)
        {
            topicExtractor = topicExtractor ?? throw new ArgumentNullException(nameof(topicExtractor));
            produced = produced ?? throw new ArgumentNullException(nameof(produced));

            var producedInternal = new ProducedInternal<K, V>(produced);

            if (producedInternal.KeySerde == null)
            {
                producedInternal.WithKeySerde(this.KeySerde);
            }

            if (producedInternal.ValueSerde == null)
            {
                producedInternal.WithValueSerde(this.ValueSerde);
            }

            this.To(topicExtractor, producedInternal);
        }

        private void To(TopicNameExtractor<K, V> topicExtractor, ProducedInternal<K, V> produced)
        {
            var Name = new NamedInternal(produced.Name).OrElseGenerateWithPrefix(this.Builder, KStream.SinkName);
            var sinkNode = new StreamSinkNode<K, V>(
               Name,
               topicExtractor,
               produced);

            this.Builder.AddGraphNode<K, V>(this.StreamsGraphNode, sinkNode);
        }

        public IKStream<KR, VR> Transform<KR, VR>(
            ITransformerSupplier<K, V, KeyValuePair<KR, VR>> transformerSupplier,
            string[] stateStoreNames)
        {
            transformerSupplier = transformerSupplier ?? throw new ArgumentNullException(nameof(transformerSupplier));
            var Name = this.Builder.NewProcessorName(KStream.TransformName);

            return this.FlatTransform(new TransformerSupplierAdapter<K, V, KR, VR>(transformerSupplier), Named.As(Name), stateStoreNames);
        }

        public IKStream<KR, VR> Transform<KR, VR>(
            ITransformerSupplier<K, V, KeyValuePair<KR, VR>> transformerSupplier,
            Named named,
            string[] stateStoreNames)
        {
            transformerSupplier = transformerSupplier ?? throw new ArgumentNullException(nameof(transformerSupplier));

            return this.FlatTransform(new TransformerSupplierAdapter<K, V, KR, VR>(transformerSupplier), named, stateStoreNames);
        }

        public IKStream<K1, V1> FlatTransform<K1, V1>(
            ITransformerSupplier<K, V, IEnumerable<KeyValuePair<K1, V1>>> transformerSupplier,
            params string[] stateStoreNames)
        {
            transformerSupplier = transformerSupplier ?? throw new ArgumentNullException(nameof(transformerSupplier));

            var Name = this.Builder.NewProcessorName(KStream.TransformName);

            return this.FlatTransform(transformerSupplier, Named.As(Name), stateStoreNames);
        }

        public IKStream<K1, V1> FlatTransform<K1, V1>(
            ITransformerSupplier<K, V, IEnumerable<KeyValuePair<K1, V1>>> transformerSupplier,
            Named named,
            string[] stateStoreNames)
        {
            transformerSupplier = transformerSupplier ?? throw new ArgumentNullException(nameof(transformerSupplier));
            named = named ?? throw new ArgumentNullException(nameof(named));

            var Name = new NamedInternal(named).Name;

            var transformNode = new StatefulProcessorNode<K, V>(
                   Name,
                   new ProcessorParameters<K, V>(new KStreamFlatTransform<K, V, K1, V1>(transformerSupplier), Name),
                   stateStoreNames)
            {
                IsKeyChangingOperation = true
            };

            this.Builder.AddGraphNode<K, V>(this.StreamsGraphNode, transformNode);

            // cannot inherit key and value serde
            return new KStream<K1, V1>(
                this.context,
                Name,
                null,
                null,
                this.SourceNodes,
                true,
                transformNode,
                this.Builder);
        }

        public IKStream<K, VR> TransformValues<VR>(IValueTransformerSupplier<V, VR> valueTransformerSupplier,
                                                    string[] stateStoreNames)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new ArgumentNullException(nameof(valueTransformerSupplier));
            return this.DoTransformValues(ToValueTransformerWithKeySupplier(valueTransformerSupplier), NamedInternal.Empty(), stateStoreNames);
        }

        public IKStream<K, VR> TransformValues<VR>(IValueTransformerSupplier<V, VR> valueTransformerSupplier,
                                                    Named named,
                                                    string[] stateStoreNames)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new ArgumentNullException(nameof(valueTransformerSupplier));
            named = named ?? throw new ArgumentNullException(nameof(named));
            return this.DoTransformValues(ToValueTransformerWithKeySupplier(valueTransformerSupplier),
                    new NamedInternal(named), stateStoreNames);
        }

        public IKStream<K, VR> TransformValues<VR>(IValueTransformerWithKeySupplier<K, V, VR> valueTransformerSupplier,
                                                    string[] stateStoreNames)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new ArgumentNullException(nameof(valueTransformerSupplier));
            return this.DoTransformValues(valueTransformerSupplier, NamedInternal.Empty(), stateStoreNames);
        }

        public IKStream<K, VR> TransformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, VR> valueTransformerSupplier,
            Named named,
            string[] stateStoreNames)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new ArgumentNullException(nameof(valueTransformerSupplier));
            named = named ?? throw new ArgumentNullException(nameof(named));
            return this.DoTransformValues(valueTransformerSupplier, new NamedInternal(named), stateStoreNames);
        }

        private IKStream<K, VR> DoTransformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, VR> valueTransformerWithKeySupplier,
            NamedInternal named,
            string[] stateStoreNames)
        {
            var Name = named.OrElseGenerateWithPrefix(this.Builder, KStream.TransformValuesName);

            var transformNode = new StatefulProcessorNode<K, V>(
               Name,
               new ProcessorParameters<K, V>(new KStreamTransformValues<K, V, VR>(valueTransformerWithKeySupplier), Name),
               stateStoreNames)
            {
                IsValueChangingOperation = true
            };

            this.Builder.AddGraphNode<K, V>(this.StreamsGraphNode, transformNode);

            // cannot inherit value serde
            return new KStream<K, VR>(
                this.context,
                Name,
                this.KeySerde,
                null,
                this.SourceNodes,
                this.repartitionRequired,
                transformNode,
                this.Builder);
        }

        public IKStream<K, VR> FlatTransformValues<VR>(
            IValueTransformerSupplier<V, IEnumerable<VR>> valueTransformerSupplier,
            string[] stateStoreNames)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new ArgumentNullException(nameof(valueTransformerSupplier));

            return this.DoFlatTransformValues(ToValueTransformerWithKeySupplier(valueTransformerSupplier), NamedInternal.Empty(), stateStoreNames);
        }

        public IKStream<K, VR> FlatTransformValues<VR>(
            IValueTransformerSupplier<V, IEnumerable<VR>> valueTransformerSupplier,
            Named named,
            string[] stateStoreNames)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new ArgumentNullException(nameof(valueTransformerSupplier));

            return this.DoFlatTransformValues(ToValueTransformerWithKeySupplier(valueTransformerSupplier), named, stateStoreNames);
        }

        public IKStream<K, VR> FlatTransformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, IEnumerable<VR>> valueTransformerSupplier,
            string[] stateStoreNames)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new ArgumentNullException(nameof(valueTransformerSupplier));

            return this.DoFlatTransformValues(valueTransformerSupplier, NamedInternal.Empty(), stateStoreNames);
        }

        public IKStream<K, VR> FlatTransformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, IEnumerable<VR>> valueTransformerSupplier,
            Named named,
            string[] stateStoreNames)
        {
            valueTransformerSupplier = valueTransformerSupplier ?? throw new ArgumentNullException(nameof(valueTransformerSupplier));

            return this.DoFlatTransformValues(valueTransformerSupplier, named, stateStoreNames);
        }

        private IKStream<K, VR> DoFlatTransformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, IEnumerable<VR>> valueTransformerWithKeySupplier,
            Named named,
            string[] stateStoreNames)
        {
            var Name = new NamedInternal(named).OrElseGenerateWithPrefix(this.Builder, KStream.TransformValuesName);

            var transformNode = new StatefulProcessorNode<K, V>(
               Name,
               new ProcessorParameters<K, V>(new KStreamFlatTransformValues<K, V, VR>(valueTransformerWithKeySupplier), Name),
               stateStoreNames)
            {
                IsValueChangingOperation = true
            };

            this.Builder.AddGraphNode<K, V>(this.StreamsGraphNode, transformNode);

            // cannot inherit value serde
            return new KStream<K, VR>(
                this.context,
                Name,
                this.KeySerde,
                null,
                this.SourceNodes,
                this.repartitionRequired,
                transformNode,
                this.Builder);
        }

        public void Process(
            IProcessorSupplier<K, V> IProcessorSupplier,
            string[] stateStoreNames)
        {
            IProcessorSupplier = IProcessorSupplier ?? throw new ArgumentNullException(nameof(IProcessorSupplier));

            var Name = this.Builder.NewProcessorName(KStream.ProcessorName);

            this.Process(IProcessorSupplier, Named.As(Name), stateStoreNames);
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

            var Name = new NamedInternal(named).Name;

            var processNode = new StatefulProcessorNode<K, V>(
                   Name,
                   new ProcessorParameters<K, V>(IProcessorSupplier, Name),
                   stateStoreNames);

            this.Builder.AddGraphNode<K, V>(this.StreamsGraphNode, processNode);
        }

        public IKStream<K, VR> Join<VO, VR>(
            IKStream<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            JoinWindows windows)
        {
            return this.Join(
                other,
                joiner,
                windows,
                Joined.With<K, V, VO>(null, null, null));
        }

        public IKStream<K, VR> Join<VO, VR>(
            IKStream<K, VO> otherStream,
            ValueJoiner<V, VO, VR> joiner,
            JoinWindows windows,
            Joined<K, V, VO> joined)
        {
            var kStreamJoin = new KStreamJoin(
                this.context,
                this.Builder,
                leftOuter: false,
                rightOuter: false);

            return this.DoJoin(otherStream,
                          joiner,
                          windows,
                          joined,
                          kStreamJoin);
        }

        public IKStream<K, VR> OuterJoin<VO, VR>(
            IKStream<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            JoinWindows windows)
        {
            return this.OuterJoin(
                other,
                joiner,
                windows,
                Joined.With<K, V, VO>(null, null, null));
        }

        public IKStream<K, VR> OuterJoin<VO, VR>(
            IKStream<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            JoinWindows windows,
            Joined<K, V, VO> joined)
        {
            return this.DoJoin(
                other,
                joiner,
                windows,
                joined,
                new KStreamJoin(this.context, this.Builder, true, true));
        }

        private IKStream<K, VR> DoJoin<VO, VR>(
            IKStream<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
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
            var Name = new NamedInternal(joinedInternal.Name);

            if (joinThis.repartitionRequired)
            {
                var joinThisName = joinThis.Name;
                var leftJoinRepartitionTopicName = Name.SuffixWithOrElseGet("-left", joinThisName);
                joinThis = joinThis.RepartitionForJoin(leftJoinRepartitionTopicName, joined.KeySerde, joined.ValueSerde);
            }

            if (joinOther.repartitionRequired)
            {
                var joinOtherName = joinOther.Name;
                var rightJoinRepartitionTopicName = Name.SuffixWithOrElseGet("-right", joinOtherName);
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
            var repartitionKeySerde = keySerdeOverride ?? this.KeySerde;
            var repartitionValueSerde = valueSerdeOverride ?? this.ValueSerde;

            var optimizableRepartitionNodeBuilder =
               OptimizableRepartitionNode.GetOptimizableRepartitionNodeBuilder<K, V>();

            var repartitionedSourceName = KStream.CreateRepartitionedSource(
                this.Builder,
                repartitionKeySerde,
                repartitionValueSerde,
                repartitionName,
                optimizableRepartitionNodeBuilder);

            var optimizableRepartitionNode = optimizableRepartitionNodeBuilder.Build();

            this.Builder.AddGraphNode<K, V>(this.StreamsGraphNode, optimizableRepartitionNode);

            return new KStream<K, V>(
                this.context,
                repartitionedSourceName,
                repartitionKeySerde,
                repartitionValueSerde,
                new HashSet<string> { repartitionedSourceName },
                false,
                optimizableRepartitionNode,
                this.Builder);
        }

        public IKStream<K, VR> LeftJoin<VO, VR>(
            IKStream<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            JoinWindows windows)
            => this.LeftJoin(
                other,
                joiner,
                windows,
                Joined.With<K, V, VO>(null, null, null));

        public IKStream<K, VR> LeftJoin<VO, VR>(
            IKStream<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            JoinWindows windows,
            Joined<K, V, VO> joined)
        {
            joined = joined ?? throw new ArgumentNullException(nameof(joined));

            return this.DoJoin(
                other,
                joiner,
                windows,
                joined,
                new KStreamJoin(this.context, this.Builder, true, false));
        }

        public IKStream<K, VR> Join<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner)
            => this.Join(
                other,
                joiner,
                Joined.With<K, V, VO>(null, null, null));

        public IKStream<K, VR> Join<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            Joined<K, V, VO> joined)
        {
            other = other ?? throw new ArgumentNullException(nameof(other));
            joiner = joiner ?? throw new ArgumentNullException(nameof(joiner));
            joined = joined ?? throw new ArgumentNullException(nameof(joined));

            var joinedInternal = new JoinedInternal<K, V, VO>(joined);
            var Name = joinedInternal.Name;
            if (this.repartitionRequired)
            {
                IKStream<K, V> thisStreamRepartitioned = this.RepartitionForJoin(
                   Name ?? this.Name,
                   joined.KeySerde,
                   joined.ValueSerde);

                return thisStreamRepartitioned.DoStreamTableJoin(
                    other,
                    joiner,
                    joined,
                    leftJoin: false);
            }
            else
            {
                return this.DoStreamTableJoin(
                    other,
                    joiner,
                    joined,
                    leftJoin: false);
            }
        }

        public IKStream<K, VR> LeftJoin<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner)
            => this.LeftJoin(
                other,
                joiner,
                Joined.With<K, V, VO>(null, null, null));

        public IKStream<K, VR> LeftJoin<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            Joined<K, V, VO> joined)
        {
            other = other ?? throw new ArgumentNullException(nameof(other));
            joiner = joiner ?? throw new ArgumentNullException(nameof(joiner));
            joined = joined ?? throw new ArgumentNullException(nameof(joined));

            var joinedInternal = new JoinedInternal<K, V, VO>(joined);
            var internalName = joinedInternal.Name;

            if (this.repartitionRequired)
            {
                IKStream<K, V> thisStreamRepartitioned = this.RepartitionForJoin(
                   internalName ?? this.Name,
                   joined.KeySerde,
                   joined.ValueSerde);

                return thisStreamRepartitioned.DoStreamTableJoin(other, joiner, joined, true);
            }
            else
            {
                return this.DoStreamTableJoin(other, joiner, joined, true);
            }
        }

        public IKStream<K, VR> Join<KG, VG, VR>(
            IGlobalKTable<KG, VG> globalTable,
            KeyValueMapper<K, V, KG> keyMapper,
            ValueJoiner<V, VG, VR> joiner)
            => this.GlobalTableJoin(
                globalTable,
                keyMapper,
                joiner,
                false,
                NamedInternal.Empty());

        public IKStream<K, VR> Join<KG, VG, VR>(
            IGlobalKTable<KG, VG> globalTable,
            KeyValueMapper<K, V, KG> keyMapper,
            ValueJoiner<V, VG, VR> joiner,
            Named named)
            => this.GlobalTableJoin(
                globalTable,
                keyMapper,
                joiner,
                false,
                named);

        public IKStream<K, VR> LeftJoin<KG, VG, VR>(
            IGlobalKTable<KG, VG> globalTable,
            KeyValueMapper<K, V, KG> keyMapper,
            ValueJoiner<V, VG, VR> joiner)
            => this.GlobalTableJoin(
                globalTable,
                keyMapper,
                joiner,
                leftJoin: true,
                NamedInternal.Empty());

        public IKStream<K, VR> LeftJoin<KG, VG, VR>(
            IGlobalKTable<KG, VG> globalTable,
            KeyValueMapper<K, V, KG> keyMapper,
            ValueJoiner<V, VG, VR> joiner,
            Named named)
            => this.GlobalTableJoin(
                globalTable,
                keyMapper,
                joiner,
                true,
                named);

        private IKStream<K, VR> GlobalTableJoin<KG, VG, VR>(
            IGlobalKTable<KG, VG> globalTable,
            KeyValueMapper<K, V, KG> keyMapper,
            ValueJoiner<V, VG, VR> joiner,
            bool leftJoin,
            Named named)
        {
            globalTable = globalTable ?? throw new ArgumentNullException(nameof(globalTable));
            keyMapper = keyMapper ?? throw new ArgumentNullException(nameof(keyMapper));
            joiner = joiner ?? throw new ArgumentNullException(nameof(joiner));
            named = named ?? throw new ArgumentNullException(nameof(named));

            IKTableValueGetterSupplier<KG, VG> valueGetterSupplier = ((GlobalKTableImpl<KG, VG>)globalTable).valueGetterSupplier;
            var Name = new NamedInternal(named).OrElseGenerateWithPrefix(this.Builder, KStream.LEFTJOIN_NAME);

            IProcessorSupplier<K, V> IProcessorSupplier = new KStreamGlobalKTableJoin<K, KG, VR, V, VG>(
               valueGetterSupplier,
               joiner,
               keyMapper,
               leftJoin);

            var processorParameters = new ProcessorParameters<K, V>(IProcessorSupplier, Name);

            var streamTableJoinNode = new StreamTableJoinNode<K, V>(
                Name,
                processorParameters,
                Array.Empty<string>(),
                null);

            this.Builder.AddGraphNode<K, V>(this.StreamsGraphNode, streamTableJoinNode);

            // do not have serde for joined result
            return new KStream<K, VR>(
                this.context,
                Name,
                this.KeySerde,
                null,
                this.SourceNodes,
                this.repartitionRequired,
                streamTableJoinNode,
                this.Builder);
        }

        public IKStream<K, VR> DoStreamTableJoin<VR, VO>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            Joined<K, V, VO> joined,
            bool leftJoin)
        {
            if (joined is null)
            {
                throw new ArgumentNullException(nameof(joined));
            }

            other = other ?? throw new ArgumentNullException(nameof(other));
            joiner = joiner ?? throw new ArgumentNullException(nameof(joiner));

            HashSet<string> allSourceNodes = this.EnsureJoinableWith((AbstractStream<K, VO>)other);

            var joinedInternal = new JoinedInternal<K, V, VO>(joined);
            var renamed = new NamedInternal(joinedInternal.Name);

            var Name = renamed.OrElseGenerateWithPrefix(this.Builder, leftJoin ? KStream.LEFTJOIN_NAME : KStream.JOIN_NAME);
            var processorSupplier = new KStreamKTableJoin<K, VR, V, VO>(
               other.ValueGetterSupplier<VO>(),
               joiner,
               leftJoin);

            var processorParameters = new ProcessorParameters<K, V>(processorSupplier, Name);
            var streamTableJoinNode = new StreamTableJoinNode<K, V>(
               Name,
               processorParameters,
               other.ValueGetterSupplier<VR>().StoreNames(),
               this.Name);

            this.Builder.AddGraphNode<K, V>(this.StreamsGraphNode, streamTableJoinNode);

            // do not have serde for joined result
            return new KStream<K, VR>(
                this.context,
                Name,
                joined.KeySerde ?? this.KeySerde,
                null,
                allSourceNodes,
                repartitionRequired: false,
                streamTableJoinNode,
                this.Builder);
        }

        public IKGroupedStream<KR, V> GroupBy<KR>(KeyValueMapper<K, V, KR> selector)
            => this.GroupBy(selector);

        public IKGroupedStream<KR, V> GroupBy<KR>(
            KeyValueMapper<K, V, KR> selector,
            Grouped<KR, V> grouped)
        {
            selector = selector ?? throw new ArgumentNullException(nameof(selector));
            grouped = grouped ?? throw new ArgumentNullException(nameof(grouped));

            var groupedInternal = new GroupedInternal<KR, V>(grouped);
            ProcessorGraphNode<K, V> selectKeyMapNode = this.InternalSelectKey(selector, new NamedInternal(groupedInternal.Name));
            selectKeyMapNode.IsKeyChangingOperation = true;

            this.Builder.AddGraphNode<K, V>(this.StreamsGraphNode, selectKeyMapNode);

            return new KGroupedStream<KR, V>(
                this.context,
                selectKeyMapNode.NodeName,
                this.SourceNodes,
                groupedInternal,
                repartitionRequired: true,
                selectKeyMapNode,
                this.Builder);
        }

        public IKGroupedStream<K, V> GroupByKey(ISerialized<K, V> serialized)
        {
            var serializedInternal = new SerializedInternal<K, V>(serialized);

            return this.GroupByKey(Grouped.With(
                serializedInternal.keySerde,
                serializedInternal.valueSerde));
        }

        public IKGroupedStream<K, V> GroupByKey()
            => this.GroupByKey(Grouped.With(this.KeySerde, this.ValueSerde));

        public IKGroupedStream<K, V> GroupByKey(Grouped<K, V> grouped)
        {
            var groupedInternal = new GroupedInternal<K, V>(grouped);
            return new KGroupedStream<K, V>(
                this.context,
                this.Name,
                this.SourceNodes,
                groupedInternal,
                this.repartitionRequired,
                this.StreamsGraphNode,
                this.Builder);
        }

        public IKStream<K, VR> MapValues<VR>(Func<V, VR> mapper)
        {
            throw new NotImplementedException();
        }
    }
}
