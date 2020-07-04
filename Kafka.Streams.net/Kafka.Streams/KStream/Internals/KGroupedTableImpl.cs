using System;
using System.Collections.Generic;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.Processors;
using Kafka.Streams.State;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.TimeStamped;
using Kafka.Streams.Temporary;

namespace Kafka.Streams.KStream.Internals
{
    public class KGroupedTableImpl<K, V> : AbstractStream<K, V>, IKGroupedTable<K, V>
    {
        private const string AGGREGATE_NAME = "KTABLE-AGGREGATE-";

        private const string REDUCE_NAME = "KTABLE-REDUCE-";

        private readonly string userProvidedRepartitionTopicName;
        private readonly Initializer<long> countInitializer = () => 0L;
        private readonly Aggregator<K, V, long> countAdder = (aggKey, value, aggregate) => aggregate + 1L;
        private readonly Aggregator<K, V, long> countSubtractor = (aggKey, value, aggregate) => aggregate - 1L;

        private StreamsGraphNode repartitionGraphNode;

        public KGroupedTableImpl(
            KafkaStreamsContext context,
            string name,
            HashSet<string> sourceNodes,
            GroupedInternal<K, V> groupedInternal,
            StreamsGraphNode streamsGraphNode)
            : base(
                  context,
                  name,
                  groupedInternal.KeySerde,
                  groupedInternal.ValueSerde,
                  sourceNodes,
                  streamsGraphNode)
        {
            this.userProvidedRepartitionTopicName = groupedInternal.Name;
        }

        private IKTable<K, T> DoAggregate<T>(
            IProcessorSupplier<K, IChange<V>> aggregateSupplier,
            string functionName,
            MaterializedInternal<K, T, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            string sinkName = this.Builder.NewProcessorName(KStream.SinkName);
            string sourceName = this.Builder.NewProcessorName(KStream.SourceName);
            string funcName = this.Builder.NewProcessorName(functionName);
            string repartitionTopic = (this.userProvidedRepartitionTopicName ?? materialized.StoreName)
               + KStream.RepartitionTopicSuffix;

            if (this.repartitionGraphNode == null || this.userProvidedRepartitionTopicName == null)
            {
                this.repartitionGraphNode = this.CreateRepartitionNode(sinkName, sourceName, repartitionTopic);
            }

            // the passed in StreamsGraphNode must be the parent of the repartition node
            this.Builder.AddGraphNode<K, V>(
                this.StreamsGraphNode,
                this.repartitionGraphNode);

            var statefulProcessorNode = new StatefulProcessorNode<K, IChange<V>, ITimestampedKeyValueStore<K, T>>(
               funcName,
               new ProcessorParameters<K, IChange<V>>(
                   aggregateSupplier,
                   funcName),
               new TimestampedKeyValueStoreMaterializer<K, T>(
                   this.Context,
                   materialized).Materialize());

            // now the repartition node must be the parent of the StateProcessorNode
            this.Builder.AddGraphNode<K, V>(
                this.repartitionGraphNode,
                statefulProcessorNode);

            // return the KTable representation with the intermediate topic as the sources
            return new KTable<K, IKeyValueStore<Bytes, byte[]>, T>(
                this.Context,
                funcName,
                materialized.KeySerde,
                materialized.ValueSerde,
                Collections.singleton(sourceName),
                materialized.QueryableStoreName(),
                aggregateSupplier,
                statefulProcessorNode);
        }

        private GroupedTableOperationRepartitionNode<K, V> CreateRepartitionNode(
            string sinkName,
            string sourceName,
            string topic)
        {
            return GroupedTableOperationRepartitionNode<K, V>.groupedTableOperationNodeBuilder<K, V>()
                 .WithRepartitionTopic(topic)
                 .WithSinkName(sinkName)
                 .WithSourceName(sourceName)
                 .WithKeySerde(this.KeySerde)
                 .WithValueSerde(this.ValueSerde)
                 .WithNodeName(sourceName)
                 .Build();
        }

        public IKTable<K, V> Reduce(
            Reducer<V> adder,
            Reducer<V> subtractor,
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            if (adder is null)
            {
                throw new ArgumentNullException(nameof(adder));
            }

            subtractor = subtractor ?? throw new ArgumentNullException(nameof(subtractor));
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));
            MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal =
               new MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>(materialized, this.Builder, AGGREGATE_NAME);

            if (materializedInternal.KeySerde == null)
            {
                materializedInternal.WithKeySerde(this.KeySerde);
            }

            if (materializedInternal.ValueSerde == null)
            {
                materializedInternal.WithValueSerde(this.ValueSerde);
            }

            IProcessorSupplier<K, IChange<V>> aggregateSupplier =
                new KTableReduce<K, V>(
                    this.Context,
                    materializedInternal.StoreName,
                    adder,
                    subtractor);

            return this.DoAggregate(
                aggregateSupplier,
                REDUCE_NAME,
                materializedInternal);
        }


        public IKTable<K, V> Reduce(
            Reducer<V> adder,
            Reducer<V> subtractor)
        {
            return this.Reduce(
                adder,
                subtractor,
                Materialized.With(this.KeySerde, this.ValueSerde));
        }


        public IKTable<K, long> Count(Materialized<K, long, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            if (materialized is null)
            {
                throw new ArgumentNullException(nameof(materialized));
            }

            var materializedInternal = new MaterializedInternal<K, long, IKeyValueStore<Bytes, byte[]>>(materialized, this.Builder, AGGREGATE_NAME);

            if (materialized.KeySerde == null)
            {
                materialized.WithKeySerde(this.KeySerde);
            }

            if (materialized.ValueSerde == null)
            {
                materialized.WithValueSerde(Serdes.Long());
            }

            IProcessorSupplier<K, IChange<V>> aggregateSupplier = new KTableAggregate<K, V, long>(
                this.Context,
                materialized.StoreName,
                this.countInitializer,
                this.countAdder,
                this.countSubtractor);

            return this.DoAggregate(
                aggregateSupplier,
                AGGREGATE_NAME,
                materializedInternal);
        }

        public IKTable<K, long> Count()
        {
            return this.Count(Materialized.With(this.KeySerde, Serdes.Long()));
        }


        public IKTable<K, VR> Aggregate<VR>(
            Initializer<VR> initializer,
            Aggregator<K, V, VR> adder,
            Aggregator<K, V, VR> subtractor,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            if (adder is null)
            {
                throw new ArgumentNullException(nameof(adder));
            }

            initializer = initializer ?? throw new ArgumentNullException(nameof(initializer));
            subtractor = subtractor ?? throw new ArgumentNullException(nameof(subtractor));
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));

            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal =
               new MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>(materialized, this.Builder, AGGREGATE_NAME);

            if (materializedInternal.KeySerde == null)
            {
                materializedInternal.WithKeySerde(this.KeySerde);
            }

            IProcessorSupplier<K, IChange<V>> aggregateSupplier =
                new KTableAggregate<K, V, VR>(
                    this.Context,
                    materializedInternal.StoreName,
                    initializer,
                    adder,
                    subtractor);

            return this.DoAggregate(
                aggregateSupplier,
                AGGREGATE_NAME,
                materializedInternal);
        }


        public IKTable<K, T> Aggregate<T>(
            Initializer<T> initializer,
            Aggregator<K, V, T> adder,
            Aggregator<K, V, T> subtractor)
        {
            return this.Aggregate(
                initializer,
                adder,
                subtractor,
                Materialized.With<K, T>(this.KeySerde, null));
        }
    }
}
