using Kafka.Streams.KStream;

namespace Kafka.Streams.KStream.Internals
{
class KGroupedStreamImpl<K, V> : AbstractStream<K, V> : KGroupedStream<K, V> {

    static  string REDUCE_NAME = "KSTREAM-REDUCE-";
    static  string AGGREGATE_NAME = "KSTREAM-AGGREGATE-";

    private  GroupedStreamAggregateBuilder<K, V> aggregateBuilder;

    KGroupedStreamImpl( string name,
                        HashSet<string> sourceNodes,
                        GroupedInternal<K, V> groupedInternal,
                        bool repartitionRequired,
                        StreamsGraphNode streamsGraphNode,
                        InternalStreamsBuilder builder)
{
        base(name, groupedInternal.keySerde(), groupedInternal.valueSerde(), sourceNodes, streamsGraphNode, builder);
        this.aggregateBuilder = new GroupedStreamAggregateBuilder<>(
            builder,
            groupedInternal,
            repartitionRequired,
            sourceNodes,
            name,
            streamsGraphNode
        );
    }

    
    public KTable<K, V> reduce( Reducer<V> reducer)
{
        return reduce(reducer, Materialized.with(keySerde, valSerde));
    }

    
    public KTable<K, V> reduce( Reducer<V> reducer,
                                Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
{
        reducer = reducer ?? throw new System.ArgumentNullException("reducer can't be null", nameof(reducer));
        materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));

         MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, REDUCE_NAME);

        if (materializedInternal.keySerde() == null)
{
            materializedInternal.withKeySerde(keySerde);
        }
        if (materializedInternal.valueSerde() == null)
{
            materializedInternal.withValueSerde(valSerde);
        }

        return doAggregate(
            new KStreamReduce<>(materializedInternal.storeName(), reducer),
            REDUCE_NAME,
            materializedInternal
        );
    }

    
    public KTable<K, VR> aggregate( Initializer<VR> initializer,
                                         Aggregator<K, V, VR> aggregator,
                                         Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
{
        initializer = initializer ?? throw new System.ArgumentNullException("initializer can't be null", nameof(initializer));
        aggregator = aggregator ?? throw new System.ArgumentNullException("aggregator can't be null", nameof(aggregator));
        materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));

         MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME);

        if (materializedInternal.keySerde() == null)
{
            materializedInternal.withKeySerde(keySerde);
        }

        return doAggregate(
            new KStreamAggregate<>(materializedInternal.storeName(), initializer, aggregator),
            AGGREGATE_NAME,
            materializedInternal
        );
    }

    
    public KTable<K, VR> aggregate( Initializer<VR> initializer,
                                         Aggregator<K, V, VR> aggregator)
{
        return aggregate(initializer, aggregator, Materialized.with(keySerde, null));
    }

    
    public KTable<K, long> count()
{
        return doCount(Materialized.with(keySerde, Serdes.long()));
    }

    
    public KTable<K, long> count( Materialized<K, long, IKeyValueStore<Bytes, byte[]>> materialized)
{
        materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));

        // TODO: Remove this when we do a topology-incompatible release
        // we used to burn a topology name here, so we have to keep doing it for compatibility
        if (new MaterializedInternal<>(materialized).storeName() == null)
{
            builder.newStoreName(AGGREGATE_NAME);
        }

        return doCount(materialized);
    }

    private KTable<K, long> doCount( Materialized<K, long, IKeyValueStore<Bytes, byte[]>> materialized)
{
         MaterializedInternal<K, long, IKeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME);

        if (materializedInternal.keySerde() == null)
{
            materializedInternal.withKeySerde(keySerde);
        }
        if (materializedInternal.valueSerde() == null)
{
            materializedInternal.withValueSerde(Serdes.long());
        }

        return doAggregate(
            new KStreamAggregate<>(materializedInternal.storeName(), aggregateBuilder.countInitializer, aggregateBuilder.countAggregator),
            AGGREGATE_NAME,
            materializedInternal);
    }

    
    public TimeWindowedKStream<K, V> windowedBy( Windows<W> windows)
{

        return new TimeWindowedKStreamImpl<>(
            windows,
            builder,
            sourceNodes,
            name,
            keySerde,
            valSerde,
            aggregateBuilder,
            streamsGraphNode
        );
    }

    
    public SessionWindowedKStream<K, V> windowedBy( SessionWindows windows)
{

        return new SessionWindowedKStreamImpl<>(
            windows,
            builder,
            sourceNodes,
            name,
            keySerde,
            valSerde,
            aggregateBuilder,
            streamsGraphNode
        );
    }

    private KTable<K, T> doAggregate( KStreamAggProcessorSupplier<K, K, V, T> aggregateSupplier,
                                          string functionName,
                                          MaterializedInternal<K, T, IKeyValueStore<Bytes, byte[]>> materializedInternal)
{
        return aggregateBuilder.build(
            functionName,
            new TimestampedKeyValueStoreMaterializer<>(materializedInternal).materialize(),
            aggregateSupplier,
            materializedInternal.queryableStoreName(),
            materializedInternal.keySerde(),
            materializedInternal.valueSerde());
    }
}
