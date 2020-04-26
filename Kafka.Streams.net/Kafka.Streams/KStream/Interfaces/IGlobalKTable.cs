namespace Kafka.Streams.KStream.Interfaces
{
    /**
     * {@code GlobalKTable} is an abstraction of a <i>changelog stream</i> from a primary-keyed table.
     * Each record in this changelog stream is an update on the primary-keyed table with the record key as the primary key.
     * <p>
     * {@code GlobalKTable} can only be used as right-hand side input for {@link KStream stream}-table joins.
     * <p>
     * In contrast to a {@link KTable} that is partitioned over All {@link KafkaStreams} instances, a {@code GlobalKTable}
     * is fully replicated per {@link KafkaStreams} instance.
     * Every partition of the underlying topic is consumed by each {@code GlobalKTable}, such that the full set of data is
     * available in every {@link KafkaStreams} instance.
     * This provides the ability to perform joins with {@link KStream} without having to repartition the input stream.
     * All joins with the {@code GlobalKTable} require that a {@link KeyValueMapper} is provided that can map from the
     * {@link KeyValuePair} of the left hand side {@link KStream} to the key of the right hand side {@code GlobalKTable}.
     * <p>
     * A {@code GlobalKTable} is created via a {@link StreamsBuilder}. For example:
     * <pre>{@code
     * builder.globalTable("topic-Name", "queryable-store-Name");
     * }</pre>
     * All {@code GlobalKTable}s are backed by a {@link IReadOnlyKeyValueStore} and are therefore queryable via the
     * interactive queries API.
     * For example:
     * <pre>{@code
     *  GlobalKTable globalOne = builder.globalTable("g1", "g1-store");
     *  GlobalKTable globalTwo = builder.globalTable("g2", "g2-store");
     * ...
     *  KafkaStreams streams = ...;
     * streams.Start()
     * ...
     * IReadOnlyKeyValueStore view = streams.Store("g1-store", QueryableStoreTypes.KeyValueStore);
     * view[key]; // can be done on any key, as All keys are present
     *}</pre>
     * Note that in contrast to {@link KTable} a {@code GlobalKTable}'s state holds a full copy of the underlying topic,
     * thus All keys can be queried locally.
     * <p>
     * Records from the source topic that have null keys are dropped.
     *
     * @param Type of primary keys
     * @param Type of value changes
     * @see KTable
     * @see StreamsBuilder#globalTable(string)
     * @see KStream#join(GlobalKTable, KeyValueMapper, ValueJoiner)
     * @see KStream#leftJoin(GlobalKTable, KeyValueMapper, ValueJoiner)
     */

    public interface IGlobalKTable<K, V>
    {
        /**
         * Get the Name of the local state store that can be used to query this {@code GlobalKTable}.
         *
         * @return the underlying state store Name, or {@code null} if this {@code GlobalKTable} cannot be queried.
         */
        string QueryableStoreName { get; }
    }
}
